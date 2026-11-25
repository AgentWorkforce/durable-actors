use super::*;
use crate::postgres::testing::with_postgres;

impl SparePool {
    pub(super) async fn host(&self, host: &str) -> Result<Option<SpareHandle>> {
        self.store
            .0
            .query_opt(
                "SELECT handle FROM durable_actors_spares WHERE host_id = $1 AND status = 'active'",
                &[&host],
            )
            .await?
            .map(|row| serde_json::from_str(row.get::<_, &str>(0)).map_err(Into::into))
            .transpose()
    }
}

#[derive(Default)]
struct RecordedClaims(std::sync::Mutex<Vec<SandboxClaim>>);

impl PlatformEvents<SandboxClaim> for RecordedClaims {
    fn record(&self, claim: SandboxClaim) {
        self.0.lock().unwrap().push(claim);
    }
}

fn pool(database: PostgresDatabase) -> Arc<SparePool> {
    SparePool::new(
        database,
        Arc::new(crate::sandbox::testing::UnusedSandboxProvider),
        config(1),
    )
}

#[tokio::test]
async fn platform_spares_follow_current_images_while_custom_spares_remain_pinned() -> Result<()> {
    use crate::control_plane::{
        RuntimeImages,
        admin::{ImagePolicy, LocalAdminRegistry},
    };
    with_postgres(async |fixture| {
        let pool = SparePool::with_runtime_images(
            PostgresDatabase::connect(&fixture.url).await?,
            Arc::new(crate::sandbox::testing::UnusedSandboxProvider),
            config(1),
            Some(RuntimeImages {
                typescript: "im-b".into(),
                python: "im-python".into(),
            }),
            Arc::new(crate::platform_events::NoPlatformEvents),
        );
        let registry = LocalAdminRegistry::default();
        let platform: HostLaunchSpec = serde_json::from_value(serde_json::json!({
            "projectId":"platform", "organizationId":"org-test", "imagePolicy":"platform", "imageRef":"im-a",
            "codeSnapshot":crate::sandbox::testing::code_artifact(1),
            "workingDirectory":"/customer", "actorEntrypoint":"actors.mjs", "secretRefs":[]
        }))?;
        let mut custom = platform.clone();
        custom.project_id = "custom".into();
        custom.image_policy = ImagePolicy::Custom;
        custom.image_ref = "im-custom".into();
        registry.register_test_deployment(&platform).await?;
        registry.register_test_deployment(&custom).await?;
        let old_key = pool.key("im-a", "region", &ResourceLimits::default());
        let old_name = reserve(&pool.store, &old_key, 1).await?.unwrap();
        let handle = SpareHandle { node: String::new(),
            name: old_name,
            resource_id: "sb-old".into(),
            route: "https://host.test".into(),
            canonical_region: "region".into(),
            control_route: String::new(),
            control_token: String::new(),
        };
        pool.store.publish(&old_key, &handle, 600).await?;
        let builds = pool.reconcile(&registry).await?;
        let images: std::collections::BTreeSet<_> =
            builds.iter().map(|build| build.target.image.as_str()).collect();
        assert_eq!(images, ["im-b", "im-custom"].into_iter().collect());
        assert_eq!(pool.store.retiring().await?, [handle.clone()]);
        for build in builds {
            pool.store
                .publish(
                    &build.target.key,
                    &SpareHandle { node: String::new(),
                        name: build.name,
                        resource_id: build.target.image,
                        ..handle.clone()
                    },
                    600,
                )
                .await?;
        }
        assert_eq!(
            pool.claim(&platform, "region", "platform-host", &ResourceLimits::default(), None, None)
                .await?
                .unwrap()
                .resource_id,
            "im-b"
        );
        assert_eq!(
            pool.claim(&custom, "region", "custom-host", &ResourceLimits::default(), None, None)
                .await?
                .unwrap()
                .resource_id,
            "im-custom"
        );
        assert_eq!(registry.launch_spec("platform").await?, Some(platform));
        Ok(())
    })
    .await
}

#[tokio::test]
async fn custom_resources_cannot_claim_default_sized_spares() -> Result<()> {
    with_postgres(async |fixture| {
        let pool = pool(PostgresDatabase::connect(&fixture.url).await?);
        let spec: HostLaunchSpec = serde_json::from_value(serde_json::json!({
            "projectId":"default","organizationId":"org-test","imageRef":"im-runtime","codeSnapshot":crate::sandbox::testing::code_artifact(1),"workingDirectory":"/customer","secretRefs":[]
        }))?;
        let key = pool.key("im-runtime", "canada", &ResourceLimits::default());
        let name = reserve(&pool.store, &key, 1).await?.unwrap();
        let spare = SpareHandle { node: String::new(),name, resource_id:"sb-test".into(), route:"https://host.test".into(), canonical_region:"canada".into(), control_route:String::new(), control_token:String::new()};
        pool.store.publish(&key, &spare, 600).await?;
        let larger = ResourceLimits {cpu_millis:2000, cpu_limit_millis:None, memory_mib:4096, memory_limit_mib:Some(4096)};
        assert!(pool.claim(&spec, "canada", "custom", &larger, None, None).await?.is_none());
        assert!(pool.claim(&spec, "canada", "default", &ResourceLimits::default(), None, None).await?.is_some());
        Ok(())
    }).await
}

#[tokio::test]
async fn misses_say_why_no_spare_was_claimed() -> Result<()> {
    with_postgres(async |fixture| {
        let pool = pool(PostgresDatabase::connect(&fixture.url).await?);
        let packaged: HostLaunchSpec = serde_json::from_value(serde_json::json!({
            "projectId":"default","organizationId":"org-test","imageRef":"im-runtime","codeSnapshot":crate::sandbox::testing::code_artifact(1),"workingDirectory":"/customer","secretRefs":[]
        }))?;
        let unpackaged: HostLaunchSpec = serde_json::from_value(serde_json::json!({
            "projectId":"default","organizationId":"org-test","imageRef":"im-runtime","workingDirectory":"/customer","secretRefs":[]
        }))?;
        let larger = ResourceLimits {cpu_millis:2000, cpu_limit_millis:None, memory_mib:4096, memory_limit_mib:Some(4096)};
        assert_eq!(pool.miss(&unpackaged, &ResourceLimits::default()), SpareMiss::NoSnapshot);
        assert_eq!(pool.miss(&packaged, &larger), SpareMiss::NoTier);
        assert!(pool.claim(&packaged, "region", "host", &ResourceLimits::default(), None, None).await?.is_none());
        assert_eq!(pool.miss(&packaged, &ResourceLimits::default()), SpareMiss::Exhausted);
        Ok(())
    }).await
}

#[tokio::test]
async fn pool_status_counts_unexpired_spares_by_status() -> Result<()> {
    with_postgres(async |fixture| {
        let pool = pool(PostgresDatabase::connect(&fixture.url).await?);
        let key = pool.key("im-runtime", "region", &ResourceLimits::default());
        let spare = |name: String| SpareHandle {
            node: String::new(),
            name,
            resource_id: "sb-test".into(),
            route: "https://host.test".into(),
            canonical_region: "region".into(),
            control_route: String::new(),
            control_token: String::new(),
        };
        let live = reserve(&pool.store, &key, 3).await?.unwrap();
        let expired = reserve(&pool.store, &key, 3).await?.unwrap();
        reserve(&pool.store, &key, 3).await?.unwrap();
        pool.store.publish(&key, &spare(live), 600).await?;
        pool.store.publish(&key, &spare(expired), 0).await?;
        let counts = pool.store.counts().await?;
        assert_eq!(counts[&(key.clone(), "ready".to_owned())], 1);
        assert_eq!(counts[&(key, "starting".to_owned())], 1);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn only_live_claims_can_become_routable() -> Result<()> {
    with_postgres(async |fixture| {
        let pool = pool(PostgresDatabase::connect(&fixture.url).await?);
        pool.reserve_host("session", "host", "revision", None)
            .await?;
        let spare = SpareHandle {
            node: String::new(),
            control_route: String::new(),
            control_token: String::new(),
            name: "do-actor-session".into(),
            resource_id: "sb-test".into(),
            route: "https://spare.test".into(),
            canonical_region: "region".into(),
        };
        assert!(pool.host("host").await?.is_none());
        assert!(
            tokio::time::timeout(Duration::from_millis(50), pool.wait_ready("host"))
                .await
                .is_err()
        );
        pool.remember("host", "revision", &spare).await?;
        pool.wait_ready("host").await?;
        assert_eq!(pool.host("host").await?, Some(spare.clone()));
        pool.failed("host").await?;
        assert!(pool.remember("host", "revision", &spare).await.is_err());
        assert!(pool.wait_ready("host").await.is_err());
        assert!(pool.host("host").await?.is_none());
        Ok(())
    })
    .await
}

#[tokio::test]
async fn expired_claims_and_outdated_runtime_spares_cannot_be_reused() -> Result<()> {
    with_postgres(async |fixture| {
           let pool = pool(PostgresDatabase::connect(&fixture.url).await?);
           pool.reserve_host("expired", "host", "revision", None).await?;
           let abandoned = reserve(&pool.store, "abandoned-runtime", 1).await?.unwrap();
           pool.store.0.execute("UPDATE durable_actors_spares SET expires_at = clock_timestamp() - interval '1 second'", &[]).await?;
           let spare = SpareHandle { node: String::new(),
control_route: String::new(),
control_token: String::new(), name: "do-actor-expired".into(), resource_id: "sb-expired".into(), route: "https://spare.test".into(), canonical_region: "region".into()};
           assert!(pool.remember("host", "revision", &spare).await.is_err());
           let old = reserve(&pool.store, "old-runtime", 1).await?.unwrap();
           pool.store.publish("old-runtime", &SpareHandle { name: old.clone(), ..spare.clone() }, 600).await?;
           let current = reserve(&pool.store, "current-runtime", 1).await?.unwrap();
           pool.store.retire_unwanted(&["current-runtime".into()], true).await?;
           let retiring = pool.store.retiring().await?;
           assert!(retiring.iter().any(|s| s.name == old));
           assert!(retiring.iter().any(|s| s.name == abandoned));
           assert!(retiring.iter().any(|s| s.name == spare.name));
           assert!(!retiring.iter().any(|s| s.name == current));
           Ok(())
       }).await
}

#[tokio::test]
async fn concurrent_pool_claims_are_exclusive_and_never_return_assigned_sandboxes() -> Result<()> {
    with_postgres(async |fixture| {
        let store = PoolStore(
            PostgresDatabase::connect(&fixture.url).await?,
            SpareKind::Actor,
        );
        let key = "runtime-a/region-a/1000/1024";
        let names =
            futures_util::future::try_join_all((0..8).map(|_| reserve(&store, key, 2))).await?;
        let names = names.into_iter().flatten().collect::<Vec<_>>();
        assert_eq!(names.len(), 2);
        assert!(
            store
                .claim(key, "host-early", "revision", None, None)
                .await?
                .is_none()
        );
        for name in &names {
            assert!(
                store
                    .publish(
                        key,
                        &SpareHandle {
                            node: String::new(),
                            control_route: String::new(),
                            control_token: String::new(),
                            name: name.clone(),
                            resource_id: format!("sb-{name}"),
                            route: "https://spare.test".into(),
                            canonical_region: "region-a".into(),
                        },
                        600
                    )
                    .await?
            );
        }
        assert!(
            store
                .claim("other-runtime", "host-wrong", "revision", None, None)
                .await?
                .is_none()
        );
        let claims = futures_util::future::try_join_all((0..8).map(|i| {
            let store = &store;
            async move {
                store
                    .claim(key, &format!("host-{i}"), "revision", None, None)
                    .await
            }
        }))
        .await?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        assert_eq!(claims.len(), 2);
        assert_ne!(claims[0].resource_id, claims[1].resource_id);
        assert!(
            store
                .claim(key, "host-retry", "revision", None, None)
                .await?
                .is_none()
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn claims_prefer_a_spare_on_the_actors_last_node() -> Result<()> {
    with_postgres(async |fixture| {
        let pool = pool(PostgresDatabase::connect(&fixture.url).await?);
        let key = "runtime-a/region-a/1000/1024";
        for node in ["node-a", "node-b"] {
            let name = reserve(&pool.store, key, 2).await?.unwrap();
            let spare = SpareHandle {
                node: node.into(),
                name,
                resource_id: format!("sb-{node}"),
                route: "https://spare.test".into(),
                canonical_region: "region-a".into(),
                control_route: String::new(),
                control_token: String::new(),
            };
            assert!(pool.store.publish(key, &spare, 600).await?);
        }
        let home = pool
            .store
            .claim(key, "host-b", "revision", Some("node-b"), None)
            .await?;
        assert_eq!(home.unwrap().node, "node-b");
        assert_eq!(pool.host_node("host-b").await?.as_deref(), Some("node-b"));
        let elsewhere = pool
            .store
            .claim(key, "host-z", "revision", Some("node-z"), None)
            .await?;
        assert_eq!(elsewhere.unwrap().node, "node-a");
        Ok(())
    })
    .await
}

#[tokio::test]
async fn claims_record_the_actor_each_sandbox_was_claimed_for() -> Result<()> {
    use claims::{ClaimedActors, PoolClaims};
    with_postgres(async |fixture| {
        let database = PostgresDatabase::connect(&fixture.url).await?;
        let recorded = Arc::new(RecordedClaims::default());
        let pool = SparePool::with_runtime_images(
            database.clone(),
            Arc::new(crate::sandbox::testing::UnusedSandboxProvider),
            config(1),
            None,
            recorded.clone(),
        );
        let claims = PoolClaims(database);
        let actor = ActorKey {
            project_id: "project".into(),
            actor_name: "Counter".into(),
            actor_id: "one".into(),
        };
        let key = "runtime-a/region-a/1000/1024";
        for _ in 0..2 {
            let name = reserve(&pool.store, key, 2).await?.unwrap();
            let spare = SpareHandle {
                node: String::new(),
                resource_id: format!("sb-{name}"),
                name,
                route: "https://spare.test".into(),
                canonical_region: "region-a".into(),
                control_route: String::new(),
                control_token: String::new(),
            };
            assert!(pool.store.publish(key, &spare, 600).await?);
        }
        let warm = pool
            .store
            .claim(key, "host-warm", "revision", None, Some(&actor))
            .await?
            .unwrap();
        let unnamed = pool
            .store
            .claim(key, "host-unnamed", "revision", None, None)
            .await?
            .unwrap();
        pool.reserve_host("cold", "host-cold", "revision", Some(&actor))
            .await?;
        // A failed assignment's kill is looked up while its sandbox waits to be retired.
        pool.failed("host-warm").await?;
        assert_eq!(claims.actor_of(&warm.name).await?, Some(actor.clone()));
        assert_eq!(claims.actor_of("do-actor-cold").await?, Some(actor.clone()));
        assert_eq!(claims.actor_of(&unnamed.name).await?, None);
        assert_eq!(claims.actor_of("unknown").await?, None);
        pool.record_claim(&actor, "do-actor-cold", &ResourceLimits::default(), 1_000);
        let recorded = recorded.0.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].sandbox_id, "do-actor-cold");
        assert_eq!(recorded[0].actor, actor);
        assert_eq!(recorded[0].at_ms, 1_000);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn zero_capacity_and_runtime_changes_retire_only_unassigned_spares() -> Result<()> {
    with_postgres(async |fixture| {
        let store = PoolStore(
            PostgresDatabase::connect(&fixture.url).await?,
            SpareKind::Actor,
        );
        assert!(reserve(&store, "runtime", 0).await?.is_none());
        let name = reserve(&store, "runtime", 1).await?.unwrap();
        let spare = SpareHandle {
            node: String::new(),
            control_route: String::new(),
            control_token: String::new(),
            name,
            resource_id: "sb-test".into(),
            route: "https://spare.test".into(),
            canonical_region: "region".into(),
        };
        store.publish("runtime", &spare, 600).await?;
        store.retire_unwanted(&[], false).await?;
        assert!(
            store
                .claim("runtime", "host", "rev", None, None)
                .await?
                .is_none()
        );
        assert_eq!(store.retiring().await?.len(), 1);
        assert!(!store.publish("runtime", &spare, 600).await?);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn reconciliation_keeps_spares_for_every_project_runtime() -> Result<()> {
    use crate::control_plane::admin::{HostLaunchSpec, LocalAdminRegistry};
    with_postgres(async |fixture| {
        let pool = pool(PostgresDatabase::connect(&fixture.url).await?);
        let registry = LocalAdminRegistry::default();
        let mut names = Vec::new();
        for (project, image) in [("team-a", "im-a"), ("team-b", "im-b")] {
            registry
                .register_test_deployment(&HostLaunchSpec {
                    image_policy: Default::default(),
                    organization_id: "org-test".into(),
                    sandboxes: Default::default(),
                    project_id: project.into(),
                    source: None,
                    image_ref: image.into(),
                    code_snapshot: Some(crate::sandbox::testing::code_artifact(1)),
                    working_directory: "/customer".into(),
                    actor_entrypoint: Some("actors.mjs".into()),
                    secret_refs: vec![],
                })
                .await?;
            let key = pool.key(image, "region", &ResourceLimits::default());
            let name = reserve(&pool.store, &key, 1).await?.unwrap();
            pool.store
                .publish(
                    &key,
                    &SpareHandle {
                        node: String::new(),
                        name: name.clone(),
                        resource_id: format!("sb-{project}"),
                        route: format!("https://{project}.test"),
                        canonical_region: "region".into(),
                        control_route: String::new(),
                        control_token: String::new(),
                    },
                    600,
                )
                .await?;
            names.push(name);
        }
        pool.reconcile(&registry).await?;
        for name in names {
            let row = pool
                .store
                .0
                .query_opt(
                    "SELECT status FROM durable_actors_spares WHERE name = $1",
                    &[&name],
                )
                .await?
                .unwrap();
            assert_eq!(row.get::<_, &str>(0), "ready");
        }
        Ok(())
    })
    .await
}

fn config(idle: u32) -> PoolConfig {
    PoolConfig {
        otlp_endpoint: None,
        control_plane_url: None,
        kind: SpareKind::Actor,
        fleet_maximum: 64,
        max_starting: 8,
        idle_ttl_seconds: 600,
        refill_interval: Duration::from_secs(1),
        retire_concurrency: 8,
        reap_interval: Duration::from_secs(30),
        pod_reap_interval: Duration::from_secs(1),
        regions: vec!["region".into()],
        tiers: vec![PoolTier {
            resources: ResourceLimits::default(),
            idle,
        }],
    }
}

pub(super) async fn reserve(store: &PoolStore, key: &str, target: u32) -> Result<Option<String>> {
    Ok(store
        .replenish(key, target, &config(target), 1)
        .await?
        .into_iter()
        .next())
}

#[path = "pool_replenishment.rs"]
mod replenishment;

#[path = "pool_tiers.rs"]
mod tiers;

#[test]
fn housekeeping_health_follows_the_loop_heartbeat() -> Result<()> {
    let pool = SparePool::new(
        PostgresDatabase::lazy("postgres://localhost/unused")?,
        Arc::new(crate::sandbox::testing::UnusedSandboxProvider),
        config(0),
    );
    assert!(pool.housekeeping_alive(Duration::from_secs(60)));
    pool.heartbeat
        .store(0, std::sync::atomic::Ordering::Relaxed);
    assert!(!pool.housekeeping_alive(Duration::from_secs(60)));
    Ok(())
}
