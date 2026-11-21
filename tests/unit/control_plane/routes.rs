use super::*;

#[tokio::test]
async fn memory_routes_follow_host_lifecycle() {
    let routes = MemoryRoutes::default();
    assert_eq!(
        routes.sealed.policy().time_to_live(),
        Some(Duration::from_secs(72 * 60 * 60))
    );
    routes_follow_host_lifecycle(&routes).await;
}

#[tokio::test]
async fn redis_routes_follow_host_lifecycle() -> anyhow::Result<()> {
    let Ok(url) = std::env::var("DURABLE_ACTORS_TEST_REDIS_URL") else {
        return Ok(());
    };
    let redis = Redis::connect(&crate::redis_client::RedisConfig { url, ca_cert: None })?;
    let namespace = format!("test-routes:{}", uuid::Uuid::new_v4());
    routes_follow_host_lifecycle(&RedisRoutes::new(redis, &namespace)).await;
    Ok(())
}

async fn routes_follow_host_lifecycle(routes: &dyn RouteStore) {
    let actor = ActorKey {
        project_id: "project".into(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
    };
    let old = RouteIdentity::of(&route(&actor, 1).placement);
    let current = route(&actor, 2);
    let identity = RouteIdentity::of(&current.placement);
    let active = Some(RouteEntry::Active(current.clone()));
    let generation = routes.generation(&actor.project_id).await;
    routes.remember(&actor, current.clone(), generation).await;
    routes.remove(&actor, &old).await;
    routes.renew(&actor, &identity).await;
    assert_eq!(routes.get(&actor).await, active);
    routes.remove(&actor, &identity).await;
    assert_eq!(routes.get(&actor).await, None);

    routes.remember(&actor, current.clone(), generation).await;
    routes.remove_project(&actor.project_id).await;
    assert_eq!(routes.get(&actor).await, None);
    routes.remember(&actor, current.clone(), generation).await;
    assert_eq!(routes.get(&actor).await, None);

    routes
        .put(&actor, RouteEntry::Draining(identity.clone()))
        .await;
    routes.remove_project(&actor.project_id).await;
    let generation = routes.generation(&actor.project_id).await;
    routes.remember(&actor, current, generation).await;
    routes.remember(&actor, route(&actor, 1), generation).await;
    assert_eq!(
        routes.get(&actor).await,
        Some(RouteEntry::Draining(identity))
    );

    let sealed = crate::placement::testing::sealed_owner(&actor, "north-america-east");
    routes.keep_sealed_owner(&actor, &sealed).await;
    let taken = routes
        .take_sealed_owner(&actor)
        .await
        .map(|owner| owner.placement());
    assert_eq!(taken, Some(sealed.placement()));
    assert!(routes.take_sealed_owner(&actor).await.is_none());
}

fn route(actor: &ActorKey, epoch: u64) -> CachedRoute {
    let host = HostId::new(format!("host-{epoch}"));
    CachedRoute {
        placement: ObjectPlacement {
            lease: crate::host_leases::HostLease {
                id: host.clone(),
                session_id: format!("session-{epoch}"),
                route: "http://127.0.0.1:8000".into(),
                expires_at_ms: 30_000,
            },
            object: actor.storage_key(),
            owner: host,
            owner_epoch: epoch,
            home_region: "north-america-east".into(),
            state_version: 0,
            state_object: None,
            last_request_id: None,
        },
        host_config_key: "config".into(),
        overrides_regions: false,
    }
}
