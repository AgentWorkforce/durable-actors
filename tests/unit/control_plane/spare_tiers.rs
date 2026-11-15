use super::*;
use crate::{
    clock::{Clock, SystemClock},
    postgres::{PostgresDatabase, testing::with_postgres},
    sandbox::{
        ActorHostHandle, CreateSpareRequest, ResourceLimits, SpareHandle, SpareKind,
        pool::{PoolConfig, PoolTier, SparePool},
    },
};

const REGION: &str = "north-america-east";

/// Labels every spare with the memory it was created with and records the spare each actor got.
#[derive(Default)]
struct TieredProvider {
    assigned: Mutex<Vec<(String, u32, Option<String>)>>,
}

#[async_trait]
impl SandboxProvider for TieredProvider {
    async fn create_spare(&self, request: &CreateSpareRequest) -> Result<SpareHandle> {
        Ok(SpareHandle {
            node: String::new(),
            name: request.name.clone(),
            resource_id: format!("{}mib/{}", request.resources.memory_mib, request.name),
            route: format!("https://{}.test", request.name),
            canonical_region: request.canonical_region.clone(),
            control_route: "https://assign.test".into(),
            control_token: "secret".into(),
        })
    }
    async fn retire_spare(&self, _: &SpareHandle) -> Result<()> {
        Ok(())
    }
    async fn stopped_spares(&self, _: &[SpareHandle]) -> Result<Vec<String>> {
        Ok(vec![])
    }
    async fn ensure_host(&self, request: &EnsureHostRequest) -> Result<ActorHostHandle> {
        let actor = request.actor.as_ref().context("actor missing")?;
        let spare = request.spare.as_ref();
        self.assigned.lock().unwrap().push((
            actor.actor_name.clone(),
            request.resources.memory_mib,
            spare.map(|spare| spare.resource_id.clone()),
        ));
        let route = spare.map_or("https://on-demand.test".into(), |spare| spare.route.clone());
        Ok(ActorHostHandle {
            initial_reply: None,
            route: route.clone(),
            lease: Some(HostLease {
                id: request.host_id.clone(),
                session_id: request.session_id.clone(),
                route,
                expires_at_ms: SystemClock.now_ms()? + 10_000,
            }),
            owner_epoch: 1,
            host_id: request.host_id.clone(),
            canonical_region: request.canonical_region.clone(),
            provisioning: Some(serde_json::from_value(serde_json::json!({
                "provider":"test", "resourceId":spare.map_or("on-demand", |spare| &spare.resource_id),
                "reused":spare.is_some(), "startedAtMs":0, "completedAtMs":0
            }))?),
        })
    }
    async fn terminate_hosts(&self, _: &TerminateHostsRequest) -> Result<HostTermination> {
        anyhow::bail!("unexpected termination")
    }
}

fn tiers() -> PoolConfig {
    let small = ResourceLimits::default();
    let large = ResourceLimits {
        memory_mib: 512,
        memory_limit_mib: Some(512),
        ..small.clone()
    };
    PoolConfig {
        otlp_endpoint: None,
        control_plane_url: None,
        kind: SpareKind::Actor,
        fleet_maximum: 64,
        max_starting: 8,
        idle_ttl_seconds: 600,
        refill_interval: Duration::from_millis(20),
        retire_concurrency: 8,
        reap_interval: Duration::from_secs(30),
        pod_reap_interval: Duration::from_secs(1),
        regions: vec![REGION.into()],
        tiers: [small, large]
            .map(|resources| PoolTier { resources, idle: 1 })
            .into(),
    }
}

/// The memory label of every ready spare, once `count` are ready.
async fn ready_spares(database: &PostgresDatabase, count: usize) -> Result<Vec<String>> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let rows = database.connection().await?.query(
                "SELECT split_part(handle::jsonb ->> 'resourceId', '/', 1) FROM durable_actors_spares WHERE status = 'ready' ORDER BY 1",
                &[],
            ).await?;
            if rows.len() == count {
                return anyhow::Ok(rows.iter().map(|row| row.get(0)).collect());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?
}

#[tokio::test]
async fn actors_start_on_a_spare_of_their_own_size_which_is_then_replaced() -> Result<()> {
    with_postgres(async |fixture| {
        let provider = Arc::new(TieredProvider::default());
        let database = PostgresDatabase::connect(&fixture.url).await?;
        let pool = SparePool::new(database.clone(), provider.clone(), tiers());
        let spec: HostLaunchSpec = serde_json::from_value(serde_json::json!({
            "projectId":"default", "organizationId":"org-test", "imageRef":"image",
            "codeSnapshot":crate::sandbox::testing::code_artifact(1),
            "workingDirectory":"/customer", "actorEntrypoint":"actors.mjs", "secretRefs":[],
            "sandboxes":{"Room":{"memoryMiB":512},"Batch":{"memoryMiB":1024}}
        }))?;
        let registry = Arc::new(LocalAdminRegistry::default());
        registry.register_test_deployment(&spec).await?;
        let stop = tokio_util::sync::CancellationToken::new();
        let _guard = stop.clone().drop_guard();
        tokio::spawn(pool.clone().run(registry, stop));
        let provisioner = SandboxHostProvisioner::new(
            provider.clone(),
            HostSandboxRuntimeConfig {
                otlp_endpoint: None,
                control_plane_url: "http://control".into(),
                jwt_issuer: "issuer".into(),
                invocation_jwt_audience: "invocation".into(),
                host_idle_timeout_ms: 10_000,
            },
            test_issuer()?,
            None,
        )
        .with_pool(pool);
        assert_eq!(ready_spares(&database, 2).await?, ["256mib", "512mib"]);

        for actor_name in ["Room", "Counter", "Batch"] {
            let actor = ActorKey {
                project_id: "default".into(),
                actor_name: actor_name.into(),
                actor_id: "one".into(),
            };
            provisioner
                .ensure_actor_host(&spec, REGION, &actor, true, None, None)
                .await?;
        }

        let assigned = provider.assigned.lock().unwrap().clone();
        let spares: Vec<_> = assigned
            .iter()
            .map(|(actor, memory_mib, spare)| {
                let size = spare
                    .as_deref()
                    .map(|spare| spare.split_once('/').unwrap().0);
                (actor.as_str(), *memory_mib, size)
            })
            .collect();
        assert_eq!(
            spares,
            [
                ("Room", 512, Some("512mib")),
                ("Counter", 256, Some("256mib")),
                ("Batch", 1024, None),
            ]
        );
        assert_eq!(ready_spares(&database, 2).await?, ["256mib", "512mib"]);
        Ok(())
    })
    .await
}
