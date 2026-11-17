use std::sync::Mutex;

use super::*;
use crate::{
    control_plane::admin::LocalAdminRegistry,
    postgres::testing::TestDatabase,
    sandbox::{
        ActorHostHandle, CreateSpareRequest, EnsureHostRequest, HostTermination,
        TerminateHostsRequest,
    },
};

const IMAGE: &str = "im-runtime";
const REGION: &str = "region";

fn small() -> ResourceLimits {
    ResourceLimits::default()
}

fn large() -> ResourceLimits {
    sized(512)
}

fn sized(memory_mib: u32) -> ResourceLimits {
    ResourceLimits {
        memory_mib,
        memory_limit_mib: Some(memory_mib),
        ..small()
    }
}

/// Labels every sandbox with the memory it was created with.
#[derive(Default)]
struct SizedProvider {
    created: Mutex<Vec<u32>>,
    retired: Mutex<Vec<String>>,
}

fn sandbox(name: &str, memory_mib: u32) -> SpareHandle {
    SpareHandle {
        node: String::new(),
        name: name.into(),
        resource_id: format!("{memory_mib}mib/{name}"),
        route: "https://host.test".into(),
        canonical_region: REGION.into(),
        control_route: "https://assign.test".into(),
        control_token: "secret".into(),
    }
}

#[async_trait::async_trait]
impl SandboxProvider for SizedProvider {
    async fn create_spare(&self, request: &CreateSpareRequest) -> Result<SpareHandle> {
        let memory_mib = request.resources.memory_mib;
        self.created.lock().unwrap().push(memory_mib);
        Ok(sandbox(&request.name, memory_mib))
    }
    async fn retire_spare(&self, spare: &SpareHandle) -> Result<()> {
        self.retired.lock().unwrap().push(spare.resource_id.clone());
        Ok(())
    }
    async fn stopped_spares(&self, _: &[SpareHandle]) -> Result<Vec<String>> {
        Ok(Vec::new())
    }
    async fn ensure_host(&self, _: &EnsureHostRequest) -> Result<ActorHostHandle> {
        anyhow::bail!("unexpected assignment")
    }
    async fn terminate_hosts(&self, _: &TerminateHostsRequest) -> Result<HostTermination> {
        anyhow::bail!("unexpected termination")
    }
}

struct Tiers {
    pool: Arc<SparePool>,
    provider: Arc<SizedProvider>,
    registry: Arc<LocalAdminRegistry>,
    spec: HostLaunchSpec,
}

/// A 256 MiB and a 512 MiB tier for one deployed image in one region.
async fn tiers(
    fixture: &TestDatabase,
    idle: (u32, u32),
    limits: impl FnOnce(&mut PoolConfig),
) -> Result<Tiers> {
    let mut config = config(0);
    config.refill_interval = Duration::from_millis(20);
    config.tiers = vec![
        PoolTier {
            resources: small(),
            idle: idle.0,
        },
        PoolTier {
            resources: large(),
            idle: idle.1,
        },
    ];
    limits(&mut config);
    let spec: HostLaunchSpec = serde_json::from_value(serde_json::json!({
        "projectId":"project", "organizationId":"org-test", "imageRef":IMAGE,
        "codeSnapshot":crate::sandbox::testing::code_artifact(1),
        "workingDirectory":"/customer", "actorEntrypoint":"actors.mjs", "secretRefs":[]
    }))?;
    let registry = Arc::new(LocalAdminRegistry::default());
    registry.register_test_deployment(&spec).await?;
    let provider = Arc::new(SizedProvider::default());
    let pool = SparePool::new(
        PostgresDatabase::connect(&fixture.url).await?,
        provider.clone(),
        config,
    );
    Ok(Tiers {
        pool,
        provider,
        registry,
        spec,
    })
}

fn memory(builds: &[Build]) -> Vec<u32> {
    let mut memory: Vec<_> = builds
        .iter()
        .map(|build| build.target.tier.resources.memory_mib)
        .collect();
    memory.sort_unstable();
    memory
}

/// One refill pass whose spares all become ready; returns the memory of each spare started.
async fn refill(tiers: &Tiers) -> Result<Vec<u32>> {
    let builds = tiers.pool.reconcile(tiers.registry.as_ref()).await?;
    let started = memory(&builds);
    for build in builds {
        tiers.pool.build(build).await?;
    }
    Ok(started)
}

async fn ready(pool: &SparePool, resources: &ResourceLimits) -> Result<Vec<String>> {
    let rows = pool.store.0.connection().await?.query(
        "SELECT name FROM durable_actors_spares WHERE pool_key = $1 AND status = 'ready' ORDER BY name",
        &[&pool.key(IMAGE, REGION, resources)],
    ).await?;
    Ok(rows.iter().map(|row| row.get(0)).collect())
}

async fn wait_for_ready(pool: &SparePool, resources: &ResourceLimits) -> Result<Vec<String>> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let names = ready(pool, resources).await?;
            if !names.is_empty() {
                return anyhow::Ok(names);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?
}

#[tokio::test]
async fn actors_claim_only_spares_of_their_own_size() -> Result<()> {
    with_postgres(async |fixture| {
        let tiers = tiers(fixture, (1, 1), |_| {}).await?;
        assert_eq!(refill(&tiers).await?, [256, 512]);
        let claim = async |host: &str, resources: ResourceLimits| {
            let spare = tiers
                .pool
                .claim(&tiers.spec, REGION, host, &resources, None, None)
                .await?;
            anyhow::Ok(spare.map(|spare| spare.resource_id))
        };
        assert_eq!(claim("untiered", sized(1024)).await?, None);
        let capped = ResourceLimits {
            cpu_limit_millis: Some(1000),
            ..large()
        };
        assert_eq!(claim("capped", capped).await?, None);
        let spare = claim("large", large()).await?.unwrap();
        assert!(spare.starts_with("512mib/"), "{spare}");
        assert_eq!(claim("second-large", large()).await?, None);
        let spare = claim("small", small()).await?.unwrap();
        assert!(spare.starts_with("256mib/"), "{spare}");
        Ok(())
    })
    .await
}

#[tokio::test]
async fn a_claimed_spare_is_replaced_in_its_own_tier() -> Result<()> {
    with_postgres(async |fixture| {
        let Tiers {
            pool,
            provider,
            registry,
            spec,
        } = tiers(fixture, (1, 1), |_| {}).await?;
        let stop = CancellationToken::new();
        let task = tokio::spawn(pool.clone().run(registry, stop.clone()));
        let guard = stop.clone().drop_guard();
        let small_spares = wait_for_ready(&pool, &small()).await?;
        let large_spares = wait_for_ready(&pool, &large()).await?;
        let claimed = pool
            .claim(&spec, REGION, "host", &large(), None, None)
            .await?
            .unwrap();
        assert_eq!(large_spares, std::slice::from_ref(&claimed.name));
        let replacement = wait_for_ready(&pool, &large()).await?;
        assert_ne!(replacement, [claimed.name]);
        assert_eq!(replacement.len(), 1);
        assert_eq!(ready(&pool, &small()).await?, small_spares);
        let mut created = provider.created.lock().unwrap().clone();
        created.sort_unstable();
        assert_eq!(created, [256, 512, 512]);
        drop(guard);
        tokio::time::timeout(Duration::from_secs(2), task).await??;
        Ok(())
    })
    .await
}

#[tokio::test]
async fn cleanup_keeps_the_ready_spares_of_every_tier() -> Result<()> {
    with_postgres(async |fixture| {
        let tiers = tiers(fixture, (1, 1), |_| {}).await?;
        refill(&tiers).await?;
        let pool = &tiers.pool;
        let (small_spares, large_spares) =
            (ready(pool, &small()).await?, ready(pool, &large()).await?);
        let untiered = pool.key(IMAGE, REGION, &sized(1024));
        let leftover = reserve(&pool.store, &untiered, 1).await?.unwrap();
        pool.store
            .publish(&untiered, &sandbox(&leftover, 1024), 600)
            .await?;
        assert!(refill(&tiers).await?.is_empty());
        pool.cleanup().await?;
        pool.forget_stopped().await?;
        assert_eq!(small_spares.len(), 1);
        assert_eq!(large_spares.len(), 1);
        assert_eq!(ready(pool, &small()).await?, small_spares);
        assert_eq!(ready(pool, &large()).await?, large_spares);
        assert_eq!(
            *tiers.provider.retired.lock().unwrap(),
            [format!("1024mib/{leftover}")]
        );
        let spares: i64 = pool
            .store
            .0
            .query_one("SELECT count(*) FROM durable_actors_spares", &[])
            .await?
            .get(0);
        assert_eq!(spares, 2);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn concurrent_claims_take_no_more_than_their_tier_holds() -> Result<()> {
    with_postgres(async |fixture| {
        let tiers = tiers(fixture, (2, 2), |_| {}).await?;
        assert_eq!(refill(&tiers).await?, [256, 256, 512, 512]);
        let claims = futures_util::future::try_join_all((0..16).map(|index| {
            let tiers = &tiers;
            async move {
                let memory_mib = if index % 2 == 0 { 256 } else { 512 };
                let spare = tiers
                    .pool
                    .claim(
                        &tiers.spec,
                        REGION,
                        &format!("host-{index}"),
                        &sized(memory_mib),
                        None,
                        None,
                    )
                    .await?;
                anyhow::Ok(spare.map(|spare| (memory_mib, spare.resource_id)))
            }
        }))
        .await?;
        let mut claimed: Vec<_> = claims.into_iter().flatten().collect();
        claimed.sort();
        claimed.dedup();
        let sizes: Vec<_> = claimed.iter().map(|(memory_mib, _)| *memory_mib).collect();
        assert_eq!(sizes, [256, 256, 512, 512]);
        for (memory_mib, spare) in claimed {
            assert!(spare.starts_with(&format!("{memory_mib}mib/")), "{spare}");
        }
        assert!(ready(&tiers.pool, &small()).await?.is_empty());
        assert!(ready(&tiers.pool, &large()).await?.is_empty());
        Ok(())
    })
    .await
}

#[tokio::test]
async fn tiers_split_the_shared_start_and_fleet_limits() -> Result<()> {
    with_postgres(async |fixture| {
        let tiers = tiers(fixture, (4, 4), |config| {
            config.max_starting = 3;
            config.fleet_maximum = 6;
        })
        .await?;
        let registry = tiers.registry.as_ref();
        let mut starting = Vec::new();
        loop {
            let builds = tiers.pool.reconcile(registry).await?;
            if builds.is_empty() {
                break;
            }
            starting.extend(builds);
        }
        assert_eq!(starting.len(), 3);
        assert!(memory(&starting).contains(&256) && memory(&starting).contains(&512));
        while !starting.is_empty() {
            for build in starting.drain(..) {
                tiers.pool.build(build).await?;
            }
            starting = tiers.pool.reconcile(registry).await?;
            assert!(starting.len() <= 3);
        }
        assert_eq!(ready(&tiers.pool, &small()).await?.len(), 3);
        assert_eq!(ready(&tiers.pool, &large()).await?.len(), 3);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn a_busy_tier_cannot_starve_another_of_starts() -> Result<()> {
    with_postgres(async |fixture| {
        let tiers = tiers(fixture, (2, 2), |config| config.max_starting = 1).await?;
        let mut started = Vec::new();
        for round in 0..4 {
            started.extend(refill(&tiers).await?);
            tiers
                .pool
                .claim(
                    &tiers.spec,
                    REGION,
                    &format!("host-{round}"),
                    &small(),
                    None,
                    None,
                )
                .await?;
        }
        started.sort_unstable();
        assert_eq!(started, [256, 256, 512, 512]);
        assert_eq!(ready(&tiers.pool, &large()).await?.len(), 2);
        assert!(ready(&tiers.pool, &small()).await?.is_empty());
        Ok(())
    })
    .await
}
