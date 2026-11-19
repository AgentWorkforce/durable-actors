use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::{CreateSpareRequest, ResourceLimits, SandboxProvider, SpareHandle, SpareKind};
use crate::{
    actor::ActorKey,
    control_plane::admin::{AdminRegistry, HostLaunchSpec},
    platform_events::{PlatformEvents, sandboxes::SandboxClaim},
    postgres::PostgresDatabase,
};

pub(crate) mod claims;
mod cleanup;
mod prewarming;
mod replenishment;

#[derive(Clone)]
pub(crate) struct PoolConfig {
    pub control_plane_url: Option<String>,
    pub otlp_endpoint: Option<String>,
    pub kind: SpareKind,
    pub fleet_maximum: u32,
    pub max_starting: u32,
    pub idle_ttl_seconds: u32,
    pub refill_interval: Duration,
    pub retire_concurrency: usize,
    pub reap_interval: Duration,
    pub pod_reap_interval: Duration,
    pub regions: Vec<String>,
    pub tiers: Vec<PoolTier>,
}

/// One spare size: `idle` ready spares per image and region, claimed by actors whose resources
/// equal `resources`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PoolTier {
    pub resources: ResourceLimits,
    pub idle: u32,
}

pub(crate) struct SparePool {
    store: PoolStore,
    provider: Arc<dyn SandboxProvider>,
    pub config: PoolConfig,
    runtime_images: Option<crate::control_plane::RuntimeImages>,
    claims: Arc<dyn PlatformEvents<SandboxClaim>>,
    heartbeat: std::sync::atomic::AtomicU64,
    refills: replenishment::RefillTurns,
}

impl SparePool {
    #[cfg(test)]
    pub fn new(
        database: PostgresDatabase,
        provider: Arc<dyn SandboxProvider>,
        config: PoolConfig,
    ) -> Arc<Self> {
        Self::with_runtime_images(
            database,
            provider,
            config,
            None,
            Arc::new(crate::platform_events::NoPlatformEvents),
        )
    }

    /// `claims` records which actor each claimed sandbox runs.
    pub fn with_runtime_images(
        database: PostgresDatabase,
        provider: Arc<dyn SandboxProvider>,
        config: PoolConfig,
        runtime_images: Option<crate::control_plane::RuntimeImages>,
        claims: Arc<dyn PlatformEvents<SandboxClaim>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            store: PoolStore(database, config.kind),
            provider,
            config,
            runtime_images,
            claims,
            heartbeat: now_ms().into(),
            refills: Default::default(),
        })
    }

    pub async fn claim(
        &self,
        spec: &HostLaunchSpec,
        region: &str,
        host: &str,
        resources: &ResourceLimits,
        node: Option<&str>,
        actor: Option<&ActorKey>,
    ) -> Result<Option<SpareHandle>> {
        if spec.code_snapshot.is_none() || !self.has_tier(resources) {
            return Ok(None);
        }
        let result = self
            .store
            .claim(
                &self.key(self.image(spec)?, region, resources),
                host,
                &spec.host_config_key(),
                node,
                actor,
            )
            .await?;
        Ok(result)
    }

    /// Records which actor a sandbox runs, from `claimed_at_ms`, once the sandbox is running it.
    pub(crate) fn record_claim(
        &self,
        actor: &ActorKey,
        sandbox_id: &str,
        resources: &ResourceLimits,
        claimed_at_ms: u64,
    ) {
        self.claims.record(SandboxClaim {
            actor: actor.clone(),
            sandbox_id: sandbox_id.to_owned(),
            resources: resources.clone(),
            at_ms: claimed_at_ms,
        });
    }

    fn has_tier(&self, resources: &ResourceLimits) -> bool {
        self.config
            .tiers
            .iter()
            .any(|tier| &tier.resources == resources)
    }

    pub async fn host_node(&self, host: &str) -> Result<Option<String>> {
        Ok(self
            .store
            .0
            .query_opt(
                "SELECT handle::jsonb ->> 'node' FROM durable_actors_spares WHERE host_id = $1",
                &[&host],
            )
            .await?
            .and_then(|row| row.get::<_, Option<String>>(0)))
    }

    pub async fn reserve_host(
        &self,
        session: &str,
        host: &str,
        config_key: &str,
        actor: Option<&ActorKey>,
    ) -> Result<()> {
        let name = format!("do-actor-{session}");
        let [project_id, actor_name, actor_id] = claims::columns(actor);
        self.store.0.execute(
            "INSERT INTO durable_actors_spares (name, pool_key, status, host_id, host_config_key, project_id, actor_name, actor_id, expires_at) \
             VALUES ($1, '', 'claimed', $2, $3, $4, $5, $6, clock_timestamp() + interval '120 seconds')",
            &[&name, &host, &config_key, &project_id, &actor_name, &actor_id],
        ).await?;
        Ok(())
    }

    pub async fn wait_ready(&self, host: &str) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                let row = self
                    .store
                    .0
                    .query_opt(
                        "SELECT status FROM durable_actors_spares WHERE host_id = $1",
                        &[&host],
                    )
                    .await?;
                match row.as_ref().map(|row| row.get::<_, &str>(0)) {
                    Some("active") => return Ok(()),
                    Some("claimed") => tokio::time::sleep(Duration::from_millis(20)).await,
                    _ => return Err(super::HostNotReady.into()),
                }
            }
        })
        .await
        .map_err(|_| super::HostNotReady)?
    }

    pub async fn remember(&self, host: &str, config_key: &str, spare: &SpareHandle) -> Result<()> {
        let updated = self.store.0.execute(
            "UPDATE durable_actors_spares SET status = 'active', handle = $2 \
             WHERE name = $1 AND host_id = $3 AND host_config_key = $4 AND status = 'claimed' AND expires_at > clock_timestamp()",
            &[&spare.name, &serde_json::to_string(spare)?, &host, &config_key],
        ).await?;
        ensure!(updated == 1, "actor sandbox claim expired or was retired");
        Ok(())
    }

    pub async fn failed(&self, host: &str) -> Result<()> {
        self.store
            .0
            .execute(
                "UPDATE durable_actors_spares SET status = 'retiring' WHERE host_id = $1",
                &[&host],
            )
            .await?;
        Ok(())
    }

    pub async fn retire_config(&self, config_key: &str) -> Result<Vec<String>> {
        let client = self.store.0.connection().await?;
        let rows = client.query("UPDATE durable_actors_spares SET status = 'retiring' WHERE host_config_key = $1 RETURNING handle", &[&config_key]).await?;
        let mut ids = Vec::new();
        for row in rows {
            if let Some(handle) = row.get::<_, Option<String>>(0) {
                ids.push(serde_json::from_str::<SpareHandle>(&handle)?.resource_id);
            }
        }
        Ok(ids)
    }

    /// Whether the housekeeping loop finished a pass within `stale_after`; a hung loop fails this.
    pub fn housekeeping_alive(&self, stale_after: Duration) -> bool {
        let last = self.heartbeat.load(std::sync::atomic::Ordering::Relaxed);
        now_ms().saturating_sub(last) <= stale_after.as_millis() as u64
    }

    /// Keeps this replica's connections to ready spares warm; every serving replica needs this.
    pub fn start(self: &Arc<Self>, stop: CancellationToken) {
        tokio::spawn(self.clone().prewarm(stop));
    }

    /// Fills, trims and cleans up the pool. Only the single pool manager runs this.
    pub async fn run(self: Arc<Self>, registry: Arc<dyn AdminRegistry>, stop: CancellationToken) {
        let mut jobs = tokio::task::JoinSet::new();
        let mut cleaning = false;
        let mut reaping = false;
        let mut reaping_pods = false;
        let mut reap_interval = tokio::time::interval(self.config.reap_interval);
        reap_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut pod_reap_interval = tokio::time::interval(self.config.pod_reap_interval);
        pod_reap_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut interval = tokio::time::interval(self.config.refill_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                () = stop.cancelled() => break,
                Some(result) = jobs.join_next(), if !jobs.is_empty() => {
                    match result {
                        Ok(Background::Build(result)) => {
                            report(result);
                        }
                        Ok(Background::Reap(result)) => {
                            reaping = false;
                            report(result);
                        }
                        Ok(Background::PodReap(result)) => {
                            reaping_pods = false;
                            report(result);
                        }
                        Ok(Background::Cleanup(result)) => {
                            cleaning = false;
                            report(result);
                        }
                        Err(error) => warn!(%error, "spare maintenance task stopped"),
                    }
                    continue;
                }
                _ = reap_interval.tick(), if !reaping => {
                    reaping = true;
                    let pool = self.clone();
                    jobs.spawn(async move { Background::Reap(pool.forget_stopped().await) });
                    continue;
                }
                _ = pod_reap_interval.tick(), if !reaping_pods => {
                    reaping_pods = true;
                    let pool = self.clone();
                    jobs.spawn(async move { Background::PodReap(pool.provider.reap_finished().await) });
                    continue;
                }
                _ = interval.tick() => {},
            }
            let plans = tokio::select! {
                () = stop.cancelled() => break,
                result = self.reconcile(registry.as_ref()) => result,
            };
            match plans {
                Ok(plans) => {
                    for plan in plans {
                        let pool = self.clone();
                        jobs.spawn(async move { Background::Build(pool.build(plan).await) });
                    }
                }
                Err(error) => report(Err(error)),
            }
            if !cleaning {
                cleaning = true;
                let pool = self.clone();
                jobs.spawn(async move { Background::Cleanup(pool.cleanup().await) });
            }
            self.heartbeat
                .store(now_ms(), std::sync::atomic::Ordering::Relaxed);
        }
    }

    async fn reconcile(&self, registry: &dyn AdminRegistry) -> Result<Vec<Build>> {
        let targets = self.targets(registry).await?;
        let keys: Vec<String> = targets.iter().map(|target| target.key.clone()).collect();
        let enabled = self.config.tiers.iter().any(|tier| tier.idle > 0);
        self.store.retire_unwanted(&keys, enabled).await?;
        let batch = (self.config.max_starting / keys.len().max(1) as u32).max(1);
        let mut builds = Vec::new();
        for target in self.refills.order(targets) {
            match self.replenish(&target, batch).await {
                Ok(names) => builds.extend(names.into_iter().map(|name| Build {
                    target: target.clone(),
                    name,
                })),
                Err(error) => report(Err(error)),
            }
        }
        Ok(builds)
    }

    async fn targets(&self, registry: &dyn AdminRegistry) -> Result<Vec<Target>> {
        let deployments = registry.launch_specs().await?;
        let images: std::collections::BTreeSet<_> = deployments
            .iter()
            .filter(|spec| spec.code_snapshot.is_some())
            .map(|spec| self.image(spec))
            .collect::<Result<_>>()?;
        let mut targets = Vec::new();
        for image in images {
            for region in &self.config.regions {
                for tier in &self.config.tiers {
                    targets.push(Target {
                        key: self.key(image, region, &tier.resources),
                        image: image.into(),
                        region: region.clone(),
                        tier: tier.clone(),
                    });
                }
            }
        }
        Ok(targets)
    }

    fn image<'a>(&'a self, spec: &'a HostLaunchSpec) -> Result<&'a str> {
        match &self.runtime_images {
            Some(images) => images.for_deployment(spec),
            None => Ok(&spec.image_ref),
        }
    }

    async fn replenish(&self, target: &Target, batch: u32) -> Result<Vec<String>> {
        let names = self
            .store
            .replenish(&target.key, target.tier.idle, &self.config, batch)
            .await?;
        if !names.is_empty() {
            self.refills.served(&target.key);
        }
        Ok(names)
    }

    async fn build(&self, build: Build) -> Result<()> {
        let key = &build.target.key;
        match self.create(&build).await {
            Ok(handle) => {
                if !self
                    .store
                    .publish(key, &handle, self.config.idle_ttl_seconds)
                    .await?
                {
                    self.provider.retire_spare(&handle).await?;
                }
                Ok(())
            }
            Err(error) => {
                self.store.build_failed(key, &build.name).await?;
                Err(error)
            }
        }
    }

    async fn create(&self, build: &Build) -> Result<SpareHandle> {
        let Target {
            image,
            region,
            tier,
            ..
        } = &build.target;
        let request = CreateSpareRequest {
            control_plane_url: self
                .config
                .control_plane_url
                .clone()
                .filter(|_| self.config.kind == SpareKind::Actor),
            otlp_endpoint: self
                .config
                .otlp_endpoint
                .clone()
                .filter(|_| self.config.kind == SpareKind::Actor),
            name: build.name.clone(),
            image_ref: image.clone(),
            canonical_region: region.clone(),
            resources: tier.resources.clone(),
            kind: self.config.kind,
        };
        let handle = tokio::time::timeout(
            Duration::from_secs(110),
            self.provider.create_spare(&request),
        )
        .await
        .context("spare creation timed out")??;
        ensure!(
            handle.name == build.name
                && handle.canonical_region == *region
                && !handle.resource_id.is_empty()
                && !handle.route.is_empty()
                && !handle.control_route.is_empty()
                && !handle.control_token.is_empty(),
            "provider returned an invalid spare"
        );
        self.provider
            .prewarm_spares(std::slice::from_ref(&handle))
            .await;
        Ok(handle)
    }

    async fn cleanup(&self) -> Result<()> {
        use futures_util::StreamExt;
        let results = futures_util::stream::iter(self.store.retiring().await?)
            .map(|handle| self.retire(handle))
            .buffer_unordered(self.config.retire_concurrency)
            .collect::<Vec<_>>()
            .await;
        for result in results {
            result?;
        }
        Ok(())
    }

    async fn retire(&self, handle: SpareHandle) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(30), self.provider.retire_spare(&handle))
            .await
            .context("spare retirement timed out")??;
        self.store
            .0
            .execute(
                "DELETE FROM durable_actors_spares WHERE name = $1 AND status = 'retiring'",
                &[&handle.name],
            )
            .await?;
        Ok(())
    }

    fn key(&self, image: &str, region: &str, resources: &ResourceLimits) -> String {
        serde_json::to_string(&(self.config.kind, image, region, resources))
            .expect("serializable pool key")
    }
}

/// The spares one tier keeps ready for one image in one region.
#[derive(Clone)]
struct Target {
    key: String,
    image: String,
    region: String,
    tier: PoolTier,
}

struct Build {
    target: Target,
    name: String,
}

enum Background {
    Reap(Result<()>),
    PodReap(Result<()>),
    Build(Result<()>),
    Cleanup(Result<()>),
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}

fn report(result: Result<()>) {
    if let Err(error) = result {
        warn!(error = %format!("{error:#}"), "generic spare reconciliation failed");
    }
}

struct PoolStore(PostgresDatabase, SpareKind);

impl PoolStore {
    async fn publish(&self, key: &str, handle: &SpareHandle, ttl: u32) -> Result<bool> {
        let published = self.0.execute(
            "UPDATE durable_actors_spares SET status = 'ready', handle = $3, expires_at = clock_timestamp() + make_interval(secs => $4) \
             WHERE name = $1 AND pool_key = $2 AND status = 'starting' AND expires_at > clock_timestamp()",
            &[&handle.name, &key, &serde_json::to_string(handle)?, &f64::from(ttl)],
        ).await? == 1;
        if published {
            self.0.execute("UPDATE durable_actors_pool_backoffs SET failures = 0, retry_after = clock_timestamp() WHERE pool_key = $1", &[&key]).await?;
        }
        Ok(published)
    }

    // Prefers a spare on `node`, whose disk may still hold the actor's state.
    async fn claim(
        &self,
        key: &str,
        host: &str,
        config_key: &str,
        node: Option<&str>,
        actor: Option<&ActorKey>,
    ) -> Result<Option<SpareHandle>> {
        let [project_id, actor_name, actor_id] = claims::columns(actor);
        self.0.query_opt(
            "UPDATE durable_actors_spares SET status = 'claimed', host_id = $2, host_config_key = $3, project_id = $5, actor_name = $6, actor_id = $7, expires_at = clock_timestamp() + interval '120 seconds' \
             WHERE name = (SELECT name FROM durable_actors_spares WHERE pool_key = $1 AND status = 'ready' AND expires_at > statement_timestamp() AND expires_at > clock_timestamp() \
             ORDER BY (handle::jsonb ->> 'node' = $4::text) IS TRUE DESC, expires_at FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING handle",
            &[&key, &host, &config_key, &node, &project_id, &actor_name, &actor_id],
        ).await?.map(|row| serde_json::from_str(row.get::<_, &str>(0)).context("decode spare handle")).transpose()
    }

    async fn retire_unwanted(&self, keys: &[String], enabled: bool) -> Result<()> {
        // Active hosts retire through their ownership lease and explicit shutdown.
        self.0.execute(
            "UPDATE durable_actors_spares SET status = 'retiring' WHERE kind = $3 AND ((expires_at <= clock_timestamp() AND status != 'active') \
             OR (status = 'ready' AND (NOT (pool_key = ANY($1)) OR NOT $2)))",
            &[&keys, &enabled, &self.1.as_str()],
        ).await?;
        self.0.execute("DELETE FROM durable_actors_pool_backoffs WHERE kind = $2 AND NOT (pool_key = ANY($1))", &[&keys, &self.1.as_str()]).await?;
        Ok(())
    }

    async fn retiring(&self) -> Result<Vec<SpareHandle>> {
        let client = self.0.connection().await?;
        client
            .query(
                "SELECT name, handle FROM durable_actors_spares WHERE status = 'retiring' AND kind = $1",
                &[&self.1.as_str()],
            )
            .await?
            .into_iter()
            .map(|row| decode_handle(&row))
            .collect()
    }
}

fn decode_handle(row: &tokio_postgres::Row) -> Result<SpareHandle> {
    match row.get::<_, Option<&str>>("handle") {
        Some(handle) => Ok(serde_json::from_str(handle)?),
        None => Ok(SpareHandle {
            node: String::new(),
            name: row.get("name"),
            resource_id: String::new(),
            route: String::new(),
            canonical_region: String::new(),
            control_route: String::new(),
            control_token: String::new(),
        }),
    }
}

#[cfg(test)]
#[path = "../../tests/unit/sandbox/pool.rs"]
mod tests;
