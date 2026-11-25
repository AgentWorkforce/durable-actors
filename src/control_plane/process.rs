use std::{collections::BTreeMap, env, future::Future, net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use tracing::{info, warn};

use crate::{
    bucket::{GcsBucket, RuntimeStorageReader},
    platform_events::{NoPlatformEvents, PlatformEvents, sandboxes::SandboxKill},
    postgres::PostgresDatabase,
    redis_client::{Redis, RedisConfig},
    request_traces::{
        TraceStore,
        live::{InProcessFeed, LiveFeed, RedisFeed},
        persistence::{NoTraceQueries, clickhouse::ClickHouseTracePersistence},
    },
    sandbox::{
        HostSandboxRuntimeConfig,
        gke::{GKE_RUNTIME_CLASS, GkeConfig, KillReporting},
        modal::ModalConfig,
        pool::claims::PoolClaims,
    },
};

use super::{ActorJwtVerifier, ControlPlaneService};

pub(crate) const DEFAULT_JWT_ISSUER: &str = "durable-actors-control-plane";
pub(crate) const DEFAULT_AUTHORITY_AUDIENCE: &str = "durable-actors-authority";
const DEFAULT_INVOCATION_AUDIENCE: &str = "durable-actors-invoke";
pub(crate) const DEFAULT_JWT_TTL_SECONDS: u64 = 86_400;
/// Bump the version when the bus's messages change.
const SOCKET_BUS_NAMESPACE: &str = "durable-actors:v1:sockets";

pub struct ControlPlaneProcessConfig {
    pub bind: SocketAddr,
    pub gateway_accept_connections: bool,
    pub max_socket_connections: usize,
    pub jwt_signing_key: String,
    pub jwt_key_id: String,
    pub jwt_issuer: String,
    pub authority_audience: String,
    pub invocation_audience: String,
    pub jwt_max_lifetime: Duration,
    pub api_key: Option<String>,
    pub invoke_api_key: Option<String>,
    pub storage: ControlPlaneStorageConfig,
    pub sandbox_provider: SandboxProviderConfig,
    /// Where the control plane exports platform events (WebSocket connections and deployments);
    /// unset turns them off.
    pub platform_telemetry: Option<super::PlatformTelemetryConfig>,
    pub usage_topic: Option<String>,
    /// Where platform events are also published for services outside the control plane.
    pub events_topic: Option<String>,
    pub region: Option<String>,
    pub alarm_poll_interval: Duration,
    pub redis: Option<RedisConfig>,
    pub organization_limits: Option<super::OrganizationLimits>,
}

pub struct ControlPlaneStorageConfig {
    pub postgres_url: String,
    pub postgres_max_connections: usize,
    pub trace_retention: Duration,
    pub analytics: AnalyticsBackend,
    pub bucket: String,
    pub persistence: crate::bucket::SnapshotStoreConfig,
    pub artifact_bucket: String,
}

#[derive(Default)]
pub enum AnalyticsBackend {
    #[default]
    None,
    ClickHouse(super::ClickHouseConfig),
}

#[derive(Clone)]
pub struct RuntimeImages {
    pub typescript: String,
    pub python: String,
}

impl RuntimeImages {
    pub(crate) fn for_deployment<'a>(
        &'a self,
        spec: &'a super::admin::HostLaunchSpec,
    ) -> Result<&'a str> {
        if spec.image_policy == super::admin::ImagePolicy::Custom {
            return Ok(&spec.image_ref);
        }
        let artifact = spec
            .code_snapshot
            .as_deref()
            .context("platform image requires a code artifact")?;
        let bundle = crate::bundle::ActorBundle::decode(artifact)?;
        self.for_entrypoint(bundle.entrypoint()?)
    }

    pub(crate) fn for_entrypoint(&self, entrypoint: &str) -> Result<&str> {
        Ok(
            match crate::bundle::ActorRuntime::from_entrypoint(entrypoint)? {
                crate::bundle::ActorRuntime::Typescript => &self.typescript,
                crate::bundle::ActorRuntime::Python => &self.python,
            },
        )
    }
}

pub struct SandboxProviderConfig {
    pub runtime_images: RuntimeImages,
    pub(super) pool: crate::sandbox::pool::PoolConfig,
    pub(super) default_resources: crate::sandbox::ResourceLimits,
    pub(crate) backend: SandboxBackend,
    pub(crate) secret_namespace: String,
    pub(crate) public_origin: String,
    pub(super) resource_bounds: crate::sandbox::ResourceBounds,
    pub runtime: HostSandboxRuntimeConfig,
}

pub(crate) enum SandboxBackend {
    Gke(GkeConfig),
    Modal(ModalConfig),
}

impl ControlPlaneProcessConfig {
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|name| env::var(name).ok())
    }
}

pub async fn serve_control_plane(
    config: ControlPlaneProcessConfig,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    let bind = config.bind;
    warn_if_authentication_disabled(bind, config.api_key.as_deref());
    let stop = tokio_util::sync::CancellationToken::new();
    let _guard = stop.clone().drop_guard();
    let routes = control_plane_routes(config, stop).await?;
    info!(bind = %bind, "durable-actors control plane is ready");
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .context("bind durable-actors control plane")?;
    serve_routes(listener, routes, shutdown).await
}

fn warn_if_authentication_disabled(bind: SocketAddr, secret: Option<&str>) {
    if secret.is_none() && !bind.ip().is_loopback() {
        warn!(
            %bind,
            "Authentication is disabled. Anyone who can reach this server can access its API. Set DURABLE_ACTORS_SECRET to enable authentication."
        );
    }
}

async fn serve_routes(
    listener: tokio::net::TcpListener,
    routes: tonic::service::Routes,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    axum::serve(listener, routes.into_axum_router())
        .with_graceful_shutdown(shutdown)
        .await
        .context("serve durable-actors control plane")
}

async fn control_plane_routes(
    config: ControlPlaneProcessConfig,
    stop: tokio_util::sync::CancellationToken,
) -> Result<tonic::service::Routes> {
    let issuer = super::ActorJwtIssuer::from_base64_pkcs8(
        &config.jwt_signing_key,
        config.jwt_key_id,
        config.jwt_issuer.clone(),
        config.authority_audience.clone(),
        config.invocation_audience,
        config.jwt_max_lifetime,
    )?;
    let auth = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        config.jwt_issuer,
        config.authority_audience,
        super::ActorTokenPurpose::ControlPlane,
        config.jwt_max_lifetime,
    )?;
    let redis = config.redis.as_ref().map(Redis::connect).transpose()?;
    let database = PostgresDatabase::lazy_with_max_connections(
        &config.storage.postgres_url,
        config.storage.postgres_max_connections,
    )?;
    let (traces, lifecycle) = telemetry_stores(&config.storage, redis.clone(), stop.clone())?;
    let retention = config
        .jwt_max_lifetime
        .saturating_add(Duration::from_millis(
            crate::host_leases::MAX_HOST_LEASE_DURATION_MS,
        ));
    let admission = config.organization_limits.map(|limits| {
        use super::organization_limits::*;
        let reservations: Arc<dyn ReservationStore> = match &redis {
            Some(redis) => Arc::new(RedisReservations::new(
                redis.clone(),
                "durable-actors:admission:v3",
                retention,
            )),
            None => Arc::new(MemoryReservations::new(
                Arc::new(crate::clock::SystemClock),
                retention,
            )),
        };
        Arc::new(ActorAdmission::new(limits, reservations).with_database(database.clone()))
    });
    let mut lifecycle = lifecycle
        .with_usage_topic(config.usage_topic.as_deref())
        .await?;
    if config.usage_topic.is_some()
        && let Some(redis) = &redis
    {
        lifecycle = lifecycle.with_usage_sessions(Arc::new(
            crate::actor_lifecycle::reconciliation::RedisUsageSessions::new(
                redis.clone(),
                "durable-actors:{usage}:v1",
            ),
        ));
    }
    let authority = Arc::new(GcsBucket::new(&config.storage.bucket).await?);
    let registry = Arc::new(super::PostgresAdminRegistry::from_database(
        database.clone(),
    ));
    let snapshots =
        snapshot_store(authority.clone(), &config.storage.persistence, stop.clone()).await?;
    let storage = Arc::new(
        RuntimeStorageReader::new(
            authority,
            Arc::new(crate::clock::SystemClock),
            Arc::new(crate::litestream::EmbeddedRestore),
        )?
        .with_persistence(config.storage.persistence, snapshots)?,
    );
    if storage.persistence.rapid_settings().is_some() {
        storage.clone().start_abandoned_recovery(stop.child_token());
    }
    let runtime_access = Arc::new(crate::bucket::access::RuntimeAccess::new(
        crate::bucket::access::BucketLocation::Gcs {
            artifact_bucket: config.storage.artifact_bucket.clone(),
            bucket: config.storage.bucket.clone(),
        },
        storage.persistence.clone(),
    )?);
    let placements = storage.clone();
    let connections = crate::sandbox::connections::HostConnections::new()?;
    let socket_bus = match &redis {
        Some(redis) => super::socket_bus::RedisSocketBus::start(
            redis.clone(),
            SOCKET_BUS_NAMESPACE,
            config.max_socket_connections,
            config.gateway_accept_connections,
            stop.child_token(),
        )?,
        None => super::socket_bus::MemorySocketHub::default().join(config.max_socket_connections),
    };
    let platform_events = platform_events(config.platform_telemetry.as_ref())?;
    let socket_events: Arc<dyn PlatformEvents<crate::platform_events::sockets::SocketEvent>> =
        match &platform_events {
            Some(events) => events.clone(),
            None => Arc::new(NoPlatformEvents),
        };
    if let Some(events) = &platform_events {
        lifecycle = lifecycle.with_events(events.clone());
    }
    let feedback: Arc<dyn PlatformEvents<crate::platform_events::feedback::Feedback>> =
        match &platform_events {
            Some(events) => events.clone(),
            None => Arc::new(NoPlatformEvents),
        };
    let feedback = super::event_topic::published(feedback, config.events_topic.as_deref()).await?;
    let socket_gateway = super::socket_gateway::SocketGateway::start(
        socket_bus,
        config.gateway_accept_connections,
        stop.child_token(),
        connections.client.clone(),
        socket_events,
    )?;
    let public_origin = config.sandbox_provider.public_origin.clone();
    let gateway = super::gateway::Gateway::new(&issuer, public_origin.clone(), socket_gateway)?;
    let claims: Arc<dyn PlatformEvents<crate::platform_events::sandboxes::SandboxClaim>> =
        match &platform_events {
            Some(events) => events.clone(),
            None => Arc::new(NoPlatformEvents),
        };
    let mut provisioner = sandbox_provisioner(
        config.sandbox_provider,
        &issuer,
        runtime_access.clone(),
        database.clone(),
        stop.clone(),
        connections,
        claims,
    )
    .await?;
    if let Some(admission) = admission {
        provisioner = provisioner.with_actor_admission(admission.clone());
        lifecycle = lifecycle.with_observer(admission);
    }
    let mut service = ControlPlaneService::new(
        placements.clone(),
        auth,
        registry.clone(),
        issuer.clone(),
        Arc::new(provisioner),
    )
    .with_runtime_access(runtime_access)
    .with_alarm_store(Arc::new(super::alarms::PostgresAlarmStore::new(database)));
    if let Some(redis) = redis {
        service.routes = Arc::new(super::routes::RedisRoutes::new(
            redis,
            "durable-actors:v1:routes",
        ));
    }
    service.lifecycle = lifecycle;
    service.gateway = Some(gateway);
    service.region = config.region;
    if let Some(events) = platform_events {
        service = service.with_deployment_events(events);
        republish_deployments(service.clone());
    }
    service.lifecycle.start_reconciliation(stop.clone());
    service.start_access_reconciliation(stop.clone());
    service.start_alarm_scheduler(config.alarm_poll_interval, stop);
    let inventory = Arc::new(super::socket_inventory::ConnectionInventoryReader::new(
        Arc::new(service.lifecycle.clone()),
        traces.clone(),
    ));
    let discovery = super::discovery::router(&issuer, &public_origin)?;
    let admin = super::admin::AdminService::new(config.api_key, registry, issuer)?
        .with_invoke_api_key(config.invoke_api_key)?;
    let inspector =
        super::inspection::ActorInspector::new(inventory, storage.clone()).with_traces(traces);
    let public_api = super::public_api::router(service.clone(), admin.clone())
        .merge(super::inspection::router(inspector, admin.clone()))
        .merge(discovery);
    let public_api = super::request_log::logged(
        public_api
            .clone()
            .merge(super::mcp::router(public_api, admin, feedback)),
    );
    let internal_api = service.into_internal_service();
    Ok(tonic::service::Routes::from(public_api).add_service(internal_api))
}

async fn snapshot_store(
    authority: Arc<GcsBucket>,
    persistence: &crate::bucket::SnapshotStoreConfig,
    stop: tokio_util::sync::CancellationToken,
) -> Result<Arc<dyn crate::bucket::SnapshotStore>> {
    if persistence.rapid_settings().is_none() {
        return Ok(Arc::new(crate::bucket::BucketSnapshots(authority)));
    }
    crate::bucket::RapidSnapshots::validate_gcs(persistence, authority.clients()).await?;
    Ok(Arc::new(crate::bucket::RapidSnapshots::gcs(
        persistence,
        authority.clients(),
        stop,
        None,
    )?))
}

fn telemetry_stores(
    storage: &ControlPlaneStorageConfig,
    redis: Option<Redis>,
    stop: tokio_util::sync::CancellationToken,
) -> Result<(TraceStore, crate::actor_lifecycle::LifecycleStore)> {
    let live: Arc<dyn LiveFeed> = match redis {
        Some(redis) => Arc::new(RedisFeed::new(redis)),
        None => Arc::new(InProcessFeed::default()),
    };
    match &storage.analytics {
        AnalyticsBackend::None => Ok((
            TraceStore::new(Arc::new(NoTraceQueries), live),
            crate::actor_lifecycle::LifecycleStore::default(),
        )),
        AnalyticsBackend::ClickHouse(config) => {
            let persistence = Arc::new(ClickHouseTracePersistence::start(
                config.clone(),
                storage.trace_retention,
                stop,
            )?);
            Ok((
                TraceStore::new(persistence.clone(), live),
                crate::actor_lifecycle::LifecycleStore::new(persistence),
            ))
        }
    }
}

async fn sandbox_provisioner(
    config: SandboxProviderConfig,
    issuer: &super::ActorJwtIssuer,
    access: Arc<crate::bucket::access::RuntimeAccess>,
    database: PostgresDatabase,
    stop: tokio_util::sync::CancellationToken,
    connections: crate::sandbox::connections::HostConnections,
    claims: Arc<dyn PlatformEvents<crate::platform_events::sandboxes::SandboxClaim>>,
) -> Result<super::service::SandboxHostProvisioner> {
    // Every replica watches the same pods, so only the pool manager reports their kills.
    let provider = sandbox_provider(&config, connections, None).await?;
    let pool = crate::sandbox::pool::SparePool::with_runtime_images(
        database,
        provider.clone(),
        config.pool,
        Some(config.runtime_images.clone()),
        claims,
    );
    pool.start(stop);
    Ok(super::service::SandboxHostProvisioner::new(
        provider,
        config.runtime,
        issuer.clone(),
        Some(config.runtime_images),
    )
    .with_runtime_access(access)
    .with_pool(pool)
    .with_default_resources(config.default_resources)
    .with_resource_bounds(config.resource_bounds))
}

async fn sandbox_provider(
    config: &SandboxProviderConfig,
    connections: crate::sandbox::connections::HostConnections,
    kills: Option<KillReporting>,
) -> Result<Arc<dyn crate::sandbox::SandboxProvider>> {
    let secrets = Arc::new(crate::sandbox::secrets::KubernetesSecrets::new(
        config.secret_namespace.clone(),
    ));
    Ok(Arc::new(match &config.backend {
        SandboxBackend::Gke(gke) => {
            crate::sandbox::gke::provider(gke.clone(), secrets, connections, kills).await?
        }
        SandboxBackend::Modal(modal) => {
            crate::sandbox::modal::provider(
                modal.clone(),
                &[
                    &config.runtime_images.typescript,
                    &config.runtime_images.python,
                ],
                secrets,
                connections,
            )
            .await?
        }
    }))
}

/// Runs the one replica that fills, trims and cleans up the warm pool for every serving replica.
pub async fn serve_pool_manager(
    config: ControlPlaneProcessConfig,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    let database = PostgresDatabase::lazy_with_max_connections(
        &config.storage.postgres_url,
        config.storage.postgres_max_connections,
    )?;
    let registry = Arc::new(super::PostgresAdminRegistry::from_database(
        database.clone(),
    ));
    let kills = kill_reporting(&config, &database).await?;
    let provider = sandbox_provider(
        &config.sandbox_provider,
        crate::sandbox::connections::HostConnections::new()?,
        kills,
    )
    .await?;
    // Serving replicas claim sandboxes; the pool manager only fills and trims the pool.
    let pool = crate::sandbox::pool::SparePool::with_runtime_images(
        database,
        provider,
        config.sandbox_provider.pool,
        Some(config.sandbox_provider.runtime_images),
        Arc::new(NoPlatformEvents),
    );
    let stale_after = (pool.config.refill_interval * 3).max(Duration::from_secs(60));
    let health = pool.clone();
    let routes = axum::Router::new().route(
        "/healthz",
        axum::routing::get(move || {
            let alive = health.housekeeping_alive(stale_after);
            async move {
                if alive {
                    axum::http::StatusCode::OK
                } else {
                    axum::http::StatusCode::SERVICE_UNAVAILABLE
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind(config.bind)
        .await
        .context("bind durable-actors pool manager")?;
    let stop = tokio_util::sync::CancellationToken::new();
    let housekeeping = tokio::spawn(pool.run(registry, stop.clone()));
    let sweep = history_sweep(&config.storage.persistence, stop.clone())?;
    info!(bind = %config.bind, "durable-actors pool manager is ready");
    axum::serve(listener, routes)
        .with_graceful_shutdown(shutdown)
        .await
        .context("serve durable-actors pool manager health")?;
    stop.cancel();
    housekeeping.await?;
    if let Some(sweep) = sweep {
        sweep.await??;
    }
    Ok(())
}

/// The history sweep of a Rapid installation, which runs beside the pool's housekeeping because
/// both need exactly one instance.
fn history_sweep(
    persistence: &crate::bucket::SnapshotStoreConfig,
    stop: tokio_util::sync::CancellationToken,
) -> Result<Option<tokio::task::JoinHandle<Result<()>>>> {
    let crate::bucket::SnapshotStoreConfig::Rapid {
        archive_bucket,
        buckets,
        ..
    } = persistence
    else {
        return Ok(None);
    };
    let config =
        crate::bucket::SweepConfig::new(archive_bucket.clone(), buckets.clone(), |name| {
            env::var(name).ok()
        })?;
    Ok(Some(tokio::spawn(crate::bucket::serve_history_sweeper(
        config,
        stop.cancelled_owned(),
    ))))
}

/// Kills are reported only where something takes them: platform telemetry, the events topic, or
/// both.
async fn kill_reporting(
    config: &ControlPlaneProcessConfig,
    database: &PostgresDatabase,
) -> Result<Option<KillReporting>> {
    let recorded = platform_events(config.platform_telemetry.as_ref())?;
    if recorded.is_none() && config.events_topic.is_none() {
        return Ok(None);
    }
    let recorded: Arc<dyn PlatformEvents<SandboxKill>> = match recorded {
        Some(events) => events,
        None => Arc::new(NoPlatformEvents),
    };
    let topic = config.events_topic.as_deref();
    Ok(Some(KillReporting {
        claims: Arc::new(PoolClaims(database.clone())),
        events: super::event_topic::published(recorded, topic).await?,
    }))
}

impl ControlPlaneProcessConfig {
    fn from_lookup(mut get: impl FnMut(&str) -> Option<String>) -> Result<Self> {
        let bind = get("DURABLE_ACTORS_CONTROL_PLANE_BIND")
            .unwrap_or_else(|| "127.0.0.1:7100".into())
            .parse()
            .context("DURABLE_ACTORS_CONTROL_PLANE_BIND must be a socket address")?;
        let jwt_signing_key = required(&mut get, "DURABLE_ACTORS_JWT_SIGNING_KEY")?;
        let jwt_key_id = get("DURABLE_ACTORS_JWT_KEY_ID").unwrap_or_else(|| "primary".into());
        let jwt_issuer =
            get("DURABLE_ACTORS_JWT_ISSUER").unwrap_or_else(|| DEFAULT_JWT_ISSUER.into());
        let authority_audience = get("DURABLE_ACTORS_AUTHORITY_JWT_AUDIENCE")
            .unwrap_or_else(|| DEFAULT_AUTHORITY_AUDIENCE.into());
        let invocation_audience = get("DURABLE_ACTORS_INVOKE_JWT_AUDIENCE")
            .unwrap_or_else(|| DEFAULT_INVOCATION_AUDIENCE.into());
        let jwt_max_lifetime = Duration::from_secs(
            get("DURABLE_ACTORS_JWT_MAX_TTL_SECONDS")
                .map(|value| value.parse())
                .transpose()
                .context("DURABLE_ACTORS_JWT_MAX_TTL_SECONDS must be an integer")?
                .unwrap_or(DEFAULT_JWT_TTL_SECONDS),
        );
        ensure!(
            !jwt_max_lifetime.is_zero(),
            "DURABLE_ACTORS_JWT_MAX_TTL_SECONDS must be positive"
        );
        let api_key = get("DURABLE_ACTORS_SECRET");
        if let Some(api_key) = &api_key {
            ensure!(
                !api_key.is_empty(),
                "DURABLE_ACTORS_SECRET must not be empty"
            );
            ensure!(
                api_key.trim() == api_key,
                "DURABLE_ACTORS_SECRET has surrounding whitespace"
            );
        }
        let invoke_api_key = get("DURABLE_ACTORS_INVOKE_API_KEY");
        if let Some(invoke_api_key) = &invoke_api_key {
            ensure!(
                !invoke_api_key.is_empty(),
                "DURABLE_ACTORS_INVOKE_API_KEY must not be empty"
            );
            ensure!(
                invoke_api_key.trim() == invoke_api_key,
                "DURABLE_ACTORS_INVOKE_API_KEY has surrounding whitespace"
            );
            ensure!(
                api_key.is_some(),
                "DURABLE_ACTORS_INVOKE_API_KEY requires DURABLE_ACTORS_SECRET"
            );
            ensure!(
                api_key.as_ref() != Some(invoke_api_key),
                "DURABLE_ACTORS_INVOKE_API_KEY must differ from DURABLE_ACTORS_SECRET"
            );
        }
        let bucket = required(&mut get, "DURABLE_ACTORS_BUCKET")?;
        crate::storage::validate_bucket(&bucket)?;
        let persistence = persistence_config(&mut get)?;
        let artifact_bucket = if persistence.rapid_settings().is_none() {
            get("DURABLE_ACTORS_ARTIFACT_BUCKET").unwrap_or_else(|| bucket.clone())
        } else {
            required(&mut get, "DURABLE_ACTORS_ARTIFACT_BUCKET")?
        };
        crate::storage::validate_bucket(&artifact_bucket)?;
        let region = get("DURABLE_ACTORS_REGION");
        if let Some(region) = &region {
            crate::placement::validate_region(region)?;
        }
        let storage = ControlPlaneStorageConfig {
            persistence,
            artifact_bucket,
            postgres_url: required(&mut get, "DURABLE_ACTORS_POSTGRES_URL")?,
            postgres_max_connections: postgres_max_connections(&mut get)?,
            trace_retention: trace_retention(&mut get)?,
            analytics: analytics_backend(&mut get)?,
            bucket,
        };
        let sandbox_provider =
            sandbox_provider_config(&mut get, &jwt_issuer, &invocation_audience)?;
        let region = region.or_else(|| sandbox_provider.pool.regions.first().cloned());
        ensure!(
            region
                .as_ref()
                .is_some_and(|region| sandbox_provider.pool.regions.contains(region)),
            "default region has no configured sandbox placement"
        );
        let alarm_poll_interval = alarm_poll_interval(&mut get)?;
        Ok(Self {
            bind,
            max_socket_connections: crate::sockets::max_connections(&mut get)?,
            gateway_accept_connections: get("DURABLE_ACTORS_GATEWAY_ACCEPT_CONNECTIONS")
                .map(|value| value.parse())
                .transpose()
                .context("DURABLE_ACTORS_GATEWAY_ACCEPT_CONNECTIONS must be boolean")?
                .unwrap_or(true),
            jwt_signing_key,
            jwt_key_id,
            jwt_issuer,
            authority_audience,
            invocation_audience,
            jwt_max_lifetime,
            api_key,
            invoke_api_key,
            storage,
            sandbox_provider,
            platform_telemetry: platform_telemetry_config(&mut get)?,
            usage_topic: get("DURABLE_ACTORS_USAGE_TOPIC"),
            events_topic: get("DURABLE_ACTORS_EVENTS_TOPIC"),
            region,
            alarm_poll_interval,
            redis: redis_config(&mut get)?,
            organization_limits: super::OrganizationLimits::from_lookup(&mut get, true)?,
        })
    }
}

fn persistence_config(
    get: &mut impl FnMut(&str) -> Option<String>,
) -> Result<crate::bucket::SnapshotStoreConfig> {
    match get("DURABLE_ACTORS_PERSISTENCE")
        .as_deref()
        .unwrap_or("standard")
    {
        "standard" => {
            for name in [
                "DURABLE_ACTORS_RAPID_BUCKETS",
                "DURABLE_ACTORS_RAPID_PLACEMENT",
                "DURABLE_ACTORS_ARCHIVE_BUCKET",
                "DURABLE_ACTORS_ARCHIVE_BATCH_BYTES",
                "DURABLE_ACTORS_ARCHIVE_BATCH_INTERVAL_MS",
            ] {
                ensure!(
                    get(name).is_none(),
                    "{name} cannot be used with standard persistence"
                );
            }
            // Local names the existing snapshot format, including snapshots backed by GCS.
            Ok(crate::bucket::SnapshotStoreConfig::Local)
        }
        "rapid" => rapid_persistence(get),
        _ => anyhow::bail!("DURABLE_ACTORS_PERSISTENCE must be standard or rapid"),
    }
}

fn rapid_persistence(
    get: &mut impl FnMut(&str) -> Option<String>,
) -> Result<crate::bucket::SnapshotStoreConfig> {
    let persistence = crate::bucket::SnapshotStoreConfig::Rapid {
        archive_bucket: required(get, "DURABLE_ACTORS_ARCHIVE_BUCKET")?,
        buckets: serde_json::from_str(&required(get, "DURABLE_ACTORS_RAPID_BUCKETS")?)?,
        archive_batch: crate::bucket::ArchiveBatchConfig {
            bytes: get("DURABLE_ACTORS_ARCHIVE_BATCH_BYTES")
                .map(|value| value.parse())
                .transpose()
                .context("DURABLE_ACTORS_ARCHIVE_BATCH_BYTES must be an integer")?
                .unwrap_or(16 * 1024 * 1024),
            interval_ms: get("DURABLE_ACTORS_ARCHIVE_BATCH_INTERVAL_MS")
                .map(|value| value.parse())
                .transpose()
                .context("DURABLE_ACTORS_ARCHIVE_BATCH_INTERVAL_MS must be an integer")?
                .unwrap_or(10_000),
        },
        placement: match get("DURABLE_ACTORS_RAPID_PLACEMENT").as_deref() {
            None | Some("all") => crate::bucket::RapidPlacement::All,
            Some("nearest") => crate::bucket::RapidPlacement::Nearest,
            Some(_) => anyhow::bail!("DURABLE_ACTORS_RAPID_PLACEMENT must be all or nearest"),
        },
    };
    persistence.validate()?;
    Ok(persistence)
}

fn postgres_max_connections(get: &mut impl FnMut(&str) -> Option<String>) -> Result<usize> {
    let connections: usize = get("DURABLE_ACTORS_POSTGRES_MAX_CONNECTIONS")
        .map(|value| value.parse())
        .transpose()
        .context("DURABLE_ACTORS_POSTGRES_MAX_CONNECTIONS must be an integer")?
        .unwrap_or(crate::postgres::DEFAULT_MAX_CONNECTIONS);
    ensure!(
        (1..=1000).contains(&connections),
        "DURABLE_ACTORS_POSTGRES_MAX_CONNECTIONS must be between 1 and 1000"
    );
    Ok(connections)
}

fn analytics_backend(get: &mut impl FnMut(&str) -> Option<String>) -> Result<AnalyticsBackend> {
    match get("DURABLE_ACTORS_ANALYTICS_BACKEND")
        .as_deref()
        .unwrap_or("none")
    {
        "none" => Ok(AnalyticsBackend::None),
        "clickhouse" => Ok(AnalyticsBackend::ClickHouse(super::ClickHouseConfig {
            url: validated_http_url(
                &required(get, "DURABLE_ACTORS_CLICKHOUSE_URL")?,
                "DURABLE_ACTORS_CLICKHOUSE_URL",
            )?,
            database: required(get, "DURABLE_ACTORS_CLICKHOUSE_DATABASE")?,
            user: required(get, "DURABLE_ACTORS_CLICKHOUSE_USER")?,
            password: required(get, "DURABLE_ACTORS_CLICKHOUSE_PASSWORD")?,
        })),
        _ => anyhow::bail!("DURABLE_ACTORS_ANALYTICS_BACKEND must be none or clickhouse"),
    }
}

fn trace_retention(get: &mut impl FnMut(&str) -> Option<String>) -> Result<Duration> {
    let days: u64 = get("DURABLE_ACTORS_ANALYTICS_RETENTION_DAYS")
        .map(|value| value.parse())
        .transpose()
        .context("DURABLE_ACTORS_ANALYTICS_RETENTION_DAYS must be an integer")?
        .unwrap_or(30);
    ensure!(
        (1..=3650).contains(&days),
        "DURABLE_ACTORS_ANALYTICS_RETENTION_DAYS must be between 1 and 3650"
    );
    Ok(Duration::from_secs(days * 86400))
}

// The URL carries its password, so errors never quote it.
fn redis_config(get: &mut impl FnMut(&str) -> Option<String>) -> Result<Option<RedisConfig>> {
    let set = |value: Option<String>| value.filter(|value| !value.trim().is_empty());
    let ca_cert = set(get("DURABLE_ACTORS_REDIS_CA_CERT"));
    let Some(url) = set(get("DURABLE_ACTORS_REDIS_URL")) else {
        ensure!(
            ca_cert.is_none(),
            "DURABLE_ACTORS_REDIS_CA_CERT requires DURABLE_ACTORS_REDIS_URL"
        );
        return Ok(None);
    };
    ensure!(
        url.starts_with("redis://") || url.starts_with("rediss://"),
        "DURABLE_ACTORS_REDIS_URL must start with redis:// or rediss://"
    );
    ensure!(
        ca_cert.is_none() || url.starts_with("rediss://"),
        "DURABLE_ACTORS_REDIS_CA_CERT requires a rediss:// URL"
    );
    Ok(Some(RedisConfig { url, ca_cert }))
}

fn alarm_poll_interval(get: &mut impl FnMut(&str) -> Option<String>) -> Result<Duration> {
    let milliseconds: u64 = get("DURABLE_ACTORS_ALARM_POLL_INTERVAL_MS")
        .map(|value| value.parse())
        .transpose()
        .context("DURABLE_ACTORS_ALARM_POLL_INTERVAL_MS must be an integer")?
        .unwrap_or(super::alarms::DEFAULT_POLL_INTERVAL.as_millis() as u64);
    ensure!(
        milliseconds > 0,
        "DURABLE_ACTORS_ALARM_POLL_INTERVAL_MS must be positive"
    );
    Ok(Duration::from_millis(milliseconds))
}

fn republish_deployments(service: ControlPlaneService) {
    tokio::spawn(async move {
        if let Err(error) = service.republish_deployments().await {
            warn!(error = %format!("{error:#}"), "deployments were not republished");
        }
    });
}

// A fresh ID per process: a restarted gateway must not vouch for the connections it lost.
fn platform_events(
    config: Option<&super::PlatformTelemetryConfig>,
) -> Result<Option<Arc<super::platform_telemetry::OtlpPlatformEvents>>> {
    config
        .map(|telemetry| {
            super::platform_telemetry::OtlpPlatformEvents::connect(
                telemetry,
                &uuid::Uuid::new_v4().to_string(),
            )
            .map(Arc::new)
        })
        .transpose()
}

fn platform_telemetry_config(
    get: &mut impl FnMut(&str) -> Option<String>,
) -> Result<Option<super::PlatformTelemetryConfig>> {
    let Some(endpoint) = get("DURABLE_ACTORS_PLATFORM_TELEMETRY_ENDPOINT") else {
        return Ok(None);
    };
    let token_file = get("DURABLE_ACTORS_PLATFORM_TELEMETRY_TOKEN_FILE").context(
        "DURABLE_ACTORS_PLATFORM_TELEMETRY_ENDPOINT requires DURABLE_ACTORS_PLATFORM_TELEMETRY_TOKEN_FILE",
    )?;
    Ok(Some(super::PlatformTelemetryConfig {
        endpoint: validated_http_url(&endpoint, "DURABLE_ACTORS_PLATFORM_TELEMETRY_ENDPOINT")?,
        token_file: token_file.into(),
    }))
}

fn sandbox_provider_config(
    get: &mut impl FnMut(&str) -> Option<String>,
    jwt_issuer: &str,
    invocation_audience: &str,
) -> Result<SandboxProviderConfig> {
    let public_origin = validated_http_url(
        &required(get, "DURABLE_ACTORS_PUBLIC_URL")?,
        "DURABLE_ACTORS_PUBLIC_URL",
    )?;
    let public_url = reqwest::Url::parse(&public_origin)?;
    ensure!(
        public_url.scheme() == "https"
            && public_url.path() == "/"
            && public_url.query().is_none()
            && public_url.fragment().is_none()
            && public_url.username().is_empty()
            && public_url.password().is_none(),
        "public gateway must be an HTTPS origin"
    );
    let control_plane_url = validated_http_url(
        &required(get, "DURABLE_ACTORS_CONTROL_PLANE_URL")?,
        "DURABLE_ACTORS_CONTROL_PLANE_URL",
    )?;
    let default_resources = host_resources(get)?;
    let fleet_maximum = pool_number(get, "DURABLE_ACTORS_SPARE_FLEET_MAX", 256, 0, u32::MAX)?;
    let tiers = pool_tiers(get, &default_resources, fleet_maximum)?;
    let (backend, regions) = sandbox_backend(get, &control_plane_url)?;
    let otlp_endpoint = get("DURABLE_ACTORS_OTLP_ENDPOINT")
        .filter(|endpoint| !endpoint.is_empty())
        .map(|endpoint| validated_http_url(&endpoint, "DURABLE_ACTORS_OTLP_ENDPOINT"))
        .transpose()?;
    Ok(SandboxProviderConfig {
        runtime_images: RuntimeImages {
            typescript: runtime_image(get, "DURABLE_ACTORS_TYPESCRIPT_IMAGE")?,
            python: runtime_image(get, "DURABLE_ACTORS_PYTHON_IMAGE")?,
        },
        pool: crate::sandbox::pool::PoolConfig {
            control_plane_url: Some(control_plane_url.clone()),
            otlp_endpoint: otlp_endpoint.clone(),
            kind: crate::sandbox::SpareKind::Actor,
            fleet_maximum,
            max_starting: pool_number(get, "DURABLE_ACTORS_SPARE_MAX_STARTING", 32, 1, u32::MAX)?,
            idle_ttl_seconds: pool_number(get, "DURABLE_ACTORS_SPARE_TTL_SECONDS", 600, 30, 3600)?,
            refill_interval: Duration::from_millis(
                pool_number(
                    get,
                    "DURABLE_ACTORS_SPARE_REFILL_INTERVAL_MS",
                    1000,
                    50,
                    60_000,
                )?
                .into(),
            ),
            retire_concurrency: pool_number(
                get,
                "DURABLE_ACTORS_SPARE_RETIRE_CONCURRENCY",
                64,
                1,
                1024,
            )? as usize,
            reap_interval: Duration::from_millis(
                pool_number(
                    get,
                    "DURABLE_ACTORS_SPARE_REAP_INTERVAL_MS",
                    30_000,
                    500,
                    600_000,
                )?
                .into(),
            ),
            pod_reap_interval: Duration::from_millis(
                pool_number(
                    get,
                    "DURABLE_ACTORS_SPARE_POD_REAP_INTERVAL_MS",
                    1000,
                    100,
                    60_000,
                )?
                .into(),
            ),
            regions,
            tiers,
        },
        default_resources,
        backend,
        secret_namespace: get("DURABLE_ACTORS_GKE_NAMESPACE")
            .unwrap_or_else(|| "terse-sandboxes".into()),
        public_origin,
        resource_bounds: resource_bounds(get)?,
        runtime: HostSandboxRuntimeConfig {
            control_plane_url,
            otlp_endpoint,
            jwt_issuer: jwt_issuer.into(),
            invocation_jwt_audience: invocation_audience.into(),
            host_idle_timeout_ms: crate::host::host_idle_timeout_ms(get)?,
        },
    })
}

fn sandbox_backend(
    get: &mut impl FnMut(&str) -> Option<String>,
    control_plane_url: &str,
) -> Result<(SandboxBackend, Vec<String>)> {
    match get("DURABLE_ACTORS_SANDBOX_PROVIDER")
        .as_deref()
        .unwrap_or("gke")
    {
        "gke" => {
            let zones = placement_zones(get)?;
            let regions = zones.keys().cloned().collect();
            Ok((
                SandboxBackend::Gke(GkeConfig {
                    namespace: get("DURABLE_ACTORS_GKE_NAMESPACE")
                        .unwrap_or_else(|| "terse-sandboxes".into()),
                    zones,
                    reap_concurrency: pool_number(
                        get,
                        "DURABLE_ACTORS_GKE_REAP_CONCURRENCY",
                        64,
                        1,
                        1024,
                    )? as usize,
                    node_cache_url: node_cache_url(get)?,
                    sqlite_slot_gib: get("DURABLE_ACTORS_GKE_SQLITE_SLOT_GIB")
                        .map(|gib| gib.parse())
                        .transpose()
                        .context("DURABLE_ACTORS_GKE_SQLITE_SLOT_GIB must be an integer")?,
                    runtime_class: get("DURABLE_ACTORS_GKE_RUNTIME_CLASS")
                        .filter(|class| !class.is_empty())
                        .unwrap_or_else(|| GKE_RUNTIME_CLASS.into()),
                    pod_annotations: get("DURABLE_ACTORS_GKE_POD_ANNOTATIONS")
                        .filter(|annotations| !annotations.is_empty())
                        .map(|annotations| serde_json::from_str(&annotations))
                        .transpose()
                        .context(
                            "DURABLE_ACTORS_GKE_POD_ANNOTATIONS must be a JSON object of strings",
                        )?
                        .unwrap_or_default(),
                }),
                regions,
            ))
        }
        "modal" => {
            let url = reqwest::Url::parse(control_plane_url)?;
            ensure!(
                url.scheme() == "https"
                    && url.path() == "/"
                    && url.query().is_none()
                    && url.fragment().is_none()
                    && url.username().is_empty()
                    && url.password().is_none(),
                "Modal requires an HTTPS control-plane origin supporting gRPC"
            );
            let regions: BTreeMap<String, String> =
                serde_json::from_str(&required(get, "DURABLE_ACTORS_MODAL_REGIONS")?)?;
            ensure!(
                !regions.is_empty(),
                "at least one Modal placement region is required"
            );
            for (canonical, modal) in &regions {
                ensure!(
                    modal_region(modal) == Some(canonical.as_str()),
                    "Modal region {modal} does not match canonical region {canonical}"
                );
            }
            let canonical = regions.keys().cloned().collect();
            let app_name = required(get, "DURABLE_ACTORS_MODAL_APP")?;
            let seconds = pool_number(
                get,
                "DURABLE_ACTORS_MODAL_TIMEOUT_SECONDS",
                3600,
                120,
                86400,
            )?;
            Ok((
                SandboxBackend::Modal(ModalConfig {
                    app_name,
                    regions,
                    timeout: Duration::from_secs(seconds.into()),
                }),
                canonical,
            ))
        }
        provider => anyhow::bail!("unsupported DURABLE_ACTORS_SANDBOX_PROVIDER {provider:?}"),
    }
}

fn node_cache_url(get: &mut impl FnMut(&str) -> Option<String>) -> Result<Option<String>> {
    get("DURABLE_ACTORS_GKE_NODE_CACHE_URL")
        .map(|url| {
            reqwest::Url::parse(&url).context("DURABLE_ACTORS_GKE_NODE_CACHE_URL must be a URL")?;
            Ok(url)
        })
        .transpose()
}

fn modal_region(region: &str) -> Option<&'static str> {
    match region {
        "ca" => Some("canada"),
        "us-east" => Some("north-america-east"),
        "us-central" => Some("north-america-central"),
        "us-south" => Some("north-america-south"),
        "us-west" => Some("north-america-west"),
        "eu-west" => Some("europe-west"),
        "ap-southeast" => Some("asia-southeast"),
        _ => None,
    }
}

fn placement_zones(
    get: &mut impl FnMut(&str) -> Option<String>,
) -> Result<BTreeMap<String, Vec<String>>> {
    if let Some(zone) = get("DURABLE_ACTORS_GKE_ZONE") {
        ensure!(
            get("DURABLE_ACTORS_GKE_ZONES").is_none(),
            "configure GKE_ZONE or GKE_ZONES, not both"
        );
        let region = super::regions::storage_region(&zone)?.to_owned();
        return compute_zones(&serde_json::to_string(&BTreeMap::from([(
            region,
            vec![zone],
        )]))?);
    }
    compute_zones(&required(get, "DURABLE_ACTORS_GKE_ZONES")?)
}

fn compute_zones(value: &str) -> Result<BTreeMap<String, Vec<String>>> {
    #[derive(serde::Deserialize)]
    #[serde(untagged)]
    enum Placement {
        Zone(String),
        Zones(Vec<String>),
    }
    let configured: BTreeMap<String, Placement> = serde_json::from_str(value)?;
    let zones: BTreeMap<String, Vec<String>> = configured
        .into_iter()
        .map(|(region, placement)| {
            (
                region,
                match placement {
                    Placement::Zone(zone) => vec![zone],
                    Placement::Zones(zones) => zones,
                },
            )
        })
        .collect();
    ensure!(
        !zones.is_empty(),
        "at least one GKE placement zone is required"
    );
    for (region, placements) in &zones {
        ensure!(
            !placements.is_empty(),
            "compute region requires at least one zone"
        );
        ensure!(
            placements
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len()
                == placements.len(),
            "duplicate compute zones"
        );
        for zone in placements {
            ensure!(
                super::regions::storage_region(zone)? == region,
                "GKE zone does not match canonical region {region}"
            );
        }
    }
    Ok(zones)
}

fn host_resources(
    get: &mut impl FnMut(&str) -> Option<String>,
) -> Result<crate::sandbox::ResourceLimits> {
    let defaults = crate::sandbox::ResourceLimits::default();
    let cpu_millis = pool_number(
        get,
        "DURABLE_ACTORS_HOST_CPU_MILLIS",
        defaults.cpu_millis,
        1,
        u32::MAX,
    )?;
    let memory_mib = pool_number(
        get,
        "DURABLE_ACTORS_HOST_MEMORY_MIB",
        defaults.memory_mib,
        1,
        u32::MAX,
    )?;
    let cpu_limit_millis = optional_number(get, "DURABLE_ACTORS_HOST_CPU_LIMIT_MILLIS")?;
    let memory_limit_mib =
        optional_number(get, "DURABLE_ACTORS_HOST_MEMORY_LIMIT_MIB")?.unwrap_or(memory_mib);
    ensure!(
        cpu_limit_millis.is_none_or(|limit| limit >= cpu_millis),
        "DURABLE_ACTORS_HOST_CPU_LIMIT_MILLIS must be at least DURABLE_ACTORS_HOST_CPU_MILLIS"
    );
    ensure!(
        memory_limit_mib >= memory_mib,
        "DURABLE_ACTORS_HOST_MEMORY_LIMIT_MIB must be at least DURABLE_ACTORS_HOST_MEMORY_MIB"
    );
    Ok(crate::sandbox::ResourceLimits {
        cpu_millis,
        cpu_limit_millis,
        memory_mib,
        memory_limit_mib: Some(memory_limit_mib),
    })
}

/// `DURABLE_ACTORS_SPARE_TIERS` lists every spare size. Without it, one tier of the default actor
/// size keeps `DURABLE_ACTORS_SPARE_IDLE` spares.
fn pool_tiers(
    get: &mut impl FnMut(&str) -> Option<String>,
    default_resources: &crate::sandbox::ResourceLimits,
    fleet_maximum: u32,
) -> Result<Vec<crate::sandbox::pool::PoolTier>> {
    let tiers = match get("DURABLE_ACTORS_SPARE_TIERS") {
        Some(tiers) => {
            ensure!(
                get("DURABLE_ACTORS_SPARE_IDLE").is_none(),
                "configure DURABLE_ACTORS_SPARE_IDLE or DURABLE_ACTORS_SPARE_TIERS, not both"
            );
            spare_tiers(&tiers)?
        }
        None => vec![crate::sandbox::pool::PoolTier {
            resources: default_resources.clone(),
            idle: pool_number(get, "DURABLE_ACTORS_SPARE_IDLE", 64, 0, u32::MAX)?,
        }],
    };
    let idle = tiers
        .iter()
        .try_fold(0u32, |idle, tier| idle.checked_add(tier.idle));
    ensure!(
        idle.is_some_and(|idle| idle <= fleet_maximum),
        "ready spares across all tiers must not exceed DURABLE_ACTORS_SPARE_FLEET_MAX"
    );
    Ok(tiers)
}

fn spare_tiers(value: &str) -> Result<Vec<crate::sandbox::pool::PoolTier>> {
    let configured: Vec<SpareTier> =
        serde_json::from_str(value).context("invalid DURABLE_ACTORS_SPARE_TIERS")?;
    ensure!(
        !configured.is_empty(),
        "DURABLE_ACTORS_SPARE_TIERS requires at least one tier"
    );
    let mut tiers = Vec::<crate::sandbox::pool::PoolTier>::new();
    for tier in configured {
        let tier = tier.validated()?;
        ensure!(
            tiers.iter().all(|other| other.resources != tier.resources),
            "DURABLE_ACTORS_SPARE_TIERS repeats a size"
        );
        tiers.push(tier);
    }
    Ok(tiers)
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SpareTier {
    cpu_millis: u32,
    // `deserialize_with` makes serde require the key, so leaving CPU uncapped takes an explicit
    // `null`.
    #[serde(deserialize_with = "serde::Deserialize::deserialize")]
    cpu_limit_millis: Option<u32>,
    #[serde(rename = "memoryMiB")]
    memory_mib: u32,
    #[serde(rename = "memoryLimitMiB")]
    memory_limit_mib: u32,
    warm: u32,
}

impl SpareTier {
    fn validated(self) -> Result<crate::sandbox::pool::PoolTier> {
        ensure!(
            self.cpu_millis > 0 && self.memory_mib > 0,
            "DURABLE_ACTORS_SPARE_TIERS requires positive cpuMillis and memoryMiB"
        );
        ensure!(
            self.cpu_limit_millis
                .is_none_or(|limit| limit >= self.cpu_millis),
            "DURABLE_ACTORS_SPARE_TIERS cpuLimitMillis must be at least cpuMillis"
        );
        ensure!(
            self.memory_limit_mib >= self.memory_mib,
            "DURABLE_ACTORS_SPARE_TIERS memoryLimitMiB must be at least memoryMiB"
        );
        Ok(crate::sandbox::pool::PoolTier {
            resources: crate::sandbox::ResourceLimits {
                cpu_millis: self.cpu_millis,
                cpu_limit_millis: self.cpu_limit_millis,
                memory_mib: self.memory_mib,
                memory_limit_mib: Some(self.memory_limit_mib),
            },
            idle: self.warm,
        })
    }
}

fn resource_bounds(
    get: &mut impl FnMut(&str) -> Option<String>,
) -> Result<crate::sandbox::ResourceBounds> {
    let defaults = crate::sandbox::ResourceBounds::default();
    Ok(crate::sandbox::ResourceBounds {
        cpu_millis: bound(get, "CPU_MILLIS", defaults.cpu_millis)?,
        memory_mib: bound(get, "MEMORY_MIB", defaults.memory_mib)?,
    })
}

fn bound(
    get: &mut impl FnMut(&str) -> Option<String>,
    unit: &str,
    default: std::ops::RangeInclusive<u32>,
) -> Result<std::ops::RangeInclusive<u32>> {
    let min_name = format!("DURABLE_ACTORS_SANDBOX_MIN_{unit}");
    let max_name = format!("DURABLE_ACTORS_SANDBOX_MAX_{unit}");
    let min = pool_number(get, &min_name, *default.start(), 0, u32::MAX)?;
    let max = pool_number(get, &max_name, *default.end(), 0, u32::MAX)?;
    ensure!(min <= max, "{min_name} must not exceed {max_name}");
    Ok(min..=max)
}

fn pool_number(
    get: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
    default: u32,
    min: u32,
    max: u32,
) -> Result<u32> {
    let value = optional_number(get, name)?.unwrap_or(default);
    ensure!(
        (min..=max).contains(&value),
        "{name} must be between {min} and {max}"
    );
    Ok(value)
}

fn optional_number(
    get: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
) -> Result<Option<u32>> {
    get(name)
        .map(|value| value.parse::<u32>())
        .transpose()
        .with_context(|| format!("invalid {name}"))
}

fn required(get: &mut impl FnMut(&str) -> Option<String>, name: &str) -> Result<String> {
    let value = get(name).with_context(|| format!("{name} is required"))?;
    ensure!(!value.is_empty(), "{name} must not be empty");
    Ok(value)
}

fn validated_http_url(value: &str, name: &str) -> Result<String> {
    let url = reqwest::Url::parse(value).with_context(|| format!("{name} must be a URL"))?;
    ensure!(
        matches!(url.scheme(), "http" | "https") && url.host_str().is_some(),
        "{name} must be HTTP or HTTPS"
    );
    Ok(url.to_string())
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/process.rs"]
mod tests;

fn runtime_image(get: &mut impl FnMut(&str) -> Option<String>, name: &str) -> Result<String> {
    let image = required(get, name)?;
    crate::sandbox::gke::validate_image(&image).with_context(|| format!("invalid {name}"))?;
    Ok(image)
}
