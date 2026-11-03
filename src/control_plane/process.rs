use std::{collections::BTreeMap, env, future::Future, net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use tracing::{info, warn};

use crate::{
    bucket::{GcsBucket, RuntimeStorageReader},
    postgres::PostgresDatabase,
    redis_client::{Redis, RedisConfig},
    request_traces::{
        TraceStore,
        persistence::{clickhouse::ClickHouseTracePersistence, postgres::PostgresTracePersistence},
    },
    sandbox::{HostSandboxRuntimeConfig, gke::GkeConfig, modal::ModalConfig},
};

use super::{ActorJwtVerifier, ControlPlaneService};

const DEFAULT_JWT_ISSUER: &str = "durable-actors-control-plane";
const DEFAULT_AUTHORITY_AUDIENCE: &str = "durable-actors-authority";
const DEFAULT_INVOCATION_AUDIENCE: &str = "durable-actors-invoke";
const DEFAULT_JWT_TTL_SECONDS: u64 = 86_400;
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
    pub socket_event_sink: Option<SocketEventSinkConfig>,
    pub usage_topic: Option<String>,
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
    Postgres,
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

pub struct SocketEventSinkConfig {
    pub url: String,
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
    let changes = crate::postgres::notifications::ChangeFeed::postgres(
        database.clone(),
        &config.storage.postgres_url,
        stop.clone(),
    )
    .await?;
    let (traces, lifecycle) = telemetry_stores(
        &config.storage,
        database.clone(),
        redis.clone(),
        stop.clone(),
    )
    .await?;
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
        ),
        None => super::socket_bus::MemorySocketHub::default().join(config.max_socket_connections),
    };
    let socket_gateway = super::socket_gateway::SocketGateway::start(
        socket_bus,
        config.gateway_accept_connections,
        stop.child_token(),
        connections.client.clone(),
    )?;
    let gateway = super::gateway::Gateway::new(
        &issuer,
        config.sandbox_provider.public_origin.clone(),
        socket_gateway,
    )?;
    let mut provisioner = sandbox_provisioner(
        config.sandbox_provider,
        &issuer,
        runtime_access.clone(),
        database.clone(),
        stop.clone(),
        connections,
    )
    .await?;
    if let Some(admission) = admission {
        provisioner = provisioner.with_actor_admission(admission.clone());
        lifecycle = lifecycle.with_observer(admission);
    }
    let socket_events = config
        .socket_event_sink
        .map(|sink| {
            super::event_sink::HttpSocketMessageEventSink::new(sink.url, config.api_key.clone())
        })
        .transpose()?
        .map(|sink| Arc::new(sink) as Arc<dyn super::event_sink::SocketMessageEventSink>);
    let mut service = ControlPlaneService::new(
        placements.clone(),
        auth,
        registry.clone(),
        issuer.clone(),
        Arc::new(provisioner),
    )
    .with_runtime_access(runtime_access)
    .with_alarm_store(Arc::new(super::alarms::PostgresAlarmStore::new(database)))
    .with_traces(traces)
    .with_socket_event_sink(socket_events);
    if let Some(redis) = redis {
        service.routes = Arc::new(super::routes::RedisRoutes::new(
            redis,
            "durable-actors:v1:routes",
        ));
    }
    service.lifecycle = lifecycle;
    service.changes = changes;
    service.gateway = Some(gateway);
    service.region = config.region;
    service.lifecycle.start_reconciliation(stop.clone());
    service.start_access_reconciliation(stop.clone());
    service.start_alarm_scheduler(config.alarm_poll_interval, stop);
    let inventory = Arc::new(super::socket_inventory::GatewayInventoryReader::new(
        Arc::new(service.lifecycle.clone()),
        service.gateway.as_ref().unwrap().connections.clone(),
    ));
    let admin = super::admin::AdminService::new(config.api_key, registry, issuer)?
        .with_invoke_api_key(config.invoke_api_key)?;
    let inspector =
        super::inspection::ActorInspector::new(inventory, storage.clone(), service.changes.clone())
            .with_traces(service.traces.clone());
    let public_api = super::public_api::router(service.clone(), admin.clone())
        .merge(super::inspection::router(inspector, admin));
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

async fn telemetry_stores(
    storage: &ControlPlaneStorageConfig,
    database: PostgresDatabase,
    redis: Option<Redis>,
    stop: tokio_util::sync::CancellationToken,
) -> Result<(TraceStore, crate::actor_lifecycle::LifecycleStore)> {
    match &storage.analytics {
        AnalyticsBackend::Postgres => {
            let persistence = Arc::new(PostgresTracePersistence::new(
                database,
                storage.trace_retention,
            ));
            let traces = TraceStore::open(persistence.clone()).await?;
            persistence.start_retention(stop);
            Ok((traces, Default::default()))
        }
        AnalyticsBackend::ClickHouse(config) => {
            let live = crate::request_traces::live::LiveTraceFeed::start(redis, stop.clone());
            let persistence = Arc::new(ClickHouseTracePersistence::start(
                config.clone(),
                storage.trace_retention,
                Arc::new(live.clone()),
                stop,
            )?);
            let mut traces = TraceStore::open(persistence.clone()).await?;
            traces.live = Some(live);
            Ok((
                traces,
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
) -> Result<super::service::SandboxHostProvisioner> {
    let provider = sandbox_provider(&config, connections).await?;
    let pool = crate::sandbox::pool::SparePool::with_runtime_images(
        database,
        provider.clone(),
        config.pool,
        Some(config.runtime_images.clone()),
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
    .with_resource_bounds(config.resource_bounds))
}

async fn sandbox_provider(
    config: &SandboxProviderConfig,
    connections: crate::sandbox::connections::HostConnections,
) -> Result<Arc<dyn crate::sandbox::SandboxProvider>> {
    let secrets = Arc::new(crate::sandbox::secrets::KubernetesSecrets::new(
        config.secret_namespace.clone(),
    ));
    Ok(Arc::new(match &config.backend {
        SandboxBackend::Gke(gke) => {
            crate::sandbox::gke::provider(gke.clone(), secrets, connections).await?
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
    let provider = sandbox_provider(
        &config.sandbox_provider,
        crate::sandbox::connections::HostConnections::new()?,
    )
    .await?;
    let pool = crate::sandbox::pool::SparePool::with_runtime_images(
        database,
        provider,
        config.sandbox_provider.pool,
        Some(config.sandbox_provider.runtime_images),
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
    info!(bind = %config.bind, "durable-actors pool manager is ready");
    axum::serve(listener, routes)
        .with_graceful_shutdown(shutdown)
        .await
        .context("serve durable-actors pool manager health")?;
    stop.cancel();
    housekeeping.await?;
    Ok(())
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
        let socket_event_sink = socket_event_sink_config(&mut get)?;
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
            socket_event_sink,
            usage_topic: get("DURABLE_ACTORS_USAGE_TOPIC"),
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
        .unwrap_or("postgres")
    {
        "postgres" => Ok(AnalyticsBackend::Postgres),
        "clickhouse" => Ok(AnalyticsBackend::ClickHouse(super::ClickHouseConfig {
            url: validated_http_url(
                &required(get, "DURABLE_ACTORS_CLICKHOUSE_URL")?,
                "DURABLE_ACTORS_CLICKHOUSE_URL",
            )?,
            database: required(get, "DURABLE_ACTORS_CLICKHOUSE_DATABASE")?,
            user: required(get, "DURABLE_ACTORS_CLICKHOUSE_USER")?,
            password: required(get, "DURABLE_ACTORS_CLICKHOUSE_PASSWORD")?,
        })),
        _ => anyhow::bail!("DURABLE_ACTORS_ANALYTICS_BACKEND must be postgres or clickhouse"),
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

fn socket_event_sink_config(
    get: &mut impl FnMut(&str) -> Option<String>,
) -> Result<Option<SocketEventSinkConfig>> {
    get("DURABLE_ACTORS_SOCKET_EVENT_URL")
        .map(|url| {
            Ok(SocketEventSinkConfig {
                url: validated_http_url(&url, "DURABLE_ACTORS_SOCKET_EVENT_URL")?,
            })
        })
        .transpose()
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
    let idle = pool_number(get, "DURABLE_ACTORS_SPARE_IDLE", 64, 0, u32::MAX)?;
    let fleet_maximum = pool_number(get, "DURABLE_ACTORS_SPARE_FLEET_MAX", 256, 0, u32::MAX)?;
    ensure!(
        idle <= fleet_maximum,
        "DURABLE_ACTORS_SPARE_IDLE must not exceed DURABLE_ACTORS_SPARE_FLEET_MAX"
    );
    let (backend, regions) = sandbox_backend(get, &control_plane_url)?;
    Ok(SandboxProviderConfig {
        runtime_images: RuntimeImages {
            typescript: runtime_image(get, "DURABLE_ACTORS_TYPESCRIPT_IMAGE")?,
            python: runtime_image(get, "DURABLE_ACTORS_PYTHON_IMAGE")?,
        },
        pool: crate::sandbox::pool::PoolConfig {
            control_plane_url: Some(control_plane_url.clone()),
            kind: crate::sandbox::SpareKind::Actor,
            idle,
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
            resources: host_resources(get)?,
        },
        backend,
        secret_namespace: get("DURABLE_ACTORS_GKE_NAMESPACE")
            .unwrap_or_else(|| "terse-sandboxes".into()),
        public_origin,
        resource_bounds: resource_bounds(get)?,
        runtime: HostSandboxRuntimeConfig {
            control_plane_url,
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
