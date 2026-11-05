use std::{
    future::Future,
    process::Stdio,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use tokio::{net::TcpListener, process::Command};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{error, info};

use crate::{
    actor::{ActorExecutorConnection, ActorExecutorListener},
    bucket::RAPID_OPERATION_TIMEOUT,
    bundle::ActorRuntime,
    clock::SystemClock,
    control_plane::{ActorJwtVerifier, ActorTokenPurpose, ControlPlaneClient},
    host::http::ActorHostHttpService,
};

use super::{
    ActorHost, HostEndpoint, HostId, HostLeaseMaintainer, LeaseRenewalTask,
    actor_host::{ActorPhase, ActorStatus},
    config::{ActorHostConfig, ExecutorConfig, HostNetwork, InvocationAuth, LeaseTiming},
    telemetry::HostTelemetry,
};

const HOST_TASK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const HOST_ACTOR_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
/// How long the executor waits for its host to attach. Falling back from Rapid can
/// spend two operation timeouts before Standard starts: the failed open, then
/// deleting the replicas that did open.
const EXECUTOR_STARTUP_TIMEOUT: Duration = RAPID_OPERATION_TIMEOUT.saturating_mul(3);

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct HostReadiness {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initial_reply: Option<serde_json::Value>,
    pub host_id: super::HostId,
    pub session_id: String,
    pub route: String,
    pub canonical_region: String,
    pub owner_epoch: u64,
    pub lease: crate::host_leases::HostLease,
}

pub async fn serve_actor_host<F>(shutdown: F) -> Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    super::protect_runtime_credentials()?;
    let timings = HostStartupTimings::start();
    let config = ActorHostConfig::from_env()?;
    serve_assigned_host(config, timings.configuration_loaded(), None, shutdown).await
}

pub(super) async fn serve_assigned_host(
    config: ActorHostConfig,
    mut timings: HostStartupTimings,
    mut warm: Option<super::spare::WarmHost>,
    shutdown: impl Future<Output = ()> + Send,
) -> Result<()> {
    let readiness = warm.as_mut().and_then(|warm| warm.readiness.take());
    let operation = warm.as_mut().and_then(|warm| warm.operation.take());
    let host_id = &config.identity.host_id;
    let prepared = match prepare_actor_host(&config, &mut timings, warm).await {
        Ok(prepared) => prepared,
        Err(error) => {
            log_startup(host_id, &timings, "failed", Some(&error));
            super::assignment::decline(readiness);
            return Err(error);
        }
    };
    let PreparedActorHostResources {
        control_plane,
        invocation_auth,
        listener,
        route,
        executor_connection,
        mut javascript,
        host,
        lease,
        renewal,
        sockets,
        credentials: _credentials,
        storage,
        database,
        telemetry,
    } = prepared;
    let mut lease_lost = renewal.lease_lost();
    let mut status = host.status();

    let service = ActorHostHttpService::new(
        host.clone(),
        config.identity.session_id.clone(),
        invocation_auth,
        sockets.clone(),
    );
    let mut started = false;
    let initialized = async {
        let owner_epoch = initialize_executor(
            &executor_connection,
            &host,
            &sockets,
            &database,
            &control_plane,
            telemetry.as_ref(),
        )
        .await?;
        let mut ready = HostReadiness {
            initial_reply: None,
            host_id: host_id.clone(),
            session_id: config.identity.session_id.clone(),
            route: route.clone(),
            canonical_region: config.storage.region.clone(),
            owner_epoch,
            lease: storage.current_lease()?,
        };
        if let Some(path) = &config.ready_file {
            let temporary = path.with_extension("tmp");
            tokio::fs::write(&temporary, serde_json::to_vec(&ready)?).await?;
            tokio::fs::rename(temporary, path).await?;
        }
        ready.initial_reply = match operation {
            Some(operation) => {
                started = true;
                Some(
                    service
                        .execute_initial(operation, owner_epoch, timings.elapsed_ms())
                        .await?,
                )
            }
            None => None,
        };
        ready.lease = storage.current_lease()?;
        anyhow::Ok(ready)
    }
    .await;
    let ready = match initialized {
        Ok(ready) => ready,
        Err(error) => {
            log_startup(host_id, &timings, "failed", Some(&error));
            let _ = lease.unregister().await;
            let _ = renewal.shutdown().await;
            if !started {
                super::assignment::decline(readiness);
            }
            return Err(error);
        }
    };
    let lifecycle = super::lifecycle::HostLifecycle::start(
        control_plane,
        ready.owner_epoch,
        config.resources.clone(),
        executor_connection.executor(),
        host.status(),
    )?;
    if let Some(readiness) = readiness {
        let _ = readiness.send(Ok(ready));
    }
    timings.executor_notified_at_ms = Some(timings.elapsed_ms());
    log_startup(host_id, &timings, "ready", None);
    let executor_stop = CancellationToken::new();
    let server_stop = CancellationToken::new();
    let serving = server_stop.clone();
    let routes = service.router();
    let mut server = Box::pin(async move {
        axum::serve(listener, routes)
            .with_graceful_shutdown(async move { serving.cancelled().await })
            .await
            .context("serve actor host endpoints")
    });
    let mut executor_task = Box::pin(executor_connection.run(executor_stop.clone()));
    let shutdown = async {
        tokio::select! { () = shutdown => {}, () = storage.stop.cancelled() => {} }
    };
    tokio::pin!(shutdown);

    info!(%host_id, route, "durable-actors host is ready");
    let stop_result = wait_for_host_stop(
        server.as_mut(),
        executor_task.as_mut(),
        &mut javascript,
        shutdown.as_mut(),
        &mut lease_lost,
        &mut status,
        config.idle_timeout,
    )
    .await;
    let shutdown_started = Instant::now();
    let shutdown_deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    // Keep answering until the actor is released: calls routed here meanwhile get a refusal they
    // can retry, rather than a reset whose outcome is unknown.
    let unregister_result = serve_during(server.as_mut(), async {
        lifecycle.draining();
        stop_executor(&host, &executor_stop, executor_task).await;
        drop(javascript);
        let result = storage
            .finish(shutdown_deadline - Duration::from_secs(2))
            .await;
        lifecycle.stop().await;
        result
    })
    .await;
    server_stop.cancel();
    let _ = tokio::time::timeout(HOST_TASK_SHUTDOWN_TIMEOUT, server).await;
    let renewal_result = renewal.shutdown().await;
    info!(
        event = "host_shutdown",
        duration_ms = shutdown_started.elapsed().as_secs_f64() * 1000.0,
        storage_released = unregister_result.is_ok(),
        error = unregister_result
            .as_ref()
            .err()
            .map(|error| format!("{error:#}"))
    );
    if let Some(telemetry) = telemetry {
        let _ = tokio::task::spawn_blocking(move || telemetry.shutdown()).await;
    }
    info!(%host_id, "durable-actors host stopped");
    stop_result?;
    renewal_result?;
    unregister_result
}

async fn initialize_executor(
    connection: &ActorExecutorConnection,
    host: &Arc<ActorHost>,
    sockets: &Arc<super::sockets::HostSockets>,
    database: &Arc<super::HostDatabase>,
    control_plane: &Arc<ControlPlaneClient>,
    telemetry: Option<&HostTelemetry>,
) -> Result<u64> {
    connection
        .mark_ready(
            host.actor().clone(),
            Some(sockets.clone()),
            Some(sockets.clone()),
            Some(database.background(host.actor().clone())),
            Some(control_plane.clone()),
            telemetry.map(HostTelemetry::log_sink),
        )
        .await?;
    Ok(host.activate().await?.owner_epoch)
}

struct PreparedActorHostResources {
    control_plane: Arc<ControlPlaneClient>,
    storage: Arc<super::storage::HostStorage>,
    credentials: DropGuard,
    sockets: Arc<super::sockets::HostSockets>,
    invocation_auth: ActorJwtVerifier,
    listener: TcpListener,
    route: String,
    executor_connection: ActorExecutorConnection,
    javascript: tokio::process::Child,
    host: Arc<ActorHost>,
    lease: Arc<HostLeaseMaintainer>,
    renewal: LeaseRenewalTask,
    database: Arc<super::HostDatabase>,
    telemetry: Option<HostTelemetry>,
}

async fn prepare_actor_host(
    config: &ActorHostConfig,
    timings: &mut HostStartupTimings,
    warm: Option<super::spare::WarmHost>,
) -> Result<PreparedActorHostResources> {
    let replication = warm
        .as_ref()
        .map(|warm| warm.replication.clone())
        .unwrap_or_else(|| Arc::new(crate::litestream::Litestream::default()));
    timings.replication_ready_at_ms = Some(timings.elapsed_ms());
    let invocation_auth = invocation_verifier(&config.invocation_auth)?;
    timings.authentication_ready_at_ms = Some(timings.elapsed_ms());
    let (warm_listener, warm_executor, warm_storage, warm_control_plane) = match warm {
        Some(warm) => (
            Some(warm.listener),
            Some((warm.executor, warm.javascript)),
            Some(warm.storage),
            warm.control_plane,
        ),
        None => (None, None, None, None),
    };
    let access = &config.control_plane;
    let (control_plane, (listener, route, endpoint)) = tokio::try_join!(
        async {
            match warm_control_plane.filter(|warm| warm.url == access.url) {
                Some(warm) => warm.client.with_host_token(&access.host_token),
                None => ControlPlaneClient::connect(&access.url, &access.host_token).await,
            }
        },
        bind_host_listener(&config.network, &config.identity.host_id, warm_listener),
    )?;
    timings.control_plane_ready_at_ms = Some(timings.elapsed_ms());
    let control_plane = Arc::new(control_plane);
    let stop = CancellationToken::new();
    let credentials = stop.clone().drop_guard();
    let assignment = &config.assignment;
    let storage = Arc::new(
        super::storage::HostStorage::new(
            config.storage.clone(),
            config.identity.host_id.clone(),
            config.identity.session_id.clone(),
            control_plane.clone(),
            stop,
            warm_storage,
            assignment.actor.clone(),
        )
        .await?
        .with_activation(assignment.new_actor, assignment.owner_hint.clone()),
    );
    let persistence = super::persistence::ActorPersistence::new(storage.clone());
    let database =
        super::HostDatabase::new(endpoint.clone(), storage.clone(), persistence, replication);
    let started = timings.started_at;
    let storage_ready = async {
        let result = prepare_storage(
            &config.identity.session_id,
            &config.lease,
            &endpoint,
            storage.clone(),
        )
        .await;
        timings.storage_ready_at_ms = Some(started.elapsed().as_secs_f64() * 1_000.0);
        let (lease, renewal) = result?;
        let result = prepare_database(&database, lease, renewal).await;
        if result.is_ok() {
            timings.database_ready_at_ms = Some(started.elapsed().as_secs_f64() * 1_000.0);
        }
        result
    };
    let executor_ready = async {
        if let Some(bundle) = &config.executor.bundle {
            storage.install_code(bundle).await?;
        }
        let result = if let Some((executor, javascript)) = warm_executor {
            let entrypoint = config
                .executor
                .entrypoint
                .as_deref()
                .context("missing customer entrypoint")?;
            let connection = tokio::time::timeout(
                Duration::from_secs(60),
                executor.load(entrypoint, &config.executor.environment),
            )
            .await??;
            Ok((connection, javascript))
        } else {
            connect_executor(
                &config.executor,
                timings.started_at,
                &mut timings.javascript_spawned_at_ms,
            )
            .await
        };
        timings.executor_ready_at_ms = Some(started.elapsed().as_secs_f64() * 1_000.0);
        result
    };
    let (lease, executor) = tokio::join!(storage_ready, executor_ready);
    let (lease, renewal) = lease?;
    let (executor_connection, javascript) = match executor {
        Ok(executor) => executor,
        Err(error) => {
            let _ = lease.unregister().await;
            let _ = renewal.shutdown().await;
            return Err(error);
        }
    };
    let sockets = Arc::new(super::sockets::HostSockets::new(
        assignment.actor.clone(),
        storage.clone(),
        control_plane.clone(),
    ));
    let telemetry = HostTelemetry::from_env(control_plane.authorization())?;
    let mut host = ActorHost::new(
        endpoint,
        assignment.actor.clone(),
        executor_connection.executor(),
        database.clone(),
        sockets.clone(),
    );
    if let Some(telemetry) = &telemetry {
        host = host.with_traces(telemetry.tracer());
    }
    let host = Arc::new(host);
    timings.lease_registered_at_ms = Some(timings.elapsed_ms());
    Ok(PreparedActorHostResources {
        control_plane,
        storage,
        credentials,
        sockets,
        invocation_auth,
        listener,
        route,
        executor_connection,
        javascript,
        host,
        lease,
        renewal,
        database,
        telemetry,
    })
}

async fn prepare_storage(
    session_id: &str,
    timing: &LeaseTiming,
    endpoint: &HostEndpoint,
    storage: Arc<super::storage::HostStorage>,
) -> Result<(Arc<HostLeaseMaintainer>, LeaseRenewalTask)> {
    let lease = Arc::new(HostLeaseMaintainer::new(
        endpoint.clone(),
        session_id.to_owned(),
        storage.clone(),
        Arc::new(SystemClock),
        timing.duration,
        timing.renew_every,
    )?);
    let renewal = lease.clone().start().await?;
    Ok((lease, renewal))
}

async fn prepare_database(
    database: &super::HostDatabase,
    lease: Arc<HostLeaseMaintainer>,
    renewal: LeaseRenewalTask,
) -> Result<(Arc<HostLeaseMaintainer>, LeaseRenewalTask)> {
    if let Err(error) = database.activate().await {
        let _ = lease.unregister().await;
        let _ = renewal.shutdown().await;
        return Err(error);
    }
    Ok((lease, renewal))
}

async fn bind_host_listener(
    network: &HostNetwork,
    host_id: &HostId,
    listener: Option<TcpListener>,
) -> Result<(TcpListener, String, HostEndpoint)> {
    let listener = match listener {
        Some(listener) => listener,
        None => TcpListener::bind(network.bind).await?,
    };
    let bound = listener.local_addr()?;
    let route = network
        .route
        .clone()
        .unwrap_or_else(|| format!("http://{bound}"));
    let endpoint = HostEndpoint {
        id: host_id.clone(),
        route: route.clone(),
    };
    Ok((listener, route, endpoint))
}

fn invocation_verifier(auth: &InvocationAuth) -> Result<ActorJwtVerifier> {
    ActorJwtVerifier::for_scope(
        &auth.public_keys,
        auth.issuer.clone(),
        auth.audience.clone(),
        ActorTokenPurpose::Invocation,
        auth.max_token_lifetime,
    )
}

pub(super) struct HostStartupTimings {
    started_at: Instant,
    configuration_loaded_at_ms: f64,
    authentication_ready_at_ms: Option<f64>,
    control_plane_ready_at_ms: Option<f64>,
    storage_ready_at_ms: Option<f64>,
    database_ready_at_ms: Option<f64>,
    executor_ready_at_ms: Option<f64>,
    replication_ready_at_ms: Option<f64>,
    javascript_spawned_at_ms: Option<f64>,
    lease_registered_at_ms: Option<f64>,
    executor_notified_at_ms: Option<f64>,
}

impl HostStartupTimings {
    pub(super) fn start() -> Self {
        Self {
            started_at: Instant::now(),
            configuration_loaded_at_ms: 0.0,
            authentication_ready_at_ms: None,
            control_plane_ready_at_ms: None,
            storage_ready_at_ms: None,
            database_ready_at_ms: None,
            executor_ready_at_ms: None,
            replication_ready_at_ms: None,
            javascript_spawned_at_ms: None,
            lease_registered_at_ms: None,
            executor_notified_at_ms: None,
        }
    }

    pub(super) fn configuration_loaded(mut self) -> Self {
        self.configuration_loaded_at_ms = self.elapsed_ms();
        self
    }

    fn elapsed_ms(&self) -> f64 {
        self.started_at.elapsed().as_secs_f64() * 1_000.0
    }
}

fn log_startup(
    host_id: &HostId,
    timings: &HostStartupTimings,
    outcome: &str,
    error: Option<&anyhow::Error>,
) {
    info!(
        event = "actor_host_startup",

        %host_id,
        started_at_ms = 0,
        configuration_loaded_at_ms = timings.configuration_loaded_at_ms,
        authentication_ready_at_ms = timings.authentication_ready_at_ms,
        control_plane_ready_at_ms = timings.control_plane_ready_at_ms,
        storage_ready_at_ms = timings.storage_ready_at_ms,
        database_ready_at_ms = timings.database_ready_at_ms,
        executor_ready_at_ms = timings.executor_ready_at_ms,
        replication_ready_at_ms = timings.replication_ready_at_ms,
        javascript_spawned_at_ms = timings.javascript_spawned_at_ms,
        lease_registered_at_ms = timings.lease_registered_at_ms,
        executor_notified_at_ms = timings.executor_notified_at_ms,
        completed_at_ms = timings.elapsed_ms(),
        outcome,
        error = error.map(|error| format!("{error:#}")),
        "actor host startup completed"
    );
}

async fn connect_executor(
    executor: &ExecutorConfig,
    started_at: Instant,
    javascript_spawned_at_ms: &mut Option<f64>,
) -> Result<(ActorExecutorConnection, tokio::process::Child)> {
    let listener = ActorExecutorListener::bind(&executor.socket).await?;
    let javascript = spawn_executor_process(
        false,
        &executor.socket.display().to_string(),
        executor.runtime,
    )?;
    *javascript_spawned_at_ms = Some(started_at.elapsed().as_secs_f64() * 1_000.0);
    Ok((listener.accept().await?, javascript))
}

async fn wait_for_host_stop<ServerFuture, ExecutorFuture, ShutdownFuture>(
    mut server: std::pin::Pin<&mut ServerFuture>,
    mut executor: std::pin::Pin<&mut ExecutorFuture>,
    javascript: &mut tokio::process::Child,
    mut shutdown: std::pin::Pin<&mut ShutdownFuture>,
    lease_lost: &mut tokio::sync::watch::Receiver<bool>,
    status: &mut tokio::sync::watch::Receiver<ActorStatus>,
    idle_timeout: Duration,
) -> Result<()>
where
    ServerFuture: Future<Output = Result<()>> + ?Sized,
    ExecutorFuture: Future<Output = Result<()>> + ?Sized,
    ShutdownFuture: Future<Output = ()> + ?Sized,
{
    loop {
        let (phase, idle_since) = {
            let current = status.borrow_and_update();
            (current.phase, current.idle_since)
        };
        if phase == ActorPhase::Stopped {
            break Err(anyhow::anyhow!(
                "actor activation stopped; host self-fenced"
            ));
        }
        tokio::select! {
            result = server.as_mut() => break result.context("serve actor host network endpoints"),
            result = executor.as_mut() => break result.context("run JavaScript actor executor"),
            result = javascript.wait() => break Err(anyhow::anyhow!("JavaScript actor executor exited with {}", result?)),
            () = shutdown.as_mut() => break Ok(()),
            changed = lease_lost.changed() => {
                if changed.is_err() || *lease_lost.borrow() {
                    break Err(anyhow::anyhow!("host lease expired; host self-fenced"));
                }
            }
            changed = status.changed() => {
                if changed.is_err() { break Err(anyhow::anyhow!("actor status tracker stopped")); }
            }
            () = idle_elapsed(idle_since, idle_timeout) => {
                if status.has_changed().unwrap_or(true) { continue; }
                break Ok(());
            },
        }
    }
}

async fn idle_elapsed(idle_since: Option<tokio::time::Instant>, timeout: Duration) {
    match idle_since {
        Some(since) => tokio::time::sleep_until(since + timeout).await,
        None => std::future::pending().await,
    }
}

async fn serve_during<T>(
    server: std::pin::Pin<&mut impl Future>,
    work: impl Future<Output = T>,
) -> T {
    tokio::pin!(work);
    tokio::select! {
        result = &mut work => result,
        _ = server => work.await,
    }
}

async fn stop_executor(host: &ActorHost, stop: &CancellationToken, executor: impl Future) {
    if let Err(error) = host.drain(HOST_ACTOR_DRAIN_TIMEOUT).await {
        error!(error = %format!("{error:#}"), "actor invocations did not drain cleanly");
    }
    stop.cancel();
    let _ = tokio::time::timeout(HOST_TASK_SHUTDOWN_TIMEOUT, executor).await;
}

pub(super) fn spawn_executor_process(
    generic: bool,
    socket: &str,
    runtime: ActorRuntime,
) -> Result<tokio::process::Child> {
    executor_command(generic, socket, runtime)
        .spawn()
        .context("start actor executor")
}

fn executor_command(generic: bool, socket: &str, runtime: ActorRuntime) -> Command {
    let mut command = if runtime == ActorRuntime::Python {
        let mut command = Command::new(
            std::env::var("DURABLE_ACTORS_PYTHON").unwrap_or_else(|_| "python3".into()),
        );
        command.args(["-m", "durable_actors.host"]);
        if generic {
            command.arg("--generic");
        }
        command
    } else {
        let mut command = Command::new("bun");
        command.args([
            "--eval",
            "import(process.env.DURABLE_ACTORS_SDK_HOST ?? \"durable-actors/host\").then(module => module[process.env.DURABLE_ACTORS_GENERIC_EXECUTOR === \"1\" ? \"runGenericHost\" : \"runActorHost\"]())",
        ]);
        command
    };
    command
        .env(
            "DURABLE_ACTORS_GENERIC_EXECUTOR",
            if generic { "1" } else { "0" },
        )
        .env("DURABLE_ACTORS_EXECUTOR_SOCKET", socket)
        .env(
            "DURABLE_ACTORS_HOST_STARTUP_MS",
            EXECUTOR_STARTUP_TIMEOUT.as_millis().to_string(),
        )
        .env_remove("DURABLE_ACTORS_SPARE_TOKEN")
        .env_remove("DURABLE_ACTORS_RUNTIME_CONFIG")
        .env_remove("DURABLE_ACTORS_HOST_TOKEN")
        .env_remove("OTEL_EXPORTER_OTLP_ENDPOINT")
        .env_remove("GOOGLE_APPLICATION_CREDENTIALS")
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
    command
}

#[cfg(test)]
#[path = "../../tests/unit/host/process.rs"]
mod tests;
