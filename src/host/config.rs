use std::{collections::HashMap, env, net::SocketAddr, path::PathBuf, time::Duration};

use anyhow::{Context, Result, ensure};

use super::HostId;
use crate::{
    actor::ActorKey,
    bucket::{OwnershipHint, access::HostStorageConfig},
    bundle::{ActorBundle, ActorRuntime},
    host_leases::max_host_lease_duration_ms,
};

const DEFAULT_HOST_IDLE_TIMEOUT_MS: u64 = 10_000;
const MAX_IDLE_TIMEOUT_MS: u64 = 86_400_000;

pub(super) struct ActorHostConfig {
    pub identity: HostIdentity,
    pub control_plane: ControlPlaneAccess,
    pub invocation_auth: InvocationAuth,
    pub network: HostNetwork,
    pub lease: LeaseTiming,
    pub idle_timeout: Duration,
    pub resources: crate::sandbox::ResourceLimits,
    pub storage: HostStorageConfig,
    pub assignment: ActorAssignment,
    pub executor: ExecutorConfig,
    pub ready_file: Option<PathBuf>,
}

impl ActorHostConfig {
    pub(super) fn from_env() -> Result<Self> {
        Self::from_lookup(|name| env::var(name).ok())
    }

    pub(super) fn from_lookup(mut get: impl FnMut(&str) -> Option<String>) -> Result<Self> {
        Ok(Self {
            identity: HostIdentity::from_lookup(&mut get)?,
            control_plane: ControlPlaneAccess::from_lookup(&mut get)?,
            invocation_auth: InvocationAuth::from_lookup(&mut get)?,
            network: HostNetwork::from_lookup(&mut get)?,
            lease: LeaseTiming::from_lookup(&mut get)?,
            idle_timeout: Duration::from_millis(host_idle_timeout_ms(&mut get)?),
            storage: serde_json::from_str(
                &get("DURABLE_ACTORS_RUNTIME_CONFIG")
                    .context("host storage configuration missing")?,
            )
            .context("parse host runtime configuration")?,
            assignment: ActorAssignment::from_lookup(&mut get)?,
            executor: ExecutorConfig::from_lookup(&mut get)?,
            resources: get("DURABLE_ACTORS_HOST_RESOURCES")
                .map(|value| serde_json::from_str(&value))
                .transpose()
                .context("parse host resource requests")?
                .unwrap_or_default(),
            ready_file: get("DURABLE_ACTORS_HOST_READY_FILE").map(PathBuf::from),
        })
    }
}

pub(super) struct HostIdentity {
    pub host_id: HostId,
    pub session_id: String,
}

impl HostIdentity {
    fn from_lookup(get: &mut impl FnMut(&str) -> Option<String>) -> Result<Self> {
        let host_id = HostId::new(required(get, "DURABLE_ACTORS_HOST_ID")?);
        ensure!(
            host_id.as_str().starts_with("host.v3."),
            "DURABLE_ACTORS_HOST_ID is invalid"
        );
        let session_id = required(get, "DURABLE_ACTORS_SESSION_ID")?;
        uuid::Uuid::parse_str(&session_id).context("DURABLE_ACTORS_SESSION_ID must be a UUID")?;
        Ok(Self {
            host_id,
            session_id,
        })
    }
}

pub(super) struct ControlPlaneAccess {
    pub url: String,
    pub host_token: String,
}

impl ControlPlaneAccess {
    fn from_lookup(get: &mut impl FnMut(&str) -> Option<String>) -> Result<Self> {
        Ok(Self {
            url: required(get, "DURABLE_ACTORS_CONTROL_PLANE_URL")?,
            host_token: required(get, "DURABLE_ACTORS_HOST_TOKEN")?,
        })
    }
}

pub(super) struct InvocationAuth {
    pub public_keys: String,
    pub issuer: String,
    pub audience: String,
    pub max_token_lifetime: Duration,
}

impl InvocationAuth {
    fn from_lookup(get: &mut impl FnMut(&str) -> Option<String>) -> Result<Self> {
        Ok(Self {
            public_keys: required(get, "DURABLE_ACTORS_JWT_PUBLIC_KEYS")?,
            issuer: get("DURABLE_ACTORS_JWT_ISSUER")
                .unwrap_or_else(|| "durable-actors-control-plane".into()),
            audience: get("DURABLE_ACTORS_INVOKE_JWT_AUDIENCE")
                .unwrap_or_else(|| "durable-actors-invoke".into()),
            max_token_lifetime: duration_seconds(
                get,
                "DURABLE_ACTORS_JWT_MAX_TTL_SECONDS",
                86_400,
            )?,
        })
    }
}

pub(super) struct HostNetwork {
    pub bind: SocketAddr,
    pub route: Option<String>,
}

impl HostNetwork {
    fn from_lookup(get: &mut impl FnMut(&str) -> Option<String>) -> Result<Self> {
        let route = get("DURABLE_ACTORS_HOST_ROUTE");
        if let Some(route) = &route {
            tonic::transport::Endpoint::from_shared(route.clone())
                .context("DURABLE_ACTORS_HOST_ROUTE must be a valid HTTP or HTTPS URI")?;
        }
        let default_bind = if route.is_some() {
            "0.0.0.0:7101"
        } else {
            "127.0.0.1:0"
        };
        let bind = get("DURABLE_ACTORS_HOST_BIND")
            .unwrap_or_else(|| default_bind.into())
            .parse()
            .context("DURABLE_ACTORS_HOST_BIND must be a socket address")?;
        Ok(Self { bind, route })
    }
}

pub(super) struct LeaseTiming {
    pub duration: Duration,
    pub renew_every: Duration,
}

impl LeaseTiming {
    fn from_lookup(get: &mut impl FnMut(&str) -> Option<String>) -> Result<Self> {
        let duration = duration_ms(get, "DURABLE_ACTORS_LEASE_MS", 30_000)?;
        let renew_every = duration_ms(get, "DURABLE_ACTORS_RENEW_MS", 10_000)?;
        ensure!(
            duration.as_millis() <= u128::from(max_host_lease_duration_ms()?),
            "DURABLE_ACTORS_LEASE_MS is too large"
        );
        ensure!(
            renew_every < duration,
            "DURABLE_ACTORS_RENEW_MS must be shorter than DURABLE_ACTORS_LEASE_MS"
        );
        Ok(Self {
            duration,
            renew_every,
        })
    }
}

pub(super) struct ActorAssignment {
    pub actor: ActorKey,
    pub new_actor: bool,
    pub owner_hint: Option<OwnershipHint>,
}

impl ActorAssignment {
    fn from_lookup(get: &mut impl FnMut(&str) -> Option<String>) -> Result<Self> {
        let actor: ActorKey = serde_json::from_str(&required(get, "DURABLE_ACTORS_ACTOR")?)?;
        actor.validate()?;
        Ok(Self {
            actor,
            new_actor: get("DURABLE_ACTORS_ACTOR_IS_NEW")
                .map(|value| value.parse())
                .transpose()?
                .unwrap_or(false),
            owner_hint: get("DURABLE_ACTORS_OWNER_HINT")
                .map(|value| serde_json::from_str(&value))
                .transpose()?,
        })
    }
}

pub(super) struct ExecutorConfig {
    pub socket: PathBuf,
    pub runtime: ActorRuntime,
    pub entrypoint: Option<String>,
    pub bundle: Option<ActorBundle>,
    pub environment: HashMap<String, String>,
}

impl ExecutorConfig {
    fn from_lookup(get: &mut impl FnMut(&str) -> Option<String>) -> Result<Self> {
        let entrypoint = get("DURABLE_ACTORS_ENTRYPOINT");
        let runtime = executor_runtime(
            entrypoint.as_deref(),
            get("DURABLE_ACTORS_EXECUTOR_RUNTIME").as_deref(),
        )?;
        Ok(Self {
            socket: get("DURABLE_ACTORS_EXECUTOR_SOCKET")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("/tmp/durable-actors-executor.sock")),
            runtime,
            entrypoint,
            bundle: get("DURABLE_ACTORS_CODE_ARTIFACT")
                .map(|value| ActorBundle::decode(&value))
                .transpose()?,
            environment: get("DURABLE_ACTORS_CUSTOMER_ENV")
                .map(|value| serde_json::from_str(&value))
                .transpose()?
                .unwrap_or_default(),
        })
    }
}

pub(super) fn executor_runtime(
    entrypoint: Option<&str>,
    configured: Option<&str>,
) -> Result<ActorRuntime> {
    let image_runtime = configured
        .map(|value| match value {
            "typescript" => Ok(ActorRuntime::Typescript),
            "python" => Ok(ActorRuntime::Python),
            _ => anyhow::bail!("DURABLE_ACTORS_EXECUTOR_RUNTIME must be typescript or python"),
        })
        .transpose()?;
    let artifact_runtime = entrypoint.map(ActorRuntime::from_entrypoint).transpose()?;
    if let (Some(image), Some(artifact)) = (image_runtime, artifact_runtime) {
        ensure!(
            image == artifact,
            "actor artifact does not match the image's executor runtime"
        );
    }
    Ok(image_runtime
        .or(artifact_runtime)
        .unwrap_or(ActorRuntime::Typescript))
}

pub(crate) fn host_idle_timeout_ms(get: &mut impl FnMut(&str) -> Option<String>) -> Result<u64> {
    let timeout = duration_ms(
        get,
        "DURABLE_ACTORS_HOST_IDLE_TIMEOUT_MS",
        DEFAULT_HOST_IDLE_TIMEOUT_MS,
    )?;
    ensure!(
        timeout.as_millis() <= u128::from(MAX_IDLE_TIMEOUT_MS),
        "DURABLE_ACTORS_HOST_IDLE_TIMEOUT_MS is too large"
    );
    Ok(timeout.as_millis() as u64)
}

fn required(get: &mut impl FnMut(&str) -> Option<String>, name: &str) -> Result<String> {
    let value = get(name).with_context(|| format!("{name} is required"))?;
    ensure!(!value.is_empty(), "{name} must not be empty");
    Ok(value)
}

fn duration_ms(
    get: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
    default: u64,
) -> Result<Duration> {
    let value = get(name)
        .map(|value| value.parse::<u64>())
        .transpose()
        .with_context(|| format!("{name} must be an integer number of milliseconds"))?
        .unwrap_or(default);
    ensure!(value > 0, "{name} must be positive");
    Ok(Duration::from_millis(value))
}

fn duration_seconds(
    get: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
    default: u64,
) -> Result<Duration> {
    let value = get(name)
        .map(|value| value.parse::<u64>())
        .transpose()
        .with_context(|| format!("{name} must be an integer number of seconds"))?
        .unwrap_or(default);
    ensure!(value > 0, "{name} must be positive");
    Ok(Duration::from_secs(value))
}

#[cfg(test)]
#[path = "../../tests/unit/host/config.rs"]
mod tests;
