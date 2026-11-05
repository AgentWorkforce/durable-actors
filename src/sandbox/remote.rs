use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;

use super::*;
use super::{connections::HostConnections, secrets::SecretResolver};

pub(crate) struct RemoteSandboxProvider {
    name: &'static str,
    cluster: Arc<dyn SandboxCluster>,
    secrets: Arc<dyn SecretResolver>,
    assignment: Arc<dyn HostAssignment>,
}

impl RemoteSandboxProvider {
    pub(super) fn new(
        name: &'static str,
        cluster: Arc<dyn SandboxCluster>,
        secrets: Arc<dyn SecretResolver>,
        connections: HostConnections,
    ) -> Result<Self> {
        Ok(Self {
            name,
            cluster,
            secrets,
            assignment: Arc::new(HttpAssignment(connections)),
        })
    }

    async fn assign(
        &self,
        request: &EnsureHostRequest,
        spare: &SpareHandle,
    ) -> Result<ActorHostHandle> {
        ensure!(
            spare.canonical_region == request.canonical_region,
            "spare region mismatch"
        );
        let secrets = self.secrets.resolve(&request.secret_refs).await?;
        let environment = assignment_environment(request, spare, secrets)?;
        let handle = self
            .assignment
            .assign(spare, environment, request.initial_operation.clone())
            .await?;
        let validated = (|| -> Result<()> {
            let lease = handle
                .lease
                .as_ref()
                .context("assigned host did not acquire ownership")?;
            ensure!(
                handle.host_id == request.host_id
                    && lease.id == request.host_id
                    && lease.session_id == request.session_id
                    && lease.expires_at_ms > now_ms()?
                    && handle.owner_epoch > 0
                    && handle.route == spare.route
                    && lease.route == spare.route
                    && handle.canonical_region == request.canonical_region,
                "assigned host identity mismatch"
            );
            Ok(())
        })();
        validated.map_err(|error| {
            if handle.initial_reply.is_some() {
                error.context(assignment::AssignmentOutcomeUnknown)
            } else {
                error
            }
        })?;
        Ok(handle)
    }
}

#[async_trait]
impl SandboxProvider for RemoteSandboxProvider {
    async fn prewarm_spares(&self, spares: &[SpareHandle]) {
        self.assignment.prewarm(spares).await;
    }

    async fn create_spare(&self, request: &CreateSpareRequest) -> Result<SpareHandle> {
        validate_image(&request.image_ref)?;
        self.cluster.create_spare(request).await
    }

    async fn retire_spare(&self, request: &SpareHandle) -> Result<()> {
        self.cluster.retire_spare(request).await
    }

    async fn stopped_spares(&self, spares: &[SpareHandle]) -> Result<Vec<String>> {
        self.cluster.stopped_spares(spares).await
    }

    async fn reap_finished(&self) -> Result<()> {
        self.cluster.reap_finished().await
    }

    async fn ensure_host(&self, request: &EnsureHostRequest) -> Result<ActorHostHandle> {
        let started_at_ms = now_ms()?;
        let reused = request.spare.is_some();
        let spare = match &request.spare {
            Some(spare) => spare.clone(),
            None => {
                self.create_spare(&CreateSpareRequest {
                    control_plane_url: Some(request.control_plane_url.clone()),
                    otlp_endpoint: request.otlp_endpoint.clone(),
                    kind: SpareKind::Actor,
                    name: format!("do-actor-{}", request.session_id),
                    image_ref: request.image_ref.clone(),
                    canonical_region: request.canonical_region.clone(),
                    resources: request.resources.clone(),
                })
                .await?
            }
        };
        let result = self.assign(request, &spare).await;
        match result {
            Ok(mut handle) => {
                let completed_at_ms = now_ms()?;
                handle.provisioning = Some(ActorHostProvisioning {
                    provider: self.name.into(),
                    resource_id: spare.resource_id,
                    reused,
                    started_at_ms,
                    completed_at_ms,
                    input_parsed_at_ms: None,
                    sdk_loaded_at_ms: None,
                    resources_resolved_at_ms: None,
                    sandbox_scheduled_at_ms: None,
                    host_ready_observed_at_ms: Some(completed_at_ms),
                    route_read_at_ms: None,
                    command_spawned_at_ms: None,
                    request_written_at_ms: None,
                    process_completed_at_ms: None,
                    response_decoded_at_ms: None,
                });
                Ok(handle)
            }
            Err(error) => {
                if let Err(cleanup) = self.retire_spare(&spare).await {
                    tracing::warn!(%cleanup, resource = %spare.resource_id, "failed to retire assigned sandbox");
                }
                Err(error)
            }
        }
    }

    async fn terminate_hosts(&self, _: &TerminateHostsRequest) -> Result<HostTermination> {
        anyhow::bail!("remote hosts must be retired through the durable spare registry")
    }
}

fn assignment_environment(
    request: &EnsureHostRequest,
    spare: &SpareHandle,
    secrets: HashMap<String, String>,
) -> Result<HashMap<String, String>> {
    let artifact = request
        .code_snapshot
        .as_deref()
        .context("compiled GCS code artifact required")?;
    let manifest = crate::bundle::ActorBundle::decode(artifact)?;
    let actor = request.actor.as_ref().context("actor identity required")?;
    actor.validate()?;
    let mut environment = HashMap::from([
        ("DURABLE_ACTORS_PROCESS_ROLE".into(), "host".into()),
        (
            "DURABLE_ACTORS_HOST_RESOURCES".into(),
            serde_json::to_string(&request.resources)?,
        ),
        (
            "DURABLE_ACTORS_HOST_TOKEN".into(),
            request.host_token.clone(),
        ),
        (
            "DURABLE_ACTORS_JWT_PUBLIC_KEYS".into(),
            request.jwt_public_keys.clone(),
        ),
        (
            "DURABLE_ACTORS_CONTROL_PLANE_URL".into(),
            request.control_plane_url.clone(),
        ),
        (
            "DURABLE_ACTORS_JWT_ISSUER".into(),
            request.jwt_issuer.clone(),
        ),
        (
            "DURABLE_ACTORS_INVOKE_JWT_AUDIENCE".into(),
            request.invocation_jwt_audience.clone(),
        ),
        ("DURABLE_ACTORS_HOST_ID".into(), request.host_id.to_string()),
        (
            "DURABLE_ACTORS_SESSION_ID".into(),
            request.session_id.clone(),
        ),
        (
            "DURABLE_ACTORS_REGION".into(),
            request.canonical_region.clone(),
        ),
        ("DURABLE_ACTORS_HOST_ROUTE".into(), spare.route.clone()),
        ("DURABLE_ACTORS_HOST_BIND".into(), "0.0.0.0:7101".into()),
        (
            "DURABLE_ACTORS_HOST_IDLE_TIMEOUT_MS".into(),
            request.host_idle_timeout_ms.to_string(),
        ),
        (
            "DURABLE_ACTORS_EXECUTOR_SOCKET".into(),
            "/tmp/durable-actors-executor.sock".into(),
        ),
        (
            "DURABLE_ACTORS_ENTRYPOINT".into(),
            format!("/customer/{}", manifest.entrypoint()?),
        ),
        ("DURABLE_ACTORS_CODE_ARTIFACT".into(), artifact.into()),
        (
            "DURABLE_ACTORS_CUSTOMER_ENV".into(),
            serde_json::to_string(&secrets)?,
        ),
        ("DURABLE_ACTORS_ACTOR".into(), serde_json::to_string(actor)?),
        (
            "DURABLE_ACTORS_ACTOR_IS_NEW".into(),
            request.actor_is_new.to_string(),
        ),
        (
            "DURABLE_ACTORS_RUNTIME_CONFIG".into(),
            request
                .runtime_config
                .clone()
                .context("host storage configuration required")?,
        ),
    ]);
    if let Some(hint) = &request.owner_hint {
        environment.insert("DURABLE_ACTORS_OWNER_HINT".into(), hint.clone());
    }
    Ok(environment)
}

pub(crate) fn validate_image(image: &str) -> Result<()> {
    let (name, digest) = image
        .rsplit_once("@sha256:")
        .context("container image must be pinned to a sha256 digest")?;
    ensure!(
        !name.is_empty()
            && !name.chars().any(char::is_whitespace)
            && digest.len() == 64
            && digest.bytes().all(|b| b.is_ascii_hexdigit()),
        "invalid container image digest"
    );
    Ok(())
}

fn now_ms() -> Result<u64> {
    crate::clock::Clock::now_ms(&crate::clock::SystemClock)
}

#[async_trait]
pub(super) trait SandboxCluster: Send + Sync {
    async fn stopped_spares(&self, spares: &[SpareHandle]) -> Result<Vec<String>>;
    async fn reap_finished(&self) -> Result<()> {
        Ok(())
    }
    async fn create_spare(&self, request: &CreateSpareRequest) -> Result<SpareHandle>;
    async fn retire_spare(&self, spare: &SpareHandle) -> Result<()>;
}

#[async_trait]
trait HostAssignment: Send + Sync {
    async fn prewarm(&self, _spares: &[SpareHandle]) {}

    async fn assign(
        &self,
        spare: &SpareHandle,
        environment: HashMap<String, String>,
        operation: Option<assignment::InitialOperation>,
    ) -> Result<ActorHostHandle>;
}

struct HttpAssignment(HostConnections);
#[async_trait]
impl HostAssignment for HttpAssignment {
    async fn prewarm(&self, spares: &[SpareHandle]) {
        use futures_util::StreamExt;

        futures_util::stream::iter(spares)
            .for_each_concurrent(16, |spare| self.0.prewarm(spare))
            .await;
    }

    async fn assign(
        &self,
        spare: &SpareHandle,
        environment: HashMap<String, String>,
        operation: Option<assignment::InitialOperation>,
    ) -> Result<ActorHostHandle> {
        let request = self
            .0
            .client
            .post(format!(
                "{}/assign",
                spare.control_route.trim_end_matches('/')
            ))
            .bearer_auth(&spare.control_token)
            .timeout(Duration::from_secs(120));
        let request = match &operation {
            Some(operation) => request.json(&assignment::AssignmentRequest {
                environment,
                operation: operation.clone(),
            }),
            None => request.json(&environment),
        };
        let response = request.send().await.map_err(|error| {
            if operation.is_some() && !error.is_connect() {
                anyhow::Error::new(error).context(assignment::AssignmentOutcomeUnknown)
            } else {
                error.into()
            }
        })?;
        let status = response.status();
        ensure!(
            !assignment::declined_before_execution(status),
            "spare declined the assignment before running it: {status}"
        );
        let result = async {
            let handle: ActorHostHandle = response
                .error_for_status()?
                .json()
                .await
                .context("decode host assignment")?;
            ensure!(
                operation.is_none() || handle.initial_reply.is_some(),
                "assignment omitted operation result"
            );
            Ok(handle)
        }
        .await;
        result.map_err(|error: anyhow::Error| {
            if operation.is_some() {
                error.context(assignment::AssignmentOutcomeUnknown)
            } else {
                error
            }
        })
    }
}

#[cfg(test)]
#[path = "../../tests/unit/sandbox/remote.rs"]
mod tests;
