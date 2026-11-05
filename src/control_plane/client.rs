use std::{
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use tonic::{
    Request,
    metadata::MetadataValue,
    transport::{Channel, ClientTlsConfig, Endpoint},
};

use crate::{
    actor::ActorKey,
    grpc::proto::actor_control_plane_service_client::ActorControlPlaneServiceClient,
};

use super::{
    CONTROL_PLANE_REQUEST_TIMEOUT, MAX_CONTROL_PLANE_MESSAGE_BYTES,
    protocol::{ControlPlaneCommand, ControlPlaneCommandReply, decode_reply, encode_command},
};

const CONTROL_PLANE_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct ControlPlaneClient {
    client: ActorControlPlaneServiceClient<Channel>,
    authorization: Arc<RwLock<MetadataValue<tonic::metadata::Ascii>>>,
}

impl ControlPlaneClient {
    pub(crate) async fn report_lifecycle(
        &self,
        report: crate::actor_lifecycle::LifecycleReport,
    ) -> Result<()> {
        self.execute(ControlPlaneCommand::Lifecycle { report })
            .await?;
        Ok(())
    }

    pub(crate) async fn socket_operation(
        &self,
        actor: &ActorKey,
        operation: super::socket_gateway::SocketOperation,
    ) -> Result<super::socket_gateway::SocketOperationReply> {
        match self
            .execute(ControlPlaneCommand::SocketOperation {
                actor: actor.clone(),
                operation,
            })
            .await?
        {
            ControlPlaneCommandReply::SocketOperation { reply } => Ok(reply),
            _ => anyhow::bail!("unexpected socket operation reply"),
        }
    }

    pub(crate) async fn get_alarm(&self) -> Result<Option<i64>> {
        match self.execute(ControlPlaneCommand::GetAlarm).await? {
            ControlPlaneCommandReply::Alarm { timestamp_ms } => Ok(timestamp_ms),
            _ => anyhow::bail!("unexpected alarm response"),
        }
    }

    pub(crate) async fn update_alarm(&self, timestamp_ms: Option<i64>) -> Result<()> {
        match self
            .execute(ControlPlaneCommand::UpdateAlarm { timestamp_ms })
            .await?
        {
            ControlPlaneCommandReply::Unit => Ok(()),
            _ => anyhow::bail!("unexpected alarm update response"),
        }
    }

    async fn invoke_actor(&self, call: crate::actor::ActorCall) -> Result<serde_json::Value> {
        match self
            .execute(ControlPlaneCommand::InvokeActor { call })
            .await?
        {
            ControlPlaneCommandReply::Invocation { outcome } => Ok(outcome),
            _ => anyhow::bail!("unexpected actor invocation reply"),
        }
    }

    pub(crate) fn authorization(&self) -> HostAuthorization {
        HostAuthorization(self.authorization.clone())
    }

    pub(crate) fn token_expires_at_ms(&self) -> Result<u64> {
        use base64::Engine;
        let authorization = self
            .authorization
            .read()
            .map_err(|_| anyhow::anyhow!("authorization lock poisoned"))?;
        let payload = authorization
            .to_str()?
            .split('.')
            .nth(1)
            .context("host JWT has no payload")?;
        let claims: serde_json::Value = serde_json::from_slice(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(payload)?,
        )?;
        claims["exp"]
            .as_u64()
            .and_then(|value| value.checked_mul(1000))
            .context("host JWT has no expiration")
    }

    pub(crate) async fn refresh_storage_access(
        &self,
    ) -> Result<Option<crate::bucket::access::StorageTokens>> {
        match self
            .execute(ControlPlaneCommand::RefreshStorageAccess)
            .await?
        {
            ControlPlaneCommandReply::StorageAccess {
                tokens,
                replacement_token,
            } => {
                self.replace_token(&replacement_token)?;
                Ok(tokens)
            }
            _ => anyhow::bail!("unexpected storage credentials response"),
        }
    }

    pub async fn connect(endpoint: impl Into<String>, token: impl AsRef<str>) -> Result<Self> {
        let channel = control_plane_endpoint(&endpoint.into())?.connect_lazy();
        Self::from_channel(channel, token.as_ref())
    }

    pub(crate) async fn prewarm(endpoint: &str) -> Result<Self> {
        let channel = control_plane_endpoint(endpoint)?
            .http2_keep_alive_interval(Duration::from_secs(30))
            .keep_alive_while_idle(true)
            .connect()
            .await
            .context("prewarm actor control-plane connection")?;
        Self::from_channel(channel, "unassigned-spare")
    }

    pub(crate) fn with_host_token(self, token: &str) -> Result<Self> {
        self.replace_token(token)?;
        Ok(self)
    }

    fn from_channel(channel: Channel, token: &str) -> Result<Self> {
        Ok(Self {
            client: ActorControlPlaneServiceClient::new(channel)
                .max_decoding_message_size(MAX_CONTROL_PLANE_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_CONTROL_PLANE_MESSAGE_BYTES),
            authorization: Arc::new(RwLock::new(bearer_authorization(token)?)),
        })
    }
}

#[derive(Clone)]
pub(crate) struct HostAuthorization(Arc<RwLock<MetadataValue<tonic::metadata::Ascii>>>);

impl tonic::service::Interceptor for HostAuthorization {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, tonic::Status> {
        let authorization = self
            .0
            .read()
            .map_err(|_| tonic::Status::internal("actor authorization lock poisoned"))?
            .clone();
        request
            .metadata_mut()
            .insert("authorization", authorization);
        Ok(request)
    }
}

#[derive(Default)]
pub(crate) struct LeaseFence {
    deadline: Option<Instant>,
    pub(crate) fenced: bool,
}

impl LeaseFence {
    pub(crate) fn begin(&mut self, now: Instant) -> Result<()> {
        if self.deadline.is_some() {
            self.check(now)?;
        }
        ensure!(!self.fenced, "host session is permanently fenced");
        Ok(())
    }

    pub(crate) fn confirm(
        &mut self,
        started: Instant,
        duration: Duration,
        now: Instant,
    ) -> Result<()> {
        self.begin(now)?;
        let window = duration.saturating_sub(Duration::from_secs(5));
        let deadline = started + window;
        if deadline <= now {
            self.fenced = true;
        }
        ensure!(
            !self.fenced,
            "host lease response arrived after the safe lease window"
        );
        self.deadline = Some(deadline);
        Ok(())
    }

    pub(crate) fn check(&mut self, now: Instant) -> Result<()> {
        let deadline = self.deadline.context("host lease is not confirmed yet")?;
        if deadline <= now {
            self.fenced = true;
        }
        ensure!(!self.fenced, "host session has no confirmed live lease");
        Ok(())
    }
}

impl ControlPlaneClient {
    async fn execute(&self, command: ControlPlaneCommand) -> Result<ControlPlaneCommandReply> {
        let mut request = Request::new(encode_command(command)?);
        request.set_timeout(CONTROL_PLANE_REQUEST_TIMEOUT);
        request.metadata_mut().insert(
            "authorization",
            self.authorization
                .read()
                .map_err(|_| anyhow::anyhow!("actor authorization lock poisoned"))?
                .clone(),
        );
        let reply = self
            .client
            .clone()
            .execute(request)
            .await
            .context("execute actor control-plane command")?
            .into_inner();
        decode_reply(reply).context("decode actor control-plane reply")
    }

    fn replace_token(&self, token: &str) -> Result<()> {
        *self
            .authorization
            .write()
            .map_err(|_| anyhow::anyhow!("actor authorization lock poisoned"))? =
            bearer_authorization(token)?;
        Ok(())
    }
}

fn control_plane_endpoint(endpoint: &str) -> Result<Endpoint> {
    let mut channel = Endpoint::new(endpoint.to_owned())
        .context("parse actor control-plane endpoint")?
        .connect_timeout(CONTROL_PLANE_CONNECT_TIMEOUT)
        .timeout(CONTROL_PLANE_REQUEST_TIMEOUT);
    if endpoint.starts_with("https:") {
        channel = channel.tls_config(ClientTlsConfig::new().with_webpki_roots())?;
    }
    Ok(channel)
}

fn bearer_authorization(token: &str) -> Result<MetadataValue<tonic::metadata::Ascii>> {
    ensure!(
        !token.is_empty() && token.trim() == token,
        "actor token is invalid"
    );
    format!("Bearer {token}")
        .parse()
        .context("actor token is not valid gRPC metadata")
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/client_lease_fence_tests.rs"]
mod lease_fence_tests;
#[cfg(test)]
#[path = "../../tests/unit/control_plane/client_refresh_tests.rs"]
mod refresh_tests;

#[async_trait::async_trait]
impl crate::actor::ActorCaller for ControlPlaneClient {
    async fn call(&self, call: crate::actor::ActorCall) -> serde_json::Value {
        self.invoke_actor(call).await.unwrap_or_else(|error| {
            serde_json::json!({"type": "failed", "code": "outcome_unknown", "message": format!("{error:#}")})
        })
    }
}

#[async_trait::async_trait]
impl crate::sockets::operations::SocketOperations for ControlPlaneClient {
    async fn execute(
        &self,
        actor: &ActorKey,
        operation: crate::sockets::operations::SocketOperation,
    ) -> Result<crate::sockets::operations::SocketOperationReply> {
        self.socket_operation(actor, operation).await
    }
}
