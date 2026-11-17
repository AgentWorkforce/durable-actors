use crate::request_tracking::HostState;
use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, State, rejection::JsonRejection},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::{
    actor::{ActorArgs, ActorExecutionResult, ActorInvocation, ActorKey, ActorSocketPublisher},
    control_plane::{ActorJwtVerifier, ActorPrincipal, OUTCOME_HEADER},
    host::{ActorHost, HostId, sockets::HostSockets},
    payload::{JsonBody, JsonPayload, JsonText},
};

pub(crate) struct ActorHostHttpService {
    host: Arc<ActorHost>,
    session_id: String,
    auth: ActorJwtVerifier,
    sockets: Arc<HostSockets>,
}

impl ActorHostHttpService {
    pub(crate) fn new(
        host: Arc<ActorHost>,
        session_id: String,
        auth: ActorJwtVerifier,
        sockets: Arc<HostSockets>,
    ) -> Self {
        Self {
            host,
            session_id,
            auth,
            sockets,
        }
    }

    pub(crate) fn router(self) -> Router {
        Router::new()
            .route(
                "/v1/projects/{project_id}/actors/{actor_name}/{actor_id}/socket-events",
                post(socket_event),
            )
            .route(
                "/v1/projects/{project_id}/actors/{actor_name}/{actor_id}/invoke",
                post(invoke),
            )
            .layer(DefaultBodyLimit::disable())
            .with_state(Arc::new(self))
    }

    pub(super) async fn execute_initial(
        &self,
        operation: crate::sandbox::assignment::InitialOperation,
        owner_epoch: u64,
        startup_ms: f64,
    ) -> anyhow::Result<JsonText> {
        use crate::sandbox::assignment::InitialRequest;
        let routing_ms = operation.routing_ms + startup_ms;
        match operation.request {
            InitialRequest::Invoke { invocation, grant } => {
                anyhow::ensure!(
                    invocation.actor == *self.host.actor(),
                    "assignment operation actor mismatch"
                );
                invocation.validate()?;
                let now =
                    i64::try_from(crate::clock::Clock::now_ms(&crate::clock::SystemClock)? / 1000)?;
                if authorize_grant(grant.as_ref(), Some(&invocation.method)).is_err()
                    || grant.as_ref().is_some_and(|grant| grant.expires_at <= now)
                {
                    return InvocationReply::failed(
                        "forbidden",
                        "operation is outside the actor grant",
                    )
                    .with_metadata(None)
                    .body()?
                    .into_text()
                    .await;
                }
                self.execute(invocation, owner_epoch, HostState::Cold, routing_ms)
                    .await
                    .body()?
                    .into_text()
                    .await
            }
            InitialRequest::Socket { invocation } => {
                anyhow::ensure!(
                    invocation.actor == *self.host.actor(),
                    "assignment operation actor mismatch"
                );
                JsonText::encode(
                    &self
                        .execute_socket(invocation, owner_epoch, HostState::Cold, routing_ms)
                        .await,
                )
            }
        }
    }

    async fn execute_socket(
        &self,
        invocation: crate::actor::ActorSocketInvocation,
        owner_epoch: u64,
        host_state: HostState,
        routing_ms: f64,
    ) -> crate::control_plane::socket_gateway::SocketEventReply {
        use crate::control_plane::socket_gateway::SocketEventReply;
        match self
            .host
            .handle_socket_event_since(
                invocation,
                owner_epoch,
                std::time::Instant::now(),
                host_state,
                routing_ms,
            )
            .await
        {
            Ok(ActorExecutionResult::Completed { effects, .. }) => {
                SocketEventReply::Completed { effects }
            }
            Ok(ActorExecutionResult::HostUnavailable) => SocketEventReply::NotExecuted,
            Ok(ActorExecutionResult::Failed { failure }) => SocketEventReply::Failed {
                message: failure.message,
            },
            Err(error) => SocketEventReply::Failed {
                message: format!("{error:#}"),
            },
        }
    }

    fn authenticate(&self, headers: &HeaderMap) -> Result<ActorPrincipal, HttpError> {
        let authorization = headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("");
        self.auth
            .authenticate_authorization(authorization)
            .map_err(|_| {
                HttpError(
                    StatusCode::UNAUTHORIZED,
                    "invalid actor invocation ticket".into(),
                )
            })
    }

    fn authorize(
        &self,
        principal: &ActorPrincipal,
        actor: &ActorKey,
        owner_epoch: u64,
    ) -> Result<(), HttpError> {
        actor.validate().map_err(bad_request)?;
        validate_host_request(
            principal,
            self.host.id(),
            &self.session_id,
            actor,
            owner_epoch,
        )?;
        if actor != self.host.actor() {
            return Err(HttpError(
                StatusCode::FORBIDDEN,
                "actor host serves another actor".into(),
            ));
        }
        Ok(())
    }

    async fn execute(
        &self,
        invocation: ActorInvocation,
        owner_epoch: u64,
        host_state: HostState,
        routing_ms: f64,
    ) -> InvocationResponse {
        let request_id = invocation.request_id.clone();
        let (result, metadata) = self
            .host
            .invoke_actor(invocation, owner_epoch, host_state, routing_ms)
            .await;
        let outcome = match result {
            Ok(ActorExecutionResult::Completed { result, effects }) => {
                if !effects.is_empty()
                    && let Err(error) = self.sockets.publish(effects).await
                {
                    warn!(request_id, error = %format!("{error:#}"), "actor completed but socket delivery failed");
                    return InvocationReply::failed(
                        "outcome_unknown",
                        "actor completed but socket effects could not be delivered",
                    )
                    .with_metadata(metadata);
                }
                InvocationReply::Completed { result }
            }
            Ok(ActorExecutionResult::Failed { failure }) => InvocationReply::Failed {
                code: failure.code,
                message: failure.message,
            },
            Ok(ActorExecutionResult::HostUnavailable) => InvocationReply::NotExecuted {
                reason: RejectionReason::HostUnavailable,
            },
            Err(error) => {
                warn!(request_id, error = %format!("{error:#}"), "actor invocation failed before execution");
                InvocationReply::failed(
                    "unavailable",
                    "actor could not start because its state was unavailable",
                )
            }
        };
        outcome.with_metadata(metadata)
    }
}

async fn socket_event(
    State(service): State<Arc<ActorHostHttpService>>,
    Path(actor): Path<ActorKey>,
    headers: HeaderMap,
    Json(request): Json<crate::control_plane::socket_gateway::SocketEventRequest>,
) -> Result<Json<crate::control_plane::socket_gateway::SocketEventReply>, HttpError> {
    let principal = service.authenticate(&headers)?;
    service.authorize(&principal, &actor, request.owner_epoch)?;
    authorize_grant(
        principal
            .invocation
            .as_ref()
            .and_then(|capability| capability.grant.as_ref()),
        None,
    )?;
    if request.invocation.actor != actor {
        return Err(HttpError(
            StatusCode::FORBIDDEN,
            "socket event actor mismatch".into(),
        ));
    }
    let reply = service
        .execute_socket(
            request.invocation,
            request.owner_epoch,
            request.host_state,
            request.routing_ms,
        )
        .await;
    Ok(Json(reply))
}

async fn invoke(
    State(service): State<Arc<ActorHostHttpService>>,
    Path(actor): Path<ActorKey>,
    headers: HeaderMap,
    body: Result<Json<InvokeRequest>, JsonRejection>,
) -> Result<InvocationResponse, HttpError> {
    let principal = service.authenticate(&headers)?;
    let Json(request) = body.map_err(json_error)?;
    service.authorize(&principal, &actor, request.owner_epoch)?;
    let grant = principal
        .invocation
        .as_ref()
        .and_then(|capability| capability.grant.as_ref());
    if let Err(error) = authorize_grant(grant, Some(&request.method)) {
        return Ok(InvocationReply::failed("forbidden", &error.1).with_metadata(None));
    }
    if let Some(grant) = grant {
        info!(event = "delegated_actor_invocation", subject = %grant.subject, grant_id = %grant.grant_id, project_id = %actor.project_id, actor_name = %actor.actor_name, actor_id = %actor.actor_id, request_id = %request.request_id, method = %request.method);
    }
    let invocation = ActorInvocation {
        actor,
        request_id: request.request_id,
        method: request.method,
        args: request.args,
    };
    invocation.validate().map_err(bad_request)?;
    Ok(service
        .execute(
            invocation,
            request.owner_epoch,
            request.host_state,
            request.routing_ms,
        )
        .await)
}

fn authorize_grant(
    grant: Option<&crate::control_plane::session::InvocationGrant>,
    method: Option<&str>,
) -> Result<(), HttpError> {
    if let Some(grant) = grant {
        let allowed = method.is_some_and(|method| {
            !matches!(
                method,
                "onConnect" | "onMessage" | "onDisconnect" | "connect" | "broadcast" | "then"
            ) && grant.methods.iter().any(|allowed| allowed == method)
        });
        if !allowed {
            return Err(HttpError(
                StatusCode::FORBIDDEN,
                "operation is outside the actor grant".into(),
            ));
        }
    }
    Ok(())
}

fn validate_host_request(
    principal: &ActorPrincipal,
    host_id: &HostId,
    session_id: &str,
    actor: &ActorKey,
    owner_epoch: u64,
) -> Result<(), HttpError> {
    if owner_epoch == 0 || owner_epoch > 9_007_199_254_740_991 {
        return Err(HttpError(
            StatusCode::BAD_REQUEST,
            "actor ownership capability is incomplete".into(),
        ));
    }
    if principal.host_id != *host_id
        || principal.session_id != session_id
        || principal.actor != *actor
    {
        return Err(HttpError(
            StatusCode::FORBIDDEN,
            "actor credential belongs to another actor or host session".into(),
        ));
    }
    if let Some(capability) = &principal.invocation
        && (capability.actor != *actor
            || capability.host_id != *host_id
            || capability.owner_epoch != owner_epoch)
    {
        return Err(HttpError(
            StatusCode::FORBIDDEN,
            "actor invocation does not match its direct capability".into(),
        ));
    }
    Ok(())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct InvokeRequest {
    #[serde(default)]
    host_state: HostState,
    routing_ms: f64,
    request_id: String,
    owner_epoch: u64,
    method: String,
    #[serde(deserialize_with = "ActorArgs::deserialize_text")]
    args: ActorArgs,
}

#[derive(Serialize)]
struct InvocationResponse {
    #[serde(flatten)]
    outcome: InvocationReply,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<crate::request_tracking::RequestMetadata>,
}

impl InvocationResponse {
    /// The reply's JSON, which shares a result's text, or passes it on as it arrives, rather than
    /// copying it.
    fn body(mut self) -> anyhow::Result<JsonBody> {
        match &mut self.outcome {
            InvocationReply::Completed { result } => {
                let result = std::mem::take(result);
                JsonBody::with_field(&self, "result", result)
            }
            _ => JsonBody::encode(&self),
        }
    }
}

impl IntoResponse for InvocationResponse {
    fn into_response(self) -> Response {
        let completed = matches!(self.outcome, InvocationReply::Completed { .. });
        match self.body() {
            Ok(body) if completed => ([(OUTCOME_HEADER, "completed")], body).into_response(),
            Ok(body) => body.into_response(),
            Err(error) => {
                HttpError(StatusCode::INTERNAL_SERVER_ERROR, format!("{error:#}")).into_response()
            }
        }
    }
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum InvocationReply {
    Completed {
        /// Written by `InvocationResponse::body`, after the rest of the reply.
        #[serde(skip)]
        result: JsonPayload,
    },
    Failed {
        code: String,
        message: String,
    },
    /// The invocation did not execute and cannot execute later from a queue.
    NotExecuted {
        reason: RejectionReason,
    },
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum RejectionReason {
    HostUnavailable,
}

impl InvocationReply {
    fn with_metadata(
        self,
        metadata: Option<crate::request_tracking::RequestMetadata>,
    ) -> InvocationResponse {
        InvocationResponse {
            outcome: self,
            metadata,
        }
    }

    fn failed(code: &str, message: &str) -> Self {
        Self::Failed {
            code: code.into(),
            message: message.into(),
        }
    }
}

#[derive(Debug)]
struct HttpError(StatusCode, String);
impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        (
            self.0,
            Json(serde_json::json!({"error": {"code": self.0.as_str(), "message": self.1}})),
        )
            .into_response()
    }
}
fn bad_request(error: anyhow::Error) -> HttpError {
    HttpError(StatusCode::BAD_REQUEST, error.to_string())
}
fn json_error(error: JsonRejection) -> HttpError {
    HttpError(error.status(), error.body_text())
}

#[cfg(test)]
#[path = "../../tests/unit/host/http.rs"]
mod tests;
