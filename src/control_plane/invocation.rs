use crate::{
    payload::{ARRIVING_BYTES, JsonBody, JsonPayload, JsonStream, JsonText},
    request_tracking::HostState,
    sandbox::assignment::{InitialOperation, InitialRequest},
    sockets::operations::SocketOperation,
};
use anyhow::Context;
use axum::{
    Json,
    extract::{Path, State, rejection::JsonRejection},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use futures_util::StreamExt;
use serde::{Deserialize, Deserializer, de::IgnoredAny};
use serde_json::{Value, json};
use std::future::Future;
use tokio::time::Instant;

use super::public_api::{
    ActorPath, ApiError, FindActorRequest, PublicApiState, resolve_actor_target,
};
use super::service::ActorTarget;
use crate::actor::{ActorArgs, ActorInvocation};

pub(super) async fn invoke_alarm(
    service: &super::ControlPlaneService,
    client: &reqwest::Client,
    alarm: &super::alarms::ClaimedAlarm,
) -> anyhow::Result<()> {
    let started = Instant::now();
    let target = service
        .resolve_actor_route(&alarm.actor, None, None, None, None)
        .await
        .context("could not route alarm")?;
    let invocation = ActorInvocation {
        actor: alarm.actor.clone(),
        request_id: format!("alarm-{}", alarm.revision),
        method: "onAlarm".into(),
        args: ActorArgs::default(),
    };
    let outcome = dispatch(
        service,
        client,
        &target,
        &invocation,
        target.host_state,
        started.elapsed().as_secs_f64() * 1000.0,
    )
    .await
    .map_err(|_| anyhow::anyhow!("alarm outcome could not be confirmed"))?;
    if !outcome.ran {
        let reply = outcome.text.into_text().await?;
        anyhow::bail!("alarm was not executed: {}", reply.get());
    }
    Ok(())
}

pub(super) async fn invoke(
    State(state): State<PublicApiState>,
    Path(path): Path<ActorPath>,
    headers: HeaderMap,
    body: Result<Json<InvokeRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    let actor = path.into_actor();
    let Json(request) = body.map_err(ApiError::json)?;
    let invocation = ActorInvocation {
        actor,
        request_id: request.request_id,
        method: request.method,
        args: request.args,
    };
    invocation.validate().map_err(ApiError::bad_request)?;
    let outcome = resolve_and_dispatch(&state, &headers, request.home_region, &invocation).await?;
    let body =
        JsonBody::with_field(&json!({}), "outcome", outcome.text).map_err(ApiError::internal)?;
    Ok(([(header::CACHE_CONTROL, "no-store")], body).into_response())
}

pub(super) async fn invoke_from_host(
    service: &super::ControlPlaneService,
    invocation: &ActorInvocation,
) -> Value {
    let outcome = match resolve_and_dispatch_from_host(service, invocation).await {
        Ok(outcome) => outcome
            .text
            .into_text()
            .await
            .map_err(|_| outcome_unknown()),
        Err(error) => Err(error),
    };
    outcome
        .and_then(|text| text.decode().map_err(ApiError::internal))
        .unwrap_or_else(ApiError::into_outcome)
}

async fn resolve_and_dispatch_from_host(
    service: &super::ControlPlaneService,
    invocation: &ActorInvocation,
) -> Result<HostReply, ApiError> {
    invocation.validate().map_err(ApiError::bad_request)?;
    let started = Instant::now();
    let deadline = started + super::CONTROL_PLANE_REQUEST_TIMEOUT;
    let resolve = || async {
        let operation = InitialOperation {
            request: InitialRequest::Invoke {
                invocation: invocation.clone(),
                grant: None,
            },
            routing_ms: started.elapsed().as_secs_f64() * 1000.0,
        };
        service
            .resolve_actor_route(&invocation.actor, None, None, None, Some(&operation))
            .await
            .map_err(ApiError::routing)
    };
    settle(
        service,
        &service.host_client(),
        invocation,
        deadline,
        resolve,
    )
    .await
}

async fn resolve_and_dispatch(
    state: &PublicApiState,
    headers: &HeaderMap,
    home_region: Option<String>,
    invocation: &ActorInvocation,
) -> Result<HostReply, ApiError> {
    let deadline = Instant::now() + super::CONTROL_PLANE_REQUEST_TIMEOUT;
    let resolve = || {
        resolve_actor_target(
            state,
            &invocation.actor,
            headers,
            Ok(Json(FindActorRequest {
                home_region: home_region.clone(),
            })),
            Some(invocation),
        )
    };
    settle(
        &state.invocations,
        &state.hosts,
        invocation,
        deadline,
        resolve,
    )
    .await
}

/// Dispatches until the invocation runs or the deadline passes, re-resolving after refusals.
async fn settle<Resolve, Resolving>(
    service: &super::ControlPlaneService,
    hosts: &reqwest::Client,
    invocation: &ActorInvocation,
    deadline: Instant,
    resolve: Resolve,
) -> Result<HostReply, ApiError>
where
    Resolve: Fn() -> Resolving,
    Resolving: Future<Output = Result<ActorTarget, ApiError>>,
{
    let started = Instant::now();
    let mut host_state = HostState::Warm;
    loop {
        let mut target = resolve().await?;
        if target.host_state == HostState::Cold {
            host_state = HostState::Cold;
        }
        let outcome = match target.initial_reply.take() {
            Some(reply) => {
                let result = HostReply::check(reply);
                if result.as_ref().map_or(true, |reply| !reply.ran) {
                    service
                        .routes
                        .remove(&invocation.actor, &target.identity)
                        .await;
                }
                result?
            }
            None => {
                dispatch(
                    service,
                    hosts,
                    &target,
                    invocation,
                    host_state,
                    started.elapsed().as_secs_f64() * 1000.0,
                )
                .await?
            }
        };
        if outcome.ran || Instant::now() >= deadline {
            return Ok(outcome);
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

pub(super) async fn publish(
    State(state): State<PublicApiState>,
    Path(path): Path<ActorPath>,
    headers: HeaderMap,
    body: Result<Json<PublishRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    let actor = path.into_actor();
    super::public_api::authorized_admin(&state.admin, &headers)?;
    actor.validate().map_err(ApiError::bad_request)?;
    state
        .invocations
        .ensure_project_access(&actor.project_id)
        .await
        .map_err(ApiError::access)?;
    let gateway = state
        .invocations
        .gateway
        .as_ref()
        .ok_or_else(|| ApiError::unauthorized("gateway is not configured"))?;
    let Json(request) = body.map_err(ApiError::json)?;
    crate::actor::validate_socket_effects(&request.effects).map_err(ApiError::bad_request)?;

    gateway
        .connections
        .operation(
            &actor,
            SocketOperation::Publish {
                effects: request.effects,
            },
        )
        .await
        .map_err(|error| {
            tracing::warn!(error = %format!("{error:#}"), "socket effects could not be delivered");
            ApiError::unavailable("socket effects could not be delivered")
        })?;
    Ok((
        StatusCode::NO_CONTENT,
        [(header::CACHE_CONTROL, "no-store")],
    )
        .into_response())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct PublishRequest {
    effects: Vec<crate::actor::ActorSocketEffect>,
}

async fn dispatch(
    service: &super::ControlPlaneService,
    client: &reqwest::Client,
    target: &ActorTarget,
    invocation: &ActorInvocation,
    host_state: HostState,
    routing_ms: f64,
) -> Result<HostReply, ApiError> {
    let result = send_invocation(client, target, invocation, host_state, routing_ms).await;
    if result.as_ref().map_or(true, |reply| !reply.ran) {
        service
            .routes
            .remove(&invocation.actor, &target.identity)
            .await;
    }
    result
}

async fn send_invocation(
    client: &reqwest::Client,
    target: &ActorTarget,
    invocation: &ActorInvocation,
    host_state: HostState,
    routing_ms: f64,
) -> Result<HostReply, ApiError> {
    let actor = &invocation.actor;
    let url = format!(
        "{}/v1/projects/{}/actors/{}/{}/invoke",
        target.route.trim_end_matches('/'),
        actor.project_id,
        actor.actor_name,
        actor.actor_id
    );
    let fields = json!({
        "requestId":invocation.request_id, "ownerEpoch":target.owner_epoch,
        "method":invocation.method, "hostState":host_state, "routingMs":routing_ms,
    });
    let body = JsonBody::with_field(&fields, "args", invocation.args.text().clone().into())
        .map_err(ApiError::internal)?;
    let response = client
        .post(url)
        .bearer_auth(&target.token)
        .header(header::CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .await;
    let response = match response {
        Ok(response) => response,
        Err(error) if error.is_connect() => {
            return Ok(HostReply::not_run(
                json!({"type":"not_executed", "reason":"upstream_not_reached"}),
            ));
        }
        Err(_) => return Err(outcome_unknown()),
    };
    if response.status() == StatusCode::UNAUTHORIZED {
        return Ok(HostReply::not_run(json!({"type":"unauthenticated"})));
    }
    if !response.status().is_success() {
        return Err(outcome_unknown());
    }
    HostReply::read(response).await
}

/// An invocation's outcome as the host's text, which the caller receives unchanged.
pub(super) struct HostReply {
    ran: bool,
    text: JsonPayload,
}

impl HostReply {
    fn not_run(reply: Value) -> Self {
        Self {
            ran: false,
            text: reply.into(),
        }
    }

    /// A large completed reply is passed on as it arrives. Any other is read whole and checked.
    async fn read(response: reqwest::Response) -> Result<Self, ApiError> {
        let len = response
            .content_length()
            .and_then(|len| usize::try_from(len).ok())
            .unwrap_or(0);
        let completed = response
            .headers()
            .get(super::OUTCOME_HEADER)
            .is_some_and(|outcome| outcome == "completed");
        if completed && len > ARRIVING_BYTES {
            let chunks = response.bytes_stream().map(|chunk| Ok(chunk?));
            return Ok(Self {
                ran: true,
                text: JsonStream::new(chunks, len).into(),
            });
        }
        let reply = read_reply(response, len)
            .await
            .map_err(|_| outcome_unknown())?;
        Self::check(reply)
    }

    fn check(text: JsonText) -> Result<Self, ApiError> {
        let head: ReplyHead = serde_json::from_str(text.get()).map_err(|_| outcome_unknown())?;
        let ran = match head.kind.as_str() {
            "not_executed"
                if matches!(
                    head.reason.as_deref(),
                    Some("host_unavailable" | "upstream_not_reached")
                ) =>
            {
                false
            }
            "completed" if head.result => true,
            "failed"
                if head.code.as_deref().is_some_and(|code| !code.is_empty())
                    && head.message.is_some() =>
            {
                true
            }
            _ => return Err(outcome_unknown()),
        };
        Ok(Self {
            ran,
            text: text.into(),
        })
    }
}

/// Reads into one buffer of the announced length, which the reply's text then keeps.
async fn read_reply(mut response: reqwest::Response, len: usize) -> anyhow::Result<JsonText> {
    let mut body = Vec::new();
    body.try_reserve_exact(len)?;
    while let Some(chunk) = response.chunk().await? {
        body.extend_from_slice(&chunk);
    }
    JsonText::from_vec(body)
}

/// The fields of a reply that decide its outcome. The result is skipped, not parsed.
#[derive(Deserialize)]
struct ReplyHead {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default, deserialize_with = "present")]
    result: bool,
    reason: Option<String>,
    code: Option<String>,
    message: Option<String>,
}

fn present<'de, D: Deserializer<'de>>(deserializer: D) -> Result<bool, D::Error> {
    IgnoredAny::deserialize(deserializer).map(|_| true)
}

fn outcome_unknown() -> ApiError {
    ApiError::new(
        StatusCode::BAD_GATEWAY,
        "outcome_unknown",
        "actor invocation was dispatched but its outcome could not be confirmed",
    )
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct InvokeRequest {
    request_id: String,
    method: String,
    #[serde(deserialize_with = "ActorArgs::deserialize_text")]
    args: ActorArgs,
    home_region: Option<String>,
}
