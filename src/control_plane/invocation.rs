use crate::{
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
use serde::Deserialize;
use serde_json::{Value, json};
use std::future::Future;
use tokio::time::Instant;

use super::public_api::{
    ActorPath, ApiError, FindActorRequest, PublicApiState, resolve_actor_target,
};
use super::service::ActorTarget;
use crate::actor::ActorInvocation;

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
        args: vec![],
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
    match outcome["type"].as_str() {
        Some("completed" | "failed") => Ok(()),
        _ => anyhow::bail!("alarm was not executed: {outcome}"),
    }
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
    Ok((
        [(header::CACHE_CONTROL, "no-store")],
        Json(json!({"outcome": outcome})),
    )
        .into_response())
}

pub(super) async fn invoke_from_host(
    service: &super::ControlPlaneService,
    invocation: &ActorInvocation,
) -> Value {
    resolve_and_dispatch_from_host(service, invocation)
        .await
        .unwrap_or_else(ApiError::into_outcome)
}

async fn resolve_and_dispatch_from_host(
    service: &super::ControlPlaneService,
    invocation: &ActorInvocation,
) -> Result<Value, ApiError> {
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
) -> Result<Value, ApiError> {
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
) -> Result<Value, ApiError>
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
                let result = validate_reply(reply);
                if result
                    .as_ref()
                    .map_or(true, |reply| reply["type"] == "not_executed")
                {
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
        if !matches!(
            outcome["type"].as_str(),
            Some("not_executed" | "unauthenticated")
        ) || Instant::now() >= deadline
        {
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
) -> Result<Value, ApiError> {
    let result = send_invocation(client, target, invocation, host_state, routing_ms).await;
    if result.as_ref().map_or(true, |reply| {
        matches!(
            reply["type"].as_str(),
            Some("not_executed" | "unauthenticated")
        )
    }) {
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
) -> Result<Value, ApiError> {
    let actor = &invocation.actor;
    let url = format!(
        "{}/v1/projects/{}/actors/{}/{}/invoke",
        target.route.trim_end_matches('/'),
        actor.project_id,
        actor.actor_name,
        actor.actor_id
    );
    let response = client
        .post(url)
        .bearer_auth(&target.token)
        .json(&json!({
            "requestId":invocation.request_id, "ownerEpoch":target.owner_epoch,
            "method":invocation.method, "args":invocation.args, "hostState":host_state, "routingMs":routing_ms,
        }))
        .send()
        .await;
    let response = match response {
        Ok(response) => response,
        Err(error) if error.is_connect() => {
            return Ok(json!({"type":"not_executed", "reason":"upstream_not_reached"}));
        }
        Err(_) => return Err(outcome_unknown()),
    };
    if response.status() == StatusCode::UNAUTHORIZED {
        return Ok(json!({"type":"unauthenticated"}));
    }
    if !response.status().is_success() {
        return Err(outcome_unknown());
    }
    let reply = read_reply(response).await?;
    validate_reply(reply)
}

fn validate_reply(reply: Value) -> Result<Value, ApiError> {
    match reply.get("type").and_then(Value::as_str) {
        Some("not_executed")
            if matches!(
                reply["reason"].as_str(),
                Some("host_unavailable" | "upstream_not_reached")
            ) =>
        {
            Ok(reply)
        }
        Some("completed") if reply.get("result").is_some() => Ok(reply),
        Some("failed")
            if reply["code"].as_str().is_some_and(|code| !code.is_empty())
                && reply["message"].is_string() =>
        {
            Ok(reply)
        }
        _ => Err(outcome_unknown()),
    }
}

async fn read_reply(response: reqwest::Response) -> Result<Value, ApiError> {
    response.json().await.map_err(|_| outcome_unknown())
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
    args: Vec<Value>,
    home_region: Option<String>,
}
