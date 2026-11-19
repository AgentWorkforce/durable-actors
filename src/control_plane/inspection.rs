use std::{sync::Arc, time::Duration};

use anyhow::Result;
use axum::{
    Json, Router,
    extract::{Path, Query, State, rejection::QueryRejection},
    http::header,
    middleware,
    response::{IntoResponse, Response},
    routing::get,
};
use futures_util::StreamExt;
use serde::Deserialize;

use super::{
    admin::AdminService,
    public_api::{ApiError, ProjectPath, project_id, require_admin},
};

pub(super) fn router(inspector: ActorInspector, admin: AdminService) -> Router {
    local_router(inspector).route_layer(middleware::from_fn_with_state(admin, require_admin))
}

pub(super) fn local_router(inspector: ActorInspector) -> Router {
    Router::new()
        .route("/v1/projects/{project_id}/observe/state", get(actor_state))
        .route(
            "/v1/projects/{project_id}/observe/state/history",
            get(state_history),
        )
        .route(
            "/v1/projects/{project_id}/observe/actors",
            get(actor_inventory),
        )
        .route(
            "/v1/projects/{project_id}/observe/residency",
            get(residency_history),
        )
        .route(
            "/v1/projects/{project_id}/observe/events",
            get(project_events),
        )
        .route(
            "/v1/projects/{project_id}/observe/events/history",
            get(event_history),
        )
        .route(
            "/v1/projects/{project_id}/observe/requests",
            get(request_history),
        )
        .route(
            "/v1/projects/{project_id}/observe/metrics",
            get(overview_metrics),
        )
        .route(
            "/v1/projects/{project_id}/observe/queue-waits",
            get(queue_waits),
        )
        .route(
            "/v1/projects/{project_id}/observe/resources",
            get(resource_usage),
        )
        .route(
            "/v1/projects/{project_id}/observe/websockets",
            get(websocket_history),
        )
        .route("/v1/projects/{project_id}/observe/logs", get(log_history))
        .route(
            "/v1/projects/{project_id}/observe/logs/histogram",
            get(log_histogram),
        )
        .with_state(InspectionApi { inspector })
}

#[derive(Clone)]
struct InspectionApi {
    inspector: ActorInspector,
}

#[derive(Clone)]
pub(super) struct ActorInspector {
    traces: crate::request_traces::TraceStore,
    state: Arc<dyn crate::bucket::ActorStateReader>,
    inventory: Arc<dyn crate::placement::ActorInventoryReader>,
}

impl ActorInspector {
    pub(super) fn new(
        inventory: Arc<dyn crate::placement::ActorInventoryReader>,
        state: Arc<dyn crate::bucket::ActorStateReader>,
    ) -> Self {
        Self {
            traces: crate::request_traces::TraceStore::default(),
            inventory,
            state,
        }
    }

    pub(super) fn with_traces(mut self, traces: crate::request_traces::TraceStore) -> Self {
        self.traces = traces;
        self
    }
}

async fn actor_inventory(
    State(state): State<InspectionApi>,
    path: Path<ProjectPath>,
) -> Result<Response, ApiError> {
    let project = project_id(path)?;
    let inventory = tokio::time::timeout(Duration::from_secs(25), read_inventory(&state, &project))
        .await
        .map_err(|_| ApiError::unavailable("Actor inventory timed out"))?
        .map_err(ApiError::internal)?;
    Ok(([(header::CACHE_CONTROL, "no-store")], Json(inventory)).into_response())
}

async fn read_inventory(
    state: &InspectionApi,
    project: &str,
) -> Result<crate::placement::ActorInventorySnapshot> {
    let (mut inventory, contract) = tokio::try_join!(
        state.inspector.inventory.actor_inventory(project),
        state.inspector.traces.deployed_contract(project),
    )?;
    let mut rows: std::collections::BTreeMap<_, _> = inventory
        .actors
        .into_iter()
        .map(|row| (row.actor_name.clone(), row))
        .collect();
    if let Some(contract) = contract {
        for (name, methods) in super::contracts::actor_methods(&contract)? {
            let row =
                rows.entry(name.clone())
                    .or_insert_with(|| crate::placement::ActorInventory {
                        actor_name: name,
                        ..Default::default()
                    });
            row.methods = Some(methods);
        }
    }
    inventory.actors = rows.into_values().collect();
    Ok(inventory)
}

/// The project's events on the requested live channels. Views read the store when they open and
/// apply these afterwards, so the stream never reads the store itself.
async fn project_events(
    State(state): State<InspectionApi>,
    path: Path<ProjectPath>,
    query: Result<Query<super::observe_events::EventsQuery>, QueryRejection>,
) -> Result<Response, ApiError> {
    use axum::response::sse::{Event, KeepAlive, Sse};
    let project = project_id(path)?;
    let Query(query) = query.map_err(ApiError::bad_request)?;
    query.signals().map_err(ApiError::bad_request)?;
    query
        .log_filter()
        .validate()
        .map_err(ApiError::bad_request)?;
    let batches = super::observe_events::subscribe(&state.inspector.traces, &project, &query)
        .await
        .map_err(|_| ApiError::unavailable("Live events are unavailable"))?;
    let events = batches.map(|batch| Event::default().event(batch.name).json_data(&batch.events));
    Ok((
        [
            (header::CACHE_CONTROL, "no-store"),
            (header::HeaderName::from_static("x-accel-buffering"), "no"),
        ],
        Sse::new(events).keep_alive(KeepAlive::new().interval(Duration::from_secs(10))),
    )
        .into_response())
}

/// The project's past events, in the shapes `project_events` streams them.
async fn event_history(
    State(state): State<InspectionApi>,
    path: Path<ProjectPath>,
    query: Result<Query<super::event_history::EventHistoryQuery>, QueryRejection>,
) -> Result<Response, ApiError> {
    let project = path.0.project_id;
    let Query(query) = query.map_err(ApiError::bad_request)?;
    let stored = query.stored().map_err(ApiError::bad_request)?;
    let history = super::event_history::read(&state.inspector.traces, &project, &stored)
        .await
        .map_err(trace_query_error)?;
    Ok(([(header::CACHE_CONTROL, "no-store")], Json(history)).into_response())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ResidencyQuery {
    actor_name: String,
    actor_id: Option<String>,
    from_ms: Option<u64>,
    to_ms: Option<u64>,
}

async fn residency_history(
    State(state): State<InspectionApi>,
    path: Path<ProjectPath>,
    query: Result<Query<ResidencyQuery>, QueryRejection>,
) -> Result<Response, ApiError> {
    use crate::clock::{Clock, SystemClock};
    let project = project_id(path)?;
    let Query(query) = query.map_err(ApiError::bad_request)?;
    crate::request_traces::history::HistoryQuery {
        actor_name: Some(query.actor_name.clone()),
        actor_id: query.actor_id.clone(),
        from_ms: query.from_ms,
        to_ms: query.to_ms,
        ..Default::default()
    }
    .validate()
    .map_err(ApiError::bad_request)?;
    let to = query
        .to_ms
        .unwrap_or(SystemClock.now_ms().map_err(ApiError::internal)?);
    let history = tokio::time::timeout(
        Duration::from_secs(25),
        state.inspector.inventory.residency_history(
            &project,
            &crate::placement::ResidencyQuery {
                actor_name: query.actor_name,
                actor_id: query.actor_id,
                from_ms: query.from_ms.unwrap_or(0),
                to_ms: to,
            },
        ),
    )
    .await
    .map_err(|_| ApiError::unavailable("Residency history timed out"))?
    .map_err(ApiError::internal)?;
    Ok(([(header::CACHE_CONTROL, "no-store")], Json(history)).into_response())
}

async fn overview_metrics(
    State(state): State<InspectionApi>,
    path: Path<ProjectPath>,
    query: Result<Query<crate::request_traces::metrics::TimeRange>, QueryRejection>,
) -> Result<Response, ApiError> {
    let project = path.0.project_id;
    let Query(query) = query.map_err(ApiError::bad_request)?;
    let result = state
        .inspector
        .traces
        .metrics(&project, &query)
        .await
        .map_err(trace_query_error)?;
    Ok(([(header::CACHE_CONTROL, "no-store")], Json(result)).into_response())
}

async fn queue_waits(
    State(state): State<InspectionApi>,
    path: Path<ProjectPath>,
    query: Result<Query<crate::request_traces::metrics::QueueWaitQuery>, QueryRejection>,
) -> Result<Response, ApiError> {
    let project = path.0.project_id;
    let Query(query) = query.map_err(ApiError::bad_request)?;
    let result = state
        .inspector
        .traces
        .queue_waits(&project, &query)
        .await
        .map_err(trace_query_error)?;
    Ok(([(header::CACHE_CONTROL, "no-store")], Json(result)).into_response())
}

async fn resource_usage(
    State(state): State<InspectionApi>,
    path: Path<ProjectPath>,
    query: Result<Query<crate::request_traces::resources::ResourceQuery>, QueryRejection>,
) -> Result<Response, ApiError> {
    let project = path.0.project_id;
    let Query(query) = query.map_err(ApiError::bad_request)?;
    let result = state
        .inspector
        .traces
        .resources(&project, &query)
        .await
        .map_err(trace_query_error)?;
    Ok(([(header::CACHE_CONTROL, "no-store")], Json(result)).into_response())
}

async fn websocket_history(
    State(state): State<InspectionApi>,
    path: Path<ProjectPath>,
    query: Result<Query<crate::request_traces::metrics::TimeRange>, QueryRejection>,
) -> Result<Response, ApiError> {
    let project = path.0.project_id;
    let Query(query) = query.map_err(ApiError::bad_request)?;
    let result = state
        .inspector
        .traces
        .websockets(&project, &query)
        .await
        .map_err(trace_query_error)?;
    Ok(([(header::CACHE_CONTROL, "no-store")], Json(result)).into_response())
}

async fn request_history(
    State(state): State<InspectionApi>,
    path: Path<ProjectPath>,
    query: Result<Query<crate::request_traces::history::HistoryQuery>, QueryRejection>,
) -> Result<Response, ApiError> {
    let project = path.0.project_id;
    let Query(query) = query.map_err(ApiError::bad_request)?;
    let page = state
        .inspector
        .traces
        .history(&project, &query)
        .await
        .map_err(trace_query_error)?;
    Ok(([(header::CACHE_CONTROL, "no-store")], Json(page)).into_response())
}

fn trace_query_error(error: anyhow::Error) -> ApiError {
    if error.is::<crate::request_traces::InvalidTraceQuery>() {
        ApiError::bad_request(error.root_cause())
    } else {
        ApiError::internal(error)
    }
}

async fn log_history(
    State(state): State<InspectionApi>,
    path: Path<ProjectPath>,
    query: Result<Query<crate::request_traces::logs::LogQuery>, QueryRejection>,
) -> Result<Response, ApiError> {
    let project = path.0.project_id;
    let Query(query) = query.map_err(ApiError::bad_request)?;
    let page = state
        .inspector
        .traces
        .logs(&project, &query)
        .await
        .map_err(trace_query_error)?;
    Ok(([(header::CACHE_CONTROL, "no-store")], Json(page)).into_response())
}

async fn log_histogram(
    State(state): State<InspectionApi>,
    path: Path<ProjectPath>,
    query: Result<Query<crate::request_traces::logs::LogHistogramQuery>, QueryRejection>,
) -> Result<Response, ApiError> {
    let Query(query) = query.map_err(ApiError::bad_request)?;
    let histogram = state
        .inspector
        .traces
        .log_histogram(&path.0.project_id, &query)
        .await
        .map_err(trace_query_error)?;
    Ok(([(header::CACHE_CONTROL, "no-store")], Json(histogram)).into_response())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StateQuery {
    actor_name: String,
    actor_id: String,
    version: Option<u64>,
    before: Option<u64>,
    limit: Option<usize>,
}

impl StateQuery {
    fn actor(&self, project_id: String) -> Result<crate::actor::ActorKey, ApiError> {
        let actor = crate::actor::ActorKey {
            project_id,
            actor_name: self.actor_name.clone(),
            actor_id: self.actor_id.clone(),
        };
        actor.validate().map_err(ApiError::bad_request)?;
        if self.version == Some(0)
            || self.before == Some(0)
            || self.limit.is_some_and(|n| n == 0 || n > 100)
        {
            return Err(ApiError::bad_request(
                "invalid state version or history limit",
            ));
        }
        Ok(actor)
    }
}

async fn actor_state(
    State(api): State<InspectionApi>,
    path: Path<ProjectPath>,
    query: Result<Query<StateQuery>, QueryRejection>,
) -> Result<Response, ApiError> {
    let Query(query) = query.map_err(ApiError::bad_request)?;
    let actor = query.actor(project_id(path)?)?;
    let snapshot = tokio::time::timeout(
        Duration::from_secs(25),
        api.inspector.state.inspect_state(&actor, query.version),
    )
    .await
    .map_err(|_| ApiError::unavailable("State inspection timed out"))?
    .map_err(ApiError::internal)?;
    let contract = api
        .inspector
        .traces
        .deployed_contract(&actor.project_id)
        .await
        .map_err(ApiError::internal)?;
    let schema = contract
        .as_ref()
        .and_then(|contract| contract["actors"].as_array())
        .and_then(|actors| {
            actors
                .iter()
                .find(|item| item["actorName"] == actor.actor_name)
        })
        .and_then(|actor| actor.get("socket"))
        .and_then(|socket| socket.get("schema"));
    Ok((
        [(header::CACHE_CONTROL, "no-store")],
        Json(serde_json::json!({ "snapshot": snapshot, "schema": schema })),
    )
        .into_response())
}

async fn state_history(
    State(api): State<InspectionApi>,
    path: Path<ProjectPath>,
    query: Result<Query<StateQuery>, QueryRejection>,
) -> Result<Response, ApiError> {
    let Query(query) = query.map_err(ApiError::bad_request)?;
    let actor = query.actor(project_id(path)?)?;
    let page = tokio::time::timeout(
        Duration::from_secs(25),
        api.inspector
            .state
            .state_history(&actor, query.before, query.limit.unwrap_or(25)),
    )
    .await
    .map_err(|_| ApiError::unavailable("State history timed out"))?
    .map_err(ApiError::internal)?;
    Ok(([(header::CACHE_CONTROL, "no-store")], Json(page)).into_response())
}
