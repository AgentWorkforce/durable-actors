use crate::control_plane::admin::validate_component;
use crate::request_tracking::HostState;
use anyhow::{Context, Result};
use opentelemetry::{
    KeyValue,
    trace::{Span, SpanKind, Status, Tracer},
};
use opentelemetry_sdk::trace::SdkTracer;
use serde::{Deserialize, Serialize};
use std::{
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

pub(crate) use crate::request_tracking::RequestOutcome;
use crate::request_tracking::{RequestCompletion, RequestObserver};

pub(crate) mod history;
pub(crate) mod live;
pub(crate) mod logs;
pub(crate) mod metrics;
pub(crate) mod otlp;
pub(crate) mod persistence;
pub(crate) mod replay;
use live::{InProcessFeed, LiveFeed, LiveStream};
use persistence::{TraceQueries, sqlite::SqliteTracePersistence};
use replay::ReplayQuery;

pub(crate) const TRACE_CAPACITY: usize = 500;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RequestTrace {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_version: Option<u64>,
    pub project_id: String,
    pub request_id: String,
    pub actor_name: String,
    pub actor_id: String,
    pub kind: RequestKind,
    pub operation: String,
    pub connection_id: Option<String>,
    pub started_at_ms: u64,
    pub routing_ms: f64,
    pub duration_ms: f64,
    pub queue_wait_ms: Option<f64>,
    #[serde(default)]
    pub host_state: HostState,
    pub outcome: RequestOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RequestKind {
    Method,
    Websocket,
}

impl RequestKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Method => "method",
            Self::Websocket => "websocket",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TraceEvent {
    #[serde(default = "new_event_id")]
    pub event_id: String,
    pub host_id: String,
    pub session_id: String,
    #[serde(flatten)]
    pub trace: RequestTrace,
}

fn new_event_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TraceRecord {
    pub sequence: u64,
    #[serde(flatten)]
    pub event: TraceEvent,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TracePage {
    pub epoch: String,
    pub cursor: u64,
    pub capacity: usize,
    pub records: Vec<TraceRecord>,
    pub next_cursor: Option<String>,
    pub resume_cursor: String,
    pub reset: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub snapshot: bool,
}

impl TracePage {
    pub(crate) fn empty(capacity: usize) -> Self {
        Self {
            epoch: "none".into(),
            cursor: 0,
            capacity,
            records: Vec::new(),
            next_cursor: None,
            resume_cursor: "snapshot".into(),
            reset: false,
            snapshot: false,
        }
    }
}

#[derive(Debug)]
pub(crate) struct InvalidTraceQuery;

impl std::fmt::Display for InvalidTraceQuery {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Invalid or incompatible request trace query")
    }
}
impl std::error::Error for InvalidTraceQuery {}

#[derive(Clone)]
pub(crate) struct TraceStore {
    queries: Arc<dyn TraceQueries>,
    live: Arc<dyn LiveFeed>,
}

impl Default for TraceStore {
    fn default() -> Self {
        Self::new(
            Arc::new(SqliteTracePersistence::in_memory()),
            Arc::new(InProcessFeed::default()),
        )
    }
}

impl TraceStore {
    pub(crate) fn new(queries: Arc<dyn TraceQueries>, live: Arc<dyn LiveFeed>) -> Self {
        Self { queries, live }
    }

    pub(crate) async fn subscribe(
        &self,
        project_id: &str,
        signal: otlp::Signal,
    ) -> Result<LiveStream> {
        validate_component("project ID", project_id, 64).context(InvalidTraceQuery)?;
        self.live
            .subscribe(otlp::live_channel(project_id, signal))
            .await
    }

    pub(crate) async fn replay(&self, project_id: &str, query: &ReplayQuery) -> Result<TracePage> {
        validate_component("project ID", project_id, 64).context(InvalidTraceQuery)?;
        query.validate().context(InvalidTraceQuery)?;
        self.queries.replay(project_id, query).await
    }

    pub(crate) async fn history(
        &self,
        project_id: &str,
        query: &history::HistoryQuery,
    ) -> Result<TracePage> {
        validate_component("project ID", project_id, 64).context(InvalidTraceQuery)?;
        query.validate().context(InvalidTraceQuery)?;
        self.queries.history(project_id, query).await
    }

    pub(crate) async fn logs(
        &self,
        project_id: &str,
        query: &logs::LogQuery,
    ) -> Result<logs::LogPage> {
        validate_component("project ID", project_id, 64).context(InvalidTraceQuery)?;
        query.validate().context(InvalidTraceQuery)?;
        self.queries.logs(project_id, query).await
    }

    pub(crate) async fn log_histogram(
        &self,
        project_id: &str,
        query: &logs::LogHistogramQuery,
    ) -> Result<logs::LogHistogram> {
        validate_component("project ID", project_id, 64).context(InvalidTraceQuery)?;
        query.validate().context(InvalidTraceQuery)?;
        self.queries.log_histogram(project_id, query).await
    }

    pub(crate) async fn metrics(
        &self,
        project_id: &str,
        query: &metrics::TimeRange,
    ) -> Result<metrics::OverviewMetrics> {
        validate_component("project ID", project_id, 64).context(InvalidTraceQuery)?;
        query.validate().context(InvalidTraceQuery)?;
        self.queries.metrics(project_id, query).await
    }

    pub(crate) async fn queue_waits(
        &self,
        project_id: &str,
        query: &metrics::QueueWaitQuery,
    ) -> Result<Vec<metrics::QueueWaitRow>> {
        validate_component("project ID", project_id, 64).context(InvalidTraceQuery)?;
        query.validate().context(InvalidTraceQuery)?;
        self.queries.queue_waits(project_id, query).await
    }

    pub(crate) async fn websockets(
        &self,
        project_id: &str,
        query: &metrics::TimeRange,
    ) -> Result<Vec<metrics::SocketSession>> {
        validate_component("project ID", project_id, 64).context(InvalidTraceQuery)?;
        query.validate().context(InvalidTraceQuery)?;
        self.queries.websockets(project_id, query).await
    }
}

pub(crate) struct TraceRecorder {
    tracer: SdkTracer,
    operation: String,
    attributes: Vec<KeyValue>,
}

impl TraceRecorder {
    pub(crate) fn new(
        tracer: SdkTracer,
        invocation: &crate::actor::ActorInvocation,
        kind: RequestKind,
        connection_id: Option<String>,
        metadata: Option<serde_json::Value>,
    ) -> Self {
        let mut attributes = vec![
            KeyValue::new(otlp::REQUEST_ID, invocation.request_id.clone()),
            KeyValue::new(otlp::KIND, kind.as_str()),
        ];
        attributes.extend(connection_id.map(|id| KeyValue::new(otlp::CONNECTION_ID, id)));
        attributes.extend(metadata.map(|value| KeyValue::new(otlp::METADATA, value.to_string())));
        Self {
            tracer,
            operation: invocation.method.clone(),
            attributes,
        }
    }
}

impl RequestObserver for TraceRecorder {
    fn record(self: Box<Self>, completion: &RequestCompletion) {
        let started = UNIX_EPOCH + Duration::from_millis(completion.started_at_ms);
        let ended = started + Duration::from_secs_f64(completion.timings.duration_ms / 1000.0);
        let mut attributes = self.attributes;
        attributes.extend(completion_attributes(completion));
        let mut span = self
            .tracer
            .span_builder(self.operation)
            .with_kind(SpanKind::Server)
            .with_start_time(started)
            .with_attributes(attributes)
            .start_with_context(&self.tracer, &opentelemetry::Context::new());
        if matches!(completion.outcome, RequestOutcome::Failed) {
            span.set_status(Status::error(completion.outcome.as_str()));
        }
        span.end_with_timestamp(ended);
    }
}

fn completion_attributes(completion: &RequestCompletion) -> impl Iterator<Item = KeyValue> {
    let timings = &completion.timings;
    [
        Some(KeyValue::new(otlp::OUTCOME, completion.outcome.as_str())),
        Some(KeyValue::new(otlp::HOST_STATE, timings.host_state.as_str())),
        Some(KeyValue::new(otlp::ROUTING_MS, timings.routing_ms)),
        timings
            .queue_wait_ms
            .map(|wait| KeyValue::new(otlp::QUEUE_WAIT_MS, wait)),
        completion
            .state_version
            .and_then(|version| i64::try_from(version).ok())
            .map(|version| KeyValue::new(otlp::STATE_VERSION, version)),
    ]
    .into_iter()
    .flatten()
}

#[cfg(test)]
#[path = "../tests/unit/request_traces/tests.rs"]
mod tests;
