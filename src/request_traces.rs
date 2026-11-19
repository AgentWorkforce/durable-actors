use crate::control_plane::admin::validate_component;
use crate::request_tracking::HostState;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub(crate) use crate::request_tracking::RequestOutcome;

use crate::clock::{Clock, SystemClock};

pub(crate) mod events;
pub(crate) mod history;
pub(crate) mod live;
pub(crate) mod logs;
pub(crate) mod metrics;
pub(crate) mod otlp;
pub(crate) mod persistence;
pub(crate) mod resources;
pub(crate) mod sockets;
use live::{InProcessFeed, LiveFeed, LiveStream};
use persistence::{TraceQueries, sqlite::SqliteTracePersistence};

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

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TraceRecord {
    pub sequence: u64,
    #[serde(flatten)]
    pub event: TraceEvent,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TracePage {
    pub records: Vec<TraceRecord>,
    pub next_cursor: Option<String>,
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

    pub(crate) async fn resources(
        &self,
        project_id: &str,
        query: &resources::ResourceQuery,
    ) -> Result<resources::ResourceUsage> {
        validate_component("project ID", project_id, 64).context(InvalidTraceQuery)?;
        query.validate().context(InvalidTraceQuery)?;
        self.queries.resources(project_id, query).await
    }

    pub(crate) async fn websockets(
        &self,
        project_id: &str,
        query: &metrics::TimeRange,
    ) -> Result<Vec<sockets::SocketSession>> {
        validate_component("project ID", project_id, 64).context(InvalidTraceQuery)?;
        query.validate().context(InvalidTraceQuery)?;
        let (from_ms, to_ms) = (query.from_ms.unwrap_or(0), query.to_ms.unwrap_or(u64::MAX));
        let (records, heartbeats) = tokio::try_join!(
            self.queries
                .socket_records(project_id, to_ms, false, sockets::SESSION_LIMIT),
            self.queries.gateway_heartbeats(project_id),
        )?;
        let mut sessions = sockets::sessions(records, &heartbeats, SystemClock.now_ms()?);
        sessions.retain(|session| sockets::overlaps(session, from_ms, to_ms));
        Ok(sessions)
    }

    /// The project's stored platform events that the query covers, newest first.
    pub(crate) async fn platform_events(
        &self,
        project_id: &str,
        query: &events::EventQuery,
    ) -> Result<events::StoredEvents> {
        validate_component("project ID", project_id, 64).context(InvalidTraceQuery)?;
        query.validate().context(InvalidTraceQuery)?;
        let events = self.queries.platform_events(project_id, query).await?;
        Ok(events::StoredEvents::from_read(events))
    }

    /// The public actor contract of the project's latest deployment.
    pub(crate) async fn deployed_contract(
        &self,
        project_id: &str,
    ) -> Result<Option<serde_json::Value>> {
        validate_component("project ID", project_id, 64).context(InvalidTraceQuery)?;
        self.queries.deployed_contract(project_id).await
    }

    /// The project's connections open now, by actor.
    pub(crate) async fn open_connections(
        &self,
        project_id: &str,
    ) -> Result<sockets::OpenConnections> {
        validate_component("project ID", project_id, 64).context(InvalidTraceQuery)?;
        let (records, heartbeats) = tokio::try_join!(
            self.queries
                .socket_records(project_id, u64::MAX, true, sockets::OPEN_LIMIT),
            self.queries.gateway_heartbeats(project_id),
        )?;
        let sessions = sockets::sessions(records, &heartbeats, SystemClock.now_ms()?);
        Ok(sockets::open_connections(project_id, sessions, &heartbeats))
    }
}
