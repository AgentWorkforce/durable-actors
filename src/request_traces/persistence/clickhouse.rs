use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Result, ensure};
use async_trait::async_trait;
use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError, mpsc};
use tokio_util::sync::CancellationToken;

use super::{TracePersistence, TraceStatus};
use crate::request_traces::{
    TRACE_BATCH_SIZE, TraceEvent, TracePage,
    history::HistoryQuery,
    live::CommittedTracePublisher,
    metrics::{OverviewMetrics, QueueWaitQuery, QueueWaitRow, SocketSession, TimeRange},
    replay::ReplayQuery,
};

mod ingestion;
mod lifecycle;
mod queries;

// Rows stay queued while ClickHouse is slow; the bound covers memory, not report count.
const MAX_QUEUED_ROWS: usize = 100_000;

#[derive(Clone)]
pub struct ClickHouseConfig {
    pub url: String,
    pub database: String,
    pub user: String,
    pub password: String,
}

pub(crate) struct ClickHouseTracePersistence {
    client: Client,
    sender: mpsc::UnboundedSender<Report>,
    queued_rows: Arc<Semaphore>,
    failed: Arc<AtomicBool>,
    retention_seconds: u64,
}

impl ClickHouseTracePersistence {
    pub(crate) fn start(
        config: ClickHouseConfig,
        retention: Duration,
        publisher: Arc<dyn CommittedTracePublisher>,
        stop: CancellationToken,
    ) -> Result<Self> {
        let url = reqwest::Url::parse(&config.url)?;
        ensure!(
            matches!(url.scheme(), "http" | "https")
                && url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none(),
            "invalid ClickHouse URL"
        );
        let client = Client::default()
            .with_url(config.url)
            .with_database(config.database)
            .with_user(config.user)
            .with_password(config.password)
            .with_setting("max_execution_time", "10");
        let (sender, receiver) = mpsc::unbounded_channel();
        let failed = Arc::new(AtomicBool::new(false));
        tokio::spawn(ingestion::run(
            client.clone(),
            receiver,
            failed.clone(),
            retention.as_secs(),
            publisher,
            stop,
        ));
        Ok(Self {
            client,
            sender,
            queued_rows: Arc::new(Semaphore::new(MAX_QUEUED_ROWS)),
            failed,
            retention_seconds: retention.as_secs(),
        })
    }

    fn enqueue(&self, events: &[TraceEvent], status: Option<StatusRow>) -> Result<()> {
        ensure!(
            events.len() <= TRACE_BATCH_SIZE,
            "trace batch exceeds limit"
        );
        self.send(events.to_vec(), status, None)
    }

    fn send(
        &self,
        events: Vec<TraceEvent>,
        status: Option<StatusRow>,
        lifecycle: Option<crate::actor_lifecycle::LifecycleEvent>,
    ) -> Result<()> {
        let rows = events.len() + usize::from(status.is_some()) + usize::from(lifecycle.is_some());
        if rows == 0 {
            return Ok(());
        }
        let permit = self
            .queued_rows
            .clone()
            .try_acquire_many_owned(u32::try_from(rows)?)
            .map_err(|error| {
                anyhow::anyhow!(
                    "ClickHouse trace queue unavailable: {}",
                    match error {
                        TryAcquireError::NoPermits => "full",
                        TryAcquireError::Closed => "closed",
                    }
                )
            })?;
        self.sender
            .send(Report {
                events,
                status,
                lifecycle,
                permit,
            })
            .map_err(|_| anyhow::anyhow!("ClickHouse trace queue unavailable: closed"))
    }
}

#[async_trait]
impl TracePersistence for ClickHouseTracePersistence {
    async fn initialize(&self) -> Result<()> {
        // Schema setup and retries run in the writer; telemetry cannot delay actor startup.
        Ok(())
    }

    async fn record(&self, project: &str, events: Vec<TraceEvent>, dropped: u64) -> Result<()> {
        self.enqueue(
            &events,
            (dropped > 0).then(|| StatusRow::new(project, dropped, false)),
        )
    }

    async fn append(&self, events: &[TraceEvent]) -> Result<()> {
        self.enqueue(events, None)
    }

    async fn record_status(&self, project: &str, dropped: u64, failed: bool) -> Result<()> {
        self.enqueue(&[], Some(StatusRow::new(project, dropped, failed)))
    }

    async fn status(&self, project: &str) -> Result<TraceStatus> {
        let row = tokio::time::timeout(
            Duration::from_secs(15),
            self.client
                .query(
                    "SELECT sum(dropped) AS dropped, max(failed) AS failed
             FROM actor_trace_status FINAL
             WHERE project_id = {project:String}
               AND received_at > now() - toIntervalSecond({retention:UInt64})",
                )
                .param("project", project)
                .param("retention", self.retention_seconds)
                .fetch_one::<StatusTotal>(),
        )
        .await??;
        Ok(TraceStatus {
            dropped: row.dropped,
            persistence_failed: row.failed != 0 || self.failed.load(Ordering::Relaxed),
        })
    }

    async fn history(&self, project: &str, query: &HistoryQuery) -> Result<TracePage> {
        self.read_history(project, query).await
    }

    async fn replay(&self, project: &str, query: &ReplayQuery) -> Result<TracePage> {
        let mut page = self
            .read_history(
                project,
                &HistoryQuery {
                    limit: query.limit,
                    ..Default::default()
                },
            )
            .await?;
        // A snapshot includes late arrivals without a globally serialized commit cursor.
        page.snapshot = true;
        page.next_cursor = None;
        Ok(page)
    }

    async fn metrics(&self, project: &str, range: &TimeRange) -> Result<OverviewMetrics> {
        self.read_metrics(project, range).await
    }

    async fn queue_waits(
        &self,
        project: &str,
        query: &QueueWaitQuery,
    ) -> Result<Vec<QueueWaitRow>> {
        self.read_queue_waits(project, query).await
    }

    async fn websockets(&self, project: &str, range: &TimeRange) -> Result<Vec<SocketSession>> {
        self.read_websockets(project, range).await
    }
}

struct Report {
    events: Vec<TraceEvent>,
    status: Option<StatusRow>,
    lifecycle: Option<crate::actor_lifecycle::LifecycleEvent>,
    permit: OwnedSemaphorePermit,
}

#[derive(Row, Serialize)]
struct EventRow {
    project_id: String,
    event_id: String,
    started_at_ms: u64,
    actor_name: String,
    actor_id: String,
    request_id: String,
    connection_id: Option<String>,
    kind: String,
    operation: String,
    outcome: String,
    duration_ms: f64,
    queue_wait_ms: Option<f64>,
    host_id: String,
    event_json: String,
}

impl EventRow {
    fn from_event(event: &TraceEvent) -> Result<Self> {
        let trace = &event.trace;
        Ok(Self {
            project_id: trace.project_id.clone(),
            event_id: event.event_id.clone(),
            started_at_ms: trace.started_at_ms,
            actor_name: trace.actor_name.clone(),
            actor_id: trace.actor_id.clone(),
            request_id: trace.request_id.clone(),
            connection_id: trace.connection_id.clone(),
            kind: trace.kind.as_str().into(),
            operation: trace.operation.clone(),
            outcome: trace.outcome.as_str().into(),
            duration_ms: trace.duration_ms,
            queue_wait_ms: trace.queue_wait_ms,
            host_id: event.host_id.clone(),
            event_json: serde_json::to_string(event)?,
        })
    }
}

#[derive(Row, Serialize)]
struct StatusRow {
    project_id: String,
    event_id: String,
    dropped: u64,
    failed: u8,
}

impl StatusRow {
    fn new(project: &str, dropped: u64, failed: bool) -> Self {
        Self {
            project_id: project.into(),
            event_id: uuid::Uuid::new_v4().to_string(),
            dropped,
            failed: failed.into(),
        }
    }
}

#[derive(Row, Deserialize)]
struct StatusTotal {
    dropped: u64,
    failed: u8,
}

#[cfg(test)]
#[path = "../../../tests/unit/request_traces/clickhouse_tests.rs"]
mod tests;
