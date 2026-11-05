use std::{sync::Arc, time::Duration};

use anyhow::{Result, ensure};
use async_trait::async_trait;
use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError, mpsc};
use tokio_util::sync::CancellationToken;

use super::TraceQueries;
use crate::request_traces::{
    TracePage,
    history::HistoryQuery,
    logs::{LogHistogram, LogHistogramQuery, LogPage, LogQuery},
    metrics::{OverviewMetrics, QueueWaitQuery, QueueWaitRow, SocketSession, TimeRange},
    replay::ReplayQuery,
};

mod ingestion;
mod lifecycle;
mod queries;

// Lifecycle rows stay queued while ClickHouse is slow; the bound covers memory, not report count.
const MAX_QUEUED_ROWS: usize = 100_000;

#[derive(Clone)]
pub struct ClickHouseConfig {
    pub url: String,
    pub database: String,
    pub user: String,
    pub password: String,
}

/// Reads request spans and actor logs the collector writes, and writes actor lifecycle events.
pub(crate) struct ClickHouseTracePersistence {
    client: Client,
    sender: mpsc::UnboundedSender<Report>,
    queued_rows: Arc<Semaphore>,
    retention_seconds: u64,
}

impl ClickHouseTracePersistence {
    pub(crate) fn start(
        config: ClickHouseConfig,
        retention: Duration,
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
        tokio::spawn(ingestion::run(
            client.clone(),
            receiver,
            retention.as_secs(),
            stop,
        ));
        Ok(Self {
            client,
            sender,
            queued_rows: Arc::new(Semaphore::new(MAX_QUEUED_ROWS)),
            retention_seconds: retention.as_secs(),
        })
    }

    fn send(&self, lifecycle: crate::actor_lifecycle::LifecycleEvent) -> Result<()> {
        let permit = self
            .queued_rows
            .clone()
            .try_acquire_owned()
            .map_err(|error| {
                anyhow::anyhow!(
                    "ClickHouse lifecycle queue unavailable: {}",
                    match error {
                        TryAcquireError::NoPermits => "full",
                        TryAcquireError::Closed => "closed",
                    }
                )
            })?;
        self.sender
            .send(Report { lifecycle, permit })
            .map_err(|_| anyhow::anyhow!("ClickHouse lifecycle queue unavailable: closed"))
    }
}

#[async_trait]
impl TraceQueries for ClickHouseTracePersistence {
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

    async fn logs(&self, project: &str, query: &LogQuery) -> Result<LogPage> {
        self.read_logs(project, query).await
    }

    async fn log_histogram(
        &self,
        project: &str,
        query: &LogHistogramQuery,
    ) -> Result<LogHistogram> {
        self.read_log_histogram(project, query).await
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
    lifecycle: crate::actor_lifecycle::LifecycleEvent,
    permit: OwnedSemaphorePermit,
}

#[cfg(test)]
#[path = "../../../tests/unit/request_traces/clickhouse_tests.rs"]
mod tests;
