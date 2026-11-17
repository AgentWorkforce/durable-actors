use std::time::Duration;

use anyhow::{Result, ensure};
use async_trait::async_trait;
use clickhouse::{Client, Row};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use super::TraceQueries;
use crate::request_traces::{
    TracePage,
    events::EventQuery,
    history::HistoryQuery,
    logs::{LogHistogram, LogHistogramQuery, LogPage, LogQuery},
    metrics::{OverviewMetrics, QueueWaitQuery, QueueWaitRow, TimeRange},
    otlp::PlatformLog,
};

mod lifecycle;
mod queries;
mod schema;

#[derive(Clone)]
pub struct ClickHouseConfig {
    pub url: String,
    pub database: String,
    pub user: String,
    pub password: String,
}

/// Reads what the collector writes: request spans, actor logs and platform events, including
/// lifecycle reports.
pub(crate) struct ClickHouseTracePersistence {
    client: Client,
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
        tokio::spawn(schema::create(client.clone(), retention.as_secs(), stop));
        Ok(Self {
            client,
            retention_seconds: retention.as_secs(),
        })
    }
}

#[async_trait]
impl TraceQueries for ClickHouseTracePersistence {
    async fn history(&self, project: &str, query: &HistoryQuery) -> Result<TracePage> {
        self.read_history(project, query).await
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

    async fn socket_records(
        &self,
        project: &str,
        to_ms: u64,
        open_only: bool,
        limit: usize,
    ) -> Result<Vec<crate::request_traces::sockets::SocketRecord>> {
        self.read_socket_records(project, to_ms, open_only, limit)
            .await
    }

    async fn gateway_heartbeats(
        &self,
        project_id: &str,
    ) -> Result<std::collections::HashMap<String, u64>> {
        self.read_gateway_heartbeats(project_id).await
    }

    async fn platform_events(
        &self,
        project_id: &str,
        query: &EventQuery,
    ) -> Result<Vec<PlatformLog>> {
        self.read_platform_events(project_id, query).await
    }

    async fn deployed_contract(&self, project_id: &str) -> Result<Option<serde_json::Value>> {
        self.read_deployed_contract(project_id).await
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/request_traces/clickhouse_tests.rs"]
mod tests;
