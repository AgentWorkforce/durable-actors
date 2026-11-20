use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result};
use async_trait::async_trait;
use rusqlite::{Connection, Transaction, params};

use super::TraceQueries;
use crate::{
    platform_events::PlatformRecord,
    request_traces::{
        TraceEvent, TracePage,
        events::EventQuery,
        history::{HistoryQuery, RequestHistogram, RequestHistogramQuery},
        logs::{ActorLog, LogHistogram, LogHistogramQuery, LogPage, LogQuery},
        metrics::{OverviewMetrics, QueueWaitQuery, QueueWaitRow, TimeRange},
        otlp::PlatformLog,
    },
};

mod history;
mod logs;
mod metrics;
mod platform;

const LOCAL_RETENTION: usize = 10_000;
// Local data is disposable: a database written by another version starts over.
const SCHEMA_VERSION: u32 = 6;

pub(crate) struct SqliteTracePersistence {
    path: PathBuf,
    retention: usize,
    connection: Arc<Mutex<Option<Connection>>>,
}

impl SqliteTracePersistence {
    pub(crate) fn new(path: PathBuf) -> Self {
        Self {
            path,
            retention: LOCAL_RETENTION,
            connection: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn in_memory() -> Self {
        Self::new(":memory:".into())
    }

    async fn run<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let path = self.path.clone();
        let shared = self.connection.clone();
        tokio::task::spawn_blocking(move || {
            let mut shared = shared.lock().unwrap();
            if shared.is_none() {
                *shared = Some(open(&path)?);
            }
            operation(shared.as_mut().unwrap())
        })
        .await?
    }
}

impl SqliteTracePersistence {
    pub(crate) async fn append(&self, events: &[TraceEvent]) -> Result<()> {
        let events = events.to_vec();
        let retention = self.retention;
        self.run(move |connection| {
            let transaction = connection.transaction()?;
            insert_events(&transaction, &events)?;
            transaction.execute(
                "DELETE FROM traces WHERE event_id IN (
                    SELECT event_id FROM traces ORDER BY started_at_ms DESC, event_id DESC LIMIT -1 OFFSET ?1)",
                [retention as i64],
            )?;
            transaction.commit()?;
            Ok(())
        })
        .await
    }

    pub(crate) async fn append_logs(&self, records: Vec<ActorLog>) -> Result<()> {
        let retention = self.retention;
        self.run(move |connection| logs::append(connection, &records, retention))
            .await
    }

    /// Stores platform events as the collector would, for local development.
    pub(crate) async fn append_platform(
        &self,
        gateway_id: String,
        records: Vec<PlatformRecord>,
    ) -> Result<()> {
        let retention = self.retention;
        self.run(move |connection| platform::append(connection, &gateway_id, &records, retention))
            .await
    }
}

#[async_trait]
impl TraceQueries for SqliteTracePersistence {
    async fn metrics(&self, project_id: &str, query: &TimeRange) -> Result<OverviewMetrics> {
        let project_id = project_id.to_owned();
        let query = query.clone();
        self.run(move |connection| metrics::overview(connection, &project_id, &query))
            .await
    }

    async fn queue_waits(
        &self,
        project_id: &str,
        query: &QueueWaitQuery,
    ) -> Result<Vec<QueueWaitRow>> {
        let project_id = project_id.to_owned();
        let query = query.clone();
        self.run(move |connection| metrics::queue_waits(connection, &project_id, &query))
            .await
    }

    async fn socket_records(
        &self,
        project_id: &str,
        to_ms: u64,
        open_only: bool,
        limit: usize,
    ) -> Result<Vec<crate::request_traces::sockets::SocketRecord>> {
        let project_id = project_id.to_owned();
        self.run(move |connection| {
            platform::socket_records(connection, &project_id, to_ms, open_only, limit)
        })
        .await
    }

    async fn gateway_heartbeats(
        &self,
        project_id: &str,
    ) -> Result<std::collections::HashMap<String, u64>> {
        let project_id = project_id.to_owned();
        self.run(move |connection| platform::heartbeats(connection, &project_id))
            .await
    }

    async fn platform_events(
        &self,
        project_id: &str,
        query: &EventQuery,
    ) -> Result<Vec<PlatformLog>> {
        let (project_id, query) = (project_id.to_owned(), query.clone());
        self.run(move |connection| platform::events(connection, &project_id, &query))
            .await
    }

    async fn deployed_contract(&self, project_id: &str) -> Result<Option<serde_json::Value>> {
        let project_id = project_id.to_owned();
        self.run(move |connection| platform::deployed_contract(connection, &project_id))
            .await
    }

    async fn history(&self, project_id: &str, query: &HistoryQuery) -> Result<TracePage> {
        let project_id = project_id.to_owned();
        let query = query.clone();
        self.run(move |connection| history::query(connection, &project_id, &query))
            .await
    }

    async fn request_histogram(
        &self,
        project_id: &str,
        query: &RequestHistogramQuery,
    ) -> Result<RequestHistogram> {
        let project_id = project_id.to_owned();
        let query = query.clone();
        self.run(move |connection| history::histogram(connection, &project_id, &query))
            .await
    }

    async fn logs(&self, project_id: &str, query: &LogQuery) -> Result<LogPage> {
        let project_id = project_id.to_owned();
        let query = query.clone();
        self.run(move |connection| logs::query(connection, &project_id, &query))
            .await
    }

    async fn log_histogram(
        &self,
        project_id: &str,
        query: &LogHistogramQuery,
    ) -> Result<LogHistogram> {
        let project_id = project_id.to_owned();
        let query = query.clone();
        self.run(move |connection| logs::histogram(connection, &project_id, &query))
            .await
    }
}

fn open(path: &Path) -> Result<Connection> {
    let connection = Connection::open(path).context("open request trace database")?;
    connection.busy_timeout(Duration::from_secs(5))?;
    let version: u32 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if version == SCHEMA_VERSION {
        return Ok(connection);
    }
    let empty: bool =
        connection.query_row("SELECT COUNT(*) = 0 FROM sqlite_master", [], |row| {
            row.get(0)
        })?;
    if !empty {
        drop(connection);
        std::fs::remove_file(path).context("remove an outdated request trace database")?;
        return open(path);
    }
    connection.execute_batch(include_str!("sqlite/schema.sql"))?;
    connection.pragma_update(None, "user_version", SCHEMA_VERSION)?;
    Ok(connection)
}

fn insert_events(transaction: &Transaction<'_>, events: &[TraceEvent]) -> Result<()> {
    let mut statement = transaction.prepare("INSERT INTO traces (event_id, event, started_at_ms, actor_name, actor_id, outcome, project_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) ON CONFLICT (event_id) DO NOTHING")?;
    for event in events {
        statement.execute(params![
            event.event_id,
            serde_json::to_string(event)?,
            i64::try_from(event.trace.started_at_ms)?,
            event.trace.actor_name,
            event.trace.actor_id,
            event.trace.outcome.as_str(),
            event.trace.project_id
        ])?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "../../../tests/unit/request_traces/persistence/sqlite/fixtures.rs"]
mod fixtures;

#[cfg(test)]
#[path = "../../../tests/unit/request_traces/persistence/sqlite/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "../../../tests/unit/request_traces/persistence/sqlite/history_tests.rs"]
mod history_tests;

#[cfg(test)]
#[path = "../../../tests/unit/request_traces/persistence/sqlite/metrics_tests.rs"]
mod metrics_tests;
