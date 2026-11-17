use std::collections::HashMap;

use super::{
    super::cursor::{Cursor, page},
    *,
};
use crate::platform_events::sockets::SocketClose;
use crate::request_traces::{
    RequestTrace, TraceEvent, TraceRecord,
    events::EVENT_LIMIT,
    logs::{ActorLog, LogHistogram, LogHistogramQuery},
    metrics::ClassMetrics,
    sockets::SocketRecord,
};

impl ClickHouseTracePersistence {
    pub(super) async fn read_history(
        &self,
        project: &str,
        query: &HistoryQuery,
    ) -> Result<TracePage> {
        let after = query
            .cursor
            .as_deref()
            .map(|value| Cursor::decode(value, project, query))
            .transpose()?;
        let rows = tokio::time::timeout(
            Duration::from_secs(15),
            self.query(include_str!("spans.sql"), project)
                .param("actor_name", query.actor_name.as_deref().unwrap_or(""))
                .param("actor_id", query.actor_id.as_deref().unwrap_or(""))
                .param("request_id", query.request_id.as_deref().unwrap_or(""))
                .param(
                    "connection_id",
                    query.connection_id.as_deref().unwrap_or(""),
                )
                .param(
                    "outcome",
                    query.outcome.map(|value| value.as_str()).unwrap_or(""),
                )
                .param("from", millis(query.from_ms.unwrap_or(0)))
                .param("to", millis(query.to_ms.unwrap_or(u64::MAX)))
                .param("has_cursor", after.is_some())
                .param("after_time", after.as_ref().map_or(0, |value| value.time))
                .param(
                    "after_id",
                    after.as_ref().map_or("", |value| value.event_id.as_str()),
                )
                .param("limit", query.limit + 1)
                .fetch_all::<SpanRow>(),
        )
        .await??;
        let records = rows
            .into_iter()
            .filter_map(SpanRow::into_event)
            .map(|event| TraceRecord {
                sequence: event.trace.started_at_ms,
                event,
            })
            .collect();
        page(project, query, records)
    }

    pub(super) async fn read_logs(&self, project: &str, query: &LogQuery) -> Result<LogPage> {
        let after = query.after()?;
        let rows = tokio::time::timeout(
            Duration::from_secs(15),
            self.query(include_str!("logs.sql"), project)
                .param("actor_name", query.actor_name.as_deref().unwrap_or(""))
                .param("actor_id", query.actor_id.as_deref().unwrap_or(""))
                .param("request_id", query.request_id.as_deref().unwrap_or(""))
                .param("levels", query.levels().unwrap_or_default().join(","))
                .param("search", query.search.as_deref().unwrap_or(""))
                .param("from", millis(query.from_ms.unwrap_or(0)))
                .param("to", millis(query.to_ms.unwrap_or(u64::MAX)))
                .param("has_cursor", after.is_some())
                .param(
                    "after_time",
                    after.as_ref().map_or(0, |value| value.timestamp_ms),
                )
                .param(
                    "after_id",
                    after.as_ref().map_or("", |value| value.log_id.as_str()),
                )
                .param("limit", query.limit() + 1)
                .fetch_all::<LogRow>(),
        )
        .await??;
        LogPage::new(
            rows.into_iter().map(LogRow::into_log).collect(),
            query.limit(),
        )
    }

    pub(super) async fn read_log_histogram(
        &self,
        project: &str,
        query: &LogHistogramQuery,
    ) -> Result<LogHistogram> {
        let rows = tokio::time::timeout(
            Duration::from_secs(15),
            self.query(include_str!("logs_histogram.sql"), project)
                .param("actor_name", query.actor_name.as_deref().unwrap_or(""))
                .param("actor_id", query.actor_id.as_deref().unwrap_or(""))
                .param("request_id", query.request_id.as_deref().unwrap_or(""))
                .param("search", query.search.as_deref().unwrap_or(""))
                .param("from", millis(query.from_ms))
                .param("to", millis(query.to_ms))
                .param("bucket", millis(query.bucket_ms))
                .fetch_all::<LogBucketRow>(),
        )
        .await??;
        let mut histogram = LogHistogram::empty(query);
        for row in rows {
            histogram.add(row.bucket, &row.level, row.lines);
        }
        Ok(histogram)
    }

    pub(super) async fn read_metrics(
        &self,
        project: &str,
        range: &TimeRange,
    ) -> Result<OverviewMetrics> {
        let rows = tokio::time::timeout(
            Duration::from_secs(15),
            self.query(include_str!("overview.sql"), project)
                .param("from", millis(range.from_ms.unwrap_or(0)))
                .param("to", millis(range.to_ms.unwrap_or(u64::MAX)))
                .fetch_all::<OverviewRow>(),
        )
        .await??;
        let mut result = OverviewMetrics::default();
        for row in rows {
            let metric = ClassMetrics {
                actor_name: row.actor_name,
                count: row.count,
                success: (row.attempts > 0)
                    .then(|| 100.0 * row.completed as f64 / row.attempts as f64),
                p95: row.p95,
                queue_p95: row.queue_p95,
            };
            if row.is_total != 0 {
                result.total = metric;
            } else {
                result.classes.push(metric);
            }
        }
        Ok(result)
    }

    pub(super) async fn read_queue_waits(
        &self,
        project: &str,
        query: &QueueWaitQuery,
    ) -> Result<Vec<QueueWaitRow>> {
        let rows = tokio::time::timeout(
            Duration::from_secs(15),
            self.query(include_str!("queue_waits.sql"), project)
                .param("from", millis(query.from_ms.unwrap_or(0)))
                .param("to", millis(query.to_ms.unwrap_or(u64::MAX)))
                .param("actor_name", query.actor_name.as_deref().unwrap_or(""))
                .fetch_all::<QueueRow>(),
        )
        .await??;
        Ok(rows
            .into_iter()
            .map(|row| QueueWaitRow {
                actor_name: row.actor_name,
                actor_id: row.actor_id,
                admitted: row.admitted,
                average_ms: row.average_ms,
                max_ms: row.max_ms,
            })
            .collect())
    }

    pub(super) async fn read_socket_records(
        &self,
        project: &str,
        to_ms: u64,
        open_only: bool,
        limit: usize,
    ) -> Result<Vec<SocketRecord>> {
        let rows = tokio::time::timeout(
            Duration::from_secs(15),
            self.query(include_str!("socket_records.sql"), project)
                .param("to", to_ms)
                .param("open_only", open_only)
                .param("limit", limit as u64)
                .fetch_all::<SocketRow>(),
        )
        .await??;
        Ok(rows.into_iter().map(SocketRecord::from).collect())
    }

    pub(super) async fn read_gateway_heartbeats(
        &self,
        project: &str,
    ) -> Result<HashMap<String, u64>> {
        let rows = tokio::time::timeout(
            Duration::from_secs(15),
            self.client
                .query(include_str!("gateway_heartbeats.sql"))
                .param("project", project)
                .param("retention", self.retention_seconds)
                .fetch_all::<HeartbeatRow>(),
        )
        .await??;
        Ok(rows
            .into_iter()
            .map(|row| (row.gateway_id, row.alive_at_ms))
            .collect())
    }

    pub(super) async fn read_platform_events(
        &self,
        project: &str,
        query: &EventQuery,
    ) -> Result<Vec<PlatformLog>> {
        let rows = tokio::time::timeout(
            Duration::from_secs(15),
            self.query(include_str!("event_history.sql"), project)
                .param("names", query.names.join(","))
                .param("actor_name", query.actor_name.as_deref().unwrap_or(""))
                .param("actor_id", query.actor_id.as_deref().unwrap_or(""))
                .param("from", millis(query.from_ms.unwrap_or(0)))
                .param("to", millis(query.to_ms.unwrap_or(u64::MAX)))
                .param("limit", EVENT_LIMIT + 1)
                .fetch_all::<EventRow>(),
        )
        .await??;
        Ok(rows.into_iter().map(PlatformLog::from).collect())
    }

    pub(super) async fn read_deployed_contract(
        &self,
        project: &str,
    ) -> Result<Option<serde_json::Value>> {
        let contract = tokio::time::timeout(
            Duration::from_secs(15),
            self.query(include_str!("deployed_contract.sql"), project)
                .fetch_optional::<String>(),
        )
        .await??;
        contract
            .filter(|contract| !contract.is_empty())
            .map(|contract| Ok(serde_json::from_str(&contract)?))
            .transpose()
    }

    fn query(&self, sql: &str, project: &str) -> clickhouse::query::Query {
        self.client
            .query(sql)
            .param("project", project)
            .param("retention", self.retention_seconds)
    }
}

fn millis(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[derive(Row, Deserialize)]
pub(super) struct SpanRow {
    pub event_id: String,
    pub host_id: String,
    pub session_id: String,
    pub project_id: String,
    pub request_id: String,
    pub actor_name: String,
    pub actor_id: String,
    pub kind: String,
    pub operation: String,
    pub connection_id: String,
    pub started_at_ms: u64,
    pub routing_ms: f64,
    pub duration_ms: f64,
    pub queue_wait_ms: Option<f64>,
    pub host_state: String,
    pub outcome: String,
    pub state_version: Option<u64>,
    pub metadata: String,
}

impl SpanRow {
    pub(super) fn into_event(self) -> Option<TraceEvent> {
        Some(TraceEvent {
            event_id: self.event_id,
            host_id: self.host_id,
            session_id: self.session_id,
            trace: RequestTrace {
                state_version: self.state_version,
                project_id: self.project_id,
                request_id: self.request_id,
                actor_name: self.actor_name,
                actor_id: self.actor_id,
                kind: parse(&self.kind)?,
                operation: self.operation,
                connection_id: Some(self.connection_id).filter(|id| !id.is_empty()),
                started_at_ms: self.started_at_ms,
                routing_ms: self.routing_ms,
                duration_ms: self.duration_ms,
                queue_wait_ms: self.queue_wait_ms,
                host_state: parse(&self.host_state).unwrap_or_default(),
                outcome: parse(&self.outcome)?,
                metadata: serde_json::from_str(&self.metadata).ok(),
            },
        })
    }
}

#[derive(Row, Deserialize)]
struct LogBucketRow {
    bucket: u64,
    level: String,
    lines: u64,
}

#[derive(Row, Deserialize)]
pub(super) struct LogRow {
    pub log_id: String,
    pub project_id: String,
    pub actor_name: String,
    pub actor_id: String,
    pub host_id: String,
    pub request_id: String,
    pub timestamp_ms: u64,
    pub level: String,
    pub message: String,
}

impl LogRow {
    pub(super) fn into_log(self) -> ActorLog {
        ActorLog {
            log_id: self.log_id,
            project_id: self.project_id,
            actor_name: self.actor_name,
            actor_id: self.actor_id,
            host_id: self.host_id,
            request_id: Some(self.request_id).filter(|id| !id.is_empty()),
            timestamp_ms: self.timestamp_ms,
            level: self.level,
            message: self.message,
        }
    }
}

fn parse<T: serde::de::DeserializeOwned>(value: &str) -> Option<T> {
    serde_json::from_value(serde_json::Value::String(value.to_owned())).ok()
}

#[derive(Row, Deserialize)]
struct OverviewRow {
    actor_name: String,
    count: u64,
    attempts: u64,
    completed: u64,
    p95: Option<f64>,
    queue_p95: Option<f64>,
    is_total: u8,
}

#[derive(Row, Deserialize)]
struct QueueRow {
    actor_name: String,
    actor_id: String,
    admitted: u64,
    average_ms: f64,
    max_ms: f64,
}

/// The exporter stores attribute values as text, so the close's numbers arrive as strings.
#[derive(Row, Deserialize)]
struct SocketRow {
    connection_id: String,
    actor_name: String,
    actor_id: String,
    gateway_id: String,
    opened_at_ms: u64,
    metadata: String,
    closed_at_ms: u64,
    close_code: String,
    close_reason: String,
    close_clean: String,
    messages_in: String,
    messages_out: String,
    bytes_in: String,
    bytes_out: String,
    duration_ms: String,
}

impl From<SocketRow> for SocketRecord {
    fn from(row: SocketRow) -> Self {
        let number = |text: &str| text.parse().unwrap_or_default();
        let closed = (row.closed_at_ms > 0).then(|| {
            (
                row.closed_at_ms,
                SocketClose {
                    code: row.close_code.parse().unwrap_or_default(),
                    reason: row.close_reason,
                    clean: row.close_clean == "true",
                    messages_in: number(&row.messages_in),
                    messages_out: number(&row.messages_out),
                    bytes_in: number(&row.bytes_in),
                    bytes_out: number(&row.bytes_out),
                    duration_ms: number(&row.duration_ms),
                },
            )
        });
        Self {
            connection_id: row.connection_id,
            actor_name: row.actor_name,
            actor_id: row.actor_id,
            gateway_id: row.gateway_id,
            opened_at_ms: row.opened_at_ms,
            metadata: serde_json::from_str(&row.metadata).ok(),
            closed,
        }
    }
}

#[derive(Row, Deserialize)]
struct HeartbeatRow {
    gateway_id: String,
    alive_at_ms: u64,
}

#[derive(Row, Deserialize)]
struct EventRow {
    name: String,
    gateway_id: String,
    at_ms: u64,
    attributes: Vec<(String, String)>,
    body: String,
}

impl From<EventRow> for PlatformLog {
    fn from(row: EventRow) -> Self {
        Self::stored(
            row.name,
            row.gateway_id,
            row.at_ms,
            row.attributes,
            Some(row.body),
        )
    }
}
