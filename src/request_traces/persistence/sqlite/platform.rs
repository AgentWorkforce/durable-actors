//! Platform events stored as the collector stores them in ClickHouse, read the same way.

use std::collections::HashMap;

use anyhow::Result;
use opentelemetry::logs::AnyValue;
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Map, Value};

use crate::{
    platform_events::{
        PlatformRecord, deployments,
        sockets::{ALIVE, CLOSED, OPENED, SocketClose},
    },
    request_traces::{
        events::{EVENT_LIMIT, EventQuery},
        otlp::{self, PlatformLog},
        sockets::SocketRecord,
    },
};

pub(super) fn append(
    connection: &mut Connection,
    gateway_id: &str,
    records: &[PlatformRecord],
    retention: usize,
) -> Result<()> {
    let transaction = connection.transaction()?;
    for record in records {
        let attributes = attributes(record);
        let text = |key: &str| {
            attributes
                .get(key)
                .and_then(Value::as_str)
                .unwrap_or_default()
        };
        transaction.execute(
            "INSERT OR REPLACE INTO platform_events
                (project_id, name, gateway_id, actor_name, actor_id, connection_id, at_ms, attributes, body)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                text(otlp::PROJECT_ID),
                record.name,
                gateway_id,
                text(otlp::ACTOR_NAME),
                text(otlp::ACTOR_ID),
                text(otlp::CONNECTION_ID),
                i64::try_from(record.at_ms)?,
                Value::Object(attributes.clone()).to_string(),
                record.body,
            ],
        )?;
    }
    transaction.execute(
        "DELETE FROM platform_events WHERE rowid IN (
            SELECT rowid FROM platform_events ORDER BY at_ms DESC LIMIT -1 OFFSET ?1)",
        [retention as i64],
    )?;
    transaction.commit()?;
    Ok(())
}

pub(super) fn socket_records(
    connection: &mut Connection,
    project_id: &str,
    to_ms: u64,
    open_only: bool,
    limit: usize,
) -> Result<Vec<SocketRecord>> {
    let mut statement = connection.prepare(
        "SELECT opened.connection_id, opened.actor_name, opened.actor_id, opened.gateway_id,
                opened.at_ms, opened.attributes, closed.at_ms, closed.attributes
         FROM platform_events opened
         LEFT JOIN platform_events closed ON closed.project_id = opened.project_id
             AND closed.name = ?5 AND closed.connection_id = opened.connection_id
         WHERE opened.project_id = ?1 AND opened.name = ?4 AND opened.at_ms <= ?2
             AND (?3 = 0 OR closed.connection_id IS NULL)
         ORDER BY opened.at_ms DESC, opened.connection_id
         LIMIT ?6",
    )?;
    let rows = statement.query_map(
        params![
            project_id,
            i64::try_from(to_ms).unwrap_or(i64::MAX),
            open_only,
            OPENED,
            CLOSED,
            limit as i64
        ],
        |row| {
            Ok((
                SocketRecord {
                    connection_id: row.get(0)?,
                    actor_name: row.get(1)?,
                    actor_id: row.get(2)?,
                    gateway_id: row.get(3)?,
                    opened_at_ms: row.get::<_, i64>(4)? as u64,
                    metadata: None,
                    closed: None,
                },
                row.get::<_, String>(5)?,
                row.get::<_, Option<i64>>(6)?,
                row.get::<_, Option<String>>(7)?,
            ))
        },
    )?;
    rows.map(|row| {
        let (mut record, opened, closed_at, closed) = row?;
        record.metadata = parse(&opened)
            .get(otlp::METADATA)
            .and_then(Value::as_str)
            .and_then(|json| serde_json::from_str(json).ok());
        if let (Some(at_ms), Some(closed)) = (closed_at, closed) {
            record.closed = Some((at_ms as u64, close(&parse(&closed))));
        }
        Ok(record)
    })
    .collect()
}

pub(super) fn heartbeats(
    connection: &mut Connection,
    project_id: &str,
) -> Result<HashMap<String, u64>> {
    let mut statement = connection.prepare(
        "SELECT gateway_id, MAX(at_ms) FROM platform_events
         WHERE project_id = ?1 AND name = ?2 GROUP BY gateway_id",
    )?;
    let rows = statement.query_map(params![project_id, ALIVE], |row| {
        Ok((row.get(0)?, row.get::<_, i64>(1)? as u64))
    })?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

/// The table holds one record per actor, connection and event name, so each keeps its latest.
pub(super) fn events(
    connection: &mut Connection,
    project_id: &str,
    query: &EventQuery,
) -> Result<Vec<PlatformLog>> {
    let mut statement = connection.prepare(
        "SELECT name, gateway_id, at_ms, attributes, body FROM platform_events
         WHERE project_id = ?1 AND name IN (SELECT value FROM json_each(?2))
             AND (?3 = '' OR actor_name = ?3) AND (?4 = '' OR actor_id = ?4)
             AND at_ms BETWEEN ?5 AND ?6
         ORDER BY at_ms DESC, name, actor_name, actor_id, connection_id
         LIMIT ?7",
    )?;
    let rows = statement.query_map(
        params![
            project_id,
            serde_json::to_string(&query.names)?,
            query.actor_name.as_deref().unwrap_or_default(),
            query.actor_id.as_deref().unwrap_or_default(),
            i64::try_from(query.from_ms.unwrap_or(0))?,
            i64::try_from(query.to_ms.unwrap_or(u64::MAX)).unwrap_or(i64::MAX),
            EVENT_LIMIT as i64 + 1,
        ],
        |row| {
            Ok(PlatformLog::stored(
                row.get(0)?,
                row.get(1)?,
                row.get::<_, i64>(2)? as u64,
                text_attributes(&row.get::<_, String>(3)?),
                row.get(4)?,
            ))
        },
    )?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

pub(super) fn deployed_contract(
    connection: &mut Connection,
    project_id: &str,
) -> Result<Option<Value>> {
    let body: Option<Option<String>> = connection
        .query_row(
            "SELECT body FROM platform_events WHERE project_id = ?1 AND name IN (?2, ?3)
             ORDER BY at_ms DESC LIMIT 1",
            params![project_id, deployments::PUBLISHED, deployments::REMOVED],
            |row| row.get(0),
        )
        .optional()?;
    Ok(body
        .flatten()
        .and_then(|json| serde_json::from_str(&json).ok()))
}

fn attributes(record: &PlatformRecord) -> Map<String, Value> {
    record
        .attributes
        .iter()
        .map(|(key, value)| {
            let value = match value {
                AnyValue::Int(value) => Value::from(*value),
                AnyValue::Boolean(value) => Value::from(*value),
                AnyValue::String(value) => Value::from(value.as_str()),
                _ => Value::Null,
            };
            (key.to_string(), value)
        })
        .collect()
}

fn parse(json: &str) -> Map<String, Value> {
    serde_json::from_str(json).unwrap_or_default()
}

fn close(attributes: &Map<String, Value>) -> SocketClose {
    let count = |key: &str| {
        attributes
            .get(key)
            .and_then(Value::as_u64)
            .unwrap_or_default()
    };
    SocketClose {
        code: count(otlp::CLOSE_CODE) as u16,
        reason: attributes
            .get(otlp::CLOSE_REASON)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        clean: attributes
            .get(otlp::CLOSE_CLEAN)
            .and_then(Value::as_bool)
            .unwrap_or_default(),
        messages_in: count(otlp::MESSAGES_IN),
        messages_out: count(otlp::MESSAGES_OUT),
        bytes_in: count(otlp::BYTES_IN),
        bytes_out: count(otlp::BYTES_OUT),
        duration_ms: count(otlp::DURATION_MS),
    }
}

/// Attribute values as the analytics store keeps them.
fn text_attributes(json: &str) -> Vec<(String, String)> {
    parse(json)
        .into_iter()
        .filter_map(|(key, value)| match value {
            Value::Null => None,
            Value::String(text) => Some((key, text)),
            other => Some((key, other.to_string())),
        })
        .collect()
}
