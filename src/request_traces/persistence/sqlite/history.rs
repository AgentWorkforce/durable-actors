use anyhow::Result;
use rusqlite::{Connection, params_from_iter, types::Value};

use super::super::cursor::{Cursor, page};
use crate::request_traces::{
    RequestOutcome, TracePage, TraceRecord,
    history::{HistoryQuery, RequestHistogram, RequestHistogramQuery},
};

pub(super) fn query(
    connection: &mut Connection,
    project_id: &str,
    query: &HistoryQuery,
) -> Result<TracePage> {
    let after = query
        .cursor
        .as_deref()
        .map(|value| Cursor::decode(value, project_id, query))
        .transpose()?;
    let records = select(connection, project_id, query, after.as_ref())?;
    page(project_id, query, records)
}

pub(super) fn histogram(
    connection: &mut Connection,
    project_id: &str,
    query: &RequestHistogramQuery,
) -> Result<RequestHistogram> {
    let (clauses, filter_values) = filters(project_id, &query.requests());
    let mut values = vec![
        Value::Integer(query.from_ms as i64),
        Value::Integer(query.bucket_ms as i64),
    ];
    values.extend(filter_values);
    let wait = "json_extract(event, '$.queueWaitMs')";
    let mut statement = connection.prepare(&format!(
        "SELECT (started_at_ms - ?) / ?, outcome, count(*), coalesce(sum({wait}), 0), count({wait}),
            coalesce(sum(json_extract(event, '$.durationMs') - coalesce({wait}, 0)), 0)
        FROM traces WHERE {} GROUP BY 1, 2",
        clauses.join(" AND ")
    ))?;
    let rows = statement.query_map(params_from_iter(values), |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, f64>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, f64>(5)?,
        ))
    })?;
    let mut histogram = RequestHistogram::empty(query);
    for row in rows {
        let (bucket, outcome, requests, queue_wait_ms, queue_waits, run_ms) = row?;
        if let (Some(counts), Some(outcome)) = (
            histogram.bucket(u64::try_from(bucket)?),
            RequestOutcome::parse(&outcome),
        ) {
            counts.add_outcome(
                outcome,
                u64::try_from(requests)?,
                queue_wait_ms,
                u64::try_from(queue_waits)?,
                run_ms,
            );
        }
    }
    Ok(histogram)
}

fn select(
    connection: &Connection,
    project_id: &str,
    query: &HistoryQuery,
    after: Option<&Cursor>,
) -> Result<Vec<TraceRecord>> {
    let (mut clauses, mut values) = filters(project_id, query);
    if let Some(after) = after {
        clauses.push("(started_at_ms, event_id) < (?, ?)");
        values.extend([
            Value::Integer(after.time as i64),
            Value::Text(after.event_id.clone()),
        ]);
    }
    values.push(Value::Integer((query.limit + 1) as i64));
    let mut statement = connection.prepare(&format!(
        "SELECT event FROM traces WHERE {} ORDER BY started_at_ms DESC, event_id DESC LIMIT ?",
        clauses.join(" AND ")
    ))?;
    let rows = statement.query_map(params_from_iter(values), |row| row.get::<_, String>(0))?;
    rows.map(|json| {
        let event: crate::request_traces::TraceEvent = serde_json::from_str(&json?)?;
        Ok(TraceRecord {
            sequence: event.trace.started_at_ms,
            event,
        })
    })
    .collect()
}

fn filters(project_id: &str, query: &HistoryQuery) -> (Vec<&'static str>, Vec<Value>) {
    let mut clauses = vec!["project_id = ?"];
    let mut values = vec![Value::Text(project_id.into())];
    for (clause, value) in [
        (
            "json_extract(event, '$.requestId') = ?",
            query.request_id.clone().map(Value::Text),
        ),
        (
            "json_extract(event, '$.connectionId') = ?",
            query.connection_id.clone().map(Value::Text),
        ),
        ("actor_name = ?", query.actor_name.clone().map(Value::Text)),
        ("actor_id = ?", query.actor_id.clone().map(Value::Text)),
        (
            "outcome = ?",
            query
                .outcome
                .map(|outcome| Value::Text(outcome.as_str().into())),
        ),
        (
            "started_at_ms >= ?",
            query.from_ms.map(|v| Value::Integer(v as i64)),
        ),
        (
            "started_at_ms <= ?",
            query.to_ms.map(|v| Value::Integer(v as i64)),
        ),
    ] {
        if let Some(value) = value {
            clauses.push(clause);
            values.push(value);
        }
    }
    (clauses, values)
}
