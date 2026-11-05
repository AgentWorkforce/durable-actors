use super::super::cursor::{Metadata, ReplayPage};
use crate::request_traces::{TracePage, TraceRecord, replay::ReplayQuery};
use anyhow::Result;
use rusqlite::{Connection, Transaction, params};

pub(super) fn query(
    connection: &mut Connection,
    project_id: &str,
    query: &ReplayQuery,
) -> Result<TracePage> {
    let transaction = connection.transaction()?;
    let page = ReplayPage::new(project_id, query, metadata(&transaction, project_id)?)?;
    let records = select(
        &transaction,
        project_id,
        query.limit,
        page.after(),
        page.head(),
    )?;
    page.finish(query.limit, records)
}

pub(super) fn metadata(transaction: &Transaction<'_>, project_id: &str) -> Result<Metadata> {
    Ok(transaction.query_row(
        "SELECT generation, COALESCE(p.pruned, 0), COALESCE(p.head, 0)
         FROM trace_meta LEFT JOIN trace_projects p ON p.project_id = ?1",
        [project_id],
        |row| {
            Ok(Metadata {
                generation: row.get(0)?,
                pruned: row.get::<_, i64>(1)? as u64,
                head: row.get::<_, i64>(2)? as u64,
            })
        },
    )?)
}

fn select(
    transaction: &Transaction<'_>,
    project_id: &str,
    limit: usize,
    after: Option<u64>,
    head: u64,
) -> Result<Vec<TraceRecord>> {
    let sql = if after.is_some() {
        "SELECT position, event FROM traces
         WHERE project_id = ?4 AND position > ?1 AND position <= ?2
         ORDER BY position ASC
         LIMIT ?3"
    } else {
        "SELECT position, event FROM traces
         WHERE project_id = ?4 AND position > ?1 AND position <= ?2
         ORDER BY started_at_ms DESC, position DESC
         LIMIT ?3"
    };
    let mut statement = transaction.prepare(sql)?;
    let rows = statement.query_map(
        params![
            after.unwrap_or(0) as i64,
            head as i64,
            (limit + 1) as i64,
            project_id
        ],
        |row| Ok((row.get::<_, i64>(0)? as u64, row.get::<_, String>(1)?)),
    )?;
    rows.map(|row| {
        let (sequence, json) = row?;
        Ok(TraceRecord {
            sequence,
            event: serde_json::from_str(&json)?,
        })
    })
    .collect()
}
