use anyhow::Result;
use async_trait::async_trait;
use serde::Serialize;
use serde_json::value::RawValue;
use std::collections::BTreeMap;

#[async_trait]
pub trait ActorDatabase: Send + Sync {
    /// A statement and a field read write their rows to `rows` and return [`DatabaseResult::Rows`].
    async fn call(&self, call: DatabaseCall, rows: Box<dyn RowSink>) -> Result<DatabaseResult>;
}

/// Receives the rows of a call as SQLite produces them, so a result is never held whole.
pub trait RowSink: Send {
    fn columns(&mut self, names: &[&str]) -> Result<()>;
    fn row(&mut self, cells: &[SqlCell<'_>]) -> Result<()>;
    /// Called once after the last row.
    fn finish(&mut self) -> Result<()>;
}

/// A value borrowed from the row SQLite is positioned on.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SqlCell<'a> {
    Null,
    Integer(i64),
    Real(f64),
    Text(&'a [u8]),
    Blob(&'a [u8]),
}

#[derive(Debug)]
pub enum DatabaseCall {
    Exec {
        sql: String,
        bindings: Vec<SqlValue>,
        immediate: bool,
    },
    /// Every stored field, or only `names` when given: a row of its name and its JSON for each.
    Fields {
        names: Option<Vec<String>>,
    },
    InitializeFields {
        fields: BTreeMap<String, Box<RawValue>>,
    },
    PersistField {
        name: String,
        value: Box<RawValue>,
        immediate: bool,
    },
    DeleteField {
        name: String,
        immediate: bool,
    },
    Alarm,
    SetAlarm {
        timestamp_ms: i64,
        immediate: bool,
    },
    DeleteAlarm {
        immediate: bool,
    },
    Commit,
    Rollback,
}

impl DatabaseCall {
    pub(crate) fn immediate(&self) -> bool {
        matches!(
            self,
            Self::Exec {
                immediate: true,
                ..
            } | Self::PersistField {
                immediate: true,
                ..
            } | Self::DeleteField {
                immediate: true,
                ..
            } | Self::SetAlarm {
                immediate: true,
                ..
            } | Self::DeleteAlarm {
                immediate: true,
                ..
            }
        )
    }
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DatabaseResult {
    /// The rows went to the call's [`RowSink`].
    Rows,
    Alarm {
        timestamp_ms: Option<i64>,
    },
    Done,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SqlValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}
