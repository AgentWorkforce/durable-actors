use super::rows::{self, Rows};
use crate::{
    actor::{ActorDatabase, DatabaseCall, DatabaseResult, SqlValue},
    litestream::{Litestream, Replicator},
};
use anyhow::{Context, Result};
use serde_json::{Value, value::RawValue};
use std::sync::{Arc, LazyLock};

static CAPTURE: LazyLock<Arc<Litestream>> = LazyLock::new(|| Arc::new(Litestream::default()));

pub async fn replication() -> Arc<dyn Replicator> {
    CAPTURE.clone()
}

pub async fn fields(database: &dyn ActorDatabase) -> Result<Value> {
    let rows = Rows::default();
    let call = DatabaseCall::Fields { names: None };
    match database.call(call, rows.sink()).await? {
        DatabaseResult::Rows => rows.collected().fields(),
        other => anyhow::bail!("unexpected fields result {other:?}"),
    }
}

pub async fn write_fields(database: &dyn ActorDatabase, fields: Value) -> Result<()> {
    persist_fields(database, fields, false).await?;
    database.call(DatabaseCall::Commit, rows::ignored()).await?;
    Ok(())
}

pub async fn persist_fields(
    database: &dyn ActorDatabase,
    fields: Value,
    immediate: bool,
) -> Result<()> {
    for (name, value) in fields.as_object().context("object fields")? {
        let call = DatabaseCall::PersistField {
            name: name.clone(),
            value: RawValue::from_string(value.to_string())?,
            immediate,
        };
        database.call(call, rows::ignored()).await?;
    }
    Ok(())
}

pub async fn exec(database: &dyn ActorDatabase, sql: &str) -> Result<Vec<Vec<SqlValue>>> {
    let rows = Rows::default();
    let call = DatabaseCall::Exec {
        sql: sql.into(),
        bindings: vec![],
        immediate: false,
    };
    match database.call(call, rows.sink()).await? {
        DatabaseResult::Rows => Ok(rows.collected().rows),
        other => anyhow::bail!("unexpected exec result {other:?}"),
    }
}
