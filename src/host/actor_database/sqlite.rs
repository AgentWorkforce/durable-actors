use crate::actor::{DatabaseCall, DatabaseResult, RowSink, SqlCell, SqlValue};
use anyhow::{Result, anyhow, bail, ensure};
use rusqlite::{
    Connection, ToSql,
    hooks::{AuthAction, AuthContext, Authorization},
    types::{ToSqlOutput, ValueRef},
};
use serde_json::value::RawValue;
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

const FILES: &str = "actor invocations own SQLite database files";
const RESERVED: &str = "SQLite runtime table names are reserved";
const PRAGMAS: &str = "this SQLite pragma is managed by the actor runtime";
const ALLOWED_PRAGMAS: [&str; 10] = [
    "table_info",
    "table_xinfo",
    "index_info",
    "index_xinfo",
    "index_list",
    "foreign_key_list",
    "foreign_key_check",
    "integrity_check",
    "quick_check",
    "user_version",
];
// What reads stored state parses it with serde_json, which reads this many levels of arrays and objects.
const MAX_NESTING: usize = 127;

pub(crate) struct SqliteDatabase {
    engine: Arc<Mutex<Engine>>,
}

#[derive(Debug)]
pub(crate) struct Executed {
    pub result: Result<DatabaseResult>,
    pub committed: bool,
}

impl SqliteDatabase {
    pub(crate) async fn open(path: PathBuf) -> Result<Self> {
        let engine = tokio::task::spawn_blocking(move || Engine::open(path)).await??;
        Ok(Self {
            engine: Arc::new(Mutex::new(engine)),
        })
    }

    pub(crate) async fn call(
        &self,
        call: DatabaseCall,
        mut rows: Box<dyn RowSink>,
    ) -> Result<Executed> {
        let engine = self.engine.clone();
        tokio::task::spawn_blocking(move || {
            engine
                .lock()
                .map_err(|_| anyhow!("actor database is poisoned"))?
                .call(call, rows.as_mut())
        })
        .await?
    }

    /// The committed database's size, or `None` while an actor transaction is open.
    pub(crate) async fn size(&self) -> Result<Option<u64>> {
        let engine = self.engine.clone();
        tokio::task::spawn_blocking(move || {
            engine
                .lock()
                .map_err(|_| anyhow!("actor database is poisoned"))?
                .size()
        })
        .await?
    }
}

struct Engine {
    connection: Connection,
    committed: Arc<AtomicBool>,
}

impl Engine {
    fn open(path: PathBuf) -> Result<Self> {
        let connection = Connection::open(path)?;
        // Commits are durable once replicated, so the local WAL need not fsync each one.
        connection.execute_batch(
            "PRAGMA busy_timeout = 5000; PRAGMA foreign_keys = ON; PRAGMA journal_mode = WAL;
            PRAGMA wal_autocheckpoint = 0; PRAGMA synchronous = NORMAL",
        )?;
        let committed = Arc::new(AtomicBool::new(false));
        let commit = committed.clone();
        connection.commit_hook(Some(move || {
            commit.store(true, Ordering::Relaxed);
            false
        }))?;
        let rollback = committed.clone();
        connection.rollback_hook(Some(move || rollback.store(false, Ordering::Relaxed)))?;
        Ok(Self {
            connection,
            committed,
        })
    }

    fn call(&mut self, call: DatabaseCall, rows: &mut dyn RowSink) -> Result<Executed> {
        let result = match call {
            DatabaseCall::Commit => self.finish("COMMIT"),
            DatabaseCall::Rollback => self.finish("ROLLBACK"),
            DatabaseCall::Fields { names } => self.fields(names.as_deref(), rows),
            DatabaseCall::Alarm
            | DatabaseCall::SetAlarm { .. }
            | DatabaseCall::DeleteAlarm { .. } => {
                unreachable!("alarm calls are handled by the control plane")
            }
            call => self.write(call, rows),
        };
        // A statement such as INSERT OR FAIL can commit rows even when it returns an error.
        let committed =
            self.committed.swap(false, Ordering::Relaxed) && self.connection.is_autocommit();
        Ok(Executed { result, committed })
    }

    fn size(&self) -> Result<Option<u64>> {
        if !self.connection.is_autocommit() {
            return Ok(None);
        }
        let pages: i64 = self
            .connection
            .query_row("PRAGMA page_count", [], |row| row.get(0))?;
        let page_size: i64 = self
            .connection
            .query_row("PRAGMA page_size", [], |row| row.get(0))?;
        Ok(Some(u64::try_from(pages * page_size)?))
    }

    fn write(&self, call: DatabaseCall, rows: &mut dyn RowSink) -> Result<DatabaseResult> {
        match call {
            DatabaseCall::Exec { sql, bindings, .. } => return self.exec(&sql, &bindings, rows),
            DatabaseCall::InitializeFields { fields } => self.initialize(&fields)?,
            DatabaseCall::PersistField { name, value, .. } => self.persist(&name, &value)?,
            DatabaseCall::DeleteField { name, .. } => self.delete(&name)?,
            DatabaseCall::Fields { .. }
            | DatabaseCall::Alarm
            | DatabaseCall::SetAlarm { .. }
            | DatabaseCall::DeleteAlarm { .. }
            | DatabaseCall::Commit
            | DatabaseCall::Rollback => {
                unreachable!("read and lifecycle calls are not writes")
            }
        }
        Ok(DatabaseResult::Done)
    }

    fn exec(
        &self,
        sql: &str,
        bindings: &[SqlValue],
        rows: &mut dyn RowSink,
    ) -> Result<DatabaseResult> {
        let statement = self.prepare_actor_statement(sql)?;
        // The commit hook misses some writes, such as VACUUM, so a statement that can write counts
        // as a commit outside a transaction. The rollback hook clears it if the statement fails.
        if !statement.readonly() {
            self.committed.store(true, Ordering::Relaxed);
        }
        send_rows(statement, rusqlite::params_from_iter(bindings), rows)
    }

    fn prepare_actor_statement(&self, sql: &str) -> Result<rusqlite::Statement<'_>> {
        let denied = Arc::new(Mutex::new(None));
        self.connection
            .authorizer(Some(authorize(denied.clone())))?;
        let prepared = self.connection.prepare(sql);
        self.connection
            .authorizer(None::<fn(AuthContext<'_>) -> Authorization>)?;
        if let Some(reason) = denied
            .lock()
            .map_err(|_| anyhow!("authorizer poisoned"))?
            .take()
        {
            bail!(reason);
        }
        match prepared {
            Err(rusqlite::Error::MultipleStatement) => {
                bail!("actor database exec accepts one SQL statement")
            }
            prepared => Ok(prepared?),
        }
    }

    fn fields(&self, names: Option<&[String]>, rows: &mut dyn RowSink) -> Result<DatabaseResult> {
        let statement = self.connection.prepare(
            "SELECT name, value FROM __terse_fields
            WHERE ?1 IS NULL OR name IN (SELECT value FROM json_each(?1))",
        )?;
        let names = names.map(serde_json::to_string).transpose()?;
        send_rows(statement, [names], rows)
    }

    fn initialize(&self, fields: &BTreeMap<String, Box<RawValue>>) -> Result<()> {
        fields.values().try_for_each(|value| storable(value))?;
        self.unchecked(|| {
            let mut statement = self.connection.prepare(
                "INSERT INTO __terse_fields (name, value) VALUES (?, ?) ON CONFLICT(name) DO NOTHING",
            )?;
            for (name, value) in fields {
                statement.execute((name, value.get()))?;
            }
            Ok(())
        })
    }

    // The sizes are compared first because SQLite reads a size without reading the value. Comparing the texts
    // reads the whole stored value into memory beside the new one.
    fn persist(&self, name: &str, value: &RawValue) -> Result<()> {
        storable(value)?;
        self.unchecked(|| {
            self.connection.execute(
                "INSERT INTO __terse_fields (name, value) VALUES (?, ?)
                ON CONFLICT(name) DO UPDATE SET value = excluded.value
                WHERE octet_length(value) <> octet_length(excluded.value) OR value <> excluded.value",
                (name, value.get()),
            )?;
            Ok(())
        })
    }

    /// Writes fields without the table's `json_valid` check. A field's value is JSON by its type, and SQLite
    /// allocates three times the value to find that out again.
    fn unchecked(&self, write: impl FnOnce() -> Result<()>) -> Result<()> {
        self.ignore_checks(true)?;
        let written = write();
        self.ignore_checks(false)?;
        written
    }

    fn ignore_checks(&self, ignored: bool) -> Result<()> {
        self.connection
            .pragma_update(None, "ignore_check_constraints", ignored)?;
        Ok(())
    }

    fn delete(&self, name: &str) -> Result<()> {
        self.connection
            .execute("DELETE FROM __terse_fields WHERE name = ?", [name])?;
        Ok(())
    }

    fn finish(&self, sql: &str) -> Result<DatabaseResult> {
        if !self.connection.is_autocommit() {
            self.connection.execute_batch(sql)?;
        }
        Ok(DatabaseResult::Done)
    }
}

fn storable(value: &RawValue) -> Result<()> {
    ensure!(
        nesting(value.get()) <= MAX_NESTING,
        "actor state nests deeper than {MAX_NESTING} arrays and objects"
    );
    Ok(())
}

/// How deep the arrays and objects of a JSON text nest.
fn nesting(json: &str) -> usize {
    let (mut depth, mut deepest) = (0_usize, 0);
    let (mut string, mut escaped) = (false, false);
    for byte in json.bytes() {
        match byte {
            _ if escaped => escaped = false,
            b'\\' if string => escaped = true,
            b'"' => string = !string,
            b'[' | b'{' if !string => {
                depth += 1;
                deepest = deepest.max(depth);
            }
            b']' | b'}' if !string => depth -= 1,
            _ => {}
        }
    }
    deepest
}

fn send_rows(
    mut statement: rusqlite::Statement<'_>,
    bindings: impl rusqlite::Params,
    rows: &mut dyn RowSink,
) -> Result<DatabaseResult> {
    rows.columns(&statement.column_names())?;
    let width = statement.column_count();
    let mut query = statement.query(bindings)?;
    while let Some(row) = query.next()? {
        let cells = (0..width)
            .map(|index| row.get_ref(index).map(SqlCell::from))
            .collect::<rusqlite::Result<Vec<_>>>()?;
        rows.row(&cells)?;
    }
    rows.finish()?;
    Ok(DatabaseResult::Rows)
}

fn authorize(
    denied: Arc<Mutex<Option<&'static str>>>,
) -> impl for<'r> FnMut(AuthContext<'r>) -> Authorization + Send + 'static {
    move |context| match denial(&context.action) {
        Some(reason) => {
            if let Ok(mut denied) = denied.lock() {
                denied.get_or_insert(reason);
            }
            Authorization::Deny
        }
        None => Authorization::Allow,
    }
}

fn denial(action: &AuthAction<'_>) -> Option<&'static str> {
    match action {
        AuthAction::Attach { .. } | AuthAction::Detach { .. } => Some(FILES),
        AuthAction::Pragma { pragma_name, .. }
            if !ALLOWED_PRAGMAS.contains(&pragma_name.to_ascii_lowercase().as_str()) =>
        {
            Some(PRAGMAS)
        }
        action if names(action).into_iter().any(reserved) => Some(RESERVED),
        _ => None,
    }
}

fn names<'a>(action: &AuthAction<'a>) -> Vec<&'a str> {
    use AuthAction::*;
    match *action {
        CreateIndex {
            index_name,
            table_name,
        }
        | CreateTempIndex {
            index_name,
            table_name,
        }
        | DropIndex {
            index_name,
            table_name,
        }
        | DropTempIndex {
            index_name,
            table_name,
        } => vec![index_name, table_name],
        CreateTrigger {
            trigger_name,
            table_name,
        }
        | CreateTempTrigger {
            trigger_name,
            table_name,
        }
        | DropTrigger {
            trigger_name,
            table_name,
        }
        | DropTempTrigger {
            trigger_name,
            table_name,
        } => vec![trigger_name, table_name],
        CreateView { view_name }
        | CreateTempView { view_name }
        | DropView { view_name }
        | DropTempView { view_name } => vec![view_name],
        CreateTable { table_name }
        | CreateTempTable { table_name }
        | DropTable { table_name }
        | DropTempTable { table_name }
        | Delete { table_name }
        | Insert { table_name }
        | Analyze { table_name }
        | Read { table_name, .. }
        | Update { table_name, .. }
        | AlterTable { table_name, .. }
        | CreateVtable { table_name, .. }
        | DropVtable { table_name, .. } => vec![table_name],
        _ => vec![],
    }
}

fn reserved(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    name.starts_with("__terse_") || name.starts_with("_litestream_")
}

impl ToSql for SqlValue {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(match self {
            Self::Null => ValueRef::Null,
            Self::Integer(value) => ValueRef::Integer(*value),
            Self::Real(value) => ValueRef::Real(*value),
            Self::Text(value) => ValueRef::Text(value.as_bytes()),
            Self::Blob(value) => ValueRef::Blob(value),
        }))
    }
}

impl<'a> From<ValueRef<'a>> for SqlCell<'a> {
    fn from(value: ValueRef<'a>) -> Self {
        match value {
            ValueRef::Null => Self::Null,
            ValueRef::Integer(value) => Self::Integer(value),
            ValueRef::Real(value) => Self::Real(value),
            ValueRef::Text(value) => Self::Text(value),
            ValueRef::Blob(value) => Self::Blob(value),
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/host/sqlite.rs"]
mod tests;
