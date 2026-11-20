#[path = "../../fixtures/rows.rs"]
mod rows;

use super::*;
use rows::{Collected, Rows};
use serde_json::json;

async fn database() -> Result<(tempfile::TempDir, SqliteDatabase)> {
    database_checking("json_valid(value)").await
}

/// A database whose fields table refuses a value unless `check` holds for it.
async fn database_checking(check: &str) -> Result<(tempfile::TempDir, SqliteDatabase)> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("actor.sqlite");
    rusqlite::Connection::open(&path)?.execute_batch(&format!(
        "PRAGMA journal_mode=WAL;
        CREATE TABLE __terse_fields (name TEXT PRIMARY KEY, value TEXT NOT NULL CHECK({check}));
        CREATE TRIGGER refused BEFORE INSERT ON __terse_fields WHEN new.name = 'refused'
        BEGIN SELECT RAISE(ABORT, 'refused'); END"
    ))?;
    let database = SqliteDatabase::open(path).await?;
    Ok((directory, database))
}

async fn persist(database: &SqliteDatabase, name: &str, value: Box<RawValue>) -> Result<()> {
    let call = DatabaseCall::PersistField {
        name: name.into(),
        value,
        immediate: false,
    };
    database.call(call, rows::ignored()).await?.result?;
    Ok(())
}

async fn exec(
    database: &SqliteDatabase,
    sql: &str,
    bindings: Vec<SqlValue>,
    immediate: bool,
) -> Result<Executed> {
    let executed = database
        .call(
            DatabaseCall::Exec {
                sql: sql.into(),
                bindings,
                immediate,
            },
            rows::ignored(),
        )
        .await?;
    executed
        .result
        .as_ref()
        .map_err(|error| anyhow!("{error:#}"))?;
    Ok(executed)
}

async fn select(database: &SqliteDatabase, sql: &str) -> Result<Collected> {
    let rows = Rows::default();
    let call = DatabaseCall::Exec {
        sql: sql.into(),
        bindings: vec![],
        immediate: false,
    };
    database.call(call, rows.sink()).await?.result?;
    Ok(rows.collected())
}

fn raw(value: serde_json::Value) -> Box<serde_json::value::RawValue> {
    serde_json::value::RawValue::from_string(value.to_string()).unwrap()
}

async fn fields(database: &SqliteDatabase) -> Result<serde_json::Value> {
    stored(database, None).await?.fields()
}

async fn stored(database: &SqliteDatabase, names: Option<Vec<String>>) -> Result<Collected> {
    let rows = Rows::default();
    database
        .call(DatabaseCall::Fields { names }, rows.sink())
        .await?
        .result?;
    Ok(rows.collected())
}

#[tokio::test]
async fn values_keep_their_sqlite_storage_classes() -> Result<()> {
    let (_directory, database) = database().await?;
    exec(&database, "CREATE TABLE items (value)", vec![], false).await?;
    let values = vec![
        SqlValue::Null,
        SqlValue::Integer(i64::MAX),
        SqlValue::Real(1.5),
        SqlValue::Text("snowman ☃".into()),
        SqlValue::Blob(vec![0, 1, 255]),
    ];
    for value in &values {
        exec(
            &database,
            "INSERT INTO items VALUES (?)",
            vec![value.clone()],
            false,
        )
        .await?;
    }
    let Collected {
        columns,
        rows,
        finished,
    } = select(
        &database,
        "SELECT value, typeof(value) AS kind FROM items ORDER BY rowid",
    )
    .await?;
    assert_eq!(columns, ["value", "kind"]);
    assert_eq!(
        rows.iter().map(|row| row[0].clone()).collect::<Vec<_>>(),
        values
    );
    assert_eq!(
        rows.iter().map(|row| row[1].clone()).collect::<Vec<_>>(),
        ["null", "integer", "real", "text", "blob"].map(|kind| SqlValue::Text(kind.into()))
    );
    assert!(finished);
    Ok(())
}

#[tokio::test]
async fn a_statement_without_rows_still_names_its_columns_and_finishes() -> Result<()> {
    let (_directory, database) = database().await?;
    exec(&database, "CREATE TABLE items (value)", vec![], false).await?;
    assert_eq!(
        select(&database, "SELECT value AS named FROM items").await?,
        Collected {
            columns: vec!["named".into()],
            rows: vec![],
            finished: true,
        }
    );
    assert_eq!(
        select(&database, "INSERT INTO items VALUES (1)").await?,
        Collected {
            columns: vec![],
            rows: vec![],
            finished: true,
        }
    );
    Ok(())
}

#[tokio::test]
async fn rows_written_before_a_statement_fails_are_not_finished() -> Result<()> {
    let (_directory, database) = database().await?;
    exec(
        &database,
        "CREATE TABLE items (value UNIQUE)",
        vec![],
        false,
    )
    .await?;
    let rows = Rows::default();
    let call = DatabaseCall::Exec {
        sql: "INSERT INTO items VALUES (1), (2), (1) RETURNING value".into(),
        bindings: vec![],
        immediate: false,
    };
    assert!(database.call(call, rows.sink()).await?.result.is_err());
    assert!(!rows.collected().finished);
    let kept = select(&database, "SELECT value FROM items").await?;
    assert!(kept.rows.is_empty());
    Ok(())
}

#[tokio::test]
async fn runtime_owned_statements_are_rejected() -> Result<()> {
    let (_directory, database) = database().await?;
    for (sql, message) in [
        (
            "ATTACH 'other.sqlite' AS other",
            "own SQLite database files",
        ),
        (
            "CREATE TABLE __terse_shadow (value TEXT)",
            "names are reserved",
        ),
        ("SELECT * FROM __terse_fields", "names are reserved"),
        (
            "PRAGMA journal_mode = DELETE",
            "managed by the actor runtime",
        ),
        ("SELECT 1; SELECT 2", "one SQL statement"),
    ] {
        let error = exec(&database, sql, vec![], false).await.unwrap_err();
        assert!(format!("{error:#}").contains(message), "{sql}: {error:#}");
    }
    Ok(())
}

#[tokio::test]
async fn single_statements_may_end_with_semicolons_comments_and_trigger_bodies() -> Result<()> {
    let (_directory, database) = database().await?;
    for sql in [
        "CREATE TABLE events (name TEXT);",
        "CREATE TABLE audit (name TEXT); -- created",
        "CREATE TRIGGER copy AFTER INSERT ON events BEGIN INSERT INTO audit VALUES (new.name); INSERT INTO audit VALUES ('after'); END",
        "PRAGMA user_version = 3",
    ] {
        exec(&database, sql, vec![], false).await?;
    }
    exec(
        &database,
        "INSERT INTO events VALUES (?)",
        vec![SqlValue::Text("created".into())],
        false,
    )
    .await?;
    let audit = select(&database, "SELECT name FROM audit ORDER BY rowid").await?;
    assert_eq!(
        audit.rows,
        [
            [SqlValue::Text("created".into())],
            [SqlValue::Text("after".into())]
        ]
    );
    let version = select(&database, "PRAGMA user_version").await?;
    assert_eq!(version.rows, [[SqlValue::Integer(3)]]);
    Ok(())
}

#[tokio::test]
async fn ordinary_sql_autocommits_and_explicit_transactions_group_writes() -> Result<()> {
    let (_directory, database) = database().await?;
    assert!(
        exec(&database, "CREATE TABLE notes (body TEXT)", vec![], false)
            .await?
            .committed
    );
    assert!(
        exec(
            &database,
            "INSERT INTO notes VALUES ('before')",
            vec![],
            false
        )
        .await?
        .committed
    );
    assert!(
        !exec(&database, "SELECT * FROM notes", vec![], true)
            .await?
            .committed
    );
    exec(&database, "BEGIN", vec![], true).await?;
    assert!(
        !exec(
            &database,
            "INSERT INTO notes VALUES ('inside')",
            vec![],
            true
        )
        .await?
        .committed
    );
    assert!(exec(&database, "COMMIT", vec![], true).await?.committed);
    assert!(
        !database
            .call(DatabaseCall::Commit, rows::ignored())
            .await?
            .committed
    );
    Ok(())
}

#[tokio::test]
async fn vacuum_counts_as_a_commit() -> Result<()> {
    let (_directory, database) = database().await?;
    exec(&database, "CREATE TABLE notes (body TEXT)", vec![], false).await?;
    exec(
        &database,
        "INSERT INTO notes VALUES ('kept')",
        vec![],
        false,
    )
    .await?;
    exec(&database, "DELETE FROM notes", vec![], false).await?;
    assert!(exec(&database, "VACUUM", vec![], false).await?.committed);
    Ok(())
}

#[tokio::test]
async fn raw_sql_savepoints_preserve_outer_writes_and_only_commit_on_outer_release() -> Result<()> {
    let (_directory, database) = database().await?;
    exec(&database, "CREATE TABLE notes (body TEXT)", vec![], true).await?;
    exec(&database, "SAVEPOINT outer_scope", vec![], true).await?;
    exec(&database, "INSERT INTO notes VALUES ('kept')", vec![], true).await?;
    exec(&database, "SAVEPOINT inner_scope", vec![], true).await?;
    exec(&database, "INSERT INTO notes VALUES ('lost')", vec![], true).await?;
    assert!(
        !exec(&database, "ROLLBACK TO inner_scope", vec![], true)
            .await?
            .committed
    );
    assert!(
        !exec(&database, "RELEASE inner_scope", vec![], true)
            .await?
            .committed
    );
    assert!(
        exec(&database, "RELEASE outer_scope", vec![], true)
            .await?
            .committed
    );
    assert_eq!(
        select(&database, "SELECT body FROM notes").await?.rows,
        [[SqlValue::Text("kept".into())]]
    );
    Ok(())
}

#[tokio::test]
async fn rollback_discards_sql_and_persisted_writes_without_reporting_a_commit() -> Result<()> {
    let (_directory, database) = database().await?;
    database
        .call(
            DatabaseCall::InitializeFields {
                fields: [("count".into(), raw(json!(1)))].into(),
            },
            rows::ignored(),
        )
        .await?
        .result?;
    exec(&database, "CREATE TABLE notes (body TEXT)", vec![], true).await?;
    exec(&database, "BEGIN", vec![], true).await?;
    database
        .call(
            DatabaseCall::PersistField {
                name: "count".into(),
                value: raw(json!(2)),
                immediate: true,
            },
            rows::ignored(),
        )
        .await?
        .result?;
    exec(&database, "INSERT INTO notes VALUES ('lost')", vec![], true).await?;
    assert!(!exec(&database, "ROLLBACK", vec![], true).await?.committed);
    assert_eq!(fields(&database).await?, json!({"count":1}));
    assert!(
        select(&database, "SELECT body FROM notes")
            .await?
            .rows
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn failed_commit_can_be_rolled_back_without_publishing() -> Result<()> {
    let (_directory, database) = database().await?;
    exec(
        &database,
        "CREATE TABLE parent (id PRIMARY KEY)",
        vec![],
        true,
    )
    .await?;
    exec(
        &database,
        "CREATE TABLE child (parent_id REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)",
        vec![],
        true,
    )
    .await?;
    exec(&database, "BEGIN", vec![], true).await?;
    exec(&database, "INSERT INTO child VALUES (42)", vec![], true).await?;
    let failed = database
        .call(
            DatabaseCall::Exec {
                sql: "COMMIT".into(),
                bindings: vec![],
                immediate: true,
            },
            rows::ignored(),
        )
        .await?;
    assert!(failed.result.is_err());
    assert!(!failed.committed);
    assert!(!exec(&database, "ROLLBACK", vec![], true).await?.committed);
    assert!(
        exec(&database, "INSERT INTO parent VALUES (42)", vec![], true)
            .await?
            .committed
    );
    Ok(())
}

#[tokio::test]
async fn sqlite_fail_conflicts_still_report_committed_rows_for_replication() -> Result<()> {
    let (_directory, database) = database().await?;
    exec(&database, "CREATE TABLE items (value UNIQUE)", vec![], true).await?;
    let failed = database
        .call(
            DatabaseCall::Exec {
                sql: "INSERT OR FAIL INTO items VALUES (1), (1)".into(),
                bindings: vec![],
                immediate: true,
            },
            rows::ignored(),
        )
        .await?;
    assert!(failed.result.is_err());
    assert!(failed.committed);
    Ok(())
}

#[tokio::test]
async fn fields_are_read_as_rows_of_the_json_text_they_were_written_as() -> Result<()> {
    let (_directory, database) = database().await?;
    database
        .call(
            DatabaseCall::InitializeFields {
                fields: [
                    (
                        "nested".to_string(),
                        serde_json::value::RawValue::from_string(
                            r#"{"z":1,"a":[true,null]}"#.into(),
                        )?,
                    ),
                    ("stale".to_string(), raw(json!("old"))),
                ]
                .into(),
            },
            rows::ignored(),
        )
        .await?;
    database
        .call(
            DatabaseCall::InitializeFields {
                fields: [("stale".to_string(), raw(json!("ignored")))].into(),
            },
            rows::ignored(),
        )
        .await?;
    let text = |value: &str| SqlValue::Text(value.into());
    let listed = stored(&database, None).await?;
    assert_eq!(listed.columns, ["name", "value"]);
    assert_eq!(
        listed.rows,
        [
            [text("nested"), text(r#"{"z":1,"a":[true,null]}"#)],
            [text("stale"), text(r#""old""#)]
        ]
    );
    let named = stored(&database, Some(vec!["stale".into(), "missing".into()])).await?;
    assert_eq!(named.rows, [[text("stale"), text(r#""old""#)]]);
    for name in ["stale", "missing"] {
        database
            .call(
                DatabaseCall::DeleteField {
                    name: name.into(),
                    immediate: false,
                },
                rows::ignored(),
            )
            .await?;
    }
    assert_eq!(
        fields(&database).await?,
        json!({"nested": {"z": 1, "a": [true, null]}})
    );
    Ok(())
}

#[tokio::test]
async fn field_writes_skip_the_tables_check_and_leave_the_actors_own_checks_in_force() -> Result<()>
{
    // No value passes this check, so a field is stored only if SQLite does not run it.
    let (_directory, database) = database_checking("0").await?;
    let sql = "CREATE TABLE items (count INTEGER CHECK(count > 0))";
    exec(&database, sql, vec![], false).await?;
    let insert_refused = || async {
        let error = exec(&database, "INSERT INTO items VALUES (0)", vec![], false).await;
        error.unwrap_err().to_string()
    };

    let fields = [("count".to_string(), raw(json!(1)))].into();
    let call = DatabaseCall::InitializeFields { fields };
    database.call(call, rows::ignored()).await?.result?;
    persist(&database, "count", raw(json!(2))).await?;
    assert_eq!(self::fields(&database).await?, json!({"count": 2}));
    assert!(insert_refused().await.contains("CHECK constraint failed"));

    let failed = persist(&database, "refused", raw(json!(1))).await;
    assert!(failed.unwrap_err().to_string().contains("refused"));
    assert!(insert_refused().await.contains("CHECK constraint failed"));
    Ok(())
}

#[tokio::test]
async fn a_field_nested_deeper_than_stored_state_is_read_is_refused() -> Result<()> {
    let (_directory, database) = database().await?;
    // The string holds what would close and open levels if it were read as structure.
    let nested = |levels: usize| {
        let (open, close) = ("[".repeat(levels - 1), "]".repeat(levels - 1));
        RawValue::from_string(format!(r#"{open}{{"a":"]}}\"[{{"}}{close}"#)).unwrap()
    };
    let read = |value: &RawValue| serde_json::from_str::<serde_json::Value>(value.get());

    persist(&database, "deepest", nested(127)).await?;
    assert!(read(&nested(127)).is_ok());

    let refused = persist(&database, "deeper", nested(128)).await;
    assert!(refused.unwrap_err().to_string().contains("nests deeper"));
    assert!(read(&nested(128)).is_err());
    let stored = stored(&database, Some(vec!["deepest".into(), "deeper".into()])).await?;
    assert_eq!(stored.rows.len(), 1);
    Ok(())
}

#[tokio::test]
async fn size_counts_committed_pages_and_waits_out_open_transactions() -> Result<()> {
    let (_directory, database) = database().await?;
    let empty = database.size().await?.unwrap();
    exec(&database, "CREATE TABLE blobs (data BLOB)", vec![], false).await?;
    exec(
        &database,
        "INSERT INTO blobs VALUES (zeroblob(65536))",
        vec![],
        false,
    )
    .await?;
    let written = database.size().await?.unwrap();
    assert!(written >= empty + 65536, "{written} after {empty}");
    exec(&database, "BEGIN", vec![], false).await?;
    assert_eq!(database.size().await?, None);
    database
        .call(DatabaseCall::Rollback, rows::ignored())
        .await?;
    assert_eq!(database.size().await?, Some(written));
    Ok(())
}
