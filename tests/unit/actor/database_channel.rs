#[path = "../../fixtures/actor_database.rs"]
mod actor_database;
#[path = "../../fixtures/database_client.rs"]
mod database_client;

use super::*;
use actor_database::DelayedDatabase;
use async_trait::async_trait;
use database_client::Answer;
use serde_json::json;
use tokio::{
    sync::Semaphore,
    task::JoinHandle,
    time::{Duration, timeout},
};

#[tokio::test]
async fn a_statement_receives_its_bindings_and_streams_every_storage_class() -> Result<()> {
    let rows = vec![
        vec![SqlValue::Null, SqlValue::Integer(i64::MIN)],
        vec![
            SqlValue::Real(0.5),
            SqlValue::Text("snowman ☃".repeat(40_000)),
        ],
        vec![
            SqlValue::Blob(vec![255; 150_000]),
            SqlValue::Text("".into()),
        ],
    ];
    let (requested, mut requests) = mpsc::unbounded_channel();
    let scopes = Arc::new(DatabaseScopes::default());
    scopes.open(
        7,
        Arc::new(StoredRows {
            requested,
            columns: vec!["first", "second"],
            rows: rows.clone(),
            failure: None,
        }),
    );
    let (mut executor, _served) = channel(scopes)?;
    let bindings = [
        SqlValue::Integer(9_007_199_254_740_993),
        SqlValue::Real(-1.5),
        SqlValue::Text("kept ☃".into()),
        SqlValue::Blob(vec![0, 1]),
        SqlValue::Null,
    ];
    let head =
        json!({"message_id": 7, "call": {"op": "exec", "sql": "SELECT ?", "immediate": true}});
    assert_eq!(
        database_client::call(&mut executor, head, &bindings).await?,
        Answer {
            columns: vec!["first".into(), "second".into()],
            rows,
            ending: Ok(json!({"type": "rows"})),
        }
    );
    assert!(matches!(
        requests.try_recv()?,
        DatabaseCall::Exec { sql, bindings: bound, immediate: true } if sql == "SELECT ?" && bound == bindings
    ));
    Ok(())
}

#[tokio::test]
async fn rows_sent_before_a_failure_are_followed_by_it_and_the_channel_stays_usable() -> Result<()>
{
    let (requested, _requests) = mpsc::unbounded_channel();
    let scopes = Arc::new(DatabaseScopes::default());
    scopes.activate(Some(Arc::new(StoredRows {
        requested,
        columns: vec!["value"],
        rows: vec![vec![SqlValue::Integer(1)]],
        failure: Some("UNIQUE constraint failed"),
    })));
    let (mut executor, _served) = channel(scopes)?;
    for _ in 0..2 {
        let head =
            json!({"call": {"op": "exec", "sql": "INSERT INTO notes VALUES (1) RETURNING 1"}});
        assert_eq!(
            database_client::call(&mut executor, head, &[]).await?,
            Answer {
                columns: vec!["value".into()],
                rows: vec![vec![SqlValue::Integer(1)]],
                ending: Err("UNIQUE constraint failed".into()),
            }
        );
    }
    Ok(())
}

#[tokio::test]
async fn calls_reach_the_activation_database_unless_they_name_a_command() -> Result<()> {
    let scopes = Arc::new(DatabaseScopes::default());
    let (mut executor, _served) = channel(scopes.clone())?;
    let delete_alarm = |message_id: Option<u64>| json!({"message_id": message_id, "call": {"op": "delete_alarm", "immediate": true}});
    let unavailable = database_client::call(&mut executor, delete_alarm(None), &[]).await?;
    assert_eq!(
        unavailable.ending,
        Err("actor database is unavailable".into())
    );

    let (requested, mut requests) = mpsc::unbounded_channel();
    scopes.activate(Some(Arc::new(DelayedDatabase {
        requested,
        release: Arc::new(Semaphore::new(3)),
        fail: false,
    })));
    let done = database_client::call(&mut executor, delete_alarm(None), &[]).await?;
    assert_eq!(done.ending, Ok(json!({"type": "done"})));
    assert!(matches!(
        requests.try_recv()?,
        DatabaseCall::DeleteAlarm { immediate: true }
    ));

    scopes.open(7, actor_database::unused());
    scopes.close(7);
    for answered in [7, 99] {
        let refused =
            database_client::call(&mut executor, delete_alarm(Some(answered)), &[]).await?;
        assert_eq!(
            refused.ending,
            Err("the command this database call belongs to has ended".into())
        );
    }
    assert!(requests.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn field_values_follow_the_call_and_stay_the_text_they_were_sent_as() -> Result<()> {
    let (requested, mut requests) = mpsc::unbounded_channel();
    let scopes = Arc::new(DatabaseScopes::default());
    scopes.activate(Some(Arc::new(DelayedDatabase {
        requested,
        release: Arc::new(Semaphore::new(2)),
        fail: false,
    })));
    let (mut executor, _served) = channel(scopes)?;
    let text = |json: &str| SqlValue::Text(json.into());
    let large = format!(r#"{{"z":1,"a":"{}"}}"#, "snowman ☃".repeat(40_000));

    let head = json!({"call": {"op": "persist_field", "name": "nested", "immediate": true}});
    database_client::call(&mut executor, head, &[text(&large)]).await?;
    assert!(matches!(
        requests.try_recv()?,
        DatabaseCall::PersistField { name, value, immediate: true }
            if name == "nested" && value.get() == large
    ));

    let head = json!({"call": {"op": "initialize_fields", "names": ["count", "ready"]}});
    database_client::call(&mut executor, head, &[text("1"), text("true")]).await?;
    let DatabaseCall::InitializeFields { fields } = requests.try_recv()? else {
        anyhow::bail!("expected initialize fields");
    };
    assert_eq!(fields["count"].get(), "1");
    assert_eq!(fields["ready"].get(), "true");
    Ok(())
}

#[tokio::test]
async fn a_malformed_call_ends_the_channel() -> Result<()> {
    let persist = || json!({"call": {"op": "persist_field", "name": "count"}});
    for request in [
        database_client::request(json!({"call": {"op": "commit"}}), &[SqlValue::Null])?,
        database_client::request(
            json!({"call": {"op": "persist_field", "name": "count", "value": 1}}),
            &[],
        )?,
        database_client::request(persist(), &[SqlValue::Text("{not json".into())])?,
        database_client::request(persist(), &[SqlValue::Integer(1)])?,
        database_client::request(
            json!({"call": {"op": "initialize_fields", "names": ["count", "ready"]}}),
            &[SqlValue::Text("1".into())],
        )?,
        database_client::request(
            json!({"call": {"op": "exec", "sql": "SELECT 1"}, "bindings": 40_000}),
            &[],
        )?,
        database_client::request(json!({"call": {"op": "unknown"}}), &[])?,
        [
            database_client::request(
                json!({"call": {"op": "exec", "sql": "SELECT ?"}, "bindings": 1}),
                &[],
            )?,
            vec![9],
        ]
        .concat(),
    ] {
        let (mut executor, served) = channel(Arc::default())?;
        executor.write_all(&request).await?;
        assert!(timeout(Duration::from_secs(1), served).await??.is_err());
    }
    Ok(())
}

#[tokio::test]
async fn an_executor_that_stops_reading_releases_the_statement() -> Result<()> {
    let (requested, _requests) = mpsc::unbounded_channel();
    let scopes = Arc::new(DatabaseScopes::default());
    scopes.activate(Some(Arc::new(StoredRows {
        requested,
        columns: vec!["value"],
        rows: vec![vec![SqlValue::Blob(vec![0; 64 * 1024])]; 256],
        failure: None,
    })));
    let (mut executor, served) = channel(scopes)?;
    let head = json!({"call": {"op": "exec", "sql": "SELECT value FROM large"}});
    executor
        .write_all(&database_client::request(head, &[])?)
        .await?;
    executor.read_u8().await?;
    drop(executor);
    assert!(timeout(Duration::from_secs(5), served).await??.is_err());
    Ok(())
}

fn channel(scopes: Arc<DatabaseScopes>) -> Result<(UnixStream, JoinHandle<Result<()>>)> {
    let (host, executor) = UnixStream::pair()?;
    Ok((executor, tokio::spawn(serve(host, scopes))))
}

/// Answers every statement with the same rows, as SQLite does: from a thread that may block.
struct StoredRows {
    requested: mpsc::UnboundedSender<DatabaseCall>,
    columns: Vec<&'static str>,
    rows: Vec<Vec<SqlValue>>,
    failure: Option<&'static str>,
}

#[async_trait]
impl ActorDatabase for StoredRows {
    async fn call(&self, call: DatabaseCall, mut sink: Box<dyn RowSink>) -> Result<DatabaseResult> {
        self.requested.send(call)?;
        let (columns, rows, failure) = (self.columns.clone(), self.rows.clone(), self.failure);
        tokio::task::spawn_blocking(move || {
            sink.columns(&columns)?;
            for row in &rows {
                sink.row(&row.iter().map(borrowed).collect::<Vec<_>>())?;
            }
            sink.finish()?;
            match failure {
                Some(failure) => anyhow::bail!(failure),
                None => Ok(DatabaseResult::Rows),
            }
        })
        .await?
    }
}

fn borrowed(value: &SqlValue) -> SqlCell<'_> {
    match value {
        SqlValue::Null => SqlCell::Null,
        SqlValue::Integer(value) => SqlCell::Integer(*value),
        SqlValue::Real(value) => SqlCell::Real(*value),
        SqlValue::Text(value) => SqlCell::Text(value.as_bytes()),
        SqlValue::Blob(value) => SqlCell::Blob(value),
    }
}
