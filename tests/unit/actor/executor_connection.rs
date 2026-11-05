#[path = "../../fixtures/actor_database.rs"]
mod actor_database;

use super::*;
use crate::actor::{DatabaseCall, DatabaseResult, SqlValue};

#[tokio::test]
async fn generic_executor_connects_before_code_and_hydrates_after_assignment() -> Result<()> {
    let root = tempfile::tempdir_in("/tmp")?;
    let path = root.path().join("executor.sock");
    let listener = ActorExecutorListener::bind(&path).await?;
    let peer = tokio::spawn(async move {
        let mut socket = BufReader::new(tokio::net::UnixStream::connect(path).await?);
        write_json_line(&mut socket, &json!({"type":"warm","protocol":32})).await?;
        let load = read_json_line(&mut socket).await?;
        assert_eq!(load["entrypoint"], "/customer/actors.mjs");
        assert_eq!(load["environment"]["CUSTOMER_KEY"], "value");
        write_json_line(
            &mut socket,
            &json!({"type":"attach","protocol":32,"actor_names":["counter"]}),
        )
        .await?;
        assert_eq!(
            read_json_line(&mut socket).await?,
            json!({
                "type": "attached",
                "protocol": 32,
                "actor": {"project_id": "default", "actor_name": "counter", "actor_id": "one"}
            })
        );
        let hydrate = read_json_line(&mut socket).await?;
        assert_eq!(
            hydrate["command"],
            json!({"type": "hydrate", "request_id": "activate"})
        );
        write_json_line(
            &mut socket,
            &json!({"type":"database_call","call_id":1,"message_id":hydrate["message_id"],
                "call":{"op":"persist_field","name":"resumed","value":true,"immediate":true}}),
        )
        .await?;
        let persisted = read_json_line(&mut socket).await?;
        assert_eq!(persisted["type"], "database_result");
        assert_eq!(persisted["call_id"], 1);
        assert_eq!(persisted["result"], json!({"type":"done"}));
        write_json_line(
            &mut socket,
            &json!({"type":"reply","message_id":hydrate["message_id"],"reply":{"type":"hydrated"}}),
        )
        .await?;
        anyhow::Ok(())
    });
    let warm = listener.accept_warm().await?;
    let connection = warm
        .load(
            "/customer/actors.mjs",
            &HashMap::from([("CUSTOMER_KEY".into(), "value".into())]),
        )
        .await?;
    connection
        .mark_ready(counter(), None, None, None, None, None)
        .await?;
    let (requested, mut requests) = mpsc::unbounded_channel();
    let database = Arc::new(actor_database::DelayedDatabase {
        requested,
        release: Arc::new(tokio::sync::Semaphore::new(1)),
        fail: false,
    });
    connection
        .executor()
        .hydrate("activate".into(), database)
        .await?;
    assert!(matches!(
        requests.try_recv()?,
        DatabaseCall::PersistField { name, value, immediate: true }
            if name == "resumed" && value.get() == "true"
    ));
    peer.await??;
    Ok(())
}
use serde_json::json;
use tempfile::TempDir;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
    time::{Duration, timeout},
};

#[tokio::test]
async fn residency_reports_are_separate_from_invocation_cache_hints() -> Result<()> {
    let root = TempDir::new_in("/tmp")?;
    let socket = root.path().join("residency.sock");
    let listener = ActorExecutorListener::bind(&socket).await?;
    let mut peer = BufReader::new(UnixStream::connect(&socket).await?);
    write_json_line(
        &mut peer,
        &json!({"type":"attach", "protocol":32, "actor_names":["Room"]}),
    )
    .await?;
    let connection = listener.accept().await?;
    connection
        .mark_ready(
            ActorKey {
                project_id: "default".into(),
                actor_name: "Room".into(),
                actor_id: "one".into(),
            },
            None,
            None,
            None,
            None,
            None,
        )
        .await?;
    let executor = connection.executor();
    assert!(executor.resident().is_none());
    let mut changes = executor
        .residency_changes()
        .expect("residency notifications");
    for resident in [true, false] {
        write_json_line(&mut peer, &json!({"type":"residency", "resident":resident})).await?;
        timeout(Duration::from_secs(1), changes.changed()).await??;
        timeout(Duration::from_secs(1), async {
            while executor.resident() != Some(resident) {
                tokio::task::yield_now().await;
            }
        })
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn connection_lookup_is_scoped_to_its_invocation_and_does_not_block_other_invocations()
-> Result<()> {
    struct Source {
        requested: mpsc::UnboundedSender<()>,
        release: Arc<tokio::sync::Semaphore>,
    }
    #[async_trait]
    impl ActorSocketSource for Source {
        async fn query(
            &self,
            query: crate::actor::SocketQuery,
        ) -> Result<crate::actor::SocketLookup> {
            self.requested.send(())?;
            self.release.acquire().await?.forget();
            if query.count_only {
                return Ok(crate::actor::SocketLookup::Count(1));
            }
            Ok(crate::actor::SocketLookup::Connections(vec![
                ActorSocketConnection {
                    id: "socket-1".into(),
                    metadata: json!({}),
                    tags: vec![],
                },
            ]))
        }
    }
    let (host, customer) = UnixStream::pair()?;
    let (reader, writer) = host.into_split();
    let (executor, running) =
        JsActorExecutor::start(BufReader::new(reader), writer, vec!["counter".into()]);
    let (requested, mut requests) = mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    executor
        .mark_ready(
            counter(),
            None,
            Some(Arc::new(Source {
                requested,
                release: release.clone(),
            })),
            None,
            None,
            None,
        )
        .await?;
    let peer = async {
        let mut customer = BufReader::new(customer);
        assert_eq!(read_json_line(&mut customer).await?["type"], "attached");
        let slow = read_json_line(&mut customer).await?;
        write_json_line(
            &mut customer,
            &json!({"type":"get_connections", "message_id":slow["message_id"]}),
        )
        .await?;
        let fast = read_json_line(&mut customer).await?;
        assert_eq!(fast["command"]["request_id"], "fast");
        write_json_line(&mut customer, &json!({"type":"reply", "message_id":fast["message_id"], "reply":{"type":"invoked", "result":42}})).await?;
        let loaded = read_json_line(&mut customer).await?;
        assert_eq!(loaded["type"], "socket_connections");
        assert_eq!(loaded["message_id"], slow["message_id"]);
        assert_eq!(loaded["connections"][0]["id"], "socket-1");
        write_json_line(&mut customer, &json!({"type":"reply", "message_id":slow["message_id"], "reply":{"type":"invoked", "result":1}})).await?;
        anyhow::Ok(())
    };
    let invoke = |id: &str| executor.invoke(invocation(id, "run"), actor_database::unused());
    let callers = async {
        let fast = async {
            requests.recv().await.context("no lookup")?;
            assert!(
                matches!(invoke("fast").await?, ActorMethodOutcome::Completed { result, .. } if result == json!(42))
            );
            release.add_permits(1);
            anyhow::Ok(())
        };
        let (slow, ()) = tokio::try_join!(invoke("slow"), fast)?;
        assert!(matches!(slow, ActorMethodOutcome::Completed { result, .. } if result == json!(1)));
        anyhow::Ok(())
    };
    let result = timeout(Duration::from_secs(2), async {
        tokio::try_join!(peer, callers)
    })
    .await;
    running.abort();
    result??;
    Ok(())
}

#[tokio::test]
async fn admission_is_a_single_permission_from_an_active_invocation() -> Result<()> {
    let (host, customer) = UnixStream::pair()?;
    let (reader, writer) = host.into_split();
    let (executor, running) =
        JsActorExecutor::start(BufReader::new(reader), writer, vec!["counter".into()]);
    let mut admission = executor.invocation_admission().unwrap();
    assert!(!admission.has_changed()?);
    let caller = tokio::spawn(async move {
        executor
            .invoke(invocation("request", "hold"), actor_database::unused())
            .await
    });
    let mut customer = BufReader::new(customer);
    let command = timeout(Duration::from_secs(1), read_json_line(&mut customer)).await??;
    assert!(!admission.has_changed()?);
    let permission = json!({"type": "ready_for_invocation", "message_id": command["message_id"]});
    write_json_line(&mut customer, &permission).await?;
    timeout(Duration::from_secs(1), admission.changed()).await??;
    write_json_line(&mut customer, &permission).await?;
    let error = timeout(Duration::from_secs(1), running)
        .await??
        .unwrap_err();
    assert!(error.to_string().contains("already granted"));
    assert!(caller.await?.is_err());
    Ok(())
}

#[tokio::test]
async fn admission_without_an_active_invocation_is_rejected() -> Result<()> {
    let (host, customer) = UnixStream::pair()?;
    let (reader, writer) = host.into_split();
    let (_executor, running) =
        JsActorExecutor::start(BufReader::new(reader), writer, vec!["counter".into()]);
    let mut customer = BufReader::new(customer);
    write_json_line(
        &mut customer,
        &json!({"type":"ready_for_invocation", "message_id": 99}),
    )
    .await?;
    let error = timeout(Duration::from_secs(1), running)
        .await??
        .unwrap_err();
    assert!(error.to_string().contains("no active call"));
    Ok(())
}

#[tokio::test]
async fn multiplexes_out_of_order_replies_before_peer_disconnect() -> Result<()> {
    let (host, customer) = UnixStream::pair()?;
    let (reader, writer) = host.into_split();
    let (executor, running) =
        JsActorExecutor::start(BufReader::new(reader), writer, vec!["counter".into()]);
    let peer = tokio::spawn(async move {
        let mut customer = BufReader::new(customer);
        let first = read_json_line(&mut customer).await?;
        let second = read_json_line(&mut customer).await?;
        for command in [second, first] {
            write_json_line(
                &mut customer,
                &json!({
                    "type": "reply", "message_id": command["message_id"],
                    "reply": {"type": "invoked", "result": command["command"]["request_id"]}
                }),
            )
            .await?;
        }
        anyhow::Ok(())
    });
    let invoke = |id: &str| executor.invoke(invocation(id, "get"), actor_database::unused());
    let replies = timeout(Duration::from_secs(2), async {
        tokio::try_join!(invoke("first"), invoke("second"))
    })
    .await?;
    peer.await??;
    let _ = running.await;
    let (first, second) = replies?;
    assert!(
        matches!(first, ActorMethodOutcome::Completed { result, .. } if result == json!("first"))
    );
    assert!(
        matches!(second, ActorMethodOutcome::Completed { result, .. } if result == json!("second"))
    );
    Ok(())
}

#[tokio::test]
async fn shutdown_does_not_wait_for_a_peer_that_stopped_reading() -> Result<()> {
    let root = TempDir::new_in("/tmp")?;
    let socket = root.path().join("executor.sock");
    let listener = ActorExecutorListener::bind(&socket).await?;
    let customer = tokio::spawn(async move {
        let stream = UnixStream::connect(socket).await?;
        let mut stream = BufReader::new(stream);
        write_json_line(
            &mut stream,
            &json!({"type":"attach", "protocol":32, "actor_names":["counter"]}),
        )
        .await?;
        let _ = read_json_line(&mut stream).await?;
        std::future::pending::<Result<()>>().await
    });
    let connection = listener.accept().await?;
    connection
        .mark_ready(counter(), None, None, None, None, None)
        .await?;
    let executor = connection.executor();
    let shutdown = CancellationToken::new();
    let mut running = tokio::spawn(connection.run(shutdown.clone()));
    let mut call = tokio::spawn(async move {
        let mut blocked = invocation("blocked-write", "accept");
        blocked.args = vec![json!("x".repeat(8 * 1024 * 1024))];
        executor.invoke(blocked, actor_database::unused()).await
    });
    assert!(timeout(Duration::from_millis(30), &mut call).await.is_err());
    shutdown.cancel();
    let stopped = timeout(Duration::from_millis(200), &mut running).await;
    running.abort();
    call.abort();
    customer.abort();
    stopped.context("executor shutdown waited for a blocked socket writer")???;
    Ok(())
}

#[tokio::test]
async fn one_javascript_executor_runs_until_host_shutdown() -> Result<()> {
    let root = TempDir::new_in("/tmp")?;
    let socket = root.path().join("actor-executor.sock");
    let host = ActorExecutorListener::bind(&socket).await?;
    let customer = tokio::spawn(run_incrementing_customer(socket.clone()));
    let connection = host.accept().await?;
    let executor = connection.executor();
    connection
        .mark_ready(counter(), None, None, None, None, None)
        .await?;
    assert!(executor.supports("counter"));

    let shutdown = CancellationToken::new();
    let connection_task = tokio::spawn(connection.run(shutdown.clone()));
    let mut increment = invocation("request-1", "increment");
    increment.args = vec![json!(2)];
    let outcome = executor.invoke(increment, actor_database::unused()).await?;
    assert_eq!(
        outcome,
        ActorMethodOutcome::Completed {
            result: json!(2),
            effects: Vec::new(),
        }
    );
    let socket_outcome = executor
        .handle_socket(
            ActorSocketInvocation {
                request_id: "socket-request-1".into(),
                actor: counter(),
                event: ActorSocketEvent::Connect {
                    connection: ActorSocketConnection {
                        id: "socket-1".into(),
                        metadata: json!({ "userId": "user-1" }),
                        tags: Vec::new(),
                    },
                },
                connections: vec![ActorSocketConnection {
                    id: "socket-1".into(),
                    metadata: json!({ "userId": "user-1" }),
                    tags: Vec::new(),
                }],
            },
            actor_database::unused(),
        )
        .await?;
    assert_eq!(
        socket_outcome,
        ActorSocketOutcome::Handled {
            effects: vec![ActorSocketEffect::Send {
                connection_id: "socket-1".into(),
                message: ActorSocketMessage::Text {
                    data: "ready".into()
                },
            }],
        }
    );
    shutdown.cancel();
    connection_task.await??;
    customer.await??;
    Ok(())
}

#[tokio::test]
async fn commands_carry_no_sqlite_state() -> Result<()> {
    let (host, customer) = UnixStream::pair()?;
    let (reader, writer) = host.into_split();
    let (executor, running) =
        JsActorExecutor::start(BufReader::new(reader), writer, vec!["counter".into()]);
    let customer = async {
        let mut reader = BufReader::new(customer);
        for _ in 0..2 {
            let message = read_json_line(&mut reader).await?;
            assert_eq!(
                message["command"],
                json!({
                    "type": "invoke",
                    "request_id": message["command"]["request_id"],
                    "method": "read",
                    "args": []
                })
            );
            write_json_line(
                &mut reader,
                &json!({"type": "reply", "message_id": message["message_id"], "reply": {"type": "invoked", "result": 1}}),
            )
            .await?;
        }
        anyhow::Ok(())
    };
    let invoke = async {
        for request in ["cold", "resident"] {
            let outcome = executor
                .invoke(invocation(request, "read"), actor_database::unused())
                .await?;
            assert_eq!(
                outcome,
                ActorMethodOutcome::Completed {
                    result: json!(1),
                    effects: vec![],
                }
            );
        }
        anyhow::Ok(())
    };
    let result = timeout(Duration::from_secs(5), async {
        tokio::try_join!(customer, invoke)
    })
    .await;
    running.abort();
    result??;
    Ok(())
}

#[test]
fn eviction_commands_name_no_actor() -> Result<()> {
    assert_eq!(
        serde_json::to_value(ExecutorCommand::Evict { graceful: true })?,
        json!({"type": "evict", "graceful": true})
    );
    Ok(())
}

#[test]
fn database_protocol_uses_tagged_sqlite_values() -> Result<()> {
    let request = serde_json::from_value::<ActorExecutorClientMessage>(json!({
        "type": "database_call", "call_id": 3, "message_id": 17,
        "call": {"op": "exec", "sql": "SELECT ?", "bindings": [{"type": "integer", "value": "9007199254740993"}, {"type": "blob", "value": "AAE="}], "immediate": true}
    }))?;
    let ActorExecutorClientMessage::DatabaseCall {
        call_id,
        message_id,
        call,
    } = request
    else {
        anyhow::bail!("expected database call");
    };
    assert_eq!((call_id, message_id), (3, Some(17)));
    let DatabaseCall::Exec {
        sql,
        bindings,
        immediate,
    } = call
    else {
        anyhow::bail!("expected exec");
    };
    assert_eq!(sql, "SELECT ?");
    assert!(immediate);
    assert_eq!(
        bindings,
        [
            SqlValue::Integer(9_007_199_254_740_993),
            SqlValue::Blob(vec![0, 1])
        ]
    );
    let reply = ActorExecutorServerMessage::DatabaseResult {
        call_id: 3,
        result: Some(DatabaseResult::Rows {
            columns: vec!["value".into()],
            rows: vec![vec![SqlValue::Real(0.5)]],
        }),
        error: None,
    };
    assert_eq!(
        serde_json::to_value(reply)?,
        json!({
            "type": "database_result", "call_id": 3,
            "result": {"type": "rows", "columns": ["value"], "rows": [[{"type": "real", "value": 0.5}]]}
        })
    );
    let request = serde_json::from_value::<ActorExecutorClientMessage>(json!({
        "type": "database_call", "call_id": 4,
        "call": {"op": "replace_fields", "fields": {"count": 1, "nested": {"ready": true}}}
    }))?;
    let ActorExecutorClientMessage::DatabaseCall {
        message_id: None,
        call: DatabaseCall::ReplaceFields { fields },
        ..
    } = request
    else {
        anyhow::bail!("expected replace fields");
    };
    assert_eq!(fields["count"].get(), "1");
    assert_eq!(fields["nested"].get(), r#"{"ready":true}"#);
    Ok(())
}

#[tokio::test]
async fn database_calls_wait_without_blocking_executor_messages() -> Result<()> {
    database_call_exchange(false).await
}

#[tokio::test]
async fn database_calls_return_the_host_error() -> Result<()> {
    database_call_exchange(true).await
}

#[tokio::test]
async fn replies_cannot_overtake_their_database_call() -> Result<()> {
    let (host, customer) = UnixStream::pair()?;
    let (reader, writer) = host.into_split();
    let (executor, running) =
        JsActorExecutor::start(BufReader::new(reader), writer, vec!["counter".into()]);
    let (requested, mut requests) = mpsc::unbounded_channel();
    let database = Arc::new(actor_database::DelayedDatabase {
        requested,
        release: Arc::new(tokio::sync::Semaphore::new(0)),
        fail: false,
    });
    let caller =
        tokio::spawn(async move { executor.invoke(invocation("slow", "run"), database).await });
    let mut customer = BufReader::new(customer);
    let command = read_json_line(&mut customer).await?;
    write_json_line(
        &mut customer,
        &json!({"type": "database_call", "call_id": 1, "message_id": command["message_id"], "call": {"op": "commit"}}),
    )
    .await?;
    timeout(Duration::from_secs(1), requests.recv()).await?;
    write_json_line(
        &mut customer,
        &json!({"type": "reply", "message_id": command["message_id"], "reply": {"type": "invoked", "result": 1}}),
    )
    .await?;
    let error = timeout(Duration::from_secs(1), running)
        .await??
        .unwrap_err();
    assert!(error.to_string().contains("database call"), "{error:#}");
    assert!(caller.await?.is_err());
    Ok(())
}

#[tokio::test]
async fn database_calls_without_a_running_command_use_the_activation_database() -> Result<()> {
    let (host, customer) = UnixStream::pair()?;
    let (reader, writer) = host.into_split();
    let (executor, _running) =
        JsActorExecutor::start(BufReader::new(reader), writer, vec!["counter".into()]);
    let (requested, mut requests) = mpsc::unbounded_channel();
    let activation = Arc::new(actor_database::DelayedDatabase {
        requested,
        release: Arc::new(tokio::sync::Semaphore::new(2)),
        fail: false,
    });
    executor
        .mark_ready(counter(), None, None, Some(activation), None, None)
        .await?;
    let mut customer = BufReader::new(customer);
    assert_eq!(read_json_line(&mut customer).await?["type"], "attached");
    for (call_id, message_id) in [(1, None), (2, Some(99))] {
        write_json_line(
            &mut customer,
            &json!({"type": "database_call", "call_id": call_id, "message_id": message_id,
                "call": {"op": "delete_alarm", "immediate": true}}),
        )
        .await?;
        let result = read_json_line(&mut customer).await?;
        assert_eq!(result["call_id"], call_id);
        assert_eq!(result["result"], json!({"type": "done"}));
        assert!(matches!(
            requests.try_recv()?,
            DatabaseCall::DeleteAlarm { immediate: true }
        ));
    }
    Ok(())
}

#[tokio::test]
async fn actor_calls_reply_with_the_hosts_outcome() -> Result<()> {
    struct Echo;
    #[async_trait]
    impl ActorCaller for Echo {
        async fn call(&self, call: ActorCall) -> Value {
            json!({"type": "completed", "result": [call.actor_name, call.actor_id, call.method, call.args]})
        }
    }
    let (host, customer) = UnixStream::pair()?;
    let (reader, writer) = host.into_split();
    let (executor, _running) =
        JsActorExecutor::start(BufReader::new(reader), writer, vec!["counter".into()]);
    executor
        .mark_ready(counter(), None, None, None, Some(Arc::new(Echo)), None)
        .await?;
    let mut customer = BufReader::new(customer);
    assert_eq!(read_json_line(&mut customer).await?["type"], "attached");
    write_json_line(
        &mut customer,
        &json!({"type": "actor_call", "call_id": 7, "call": {"request_id": "call-7",
            "actor_name": "DocIndex", "actor_id": "main", "method": "add", "args": ["doc-1"]}}),
    )
    .await?;
    assert_eq!(
        read_json_line(&mut customer).await?,
        json!({"type": "actor_result", "call_id": 7,
            "outcome": {"type": "completed", "result": ["DocIndex", "main", "add", ["doc-1"]]}})
    );
    Ok(())
}

#[tokio::test]
async fn logs_carry_the_request_id_of_their_running_command() -> Result<()> {
    struct Captured(Mutex<Vec<ActorLogRecord>>);
    impl ActorLogSink for Captured {
        fn emit(&self, record: ActorLogRecord) {
            self.0.lock().unwrap().push(record);
        }
    }
    let logs = Arc::new(Captured(Mutex::new(Vec::new())));
    let (host, customer) = UnixStream::pair()?;
    let (reader, writer) = host.into_split();
    let (executor, _running) =
        JsActorExecutor::start(BufReader::new(reader), writer, vec!["counter".into()]);
    executor
        .mark_ready(counter(), None, None, None, None, Some(logs.clone()))
        .await?;
    let mut customer = BufReader::new(customer);
    assert_eq!(read_json_line(&mut customer).await?["type"], "attached");
    let caller = tokio::spawn({
        let executor = executor.clone();
        async move {
            executor
                .invoke(
                    invocation("request-1", "increment"),
                    actor_database::unused(),
                )
                .await
        }
    });
    let command = read_json_line(&mut customer).await?;
    for line in [
        json!({"type": "log", "message_id": command["message_id"], "level": "info", "message": "inside"}),
        json!({"type": "log", "message_id": 999, "level": "warn", "message": "unknown"}),
        json!({"type": "log", "level": "error", "message": "outside"}),
        json!({"type": "reply", "message_id": command["message_id"], "reply": {"type": "invoked", "result": 1}}),
    ] {
        write_json_line(&mut customer, &line).await?;
    }
    caller.await??;
    let record = |request_id: Option<&str>, level, message: &str| ActorLogRecord {
        request_id: request_id.map(str::to_owned),
        level,
        message: message.into(),
    };
    assert_eq!(
        *logs.0.lock().unwrap(),
        vec![
            record(Some("request-1"), LogLevel::Info, "inside"),
            record(None, LogLevel::Warn, "unknown"),
            record(None, LogLevel::Error, "outside"),
        ]
    );
    Ok(())
}

async fn database_call_exchange(fail: bool) -> Result<()> {
    let (host, customer) = UnixStream::pair()?;
    let (reader, writer) = host.into_split();
    let (executor, running) =
        JsActorExecutor::start(BufReader::new(reader), writer, vec!["counter".into()]);
    executor
        .mark_ready(counter(), None, None, None, None, None)
        .await?;
    let (requested, mut requests) = mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let database = Arc::new(actor_database::DelayedDatabase {
        requested,
        release: release.clone(),
        fail,
    });
    let peer = async {
        let mut customer = BufReader::new(customer);
        assert_eq!(read_json_line(&mut customer).await?["type"], "attached");
        let slow = read_json_line(&mut customer).await?;
        write_json_line(
            &mut customer,
            &json!({"type": "database_call", "call_id": 1, "message_id": slow["message_id"],
                "call": {"op": "exec", "sql": "INSERT INTO notes VALUES (?)", "bindings": [{"type": "text", "value": "kept"}], "immediate": true}}),
        )
        .await?;
        let fast = read_json_line(&mut customer).await?;
        assert_eq!(fast["command"]["request_id"], "fast");
        write_json_line(
            &mut customer,
            &json!({"type": "reply", "message_id": fast["message_id"], "reply": {"type": "invoked", "result": 1}}),
        )
        .await?;
        let result = read_json_line(&mut customer).await?;
        assert_eq!(result["type"], "database_result");
        assert_eq!(result["call_id"], 1);
        let reply = if fail {
            assert_eq!(result["error"], "storage rejected immediate write");
            assert!(result.get("result").is_none());
            json!({"type": "failed", "code": "actor_database_failed", "message": result["error"]})
        } else {
            assert_eq!(result["result"], json!({"type": "done"}));
            json!({"type": "invoked", "result": 42})
        };
        write_json_line(
            &mut customer,
            &json!({"type": "reply", "message_id": slow["message_id"], "reply": reply}),
        )
        .await?;
        anyhow::Ok(())
    };
    let callers = async {
        let fast = async {
            let call = requests.recv().await.context("missing database call")?;
            assert!(matches!(
                call,
                DatabaseCall::Exec { ref sql, ref bindings, immediate: true }
                    if sql == "INSERT INTO notes VALUES (?)" && bindings == &[SqlValue::Text("kept".into())]
            ));
            assert!(matches!(
                executor
                    .invoke(invocation("fast", "run"), actor_database::unused())
                    .await?,
                ActorMethodOutcome::Completed { .. }
            ));
            release.add_permits(1);
            anyhow::Ok(())
        };
        let (slow, ()) =
            tokio::try_join!(executor.invoke(invocation("slow", "run"), database), fast)?;
        assert_eq!(matches!(slow, ActorMethodOutcome::Failed(_)), fail);
        anyhow::Ok(())
    };
    let result = timeout(Duration::from_secs(3), async {
        tokio::try_join!(peer, callers)
    })
    .await;
    running.abort();
    result??;
    Ok(())
}

fn counter() -> ActorKey {
    ActorKey {
        project_id: "default".into(),
        actor_name: "counter".into(),
        actor_id: "one".into(),
    }
}

fn invocation(id: &str, method: &str) -> ActorMethodInvocation {
    ActorMethodInvocation {
        request_id: id.into(),
        method: method.into(),
        args: vec![],
    }
}

async fn run_incrementing_customer(socket: PathBuf) -> Result<()> {
    let stream = UnixStream::connect(socket).await?;
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    writer
        .write_all(b"{\"type\":\"attach\",\"protocol\":32,\"actor_names\":[\"counter\"]}\n")
        .await?;
    ensure!(
        read_json_line(&mut reader).await?
            == json!({
                "type": "attached",
                "protocol": 32,
                "actor": {"project_id": "default", "actor_name": "counter", "actor_id": "one"}
            })
    );

    let invocation = read_json_line(&mut reader).await?;
    let invocation_id = invocation["message_id"]
        .as_u64()
        .context("invocation message ID")?;
    ensure!(invocation["command"]["type"] == "invoke");
    ensure!(invocation["command"].get("timeout_ms").is_none());
    write_json_line(
        &mut writer,
        &json!({
            "type": "reply",
            "message_id": invocation_id,
            "reply": {"type": "invoked", "result": 2}
        }),
    )
    .await?;

    let socket_event = read_json_line(&mut reader).await?;
    let socket_event_id = socket_event["message_id"]
        .as_u64()
        .context("socket event message ID")?;
    ensure!(socket_event["command"]["type"] == "websocket_event");
    ensure!(socket_event["command"]["event"]["type"] == "connect");
    ensure!(socket_event["command"].get("actor").is_none());
    write_json_line(
        &mut writer,
        &json!({
            "type": "reply",
            "message_id": socket_event_id,
            "reply": {
                "type": "websocket_handled",
                "effects": [{
                    "type": "send",
                    "connection_id": "socket-1",
                    "message": { "type": "text", "data": "ready" }
                }]
            }
        }),
    )
    .await?;

    let mut trailing = String::new();
    ensure!(
        reader.read_line(&mut trailing).await? == 0,
        "expected Rust host to close the actor executor"
    );
    Ok(())
}

async fn read_json_line<R>(reader: &mut R) -> Result<Value>
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    let mut line = String::new();
    ensure!(reader.read_line(&mut line).await? > 0, "expected JSON line");
    Ok(serde_json::from_str(line.trim_end())?)
}

async fn write_json_line<W>(writer: &mut W, value: &Value) -> Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    writer
        .write_all(serde_json::to_string(value)?.as_bytes())
        .await?;
    writer.write_all(b"\n").await?;
    Ok(())
}
