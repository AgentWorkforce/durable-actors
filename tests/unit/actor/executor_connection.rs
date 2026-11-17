#[path = "../../fixtures/actor_database.rs"]
mod actor_database;
#[path = "../../fixtures/database_client.rs"]
mod database_client;

use super::*;
use crate::actor::{DatabaseCall, SqlValue};

#[tokio::test]
async fn generic_executor_connects_before_code_and_hydrates_after_assignment() -> Result<()> {
    let root = tempfile::tempdir_in("/tmp")?;
    let path = root.path().join("executor.sock");
    let listener = ActorExecutorListener::bind(&path).await?;
    let peer = tokio::spawn(async move {
        let mut socket = BufReader::new(tokio::net::UnixStream::connect(&path).await?);
        write_message(&mut socket, &json!({"type":"warm","protocol":34})).await?;
        let load = read_message(&mut socket).await?;
        assert_eq!(load["entrypoint"], "/customer/actors.mjs");
        assert_eq!(load["environment"]["CUSTOMER_KEY"], "value");
        write_message(
            &mut socket,
            &json!({"type":"attach","protocol":34,"actor_names":["counter"]}),
        )
        .await?;
        assert_eq!(
            read_message(&mut socket).await?,
            json!({
                "type": "attached",
                "protocol": 34,
                "actor": {"project_id": "default", "actor_name": "counter", "actor_id": "one"}
            })
        );
        let hydrate = read_message(&mut socket).await?;
        assert_eq!(
            hydrate["command"],
            json!({"type": "hydrate", "request_id": "activate"})
        );
        let mut database = tokio::net::UnixStream::connect(&path).await?;
        let persisted = database_client::call(
            &mut database,
            json!({"message_id":hydrate["message_id"],
                "call":{"op":"persist_field","name":"resumed","immediate":true}}),
            &[SqlValue::Text("true".into())],
        )
        .await?;
        assert_eq!(persisted.ending, Ok(json!({"type":"done"})));
        write_message(
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
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
    time::{Duration, timeout},
};

#[tokio::test]
async fn residency_reports_are_separate_from_invocation_cache_hints() -> Result<()> {
    let root = TempDir::new_in("/tmp")?;
    let socket = root.path().join("residency.sock");
    let listener = ActorExecutorListener::bind(&socket).await?;
    let mut peer = BufReader::new(UnixStream::connect(&socket).await?);
    write_message(
        &mut peer,
        &json!({"type":"attach", "protocol":34, "actor_names":["Room"]}),
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
        write_message(&mut peer, &json!({"type":"residency", "resident":resident})).await?;
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
    let (executor, running) = JsActorExecutor::start(
        BufReader::new(reader),
        writer,
        vec!["counter".into()],
        Arc::default(),
    );
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
        assert_eq!(read_message(&mut customer).await?["type"], "attached");
        let slow = read_message(&mut customer).await?;
        write_message(
            &mut customer,
            &json!({"type":"get_connections", "message_id":slow["message_id"]}),
        )
        .await?;
        let fast = read_message(&mut customer).await?;
        assert_eq!(fast["command"]["request_id"], "fast");
        write_message(&mut customer, &json!({"type":"reply", "message_id":fast["message_id"], "reply":{"type":"invoked", "result":42}})).await?;
        let loaded = read_message(&mut customer).await?;
        assert_eq!(loaded["type"], "socket_connections");
        assert_eq!(loaded["message_id"], slow["message_id"]);
        assert_eq!(loaded["connections"][0]["id"], "socket-1");
        write_message(&mut customer, &json!({"type":"reply", "message_id":slow["message_id"], "reply":{"type":"invoked", "result":1}})).await?;
        anyhow::Ok(())
    };
    let invoke = |id: &str| executor.invoke(invocation(id, "run"), actor_database::unused());
    let callers = async {
        let fast = async {
            requests.recv().await.context("no lookup")?;
            assert!(
                matches!(invoke("fast").await?, ActorMethodOutcome::Completed { result, .. } if result == json!(42).into())
            );
            release.add_permits(1);
            anyhow::Ok(())
        };
        let (slow, ()) = tokio::try_join!(invoke("slow"), fast)?;
        assert!(
            matches!(slow, ActorMethodOutcome::Completed { result, .. } if result == json!(1).into())
        );
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
    let (executor, running) = JsActorExecutor::start(
        BufReader::new(reader),
        writer,
        vec!["counter".into()],
        Arc::default(),
    );
    let mut admission = executor.invocation_admission().unwrap();
    assert!(!admission.has_changed()?);
    let caller = tokio::spawn(async move {
        executor
            .invoke(invocation("request", "hold"), actor_database::unused())
            .await
    });
    let mut customer = BufReader::new(customer);
    let command = timeout(Duration::from_secs(1), read_message(&mut customer)).await??;
    assert!(!admission.has_changed()?);
    let permission = json!({"type": "ready_for_invocation", "message_id": command["message_id"]});
    write_message(&mut customer, &permission).await?;
    timeout(Duration::from_secs(1), admission.changed()).await??;
    write_message(&mut customer, &permission).await?;
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
    let (_executor, running) = JsActorExecutor::start(
        BufReader::new(reader),
        writer,
        vec!["counter".into()],
        Arc::default(),
    );
    let mut customer = BufReader::new(customer);
    write_message(
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
    let (executor, running) = JsActorExecutor::start(
        BufReader::new(reader),
        writer,
        vec!["counter".into()],
        Arc::default(),
    );
    let peer = tokio::spawn(async move {
        let mut customer = BufReader::new(customer);
        let first = read_message(&mut customer).await?;
        let second = read_message(&mut customer).await?;
        for command in [second, first] {
            write_message(
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
        matches!(first, ActorMethodOutcome::Completed { result, .. } if result == json!("first").into())
    );
    assert!(
        matches!(second, ActorMethodOutcome::Completed { result, .. } if result == json!("second").into())
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
        write_message(
            &mut stream,
            &json!({"type":"attach", "protocol":34, "actor_names":["counter"]}),
        )
        .await?;
        let _ = read_message(&mut stream).await?;
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
        blocked.args = vec![json!("x".repeat(8 * 1024 * 1024))].into();
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
    increment.args = vec![json!(2)].into();
    let outcome = executor.invoke(increment, actor_database::unused()).await?;
    assert_eq!(
        outcome,
        ActorMethodOutcome::Completed {
            result: json!(2).into(),
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
    let (executor, running) = JsActorExecutor::start(
        BufReader::new(reader),
        writer,
        vec!["counter".into()],
        Arc::default(),
    );
    let customer = async {
        let mut reader = BufReader::new(customer);
        for _ in 0..2 {
            let message = read_message(&mut reader).await?;
            assert_eq!(
                message["command"],
                json!({
                    "type": "invoke",
                    "request_id": message["command"]["request_id"],
                    "method": "read",
                    "args": []
                })
            );
            write_message(
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
                    result: json!(1).into(),
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

#[tokio::test]
async fn invocation_arguments_follow_the_command_line_as_they_arrived() -> Result<()> {
    const ARGS: &str = "[\"a\",\n {\"n\": 1.50}]";
    let (host, customer) = UnixStream::pair()?;
    let (reader, writer) = host.into_split();
    let (executor, running) = JsActorExecutor::start(
        BufReader::new(reader),
        writer,
        vec!["counter".into()],
        Arc::default(),
    );
    let customer = async {
        let mut reader = BufReader::new(customer);
        let mut line = String::new();
        reader.read_line(&mut line).await?;
        let head: Value = serde_json::from_str(&line)?;
        assert_eq!(
            head["command"],
            json!({"type": "invoke", "request_id": "r", "method": "read"})
        );
        assert_eq!(head["parts"], json!([ARGS.len()]));
        let mut args = vec![0; ARGS.len()];
        reader.read_exact(&mut args).await?;
        assert_eq!(args, ARGS.as_bytes());
        write_message(
            &mut reader,
            &json!({"type": "reply", "message_id": head["message_id"], "reply": {"type": "invoked", "result": 1}}),
        )
        .await
    };
    let invoke = async {
        let mut call = invocation("r", "read");
        call.args = ActorArgs::deserialize_text(&mut serde_json::Deserializer::from_str(ARGS))?;
        executor.invoke(call, actor_database::unused()).await
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

#[tokio::test]
async fn a_reply_is_a_line_followed_by_its_effects_and_the_text_of_each_message() -> Result<()> {
    let (host, customer) = UnixStream::pair()?;
    let (reader, writer) = host.into_split();
    let (executor, running) = JsActorExecutor::start(
        BufReader::new(reader),
        writer,
        vec!["counter".into()],
        Arc::default(),
    );
    let effects = r#"[{"type":"send","connection_id":"socket-1","message":{"type":"text"}}]"#;
    let text = "two \"lines\"\n\u{2028}caf\u{e9}";
    let peer = async {
        let mut customer = BufReader::new(customer);
        let command = read_message(&mut customer).await?;
        let reply = format!(
            "{{\"type\":\"reply\",\"message_id\":{},\"reply\":{{\"type\":\"websocket_handled\"}},\"parts\":[{},{}]}}\n{effects}{text}",
            command["message_id"],
            effects.len(),
            text.len()
        );
        customer.write_all(reply.as_bytes()).await?;
        anyhow::Ok(())
    };
    let event = ActorSocketInvocation {
        request_id: "connect".into(),
        actor: counter(),
        event: ActorSocketEvent::Connect {
            connection: ActorSocketConnection {
                id: "socket-1".into(),
                metadata: json!(null),
                tags: Vec::new(),
            },
        },
        connections: Vec::new(),
    };
    let result = timeout(Duration::from_secs(2), async {
        tokio::try_join!(
            executor.handle_socket(event, actor_database::unused()),
            peer
        )
    })
    .await;
    running.abort();
    assert_eq!(
        result??.0,
        ActorSocketOutcome::Handled {
            effects: vec![ActorSocketEffect::Send {
                connection_id: "socket-1".into(),
                message: ActorSocketMessage::Text { data: text.into() },
            }],
        }
    );
    Ok(())
}

#[tokio::test]
async fn large_results_and_messages_arrive_whole_beside_inline_effects() -> Result<()> {
    let (host, customer) = UnixStream::pair()?;
    let (reader, writer) = host.into_split();
    let (executor, running) = JsActorExecutor::start(
        BufReader::new(reader),
        writer,
        vec!["counter".into()],
        Arc::default(),
    );
    let html = "<p class=\"note\">caf\u{e9} \\ \u{1f600}</p>\n".repeat(100_000);
    let text = |data: &str| ActorSocketMessage::Text { data: data.into() };
    let effects = vec![
        ActorSocketEffect::Send {
            connection_id: "socket-1".into(),
            message: text(&html),
        },
        ActorSocketEffect::Send {
            connection_id: "socket-1".into(),
            message: ActorSocketMessage::Binary {
                data: "AAE=".into(),
            },
        },
        ActorSocketEffect::SetTags {
            connection_id: "socket-1".into(),
            tags: vec!["room".into()],
        },
        ActorSocketEffect::Broadcast {
            message: text(""),
            except_connection_ids: Vec::new(),
            tags: Vec::new(),
            tag_match: ActorSocketTagMatch::All,
        },
        ActorSocketEffect::Broadcast {
            message: text("last"),
            except_connection_ids: vec!["socket-1".into()],
            tags: Vec::new(),
            tag_match: ActorSocketTagMatch::Any,
        },
    ];
    let peer = async {
        let mut customer = BufReader::new(customer);
        let command = read_message(&mut customer).await?;
        write_message(
            &mut customer,
            &json!({"type": "reply", "message_id": command["message_id"], "reply": {
                "type": "invoked", "sequence": 4, "result": {"html": html}, "effects": effects
            }}),
        )
        .await
    };
    let result = timeout(Duration::from_secs(5), async {
        tokio::try_join!(
            executor.invoke(invocation("request", "snapshot"), actor_database::unused()),
            peer
        )
    })
    .await;
    running.abort();
    assert_eq!(
        result??.0,
        ActorMethodOutcome::Interleaved(ActorInterleavedOutcome {
            sequence: 4,
            result: json!({"html": html}).into(),
            effects,
        })
    );
    Ok(())
}

#[tokio::test]
async fn large_results_pass_on_as_they_arrive_and_unread_ones_are_skipped() -> Result<()> {
    let (host, customer) = UnixStream::pair()?;
    let (reader, writer) = host.into_split();
    let (executor, running) = JsActorExecutor::start(
        BufReader::new(reader),
        writer,
        vec!["counter".into()],
        Arc::default(),
    );
    let rows = json!({"rows": "x".repeat(2 * ARRIVING_BYTES)});
    let peer = async {
        let mut customer = BufReader::new(customer);
        for _ in 0..2 {
            let command = read_message(&mut customer).await?;
            write_message(
                &mut customer,
                &json!({"type": "reply", "message_id": command["message_id"], "reply": {
                    "type": "invoked", "result": rows
                }}),
            )
            .await?;
        }
        anyhow::Ok(())
    };
    let calls = async {
        for request in ["unread", "read"] {
            let outcome = executor
                .invoke(invocation(request, "rows"), actor_database::unused())
                .await?;
            let ActorMethodOutcome::Completed {
                result: result @ JsonPayload::Arriving(_),
                effects,
            } = outcome
            else {
                anyhow::bail!("expected a result that is still arriving");
            };
            assert!(effects.is_empty());
            if request == "read" {
                assert_eq!(result.into_text().await?.decode::<Value>()?, rows);
            }
        }
        anyhow::Ok(())
    };
    let result = timeout(Duration::from_secs(5), async {
        tokio::try_join!(peer, calls)
    })
    .await;
    running.abort();
    result??;
    Ok(())
}

#[tokio::test]
async fn a_result_cut_off_as_it_arrives_fails_and_ends_the_session() -> Result<()> {
    let (host, customer) = UnixStream::pair()?;
    let (reader, writer) = host.into_split();
    let (executor, running) = JsActorExecutor::start(
        BufReader::new(reader),
        writer,
        vec!["counter".into()],
        Arc::default(),
    );
    let peer = async {
        let mut customer = BufReader::new(customer);
        let command = read_message(&mut customer).await?;
        let line = json!({"type": "reply", "message_id": command["message_id"],
            "reply": {"type": "invoked"}, "parts": [2 * ARRIVING_BYTES, 2]});
        let cut = format!("{line}\n\"{}", "x".repeat(ARRIVING_BYTES));
        customer.write_all(cut.as_bytes()).await?;
        anyhow::Ok(())
    };
    let call = async {
        let outcome = executor
            .invoke(invocation("cut", "rows"), actor_database::unused())
            .await?;
        let ActorMethodOutcome::Completed { result, .. } = outcome else {
            anyhow::bail!("expected a completed result");
        };
        anyhow::Ok(result.into_text().await)
    };
    let ((), arrived) = timeout(Duration::from_secs(5), async {
        tokio::try_join!(peer, call)
    })
    .await??;
    let error = arrived.unwrap_err();
    assert!(
        format!("{error:#}").contains("ended before all of it arrived"),
        "{error:#}"
    );
    let session = timeout(Duration::from_secs(1), running)
        .await??
        .unwrap_err();
    assert!(
        format!("{session:#}").contains("disconnected inside a message"),
        "{session:#}"
    );
    Ok(())
}

#[tokio::test]
async fn messages_with_missing_extra_or_impossible_parts_end_the_session() -> Result<()> {
    for (message, problem) in [
        (
            &b"{\"type\":\"reply\",\"message_id\":1,\"reply\":{\"type\":\"invoked\"},\"parts\":[4]}\nnull"[..],
            "a part is missing",
        ),
        (
            b"{\"type\":\"reply\",\"message_id\":1,\"reply\":{\"type\":\"evicted\"},\"parts\":[0]}\n",
            "a part is left over",
        ),
        (
            b"{\"type\":\"socket_effects\",\"message_id\":1,\"parts\":[29,1]}\n[{\"message\":{\"type\":\"text\"}}]\xff",
            "invalid utf-8",
        ),
        (
            b"{\"type\":\"socket_effects\",\"message_id\":1,\"parts\":[18446744073709551615]}\n",
            "too large",
        ),
        (
            b"{\"type\":\"socket_effects\",\"message_id\":1,\"parts\":[9]}\n[]",
            "disconnected inside a message",
        ),
    ] {
        let (host, mut customer) = UnixStream::pair()?;
        let (reader, writer) = host.into_split();
        let (_executor, running) =
            JsActorExecutor::start(BufReader::new(reader), writer, vec!["counter".into()], Arc::default());
        customer.write_all(message).await?;
        customer.shutdown().await?;
        let error = timeout(Duration::from_secs(1), running)
            .await??
            .unwrap_err();
        assert!(format!("{error:#}").contains(problem), "{error:#}");
    }
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
async fn a_command_reaches_its_database_only_until_it_replies() -> Result<()> {
    let (host, customer) = UnixStream::pair()?;
    let (reader, writer) = host.into_split();
    let scopes = Arc::new(DatabaseScopes::default());
    let (executor, _running) = JsActorExecutor::start(
        BufReader::new(reader),
        writer,
        vec!["counter".into()],
        scopes.clone(),
    );
    let (host, mut database) = UnixStream::pair()?;
    tokio::spawn(database_channel::serve(host, scopes));
    let delayed = |requested| {
        Arc::new(actor_database::DelayedDatabase {
            requested,
            release: Arc::new(tokio::sync::Semaphore::new(2)),
            fail: false,
        })
    };
    let (requested, mut activation_calls) = mpsc::unbounded_channel();
    executor
        .mark_ready(counter(), None, None, Some(delayed(requested)), None, None)
        .await?;
    let (requested, mut command_calls) = mpsc::unbounded_channel();
    let caller = tokio::spawn({
        let (executor, database) = (executor.clone(), delayed(requested));
        async move { executor.invoke(invocation("write", "run"), database).await }
    });
    let mut customer = BufReader::new(customer);
    assert_eq!(read_message(&mut customer).await?["type"], "attached");
    let command = read_message(&mut customer).await?;
    let named = json!({"message_id": command["message_id"], "call": {"op": "commit"}});
    database_client::call(&mut database, named.clone(), &[]).await?;
    assert!(matches!(command_calls.try_recv()?, DatabaseCall::Commit));
    write_message(
        &mut customer,
        &json!({"type": "reply", "message_id": command["message_id"], "reply": {"type": "invoked", "result": 1}}),
    )
    .await?;
    caller.await??;
    let refused = database_client::call(&mut database, named, &[]).await?;
    assert_eq!(
        refused.ending,
        Err("the command this database call belongs to has ended".into())
    );
    assert!(command_calls.try_recv().is_err());
    database_client::call(&mut database, json!({"call": {"op": "commit"}}), &[]).await?;
    assert!(matches!(activation_calls.try_recv()?, DatabaseCall::Commit));
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
    let (executor, _running) = JsActorExecutor::start(
        BufReader::new(reader),
        writer,
        vec!["counter".into()],
        Arc::default(),
    );
    executor
        .mark_ready(counter(), None, None, None, Some(Arc::new(Echo)), None)
        .await?;
    let mut customer = BufReader::new(customer);
    assert_eq!(read_message(&mut customer).await?["type"], "attached");
    write_message(
        &mut customer,
        &json!({"type": "actor_call", "call_id": 7, "call": {"request_id": "call-7",
            "actor_name": "DocIndex", "actor_id": "main", "method": "add", "args": ["doc-1"]}}),
    )
    .await?;
    assert_eq!(
        read_message(&mut customer).await?,
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
    let (executor, _running) = JsActorExecutor::start(
        BufReader::new(reader),
        writer,
        vec!["counter".into()],
        Arc::default(),
    );
    executor
        .mark_ready(counter(), None, None, None, None, Some(logs.clone()))
        .await?;
    let mut customer = BufReader::new(customer);
    assert_eq!(read_message(&mut customer).await?["type"], "attached");
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
    let command = read_message(&mut customer).await?;
    for line in [
        json!({"type": "log", "message_id": command["message_id"], "level": "info", "message": "inside"}),
        json!({"type": "log", "message_id": 999, "level": "warn", "message": "unknown"}),
        json!({"type": "log", "level": "error", "message": "outside"}),
        json!({"type": "reply", "message_id": command["message_id"], "reply": {"type": "invoked", "result": 1}}),
    ] {
        write_message(&mut customer, &line).await?;
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
    let scopes = Arc::new(DatabaseScopes::default());
    let (executor, running) = JsActorExecutor::start(
        BufReader::new(reader),
        writer,
        vec!["counter".into()],
        scopes.clone(),
    );
    let (host, mut channel) = UnixStream::pair()?;
    tokio::spawn(database_channel::serve(host, scopes));
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
        assert_eq!(read_message(&mut customer).await?["type"], "attached");
        let slow = read_message(&mut customer).await?;
        let insert = json!({"message_id": slow["message_id"],
            "call": {"op": "exec", "sql": "INSERT INTO notes VALUES (?)", "immediate": true}});
        channel
            .write_all(&database_client::request(
                insert,
                &[SqlValue::Text("kept".into())],
            )?)
            .await?;
        let fast = read_message(&mut customer).await?;
        assert_eq!(fast["command"]["request_id"], "fast");
        write_message(
            &mut customer,
            &json!({"type": "reply", "message_id": fast["message_id"], "reply": {"type": "invoked", "result": 1}}),
        )
        .await?;
        let result = database_client::read_answer(&mut channel).await?.ending;
        let reply = if fail {
            let message = result.unwrap_err();
            assert_eq!(message, "storage rejected immediate write");
            json!({"type": "failed", "code": "actor_database_failed", "message": message})
        } else {
            assert_eq!(result, Ok(json!({"type": "done"})));
            json!({"type": "invoked", "result": 42})
        };
        write_message(
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
        args: Default::default(),
    }
}

async fn run_incrementing_customer(socket: PathBuf) -> Result<()> {
    let stream = UnixStream::connect(socket).await?;
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    writer
        .write_all(b"{\"type\":\"attach\",\"protocol\":34,\"actor_names\":[\"counter\"]}\n")
        .await?;
    ensure!(
        read_message(&mut reader).await?
            == json!({
                "type": "attached",
                "protocol": 34,
                "actor": {"project_id": "default", "actor_name": "counter", "actor_id": "one"}
            })
    );

    let invocation = read_message(&mut reader).await?;
    let invocation_id = invocation["message_id"]
        .as_u64()
        .context("invocation message ID")?;
    ensure!(invocation["command"]["type"] == "invoke");
    ensure!(invocation["command"].get("timeout_ms").is_none());
    write_message(
        &mut writer,
        &json!({
            "type": "reply",
            "message_id": invocation_id,
            "reply": {"type": "invoked", "result": 2}
        }),
    )
    .await?;

    let socket_event = read_message(&mut reader).await?;
    let socket_event_id = socket_event["message_id"]
        .as_u64()
        .context("socket event message ID")?;
    ensure!(socket_event["command"]["type"] == "websocket_event");
    ensure!(socket_event["command"]["event"]["type"] == "connect");
    ensure!(socket_event["command"].get("actor").is_none());
    write_message(
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

/// Reads a message as an SDK does: an invocation's arguments follow the line as a part.
async fn read_message<R>(reader: &mut R) -> Result<Value>
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    let mut line = String::new();
    ensure!(reader.read_line(&mut line).await? > 0, "expected a message");
    let mut message: Value = serde_json::from_str(line.trim_end())?;
    let Some(sizes) = message
        .as_object_mut()
        .and_then(|fields| fields.remove("parts"))
    else {
        return Ok(message);
    };
    ensure!(
        message["command"]["type"] == "invoke" && sizes == json!([sizes[0]]),
        "only an invocation's arguments follow a line"
    );
    let mut args = vec![0; sizes[0].as_u64().context("part size")? as usize];
    reader.read_exact(&mut args).await?;
    message["command"]["args"] = serde_json::from_slice(&args)?;
    Ok(message)
}

/// Writes a message as an SDK does: a result, socket effects and the text of each message
/// follow the line as parts.
async fn write_message<W>(writer: &mut W, message: &Value) -> Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    let (head, parts) = detach(message.clone())?;
    let mut bytes = serde_json::to_vec(&head)?;
    bytes.push(b'\n');
    bytes.extend(parts.into_iter().flatten());
    writer.write_all(&bytes).await?;
    Ok(())
}

fn detach(mut message: Value) -> Result<(Value, Vec<Vec<u8>>)> {
    let carrier = if message["type"] == "reply" {
        &mut message["reply"]
    } else {
        &mut message
    };
    let fields = carrier.as_object_mut().context("message object")?;
    let (result, effects) = (fields.remove("result"), fields.remove("effects"));
    if result.is_none() && effects.is_none() {
        return Ok((message, Vec::new()));
    }
    let mut effects = effects.unwrap_or_else(|| json!([]));
    let mut texts = Vec::new();
    for effect in effects.as_array_mut().context("effects")? {
        if effect["message"]["type"] == "text" {
            let text = effect["message"]["data"].take();
            effect["message"] = json!({"type": "text"});
            texts.push(text.as_str().context("text")?.as_bytes().to_vec());
        }
    }
    let mut parts = Vec::from_iter(result.map(|result| result.to_string().into_bytes()));
    parts.push(effects.to_string().into_bytes());
    parts.extend(texts);
    message["parts"] = parts.iter().map(Vec::len).collect();
    Ok((message, parts))
}
