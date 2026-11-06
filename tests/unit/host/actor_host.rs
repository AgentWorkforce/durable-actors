#[path = "../../fixtures/spans.rs"]
mod spans;
use crate::request_tracking::HostState;
use crate::{
    actor::{
        ActorDatabase, ActorMethodInvocation, ActorMethodOutcome, ActorSocketEffect,
        ActorSocketOutcome, DatabaseCall, SqlValue,
    },
    state_log::StateSnapshot,
    state_transport::StateWrite,
    storage::WritePlan,
};
use async_trait::async_trait;
use serde_json::Value;
use std::sync::{
    Mutex,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};

use serde_json::json;

use super::*;
use crate::actor::ActorKey;
use crate::host::actor_database::ActorStorage;

struct EmptySocketPublisher;

#[async_trait]
impl ActorSocketPublisher for EmptySocketPublisher {
    async fn publish(&self, _: Vec<ActorSocketEffect>) -> Result<()> {
        Ok(())
    }
}

struct IncrementingExecutor {
    invocations: AtomicU64,
}

#[path = "../../fixtures/host_sqlite.rs"]
mod sqlite;
use sqlite::{exec, fields, persist_fields, replication, write_fields};

fn counter() -> ActorKey {
    ActorKey {
        project_id: "default".into(),
        actor_name: "Counter".into(),
        actor_id: "counter-1".into(),
    }
}

fn create_host(
    endpoint: HostEndpoint,
    executor: Arc<dyn ActorExecutor>,
    storage: Arc<dyn ActorStorage>,
    writer: Arc<dyn crate::state_transport::SnapshotWriter>,
    publisher: Arc<dyn ActorSocketPublisher>,
    replication: Arc<dyn crate::litestream::Replicator>,
) -> ActorHost {
    let database = HostDatabase::new(endpoint.clone(), storage, writer, replication);
    ActorHost::new(endpoint, counter(), executor, database, publisher)
}

type RestoredDatabase = (Value, Vec<Vec<SqlValue>>);

#[tokio::test]
async fn commits_fields_and_sql_and_recovers_them_at_one_version() -> Result<()> {
    struct Executor {
        restored: Mutex<Option<RestoredDatabase>>,
    }
    #[async_trait]
    impl ActorExecutor for Executor {
        fn supports(&self, _: &str) -> bool {
            true
        }
        async fn invoke(
            &self,
            invocation: ActorMethodInvocation,
            database: Arc<dyn ActorDatabase>,
        ) -> Result<ActorMethodOutcome> {
            match invocation.request_id.as_str() {
                "sql" => {
                    exec(&*database, "CREATE TABLE entries(value TEXT)").await?;
                    exec(&*database, "INSERT INTO entries VALUES ('retained')").await?;
                }
                "schema" => {
                    exec(
                        &*database,
                        "ALTER TABLE entries ADD COLUMN enabled INTEGER DEFAULT 1",
                    )
                    .await?;
                }
                "read-restored" => {
                    *self.restored.lock().unwrap() = Some((
                        fields(&*database).await?,
                        exec(&*database, "SELECT value, enabled FROM entries").await?,
                    ));
                }
                _ => {}
            }
            let mut values = fields(&*database).await?;
            if invocation.request_id == "initial" {
                values["count"] = json!(0);
            }
            if invocation.request_id == "fields" {
                values["count"] = json!(1);
            }
            let result = values["count"].clone();
            write_fields(&*database, values).await?;
            Ok(ActorMethodOutcome::Completed {
                result,
                effects: vec![],
            })
        }
    }
    let executor = Arc::new(Executor {
        restored: Mutex::new(None),
    });
    let writes = Arc::new(FakeStateTransport::default());
    let endpoint = HostEndpoint {
        id: super::super::HostId::new("host-1"),
        route: "http://host.invalid/".into(),
    };
    let host = create_host(
        endpoint.clone(),
        executor.clone(),
        Arc::new(FakeAuthority::default()),
        writes.clone(),
        Arc::new(EmptySocketPublisher),
        replication().await,
    );
    for request in [
        "initial", "read-1", "sql", "read-2", "fields", "read-3", "schema", "read-4",
    ] {
        if request == "sql" {
            writes.failures.store(1, Ordering::SeqCst);
            assert!(matches!(
                invoke(&host, request).await?,
                ActorExecutionResult::Failed { .. }
            ));
        } else {
            assert!(matches!(
                invoke(&host, request).await?,
                ActorExecutionResult::Completed { .. }
            ));
        }
    }
    host.drain(Duration::from_secs(1)).await?;
    let mut snapshots = writes.writes.lock().unwrap().clone();
    assert_eq!(
        snapshots[1], snapshots[2],
        "retry preserves the exact commit"
    );
    snapshots.dedup();
    assert_eq!(snapshots.len(), 4, "reads preserve the durable version");
    let decoded = snapshots
        .iter()
        .map(|bytes| StateSnapshot::decode(bytes))
        .collect::<Result<Vec<_>>>()?;
    let history = decoded
        .iter()
        .skip(1)
        .zip(snapshots.iter())
        .map(|(next, bytes)| {
            (
                next.sqlite.parent.as_ref().unwrap().object.clone(),
                bytes.clone(),
            )
        })
        .collect();
    let restored = create_host(
        endpoint,
        executor.clone(),
        Arc::new(FakeAuthority {
            initial_state: Some((4, snapshots[3].clone().into())),
            history,
            ..Default::default()
        }),
        writes.clone(),
        Arc::new(EmptySocketPublisher),
        replication().await,
    );
    assert_eq!(invoke(&restored, "read-restored").await?, completed(1));
    assert_eq!(
        executor.restored.lock().unwrap().clone().unwrap(),
        (
            json!({"count": 1}),
            vec![vec![
                SqlValue::Text("retained".into()),
                SqlValue::Integer(1)
            ]]
        )
    );
    assert_eq!(writes.writes.lock().unwrap().len(), 5);
    restored.drain(Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn ordinary_methods_execute_and_commit_without_a_socket_gateway() -> Result<()> {
    let executor = Arc::new(IncrementingExecutor {
        invocations: AtomicU64::new(0),
    });
    let state = Arc::new(FakeStateTransport::default());
    let host = create_host(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host.invalid/".into(),
        },
        executor.clone(),
        Arc::new(FakeAuthority::default()),
        state.clone(),
        Arc::new(EmptySocketPublisher),
        replication().await,
    );
    assert!(matches!(
        invoke(&host, "request-1").await?,
        ActorExecutionResult::Completed { result, .. } if result == json!(1)
    ));
    assert_eq!(executor.invocations.load(Ordering::Relaxed), 1);
    assert_eq!(state.writes.lock().unwrap().len(), 1);
    let snapshot = StateSnapshot::decode(&state.writes.lock().unwrap()[0])?;
    let attribution = snapshot.attribution.unwrap();
    assert_eq!(attribution.operation, "increment");
    assert!(attribution.connection_id.is_none());
    assert!(attribution.committed_at_ms > 0);
    Ok(())
}

struct SerialAdmissionExecutor {
    inner: IncrementingExecutor,
    admission: watch::Sender<()>,
}

impl SerialAdmissionExecutor {
    fn new() -> Self {
        Self {
            inner: IncrementingExecutor {
                invocations: AtomicU64::new(0),
            },
            admission: watch::channel(()).0,
        }
    }
}

#[async_trait]
impl ActorExecutor for SerialAdmissionExecutor {
    fn supports(&self, _: &str) -> bool {
        true
    }
    fn invocation_admission(&self) -> Option<watch::Receiver<()>> {
        Some(self.admission.subscribe())
    }
    async fn invoke(
        &self,
        invocation: ActorMethodInvocation,
        database: Arc<dyn ActorDatabase>,
    ) -> Result<ActorMethodOutcome> {
        self.inner.invoke(invocation, database).await
    }
}

struct ExhaustedExecutor;

#[tokio::test]
async fn reentrant_completions_commit_in_snapshot_order_even_when_delivered_out_of_order()
-> Result<()> {
    assert_reentrant_commit_order(false).await
}

#[tokio::test]
async fn a_failed_reentrant_commit_stops_the_activation_without_acknowledging_later_writes()
-> Result<()> {
    assert_reentrant_commit_order(true).await
}

async fn assert_reentrant_commit_order(fail_write: bool) -> Result<()> {
    struct ReentrantExecutor {
        admission: watch::Sender<()>,
        first_started: tokio::sync::Notify,
        second_started: tokio::sync::Notify,
    }
    #[async_trait]
    impl ActorExecutor for ReentrantExecutor {
        fn supports(&self, _: &str) -> bool {
            true
        }
        fn invocation_admission(&self) -> Option<watch::Receiver<()>> {
            Some(self.admission.subscribe())
        }
        async fn invoke(
            &self,
            invocation: ActorMethodInvocation,
            database: Arc<dyn ActorDatabase>,
        ) -> Result<ActorMethodOutcome> {
            let count = if invocation.request_id == "first" {
                1
            } else {
                2
            };
            write_fields(&*database, json!({ "count": count })).await?;
            let sequence = if invocation.request_id == "first" {
                self.admission.send_replace(());
                self.first_started.notify_one();
                self.second_started.notified().await;
                tokio::time::sleep(Duration::from_millis(30)).await;
                1
            } else {
                self.second_started.notify_one();
                2
            };
            Ok(ActorMethodOutcome::Interleaved(
                crate::actor::ActorInterleavedOutcome {
                    sequence,
                    result: json!(sequence),
                    effects: vec![],
                },
            ))
        }
    }
    let executor = Arc::new(ReentrantExecutor {
        admission: watch::channel(()).0,
        first_started: Default::default(),
        second_started: Default::default(),
    });
    let state = Arc::new(FakeStateTransport::default());
    if fail_write {
        state.failures.store(1, Ordering::SeqCst);
    }
    let host = create_host(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host.invalid/".into(),
        },
        executor.clone(),
        Arc::new(FakeAuthority::default()),
        state.clone(),
        Arc::new(EmptySocketPublisher),
        replication().await,
    );
    let operation = async {
        let first = invoke(&host, "first");
        let second = async {
            executor.first_started.notified().await;
            invoke(&host, "second").await
        };
        let (first, second) = tokio::join!(first, second);
        if fail_write {
            for result in [first?, second?] {
                assert!(
                    matches!(result, ActorExecutionResult::Failed { failure } if failure.code == "outcome_unknown")
                );
            }
            assert_eq!(state.writes.lock().unwrap().len(), 1);
            assert_eq!(
                invoke(&host, "after-failure").await?,
                ActorExecutionResult::HostUnavailable
            );
            assert_eq!(host.status().borrow().phase, ActorPhase::Stopped);
            return Ok(());
        }
        assert_eq!(first?, completed(1));
        assert_eq!(second?, completed(2));
        let writes = state.writes.lock().unwrap();
        assert_eq!(writes.len(), 2);
        assert_eq!(
            StateSnapshot::decode(&writes[0])?
                .completion
                .unwrap()
                .result,
            json!(1)
        );
        assert_eq!(
            StateSnapshot::decode(&writes[1])?
                .completion
                .unwrap()
                .result,
            json!(2)
        );
        Ok::<_, anyhow::Error>(())
    };
    tokio::time::timeout(Duration::from_secs(2), operation).await??;
    host.drain(Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn an_unconfirmed_interleaved_commit_evicts_once_and_stops_the_actor() -> Result<()> {
    struct Executor {
        evictions: AtomicUsize,
    }
    #[async_trait]
    impl ActorExecutor for Executor {
        fn supports(&self, _: &str) -> bool {
            true
        }
        async fn invoke(
            &self,
            _: ActorMethodInvocation,
            database: Arc<dyn ActorDatabase>,
        ) -> Result<ActorMethodOutcome> {
            write_fields(&*database, json!({ "count": 1 })).await?;
            Ok(ActorMethodOutcome::Interleaved(
                crate::actor::ActorInterleavedOutcome {
                    sequence: 1,
                    result: json!(1),
                    effects: vec![],
                },
            ))
        }
        async fn evict(&self, _: bool, _: Arc<dyn ActorDatabase>) -> Result<()> {
            self.evictions.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    let executor = Arc::new(Executor {
        evictions: AtomicUsize::new(0),
    });
    let state = Arc::new(FakeStateTransport::default());
    state.failures.store(1, Ordering::SeqCst);
    let host = create_host(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host.invalid/".into(),
        },
        executor.clone(),
        Arc::new(FakeAuthority::default()),
        state,
        Arc::new(EmptySocketPublisher),
        replication().await,
    );
    assert!(matches!(
        invoke(&host, "first").await?,
        ActorExecutionResult::Failed { failure } if failure.code == "outcome_unknown"
    ));
    host.drain(Duration::from_secs(1)).await?;
    assert_eq!(host.status().borrow().phase, ActorPhase::Stopped);
    assert_eq!(executor.evictions.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn a_reentrant_completion_cannot_open_an_ordinary_invocations_gate() -> Result<()> {
    struct Executor {
        admission: watch::Sender<()>,
        started: mpsc::UnboundedSender<String>,
        release_background: tokio::sync::Notify,
        release_ordinary: tokio::sync::Notify,
    }
    #[async_trait]
    impl ActorExecutor for Executor {
        fn supports(&self, _: &str) -> bool {
            true
        }
        fn invocation_admission(&self) -> Option<watch::Receiver<()>> {
            Some(self.admission.subscribe())
        }
        async fn invoke(
            &self,
            invocation: ActorMethodInvocation,
            database: Arc<dyn ActorDatabase>,
        ) -> Result<ActorMethodOutcome> {
            self.started.send(invocation.request_id.clone())?;
            let sequence = match invocation.request_id.as_str() {
                "background" => {
                    self.admission.send_replace(());
                    self.release_background.notified().await;
                    1
                }
                "ordinary" => {
                    self.release_ordinary.notified().await;
                    2
                }
                "next" => 3,
                _ => unreachable!(),
            };
            write_fields(&*database, json!({ "count": sequence })).await?;
            Ok(ActorMethodOutcome::Interleaved(
                crate::actor::ActorInterleavedOutcome {
                    sequence,
                    result: json!(sequence),
                    effects: vec![],
                },
            ))
        }
    }
    let (started, mut starts) = mpsc::unbounded_channel();
    let executor = Arc::new(Executor {
        admission: watch::channel(()).0,
        started,
        release_background: Default::default(),
        release_ordinary: Default::default(),
    });
    let host = Arc::new(create_host(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host.invalid/".into(),
        },
        executor.clone(),
        Arc::new(FakeAuthority::default()),
        Arc::new(FakeStateTransport::default()),
        Arc::new(EmptySocketPublisher),
        replication().await,
    ));
    let operation = async {
        let caller = host.clone();
        let background = tokio::spawn(async move { invoke(&caller, "background").await });
        assert_eq!(starts.recv().await.as_deref(), Some("background"));
        let caller = host.clone();
        let ordinary = tokio::spawn(async move { invoke(&caller, "ordinary").await });
        assert_eq!(starts.recv().await.as_deref(), Some("ordinary"));
        let caller = host.clone();
        let next = tokio::spawn(async move { invoke(&caller, "next").await });
        executor.release_background.notify_one();
        assert_eq!(background.await??, completed(1));
        assert!(
            tokio::time::timeout(Duration::from_millis(30), starts.recv())
                .await
                .is_err()
        );
        executor.release_ordinary.notify_one();
        assert_eq!(ordinary.await??, completed(2));
        assert_eq!(starts.recv().await.as_deref(), Some("next"));
        assert_eq!(next.await??, completed(3));
        Ok::<_, anyhow::Error>(())
    };
    tokio::time::timeout(Duration::from_secs(2), operation).await??;
    host.drain(Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn application_errors_preserve_the_worker_and_committed_state() -> Result<()> {
    struct FailingExecutor {
        evictions: AtomicUsize,
    }

    #[async_trait]
    impl ActorExecutor for FailingExecutor {
        fn supports(&self, _: &str) -> bool {
            true
        }

        async fn invoke(
            &self,
            invocation: ActorMethodInvocation,
            database: Arc<dyn ActorDatabase>,
        ) -> Result<ActorMethodOutcome> {
            if invocation.request_id.starts_with("fail") {
                if invocation.request_id == "fail-fatal" {
                    persist_fields(&*database, json!({"count": 999}), false).await?;
                    exec(&*database, "CREATE TABLE abandoned(value INTEGER)").await?;
                }
                return Ok(ActorMethodOutcome::Failed(ActorInvocationFailure {
                    code: if invocation.request_id == "fail-application" {
                        "actor_method_failed"
                    } else {
                        "invalid_actor_state"
                    }
                    .into(),
                    message: "failed".into(),
                }));
            }
            if invocation.request_id == "after-failure" {
                assert_eq!(
                    exec(
                        &*database,
                        "SELECT COUNT(*) FROM sqlite_schema WHERE name='abandoned'"
                    )
                    .await?,
                    vec![vec![SqlValue::Integer(0)]]
                );
            }
            increment(&*database).await
        }

        async fn evict(&self, _: bool, _: Arc<dyn ActorDatabase>) -> Result<()> {
            self.evictions.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    let executor = Arc::new(FailingExecutor {
        evictions: AtomicUsize::new(0),
    });
    let state = Arc::new(FakeStateTransport::default());
    let host = create_host(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host.invalid/".into(),
        },
        executor.clone(),
        Arc::new(FakeAuthority {
            persisted: Some(state.clone()),
            ..Default::default()
        }),
        state.clone(),
        Arc::new(EmptySocketPublisher),
        replication().await,
    );
    assert_eq!(invoke(&host, "first").await?, completed(1));
    for request in ["fail-application", "fail-fatal"] {
        assert!(
            matches!(invoke(&host, request).await?, ActorExecutionResult::Failed { failure } if failure.code == "actor_error")
        );
        assert_eq!(
            executor.evictions.load(Ordering::Relaxed),
            usize::from(request == "fail-fatal")
        );
        assert_eq!(state.writes.lock().unwrap().len(), 1);
    }
    assert_eq!(invoke(&host, "after-failure").await?, completed(2));
    host.drain(Duration::from_secs(1)).await?;
    Ok(())
}

struct InvalidEffectsExecutor;

struct ControlledExecutor {
    started: tokio::sync::mpsc::UnboundedSender<String>,
    release: Arc<tokio::sync::Semaphore>,
}

#[async_trait]
impl ActorExecutor for ControlledExecutor {
    fn supports(&self, _: &str) -> bool {
        true
    }

    async fn invoke(
        &self,
        invocation: ActorMethodInvocation,
        database: Arc<dyn ActorDatabase>,
    ) -> Result<ActorMethodOutcome> {
        self.started.send(invocation.request_id.clone())?;
        if invocation.request_id == "panic" {
            panic!("actor executor panicked");
        }
        if invocation.request_id == "first" {
            self.release.acquire().await?.forget();
        }
        increment(&*database).await
    }

    async fn handle_socket(
        &self,
        invocation: ActorSocketInvocation,
        database: Arc<dyn ActorDatabase>,
    ) -> Result<ActorSocketOutcome> {
        let result = self
            .invoke(
                ActorMethodInvocation {
                    request_id: invocation.request_id,
                    method: "onMessage".into(),
                    args: Vec::new(),
                },
                database,
            )
            .await?;
        match result {
            ActorMethodOutcome::Interleaved(outcome) => {
                Ok(ActorSocketOutcome::Interleaved(outcome))
            }
            ActorMethodOutcome::Completed { effects, .. } => {
                Ok(ActorSocketOutcome::Handled { effects })
            }
            ActorMethodOutcome::Failed(failure) => Ok(ActorSocketOutcome::Failed(failure)),
        }
    }
}

async fn controlled_host() -> (
    Arc<ActorHost>,
    tokio::sync::mpsc::UnboundedReceiver<String>,
    Arc<tokio::sync::Semaphore>,
) {
    let (started, receiver) = tokio::sync::mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let host = create_host(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host.invalid/".into(),
        },
        Arc::new(ControlledExecutor {
            started,
            release: release.clone(),
        }),
        Arc::new(FakeAuthority::default()),
        Arc::new(FakeStateTransport::default()),
        Arc::new(EmptySocketPublisher),
        replication().await,
    );
    (Arc::new(host), receiver, release)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_actor_task_releases_admission_and_does_not_restart_unknown_state() -> Result<()> {
    for _ in 0..64 {
        let (host, mut started, release) = controlled_host().await;
        let mut status = host.status();
        let caller = host.clone();
        let first = tokio::spawn(async move { invoke(&caller, "first").await });
        assert_eq!(started.recv().await.as_deref(), Some("first"));
        let caller = host.clone();
        let panicking = tokio::spawn(async move { invoke(&caller, "panic").await });
        tokio::time::timeout(
            Duration::from_secs(2),
            status.wait_for(|status| status.waiting.len() == 1),
        )
        .await??;
        release.add_permits(1);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), first).await???,
            completed(1)
        );
        let result = tokio::time::timeout(Duration::from_secs(2), panicking).await???;
        assert!(
            matches!(result, ActorExecutionResult::Failed { failure } if failure.code == "outcome_unknown")
        );
        assert_eq!(
            invoke(&host, "after-panic").await?,
            ActorExecutionResult::HostUnavailable
        );
        host.drain(Duration::from_secs(1)).await?;
        assert_eq!(status.borrow().phase, ActorPhase::Stopped);
        assert!(status.borrow().waiting.is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn requests_that_never_started_are_unavailable_when_the_actor_stops() -> Result<()> {
    struct Executor(tokio::sync::Semaphore);
    #[async_trait]
    impl ActorExecutor for Executor {
        fn supports(&self, _: &str) -> bool {
            true
        }
        async fn invoke(
            &self,
            _: ActorMethodInvocation,
            _: Arc<dyn ActorDatabase>,
        ) -> Result<ActorMethodOutcome> {
            self.0.acquire().await?.forget();
            panic!("actor executor panicked");
        }
    }
    let executor = Arc::new(Executor(tokio::sync::Semaphore::new(0)));
    let host = Arc::new(create_host(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host.invalid/".into(),
        },
        executor.clone(),
        Arc::new(FakeAuthority::default()),
        Arc::new(FakeStateTransport::default()),
        Arc::new(EmptySocketPublisher),
        replication().await,
    ));
    let mut status = host.status();
    let caller = host.clone();
    let first = tokio::spawn(async move { invoke(&caller, "first").await });
    tokio::time::timeout(
        Duration::from_secs(2),
        status.wait_for(|status| status.idle_since.is_none()),
    )
    .await??;
    let caller = host.clone();
    let queued = tokio::spawn(async move { invoke(&caller, "queued").await });
    tokio::time::timeout(
        Duration::from_secs(2),
        status.wait_for(|status| status.waiting.len() == 1),
    )
    .await??;
    executor.0.add_permits(1);
    assert!(matches!(
        first.await??,
        ActorExecutionResult::Failed { failure } if failure.code == "outcome_unknown"
    ));
    assert_eq!(queued.await??, ActorExecutionResult::HostUnavailable);
    Ok(())
}

#[tokio::test]
async fn interleaved_actors_replay_durable_results_without_reexecution() -> Result<()> {
    struct Executor(AtomicU64);
    #[async_trait]
    impl ActorExecutor for Executor {
        fn supports(&self, _: &str) -> bool {
            true
        }
        async fn invoke(
            &self,
            _: ActorMethodInvocation,
            database: Arc<dyn ActorDatabase>,
        ) -> Result<ActorMethodOutcome> {
            let sequence = self.0.fetch_add(1, Ordering::Relaxed) + 1;
            write_fields(&*database, json!({"count": sequence})).await?;
            Ok(ActorMethodOutcome::Interleaved(
                crate::actor::ActorInterleavedOutcome {
                    sequence,
                    result: json!(sequence),
                    effects: vec![],
                },
            ))
        }
    }
    let executor = Arc::new(Executor(AtomicU64::new(0)));
    let host = create_host(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host.invalid/".into(),
        },
        executor.clone(),
        Arc::new(FakeAuthority::default()),
        Arc::new(FakeStateTransport::default()),
        Arc::new(EmptySocketPublisher),
        replication().await,
    );
    assert_eq!(invoke(&host, "first").await?, completed(1));
    assert_eq!(invoke(&host, "first").await?, completed(1));
    assert_eq!(executor.0.load(Ordering::Relaxed), 1);
    assert_eq!(invoke(&host, "second").await?, completed(2));
    host.drain(Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn interleaved_admission_continues_while_database_publication_is_pending() -> Result<()> {
    struct Executor {
        admission: watch::Sender<()>,
        immediate: bool,
    }
    #[async_trait]
    impl ActorExecutor for Executor {
        fn supports(&self, _: &str) -> bool {
            true
        }
        fn invocation_admission(&self) -> Option<watch::Receiver<()>> {
            Some(self.admission.subscribe())
        }
        async fn invoke(
            &self,
            invocation: ActorMethodInvocation,
            database: Arc<dyn ActorDatabase>,
        ) -> Result<ActorMethodOutcome> {
            let sequence = if invocation.request_id == "first" {
                1
            } else {
                2
            };
            persist_fields(&*database, json!({"count": sequence}), self.immediate).await?;
            database.call(DatabaseCall::Commit).await?;
            Ok(ActorMethodOutcome::Interleaved(
                crate::actor::ActorInterleavedOutcome {
                    sequence,
                    result: json!(sequence),
                    effects: vec![],
                },
            ))
        }
    }

    for immediate in [false, true] {
        let (started, mut saving) = mpsc::unbounded_channel();
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let executor = Arc::new(Executor {
            admission: watch::channel(()).0,
            immediate,
        });
        let host = Arc::new(create_host(
            HostEndpoint {
                id: super::super::HostId::new("host-1"),
                route: "http://host.invalid/".into(),
            },
            executor.clone(),
            Arc::new(FakeAuthority::default()),
            Arc::new(FakeStateTransport {
                paused_commit: Some((started, release.clone())),
                ..Default::default()
            }),
            Arc::new(EmptySocketPublisher),
            replication().await,
        ));
        let caller = host.clone();
        let first = tokio::spawn(async move { invoke(&caller, "first").await });
        tokio::time::timeout(Duration::from_secs(2), saving.recv())
            .await?
            .unwrap();
        let mut status = host.status();
        let caller = host.clone();
        let second = tokio::spawn(async move { invoke(&caller, "second").await });
        tokio::time::timeout(
            Duration::from_secs(2),
            status.wait_for(|status| !status.waiting.is_empty()),
        )
        .await??;
        executor.admission.send_replace(());
        let admitted = tokio::time::timeout(
            Duration::from_secs(1),
            status.wait_for(|status| status.waiting.is_empty()),
        )
        .await
        .is_ok();
        assert!(!first.is_finished(), "write acknowledged before storage");
        release.add_permits(1);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), first).await???,
            completed(1)
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), second).await???,
            completed(2)
        );
        assert!(
            admitted,
            "pending database publication blocked mailbox admission (immediate={immediate})"
        );
        host.drain(Duration::from_secs(1)).await?;
    }
    Ok(())
}

#[tokio::test]
async fn caller_cancellation_cannot_release_an_actor_during_its_commit() -> Result<()> {
    let (commit_started, mut committing) = mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let authority = Arc::new(FakeAuthority::default());
    let state = Arc::new(FakeStateTransport {
        paused_commit: Some((commit_started, release.clone())),
        ..Default::default()
    });
    let executor = Arc::new(SerialAdmissionExecutor::new());
    let host = Arc::new(create_host(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host.invalid/".into(),
        },
        executor.clone(),
        authority.clone(),
        state.clone(),
        Arc::new(EmptySocketPublisher),
        replication().await,
    ));
    let caller = host.clone();
    let first = tokio::spawn(async move { invoke(&caller, "first").await });
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), committing.recv()).await?,
        Some(())
    );
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    let caller = host.clone();
    let mut second = tokio::spawn(async move { invoke(&caller, "second").await });
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut second)
            .await
            .is_err()
    );
    assert_eq!(executor.inner.invocations.load(Ordering::Relaxed), 1);
    release.add_permits(1);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), second).await???,
        completed(2)
    );
    assert_eq!(state.writes.lock().unwrap().len(), 2);
    host.drain(Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn traces_measure_queue_wait_and_record_panics() -> Result<()> {
    let (host, mut started, release) = controlled_host().await;
    let (tracer, mut traces) = spans::request_spans();
    let host = Arc::new(Arc::try_unwrap(host).ok().unwrap().with_traces(tracer));
    let caller = host.clone();
    let first = tokio::spawn(async move { invoke(&caller, "first").await });
    assert_eq!(started.recv().await.as_deref(), Some("first"));
    let caller = host.clone();
    let second = tokio::spawn(async move { invoke(&caller, "second").await });
    let mut status = host.status();
    tokio::time::timeout(
        Duration::from_secs(2),
        status.wait_for(|status| status.waiting.len() == 1),
    )
    .await??;
    tokio::time::sleep(Duration::from_millis(25)).await;
    release.add_permits(1);
    first.await??;
    second.await??;
    let first_trace = traces.recv().await.unwrap();
    let second_trace = traces.recv().await.unwrap();
    assert_eq!(first_trace.request_id, "first");
    assert_eq!(second_trace.request_id, "second");
    assert_eq!(first_trace.state_version, Some(1));
    assert_eq!(second_trace.state_version, Some(2));
    assert!(second_trace.queue_wait_ms.unwrap() >= 25.0);
    assert!(second_trace.duration_ms >= second_trace.queue_wait_ms.unwrap());
    let _ = invoke(&host, "panic").await?;
    let interrupted = traces.recv().await.unwrap();
    assert!(matches!(
        interrupted.outcome,
        crate::request_traces::RequestOutcome::Interrupted
    ));
    host.drain(Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn cancelled_callers_do_not_interrupt_accepted_actor_operations() -> Result<()> {
    for socket in [false, true] {
        let (host, mut started, release) = controlled_host().await;
        let caller = host.clone();
        let first = tokio::spawn(async move {
            if socket {
                caller
                    .submit(
                        ActorOperation::Socket(ActorSocketInvocation {
                            request_id: "first".into(),
                            actor: ActorKey {
                                project_id: "default".into(),
                                actor_name: "Counter".into(),
                                actor_id: "counter-1".into(),
                            },
                            event: crate::actor::ActorSocketEvent::Message {
                                connection_id: "socket-1".into(),
                                message: crate::actor::ActorSocketMessage::Text {
                                    data: "increment".into(),
                                },
                            },
                            connections: Vec::new(),
                        }),
                        1,
                    )
                    .await
            } else {
                invoke(&caller, "first").await
            }
        });
        assert_eq!(started.recv().await.as_deref(), Some("first"));
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        let caller = host.clone();
        let second = tokio::spawn(async move { invoke(&caller, "second").await });
        assert!(
            tokio::time::timeout(Duration::from_millis(30), started.recv())
                .await
                .is_err(),
            "the next call overtook an accepted operation"
        );
        release.add_permits(1);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), second).await???,
            completed(2)
        );
        host.drain(Duration::from_secs(1)).await?;
    }
    Ok(())
}

#[tokio::test]
async fn queued_actor_bursts_respect_shutdown_fencing() -> Result<()> {
    let (host, mut started, release) = controlled_host().await;
    let mut status = host.status();
    let caller = host.clone();
    let first = tokio::spawn(async move { invoke(&caller, "first").await });
    assert_eq!(started.recv().await.as_deref(), Some("first"));
    let mut queued = Vec::new();
    for index in 0..128 {
        let caller = host.clone();
        queued.push(tokio::spawn(async move {
            invoke(&caller, &format!("queued-{index}")).await
        }));
    }
    tokio::time::timeout(
        Duration::from_secs(2),
        status.wait_for(|status| status.waiting.len() == 128),
    )
    .await??;
    assert!(host.drain(Duration::from_millis(30)).await.is_err());
    assert_eq!(
        invoke(&host, "draining").await?,
        ActorExecutionResult::HostUnavailable
    );
    release.add_permits(1);
    assert_eq!(first.await??, completed(1));
    for caller in queued {
        assert_eq!(caller.await??, ActorExecutionResult::HostUnavailable);
    }
    host.drain(Duration::from_secs(1)).await?;
    assert!(status.borrow().idle_since.is_some());
    assert!(status.borrow().waiting.is_empty());
    Ok(())
}

#[async_trait]
impl ActorExecutor for IncrementingExecutor {
    fn supports(&self, actor_name: &str) -> bool {
        actor_name == "Counter"
    }

    async fn invoke(
        &self,
        _invocation: ActorMethodInvocation,
        database: Arc<dyn ActorDatabase>,
    ) -> Result<ActorMethodOutcome> {
        self.invocations.fetch_add(1, Ordering::Relaxed);
        increment(&*database).await
    }

    async fn handle_socket(
        &self,
        _invocation: ActorSocketInvocation,
        database: Arc<dyn ActorDatabase>,
    ) -> Result<ActorSocketOutcome> {
        let count = fields(&*database).await?["count"].as_u64().unwrap_or(0) + 1;
        write_fields(&*database, json!({ "count": count })).await?;
        Ok(ActorSocketOutcome::Handled {
            effects: vec![ActorSocketEffect::Send {
                connection_id: "socket-1".into(),
                message: crate::actor::ActorSocketMessage::Text {
                    data: "ready".into(),
                },
            }],
        })
    }
}

#[async_trait]
impl ActorExecutor for ExhaustedExecutor {
    fn supports(&self, _actor_name: &str) -> bool {
        true
    }

    async fn invoke(
        &self,
        _invocation: ActorMethodInvocation,
        _database: Arc<dyn ActorDatabase>,
    ) -> Result<ActorMethodOutcome> {
        Ok(ActorMethodOutcome::Failed(ActorInvocationFailure {
            code: "resource_exhausted".into(),
            message: "actor session message is too large".into(),
        }))
    }
}

#[async_trait]
impl ActorExecutor for InvalidEffectsExecutor {
    fn supports(&self, _actor_name: &str) -> bool {
        true
    }

    async fn invoke(
        &self,
        _invocation: ActorMethodInvocation,
        database: Arc<dyn ActorDatabase>,
    ) -> Result<ActorMethodOutcome> {
        write_fields(&*database, json!({ "count": 1 })).await?;
        Ok(ActorMethodOutcome::Completed {
            result: Value::Null,
            effects: vec![ActorSocketEffect::Close {
                connection_id: "socket-1".into(),
                code: 1001,
                reason: String::new(),
            }],
        })
    }
}

#[tokio::test]
async fn activation_acquires_on_host_without_preparing_a_write() -> Result<()> {
    let authority = Arc::new(FakeAuthority {
        ..Default::default()
    });
    let actor = counter();
    let host = create_host(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host".into(),
        },
        Arc::new(IncrementingExecutor {
            invocations: AtomicU64::new(0),
        }),
        authority.clone(),
        Arc::new(FakeStateTransport::default()),
        Arc::new(EmptySocketPublisher),
        replication().await,
    );
    let activation = host.activate().await?;
    assert_eq!(activation.owner_epoch, 7);
    assert!(authority.preparations.lock().unwrap().is_empty());
    let invoke = |request_id: &str| ActorInvocation {
        request_id: request_id.into(),
        actor: actor.clone(),
        method: "increment".into(),
        args: vec![],
    };
    assert_eq!(
        host.invoke_actor(invoke("one"), 7, HostState::Warm, 0.0)
            .await
            .0?,
        completed(1)
    );
    host.activate().await?;
    assert_eq!(
        host.invoke_actor(invoke("two"), 7, HostState::Warm, 0.0)
            .await
            .0?,
        completed(2)
    );
    assert_eq!(*authority.preparations.lock().unwrap(), vec![0]);

    authority.fenced.store(true, Ordering::SeqCst);
    assert!(host.activate().await.is_err());
    Ok(())
}

#[tokio::test]
async fn activation_services_immediate_writes_during_hydration() -> Result<()> {
    struct HydratingExecutor {
        admission: watch::Sender<()>,
        hydrated: mpsc::UnboundedSender<oneshot::Sender<()>>,
        started: mpsc::UnboundedSender<String>,
    }

    #[async_trait]
    impl ActorExecutor for HydratingExecutor {
        fn supports(&self, _: &str) -> bool {
            true
        }

        fn invocation_admission(&self) -> Option<watch::Receiver<()>> {
            Some(self.admission.subscribe())
        }

        async fn hydrate(
            &self,
            request_id: String,
            database: Arc<dyn ActorDatabase>,
        ) -> Result<()> {
            assert_eq!(request_id, "activate");
            self.admission.send_replace(());
            persist_fields(&*database, json!({"resumed": true}), true).await?;
            let (reply, acknowledged) = oneshot::channel();
            self.hydrated.send(reply)?;
            acknowledged.await?;
            Ok(())
        }

        async fn invoke(
            &self,
            invocation: ActorMethodInvocation,
            _: Arc<dyn ActorDatabase>,
        ) -> Result<ActorMethodOutcome> {
            self.started.send(invocation.request_id)?;
            Ok(ActorMethodOutcome::Failed(ActorInvocationFailure {
                code: "actor_method_failed".into(),
                message: "finished".into(),
            }))
        }
    }

    let (hydrated, mut captured) = mpsc::unbounded_channel();
    let (started, mut invocations) = mpsc::unbounded_channel();
    let state = Arc::new(FakeStateTransport::default());
    let actor = counter();
    let host = Arc::new(create_host(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host".into(),
        },
        Arc::new(HydratingExecutor {
            admission: watch::channel(()).0,
            hydrated,
            started,
        }),
        Arc::new(FakeAuthority::default()),
        state.clone(),
        Arc::new(EmptySocketPublisher),
        replication().await,
    ));

    let caller = host.clone();
    let activating = tokio::spawn(async move { caller.activate().await });
    let acknowledged = tokio::time::timeout(Duration::from_secs(1), captured.recv())
        .await?
        .context("hydration did not write")?;
    assert_eq!(state.writes.lock().unwrap().len(), 1);
    let caller = host.clone();
    let invocation_actor = actor.clone();
    let invoked = tokio::spawn(async move {
        caller
            .invoke_actor(
                ActorInvocation {
                    request_id: "during-hydration".into(),
                    actor: invocation_actor,
                    method: "read".into(),
                    args: vec![],
                },
                7,
                HostState::Warm,
                0.0,
            )
            .await
            .0
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(30), invocations.recv())
            .await
            .is_err()
    );
    acknowledged.send(()).unwrap();

    let activation = activating.await??;
    assert_eq!(activation.owner_epoch, 7);
    assert_eq!(activation.state_version, 1);
    let snapshots = state.writes.lock().unwrap();
    assert_eq!(snapshots.len(), 1);
    let snapshot = StateSnapshot::decode(&snapshots[0])?;
    assert_eq!(snapshot.request_id, "activate");
    assert!(snapshot.completion.is_none());
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), invocations.recv())
            .await?
            .as_deref(),
        Some("during-hydration")
    );
    assert!(matches!(
        invoked.await??,
        ActorExecutionResult::Failed { failure } if failure.code == "actor_error"
    ));
    host.drain(Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn activation_tolerates_immediate_writes_that_leave_sqlite_unchanged() -> Result<()> {
    struct UpsertingExecutor;

    #[async_trait]
    impl ActorExecutor for UpsertingExecutor {
        fn supports(&self, _: &str) -> bool {
            true
        }

        async fn hydrate(&self, _: String, database: Arc<dyn ActorDatabase>) -> Result<()> {
            let upsert = |value: &str| DatabaseCall::Exec {
                sql: "INSERT INTO kv (key, value) VALUES ('sandbox', ?) \
                    ON CONFLICT(key) DO UPDATE SET value = excluded.value"
                    .into(),
                bindings: vec![SqlValue::Text(value.into())],
                immediate: true,
            };
            database
                .call(DatabaseCall::Exec {
                    sql: "CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)".into(),
                    bindings: vec![],
                    immediate: true,
                })
                .await?;
            database.call(upsert("sb-1")).await?;
            database.call(upsert("sb-1")).await?;
            database.call(upsert("sb-2")).await?;
            Ok(())
        }

        async fn invoke(
            &self,
            _: ActorMethodInvocation,
            _: Arc<dyn ActorDatabase>,
        ) -> Result<ActorMethodOutcome> {
            unreachable!("the test only activates")
        }
    }

    let state = Arc::new(FakeStateTransport::default());
    let host = create_host(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host".into(),
        },
        Arc::new(UpsertingExecutor),
        Arc::new(FakeAuthority::default()),
        state.clone(),
        Arc::new(EmptySocketPublisher),
        replication().await,
    );

    let activation = tokio::time::timeout(Duration::from_secs(1), host.activate()).await??;
    assert_eq!(activation.state_version, 3);
    assert_eq!(state.writes.lock().unwrap().len(), 3);
    host.drain(Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn activation_reuses_recovered_bytes_and_publishes_readiness_without_a_write_ticket()
-> Result<()> {
    let snapshot = StateSnapshot::new(
        3,
        6,
        "previous".into(),
        crate::test_sqlite::snapshot(json!({"count": 41}))?,
        json!(41),
    )?
    .encode()?;
    let authority = Arc::new(FakeAuthority {
        activation_state: Some(snapshot),
        ..Default::default()
    });
    let transport = Arc::new(FakeStateTransport::default());
    let actor = counter();
    let host = create_host(
        HostEndpoint {
            id: super::super::HostId::new("host"),
            route: "http://host".into(),
        },
        Arc::new(IncrementingExecutor {
            invocations: AtomicU64::new(0),
        }),
        authority.clone(),
        transport.clone(),
        Arc::new(EmptySocketPublisher),
        replication().await,
    );
    let activation = host.activate().await?;
    assert_eq!(activation.state_version, 3);
    assert!(authority.preparations.lock().unwrap().is_empty());
    let result = host
        .invoke_actor(
            ActorInvocation {
                actor,
                request_id: "next".into(),
                method: "increment".into(),
                args: vec![],
            },
            7,
            HostState::Warm,
            0.0,
        )
        .await
        .0?;
    assert_eq!(result, completed(42));
    assert_eq!(authority.loads.load(Ordering::SeqCst), 0);
    assert_eq!(*authority.preparations.lock().unwrap(), vec![3]);
    Ok(())
}

#[tokio::test]
async fn early_database_preparation_restores_once_without_hydrating_customer_code() -> Result<()> {
    struct Executor(AtomicUsize);
    #[async_trait]
    impl ActorExecutor for Executor {
        fn supports(&self, _: &str) -> bool {
            true
        }
        async fn hydrate(&self, _: String, database: Arc<dyn ActorDatabase>) -> Result<()> {
            assert_eq!(fields(&*database).await?, json!({"count": 41}));
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn invoke(
            &self,
            _: ActorMethodInvocation,
            _: Arc<dyn ActorDatabase>,
        ) -> Result<ActorMethodOutcome> {
            unreachable!()
        }
    }
    let sqlite = crate::test_sqlite::snapshot(json!({"count": 41}))?;
    let snapshot = StateSnapshot::new(3, 6, "previous".into(), sqlite, json!(41))?.encode()?;
    let authority = Arc::new(FakeAuthority {
        activation_state: Some(snapshot),
        ..Default::default()
    });
    let endpoint = HostEndpoint {
        id: super::super::HostId::new("prepared-host"),
        route: "http://host".into(),
    };
    let database = HostDatabase::new(
        endpoint.clone(),
        authority.clone(),
        Arc::new(FakeStateTransport::default()),
        replication().await,
    );
    let executor = Arc::new(Executor(AtomicUsize::new(0)));
    for _ in 0..2 {
        assert!(matches!(
            database.activate().await?,
            PreparedActivation::Hydrate { .. }
        ));
    }
    assert_eq!(executor.0.load(Ordering::SeqCst), 0);
    let host = ActorHost::new(
        endpoint,
        counter(),
        executor.clone(),
        database,
        Arc::new(EmptySocketPublisher),
    );
    assert_eq!(host.activate().await?.state_version, 3);
    host.activate().await?;
    assert_eq!(executor.0.load(Ordering::SeqCst), 1);
    assert_eq!(authority.sqlite_reads.load(Ordering::SeqCst), 1);
    Ok(())
}

#[derive(Default)]
struct FakeAuthority {
    persisted: Option<Arc<FakeStateTransport>>,
    history: std::collections::HashMap<String, bytes::Bytes>,
    fenced: std::sync::atomic::AtomicBool,
    loads: AtomicUsize,
    sqlite_reads: AtomicUsize,
    initial_state: Option<(u64, bytes::Bytes)>,
    activation_state: Option<bytes::Bytes>,
    preparations: Mutex<Vec<u64>>,
}

#[async_trait]
impl ActorStorage for FakeAuthority {
    async fn get_alarm(&self) -> Result<Option<i64>> {
        Ok(None)
    }

    async fn update_alarm(&self, _timestamp_ms: Option<i64>) -> Result<()> {
        Ok(())
    }

    async fn acquire_actor(&self) -> Result<ActorActivation> {
        Ok(ActorActivation {
            owner_epoch: 7,
            state_version: self
                .activation_state
                .as_ref()
                .map(|bytes| StateSnapshot::decode(bytes).map(|s| s.state_version))
                .transpose()?
                .unwrap_or(0),
            state: self.activation_state.clone().map(Into::into),
        })
    }

    fn ensure_authority(&self) -> Result<()> {
        anyhow::ensure!(!self.fenced.load(Ordering::SeqCst), "host lease expired");
        Ok(())
    }
    async fn load_actor_state(&self, _: u64) -> Result<(u64, bytes::Bytes)> {
        self.loads.fetch_add(1, Ordering::SeqCst);
        if let Some(bytes) = self
            .persisted
            .as_ref()
            .and_then(|state| state.writes.lock().unwrap().last().cloned())
        {
            return Ok((StateSnapshot::decode(&bytes)?.state_version, bytes.into()));
        }
        Ok(self.initial_state.clone().unwrap_or_default())
    }
    async fn read_sqlite(&self, bytes: &[u8]) -> Result<crate::state_log::SqliteSnapshot> {
        self.sqlite_reads.fetch_add(1, Ordering::SeqCst);
        let snapshot = StateSnapshot::decode(bytes)?;
        let object = crate::storage::snapshot_object_name(
            &counter(),
            snapshot.state_version,
            &format!("{:032x}", snapshot.owner_epoch),
        )?;
        Ok(crate::bucket::recovery::resolve(
            &mut FakeHistory(&self.history),
            &object,
            bytes::Bytes::copy_from_slice(bytes),
            None,
        )
        .await?
        .sqlite)
    }
    async fn prepare_state_write(
        &self,
        owner_epoch: u64,
        expected_version: u64,
    ) -> Result<WritePlan> {
        self.preparations.lock().unwrap().push(expected_version);
        let mut ticket = ticket(expected_version + 1);
        {
            let snapshots = crate::storage_paths::snapshots(&counter())?;
            let stream = crate::storage::StateStream {
                prefix: format!("{snapshots}epoch/"),
                session: format!("{snapshots}epoch/sessions/one/"),
                owner_epoch,
                base_version: 0,
            };
            ticket.object_name = stream.object(ticket.state_version);
            ticket.stream = stream;
        }
        Ok(ticket)
    }
}

struct FakeHistory<'a>(&'a std::collections::HashMap<String, bytes::Bytes>);

#[async_trait]
impl crate::bucket::recovery::SnapshotHistory for FakeHistory<'_> {
    async fn read(&mut self, object: &str) -> Result<bytes::Bytes> {
        self.0
            .get(object)
            .cloned()
            .map(Into::into)
            .ok_or_else(|| anyhow::anyhow!("missing snapshot dependency"))
    }
    async fn checkpoint(&mut self, _: &str) -> Result<Option<bytes::Bytes>> {
        Ok(None)
    }
}

#[derive(Default)]
struct FakeStateTransport {
    failures: AtomicUsize,
    writes: Mutex<Vec<bytes::Bytes>>,
    paused_commit: Option<(mpsc::UnboundedSender<()>, Arc<tokio::sync::Semaphore>)>,
}

#[async_trait]
impl crate::state_transport::SnapshotWriter for FakeStateTransport {
    async fn write_snapshot(&self, _ticket: &WritePlan, bytes: bytes::Bytes) -> Result<StateWrite> {
        if StateSnapshot::decode(&bytes)?.request_id == "first"
            && let Some((started, release)) = &self.paused_commit
        {
            started.send(())?;
            release.acquire().await?.forget();
        }
        if self
            .failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            self.writes.lock().unwrap().push(bytes);
            anyhow::bail!("write response lost");
        }

        self.writes.lock().unwrap().push(bytes);
        Ok(StateWrite::Written)
    }
}

#[tokio::test]
async fn cold_actors_load_the_current_head_instead_of_a_cached_route_snapshot() -> Result<()> {
    let snapshot = StateSnapshot::new(
        8,
        1,
        "previous".into(),
        crate::test_sqlite::snapshot(json!({"count":8}))?,
        json!(8),
    )?
    .encode()?;
    let authority = Arc::new(FakeAuthority {
        initial_state: Some((8, snapshot.into())),
        ..Default::default()
    });
    let state = Arc::new(FakeStateTransport::default());
    let host = create_host(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host.invalid/".into(),
        },
        Arc::new(IncrementingExecutor {
            invocations: AtomicU64::new(0),
        }),
        authority.clone(),
        state.clone(),
        Arc::new(EmptySocketPublisher),
        replication().await,
    );
    assert_eq!(invoke(&host, "request-1").await?, completed(9));
    assert_eq!(authority.loads.load(Ordering::Relaxed), 1);
    assert_eq!(*authority.preparations.lock().unwrap(), [8]);
    Ok(())
}

#[tokio::test]
async fn read_only_results_are_withheld_if_the_lease_expires_during_execution() -> Result<()> {
    struct ExpiringExecutor(Arc<FakeAuthority>);
    #[async_trait]
    impl ActorExecutor for ExpiringExecutor {
        fn supports(&self, _: &str) -> bool {
            true
        }
        async fn invoke(
            &self,
            _: ActorMethodInvocation,
            _: Arc<dyn ActorDatabase>,
        ) -> Result<ActorMethodOutcome> {
            self.0.fenced.store(true, Ordering::SeqCst);
            Ok(ActorMethodOutcome::Completed {
                result: json!(8),
                effects: vec![],
            })
        }
    }
    let snapshot = StateSnapshot::new(
        8,
        1,
        "previous".into(),
        crate::test_sqlite::snapshot(json!({"count":8}))?,
        json!(8),
    )?
    .encode()?;
    let authority = Arc::new(FakeAuthority {
        initial_state: Some((8, snapshot.into())),
        ..Default::default()
    });
    let state = Arc::new(FakeStateTransport::default());
    let host = create_host(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host.invalid/".into(),
        },
        Arc::new(ExpiringExecutor(authority.clone())),
        authority,
        state.clone(),
        Arc::new(EmptySocketPublisher),
        replication().await,
    );
    assert!(invoke(&host, "request-1").await.is_err());
    assert!(state.writes.lock().unwrap().is_empty());
    Ok(())
}

#[tokio::test]
async fn write_results_are_withheld_if_the_lease_expires_during_persistence() -> Result<()> {
    {
        let (commit_started, mut committing) = mpsc::unbounded_channel();
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let authority = Arc::new(FakeAuthority::default());
        let state = Arc::new(FakeStateTransport {
            paused_commit: Some((commit_started, release.clone())),
            ..Default::default()
        });
        let host = Arc::new(create_host(
            HostEndpoint {
                id: super::super::HostId::new("host-1"),
                route: "http://host.invalid/".into(),
            },
            Arc::new(IncrementingExecutor {
                invocations: AtomicU64::new(0),
            }),
            authority.clone(),
            state.clone(),
            Arc::new(EmptySocketPublisher),
            replication().await,
        ));
        let caller = host.clone();
        let result = tokio::spawn(async move { invoke(&caller, "first").await });
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), committing.recv()).await?,
            Some(())
        );
        authority.fenced.store(true, Ordering::SeqCst);
        release.add_permits(1);
        let error = tokio::time::timeout(Duration::from_secs(2), result)
            .await??
            .unwrap_err();
        assert!(error.to_string().contains("host lease expired"));
        assert_eq!(state.writes.lock().unwrap().len(), 1);
        host.drain(Duration::from_secs(1)).await?;
    }
    Ok(())
}

#[tokio::test]
async fn epoch_snapshot_proofs_commit_locally_and_retry_an_ambiguous_write() -> Result<()> {
    let authority = Arc::new(FakeAuthority {
        ..Default::default()
    });
    let state = Arc::new(FakeStateTransport {
        failures: AtomicUsize::new(1),
        ..Default::default()
    });
    let executor = Arc::new(IncrementingExecutor {
        invocations: AtomicU64::new(0),
    });
    let host = create_host(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host.invalid/".into(),
        },
        executor.clone(),
        authority.clone(),
        state.clone(),
        Arc::new(EmptySocketPublisher),
        replication().await,
    );
    assert!(matches!(
        invoke(&host, "request-1").await?,
        ActorExecutionResult::Failed { .. }
    ));
    assert_eq!(invoke(&host, "request-1").await?, completed(1));
    assert_eq!(invoke(&host, "request-2").await?, completed(2));
    assert_eq!(executor.invocations.load(Ordering::Relaxed), 2);

    assert_eq!(*authority.preparations.lock().unwrap(), [0, 0]);
    let writes = state.writes.lock().unwrap();
    assert_eq!(writes[0], writes[1]);
    assert_eq!(writes.len(), 3);
    Ok(())
}

#[tokio::test]
async fn durable_snapshot_commits_locally_before_success() -> Result<()> {
    let authority = Arc::new(FakeAuthority::default());
    let state = Arc::new(FakeStateTransport {
        ..Default::default()
    });
    let host = create_host(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host.invalid/".into(),
        },
        Arc::new(IncrementingExecutor {
            invocations: AtomicU64::new(0),
        }),
        authority.clone(),
        state.clone(),
        Arc::new(EmptySocketPublisher),
        replication().await,
    );
    assert_eq!(invoke(&host, "request-1").await?, completed(1));
    assert_eq!(state.writes.lock().unwrap().len(), 1);
    Ok(())
}

#[tokio::test]
async fn resident_actor_uses_immutable_snapshots_and_replays_the_last_request() -> Result<()> {
    let authority = Arc::new(FakeAuthority::default());
    let state = Arc::new(FakeStateTransport::default());
    let executor = Arc::new(IncrementingExecutor {
        invocations: AtomicU64::new(0),
    });
    let host = create_host(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host.invalid/".into(),
        },
        executor.clone(),
        authority.clone(),
        state.clone(),
        Arc::new(EmptySocketPublisher),
        replication().await,
    );

    assert_eq!(invoke(&host, "request-1").await?, completed(1));
    assert_eq!(invoke(&host, "request-2").await?, completed(2));
    assert_eq!(invoke(&host, "request-2").await?, completed(2));

    assert_eq!(executor.invocations.load(Ordering::Relaxed), 2);
    assert_eq!(authority.loads.load(Ordering::Relaxed), 1);
    assert_eq!(state.writes.lock().unwrap().len(), 2);
    assert_eq!(*authority.preparations.lock().unwrap(), [0]);
    let snapshots = state
        .writes
        .lock()
        .unwrap()
        .iter()
        .map(|bytes| StateSnapshot::decode(bytes))
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(snapshots[0].state_version, 1);
    assert_eq!(snapshots[1].state_version, 2);
    Ok(())
}

#[tokio::test]
async fn retries_an_ambiguous_commit_without_executing_the_request_twice() -> Result<()> {
    let authority = Arc::new(FakeAuthority::default());
    let state = Arc::new(FakeStateTransport::default());
    state.failures.store(1, Ordering::SeqCst);
    let executor = Arc::new(SerialAdmissionExecutor::new());
    let host = create_host(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host.invalid/".into(),
        },
        executor.clone(),
        authority.clone(),
        state.clone(),
        Arc::new(EmptySocketPublisher),
        replication().await,
    );

    let first = invoke(&host, "request-1").await?;
    assert!(matches!(
        first,
        ActorExecutionResult::Failed { ref failure } if failure.code == "outcome_unknown"
    ));
    assert_eq!(invoke(&host, "request-1").await?, completed(1));

    assert_eq!(executor.inner.invocations.load(Ordering::Relaxed), 1);
    assert_eq!(state.writes.lock().unwrap().len(), 2);
    assert_eq!(state.writes.lock().unwrap().len(), 2);
    Ok(())
}

#[tokio::test]
async fn preserves_executor_resource_exhaustion() -> Result<()> {
    let host = create_host(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host.invalid/".into(),
        },
        Arc::new(ExhaustedExecutor),
        Arc::new(FakeAuthority::default()),
        Arc::new(FakeStateTransport::default()),
        Arc::new(EmptySocketPublisher),
        replication().await,
    );

    assert!(matches!(
        invoke(&host, "request-1").await?,
        ActorExecutionResult::Failed { ref failure } if failure.code == "resource_exhausted"
    ));
    Ok(())
}

#[tokio::test]
async fn invalid_socket_effects_do_not_commit_actor_state() -> Result<()> {
    let authority = Arc::new(FakeAuthority::default());
    let state = Arc::new(FakeStateTransport::default());
    let host = create_host(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host.invalid/".into(),
        },
        Arc::new(InvalidEffectsExecutor),
        authority.clone(),
        state.clone(),
        Arc::new(EmptySocketPublisher),
        replication().await,
    );

    assert!(matches!(
        invoke(&host, "request-1").await?,
        ActorExecutionResult::Failed { ref failure }
            if failure.code == "actor_error" && failure.message.contains("invalid socket effects")
    ));
    assert!(state.writes.lock().unwrap().is_empty());

    Ok(())
}

#[tokio::test]
async fn publishes_automatic_state_after_commit() -> Result<()> {
    struct Emitter;
    #[async_trait]
    impl ActorExecutor for Emitter {
        fn supports(&self, _: &str) -> bool {
            true
        }
        async fn invoke(
            &self,
            _: ActorMethodInvocation,
            database: Arc<dyn ActorDatabase>,
        ) -> Result<ActorMethodOutcome> {
            let count = fields(&*database).await?["count"].as_u64().unwrap_or(0) + 1;
            write_fields(&*database, json!({"count": count})).await?;
            Ok(ActorMethodOutcome::Completed {
                result: json!(count),
                effects: serde_json::from_value(
                    json!([{ "type":"state_update", "changes":{"count":count}, "removed":[] }]),
                )?,
            })
        }
    }
    struct Publisher {
        state: Arc<FakeStateTransport>,
        values: tokio::sync::mpsc::UnboundedSender<Value>,
    }
    #[async_trait]
    impl crate::actor::ActorSocketPublisher for Publisher {
        async fn publish(&self, effects: Vec<ActorSocketEffect>) -> Result<()> {
            let persisted = self.state.writes.lock().unwrap();
            let version = StateSnapshot::decode(
                persisted
                    .last()
                    .expect("state must persist before publishing"),
            )?
            .state_version;
            let value = serde_json::to_value(effects)?;
            assert_eq!(value[0]["version"], version);
            self.values.send(value)?;
            Ok(())
        }
    }
    let authority = Arc::new(FakeAuthority::default());
    let state = Arc::new(FakeStateTransport::default());
    let (values, mut published) = tokio::sync::mpsc::unbounded_channel();
    let publisher = Arc::new(Publisher {
        state: state.clone(),
        values,
    });
    let host = create_host(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host.invalid/".into(),
        },
        Arc::new(Emitter),
        authority.clone(),
        state.clone(),
        publisher.clone(),
        replication().await,
    );
    assert_eq!(invoke(&host, "one").await?, completed(1));
    assert_eq!(invoke(&host, "two").await?, completed(2));
    let mut counts = Vec::new();
    for _ in 0..2 {
        let value = tokio::time::timeout(Duration::from_secs(1), published.recv()).await?;
        counts.push(value.unwrap()[0]["changes"]["count"].as_u64());
    }
    counts.sort();
    assert_eq!(counts, [Some(1), Some(2)]);
    state.failures.store(1, Ordering::SeqCst);
    assert!(matches!(
        invoke(&host, "failed").await?,
        ActorExecutionResult::Failed { .. }
    ));
    assert!(published.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn socket_events_return_effects_only_after_committing_state() -> Result<()> {
    let authority = Arc::new(FakeAuthority::default());
    let state = Arc::new(FakeStateTransport::default());
    let actor = ActorKey {
        project_id: "default".into(),
        actor_name: "Counter".into(),
        actor_id: "counter-1".into(),
    };
    let connection = crate::actor::ActorSocketConnection {
        id: "socket-1".into(),
        metadata: json!({ "userId": "user-1" }),
        tags: Vec::new(),
    };
    let executor = Arc::new(IncrementingExecutor {
        invocations: AtomicU64::new(0),
    });
    let host = create_host(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host.invalid/".into(),
        },
        executor.clone(),
        authority.clone(),
        state.clone(),
        Arc::new(EmptySocketPublisher),
        replication().await,
    );

    let invocation = |request_id: &str| ActorSocketInvocation {
        request_id: request_id.into(),
        actor: actor.clone(),
        event: crate::actor::ActorSocketEvent::Connect {
            connection: connection.clone(),
        },
        connections: Vec::new(),
    };
    let status = host.status();
    authority.fenced.store(true, Ordering::SeqCst);
    assert_eq!(
        host.submit(ActorOperation::Socket(invocation("fenced")), 1)
            .await?,
        ActorExecutionResult::HostUnavailable
    );
    assert_eq!(executor.invocations.load(Ordering::SeqCst), 0);
    authority.fenced.store(false, Ordering::SeqCst);
    let before_connect = status.borrow().idle_since;
    let result = host
        .submit(ActorOperation::Socket(invocation("committed")), 1)
        .await?;
    assert!(status.borrow().idle_since > before_connect);

    assert!(matches!(
        result,
        ActorExecutionResult::Completed { result: Value::Null, ref effects } if effects.len() == 1
    ));
    assert_eq!(state.writes.lock().unwrap().len(), 1);
    let saved = state.writes.lock().unwrap()[0].clone();
    let snapshot = crate::state_log::StateSnapshot::decode(&saved)?;
    let attribution = snapshot.attribution.unwrap();
    assert_eq!(snapshot.request_id, "committed");
    assert_eq!(attribution.connection_id.as_deref(), Some("socket-1"));
    assert_eq!(attribution.operation, "onConnect");
    assert!(!attribution.interleaved);

    state.failures.store(1, Ordering::SeqCst);
    let failed = host
        .submit(ActorOperation::Socket(invocation("failed")), 1)
        .await?;
    assert!(
        matches!(failed, ActorExecutionResult::Failed { failure } if failure.code == "outcome_unknown")
    );

    host.drain(Duration::from_secs(1)).await?;
    assert_eq!(
        host.submit(ActorOperation::Socket(invocation("drained")), 1)
            .await?,
        ActorExecutionResult::HostUnavailable
    );
    assert_eq!(state.writes.lock().unwrap().len(), 2);
    Ok(())
}

struct ImmediateWriter {
    admission: watch::Sender<()>,
    written: mpsc::UnboundedSender<u64>,
}
#[async_trait]
impl ActorExecutor for ImmediateWriter {
    fn supports(&self, _: &str) -> bool {
        true
    }
    fn invocation_admission(&self) -> Option<watch::Receiver<()>> {
        Some(self.admission.subscribe())
    }
    async fn invoke(
        &self,
        invocation: ActorMethodInvocation,
        database: Arc<dyn ActorDatabase>,
    ) -> Result<ActorMethodOutcome> {
        let count = fields(&*database).await?["count"].as_u64().unwrap_or(0);
        if invocation.request_id == "first" {
            for value in [count + 1, count + 2] {
                persist_fields(&*database, json!({ "count": value }), true).await?;
                self.written.send(value)?;
            }
            return Ok(ActorMethodOutcome::Failed(ActorInvocationFailure {
                code: "actor_method_failed".into(),
                message: "failed after durable writes".into(),
            }));
        }
        write_fields(&*database, json!({ "count": count + 1 })).await?;
        Ok(ActorMethodOutcome::Completed {
            result: json!(count + 1),
            effects: vec![],
        })
    }
}

#[tokio::test]
async fn immediate_writes_wait_for_durability_and_share_atomic_versions() -> Result<()> {
    let (written, mut writes_seen) = mpsc::unbounded_channel();
    let (started, mut saving) = mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let writes = Arc::new(FakeStateTransport {
        paused_commit: Some((started, release.clone())),
        ..Default::default()
    });
    let host = Arc::new(create_host(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host.invalid/".into(),
        },
        Arc::new(ImmediateWriter {
            admission: watch::channel(()).0,
            written,
        }),
        Arc::new(FakeAuthority::default()),
        writes.clone(),
        Arc::new(EmptySocketPublisher),
        replication().await,
    ));
    assert_eq!(invoke(&host, "atomic-before").await?, completed(1));
    let caller = host.clone();
    let invocation = tokio::spawn(async move { invoke(&caller, "first").await });
    for value in [2, 3] {
        tokio::time::timeout(Duration::from_secs(3), saving.recv())
            .await?
            .unwrap();
        assert!(
            writes_seen.try_recv().is_err(),
            "immediate write acknowledged before storage"
        );
        assert!(
            !invocation.is_finished(),
            "method resumed before durability"
        );
        release.add_permits(1);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), writes_seen.recv()).await?,
            Some(value)
        );
    }
    assert!(matches!(
        invocation.await??,
        ActorExecutionResult::Failed { .. }
    ));
    assert_eq!(invoke(&host, "atomic-after").await?, completed(4));
    let snapshots = writes
        .writes
        .lock()
        .unwrap()
        .iter()
        .map(|bytes| StateSnapshot::decode(bytes))
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(
        snapshots
            .iter()
            .map(|s| s.state_version)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );
    assert!(snapshots[0].completion.is_some());
    assert!(snapshots[1].completion.is_none());
    assert!(snapshots[2].completion.is_none());
    assert_eq!(snapshots[3].completion.as_ref().unwrap().result, json!(4));
    for pair in snapshots.windows(2) {
        assert_eq!(
            pair[1].sqlite.parent.as_ref().unwrap().state_version,
            pair[0].state_version
        );
    }
    Ok(())
}

struct TransactionWriter {
    admission: watch::Sender<()>,
}
#[async_trait]
impl ActorExecutor for TransactionWriter {
    fn supports(&self, _: &str) -> bool {
        true
    }
    fn invocation_admission(&self) -> Option<watch::Receiver<()>> {
        Some(self.admission.subscribe())
    }
    async fn invoke(
        &self,
        invocation: ActorMethodInvocation,
        database: Arc<dyn ActorDatabase>,
    ) -> Result<ActorMethodOutcome> {
        let count = fields(&*database).await?["count"].as_u64().unwrap_or(0);
        if invocation.request_id == "savepoints" {
            exec(&*database, "BEGIN").await?;
            persist_fields(&*database, json!({ "count": count + 1 }), true).await?;
            persist_fields(&*database, json!({ "count": count + 2 }), true).await?;
            database
                .call(DatabaseCall::Exec {
                    sql: "COMMIT".into(),
                    bindings: vec![],
                    immediate: true,
                })
                .await?;
            exec(&*database, "BEGIN").await?;
            persist_fields(&*database, json!({ "count": count + 100 }), true).await?;
            exec(&*database, "ROLLBACK").await?;
        }
        Ok(ActorMethodOutcome::Completed {
            result: json!(fields(&*database).await?["count"].as_u64().unwrap_or(0)),
            effects: vec![],
        })
    }
}

#[tokio::test]
async fn sql_transactions_publish_once_on_commit_and_never_on_rollback() -> Result<()> {
    let writes = Arc::new(FakeStateTransport::default());
    let host = create_host(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host.invalid/".into(),
        },
        Arc::new(TransactionWriter {
            admission: watch::channel(()).0,
        }),
        Arc::new(FakeAuthority::default()),
        writes.clone(),
        Arc::new(EmptySocketPublisher),
        replication().await,
    );
    assert_eq!(invoke(&host, "savepoints").await?, completed(2));
    let immediate = writes
        .writes
        .lock()
        .unwrap()
        .iter()
        .map(|bytes| StateSnapshot::decode(bytes))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .filter(|snapshot| snapshot.completion.is_none())
        .count();
    assert_eq!(immediate, 1, "only the committed transaction publishes");
    assert_eq!(invoke(&host, "read").await?, completed(2));
    Ok(())
}

#[tokio::test]
async fn failed_immediate_write_stops_the_actor_without_acknowledging_the_write() -> Result<()> {
    let (written, mut writes_seen) = mpsc::unbounded_channel();
    let writes = Arc::new(FakeStateTransport {
        failures: AtomicUsize::new(1),
        ..Default::default()
    });
    let host = Arc::new(create_host(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host.invalid/".into(),
        },
        Arc::new(ImmediateWriter {
            admission: watch::channel(()).0,
            written,
        }),
        Arc::new(FakeAuthority::default()),
        writes,
        Arc::new(EmptySocketPublisher),
        replication().await,
    ));
    assert!(
        matches!(invoke(&host, "first").await?, ActorExecutionResult::Failed { failure } if failure.code == "outcome_unknown")
    );
    assert!(writes_seen.try_recv().is_err());
    assert_eq!(
        invoke(&host, "after").await?,
        ActorExecutionResult::HostUnavailable
    );
    Ok(())
}

#[tokio::test]
async fn delayed_interleaved_completion_cannot_rewind_an_immediate_write() -> Result<()> {
    use crate::actor::ActorInterleavedOutcome;
    struct Executor {
        admission: watch::Sender<()>,
        captured: mpsc::UnboundedSender<oneshot::Sender<()>>,
    }
    #[async_trait]
    impl ActorExecutor for Executor {
        fn supports(&self, _: &str) -> bool {
            true
        }
        fn invocation_admission(&self) -> Option<watch::Receiver<()>> {
            Some(self.admission.subscribe())
        }
        async fn invoke(
            &self,
            invocation: ActorMethodInvocation,
            database: Arc<dyn ActorDatabase>,
        ) -> Result<ActorMethodOutcome> {
            let count = fields(&*database).await?["count"].as_u64().unwrap_or(0) + 1;
            let immediate = invocation.request_id == "newer";
            persist_fields(&*database, json!({ "count": count }), immediate).await?;
            database.call(DatabaseCall::Commit).await?;
            if count <= 2 {
                let (reply, ack) = oneshot::channel();
                self.captured.send(reply)?;
                self.admission.send_replace(());
                ack.await?;
            }
            Ok(ActorMethodOutcome::Interleaved(ActorInterleavedOutcome {
                sequence: count,
                result: json!(count),
                effects: vec![],
            }))
        }
    }
    let (captured, mut captures) = mpsc::unbounded_channel();
    let writes = Arc::new(FakeStateTransport::default());
    let host = Arc::new(create_host(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host.invalid/".into(),
        },
        Arc::new(Executor {
            admission: watch::channel(()).0,
            captured,
        }),
        Arc::new(FakeAuthority::default()),
        writes.clone(),
        Arc::new(EmptySocketPublisher),
        replication().await,
    ));
    let caller = host.clone();
    let older = tokio::spawn(async move { invoke(&caller, "older").await });
    let release_older = tokio::time::timeout(Duration::from_secs(3), captures.recv())
        .await?
        .unwrap();
    let caller = host.clone();
    let newer = tokio::spawn(async move { invoke(&caller, "newer").await });
    let release_newer = tokio::time::timeout(Duration::from_secs(3), captures.recv())
        .await?
        .unwrap();
    assert_eq!(writes.writes.lock().unwrap().len(), 1);
    release_older.send(()).unwrap();
    assert_eq!(older.await??, completed(1));
    release_newer.send(()).unwrap();
    assert_eq!(newer.await??, completed(2));
    assert_eq!(invoke(&host, "after").await?, completed(3));
    let snapshots = writes
        .writes
        .lock()
        .unwrap()
        .iter()
        .map(|bytes| StateSnapshot::decode(bytes))
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(snapshots.len(), 2);
    assert_eq!(snapshots[0].request_id, "newer");
    assert!(snapshots[0].completion.is_none());
    assert_eq!(snapshots[1].state_version, 2);
    assert!(snapshots[1].sqlite.txid > snapshots[0].sqlite.txid);
    Ok(())
}

#[tokio::test]
async fn database_handles_expire_with_their_invocation() -> Result<()> {
    struct Executor(Mutex<Option<Arc<dyn ActorDatabase>>>);
    #[async_trait]
    impl ActorExecutor for Executor {
        fn supports(&self, _: &str) -> bool {
            true
        }
        async fn invoke(
            &self,
            _: ActorMethodInvocation,
            database: Arc<dyn ActorDatabase>,
        ) -> Result<ActorMethodOutcome> {
            *self.0.lock().unwrap() = Some(database.clone());
            increment(&*database).await
        }
    }
    let executor = Arc::new(Executor(Mutex::new(None)));
    let host = create_host(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host.invalid/".into(),
        },
        executor.clone(),
        Arc::new(FakeAuthority::default()),
        Arc::new(FakeStateTransport::default()),
        Arc::new(EmptySocketPublisher),
        replication().await,
    );
    assert_eq!(invoke(&host, "request-1").await?, completed(1));
    let retained = executor.0.lock().unwrap().clone().unwrap();
    let error = retained
        .call(DatabaseCall::Fields { names: None })
        .await
        .unwrap_err();
    assert!(error.to_string().contains("no active invocation"));
    host.drain(Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn background_database_writes_outlive_invocations() -> Result<()> {
    let endpoint = HostEndpoint {
        id: super::super::HostId::new("host-1"),
        route: "http://host.invalid/".into(),
    };
    let writes = Arc::new(FakeStateTransport::default());
    let database = HostDatabase::new(
        endpoint.clone(),
        Arc::new(FakeAuthority::default()),
        writes.clone(),
        replication().await,
    );
    let background = database.background(counter());
    let host = ActorHost::new(
        endpoint,
        counter(),
        Arc::new(IncrementingExecutor {
            invocations: AtomicU64::new(0),
        }),
        database,
        Arc::new(EmptySocketPublisher),
    );
    assert_eq!(invoke(&host, "request-1").await?, completed(1));
    persist_fields(&*background, json!({"count": 5}), true).await?;
    assert_eq!(invoke(&host, "request-2").await?, completed(6));
    let background_writes = writes
        .writes
        .lock()
        .unwrap()
        .iter()
        .map(|bytes| StateSnapshot::decode(bytes))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .filter(|snapshot| snapshot.request_id == "background")
        .count();
    assert_eq!(background_writes, 1);
    host.drain(Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn background_database_follows_the_owner_epoch_across_activations() -> Result<()> {
    struct Executor;
    #[async_trait]
    impl ActorExecutor for Executor {
        fn supports(&self, _: &str) -> bool {
            true
        }
        async fn invoke(
            &self,
            invocation: ActorMethodInvocation,
            database: Arc<dyn ActorDatabase>,
        ) -> Result<ActorMethodOutcome> {
            if invocation.method == "reset" {
                return Ok(ActorMethodOutcome::Failed(
                    crate::actor::ActorInvocationFailure {
                        code: "actor_worker_failed".into(),
                        message: "reset".into(),
                    },
                ));
            }
            increment(&*database).await
        }
    }
    let endpoint = HostEndpoint {
        id: super::super::HostId::new("host-1"),
        route: "http://host.invalid/".into(),
    };
    let database = HostDatabase::new(
        endpoint.clone(),
        Arc::new(FakeAuthority::default()),
        Arc::new(FakeStateTransport::default()),
        replication().await,
    );
    let background = database.background(counter());
    let host = ActorHost::new(
        endpoint,
        counter(),
        Arc::new(Executor),
        database,
        Arc::new(EmptySocketPublisher),
    );
    let call = |method: &'static str, owner_epoch: u64| {
        host.invoke_actor(
            ActorInvocation {
                request_id: method.into(),
                actor: counter(),
                method: method.into(),
                args: Vec::new(),
            },
            owner_epoch,
            HostState::Warm,
            0.0,
        )
    };
    assert_eq!(call("first", 1).await.0?, completed(1));
    assert!(matches!(
        call("reset", 1).await.0?,
        ActorExecutionResult::Failed { .. }
    ));
    assert_eq!(call("second", 2).await.0?, completed(1));
    persist_fields(&*background, json!({"count": 5}), true).await?;
    assert_eq!(call("third", 2).await.0?, completed(6));
    host.drain(Duration::from_secs(1)).await?;
    Ok(())
}

async fn increment(database: &dyn ActorDatabase) -> Result<ActorMethodOutcome> {
    let count = fields(database).await?["count"].as_u64().unwrap_or(0) + 1;
    write_fields(database, json!({ "count": count })).await?;
    Ok(ActorMethodOutcome::Completed {
        result: json!(count),
        effects: Vec::new(),
    })
}

async fn invoke(host: &ActorHost, request_id: &str) -> Result<ActorExecutionResult> {
    host.invoke_actor(
        ActorInvocation {
            request_id: request_id.into(),
            actor: ActorKey {
                project_id: "default".into(),
                actor_name: "Counter".into(),
                actor_id: "counter-1".into(),
            },
            method: "increment".into(),
            args: Vec::new(),
        },
        1,
        HostState::Warm,
        0.0,
    )
    .await
    .0
}

fn completed(count: u64) -> ActorExecutionResult {
    ActorExecutionResult::Completed {
        result: json!(count),
        effects: Vec::new(),
    }
}

fn ticket(state_version: u64) -> WritePlan {
    WritePlan {
        stream: crate::storage::StateStream {
            prefix: "snapshots/epoch/".into(),
            session: "session".into(),
            owner_epoch: 1,
            base_version: 0,
        },

        object_name: format!("snapshots/{state_version}.json"),
        state_version,
    }
}

#[path = "http_invocation.rs"]
mod http_invocation;

#[path = "graceful_eviction.rs"]
mod graceful_eviction;

#[tokio::test]
async fn restored_request_only_returns_a_saved_result_when_completion_is_present() -> Result<()> {
    for complete in [false, true] {
        let mut snapshot = StateSnapshot::new(
            1,
            1,
            "request".into(),
            crate::test_sqlite::snapshot(json!({"count": 3}))?,
            Value::Null,
        )?;
        if !complete {
            snapshot.completion = None;
        }
        let executor = Arc::new(IncrementingExecutor {
            invocations: AtomicU64::new(0),
        });
        let host = create_host(
            HostEndpoint {
                id: super::super::HostId::new("host-1"),
                route: "http://host.invalid/".into(),
            },
            executor.clone(),
            Arc::new(FakeAuthority {
                initial_state: Some((1, snapshot.encode()?)),
                ..Default::default()
            }),
            Arc::new(FakeStateTransport::default()),
            Arc::new(EmptySocketPublisher),
            replication().await,
        );
        let result = invoke(&host, "request").await?;
        if complete {
            assert!(matches!(
                result,
                ActorExecutionResult::Completed {
                    result: Value::Null,
                    ..
                }
            ));
            assert_eq!(executor.invocations.load(Ordering::Relaxed), 0);
        } else {
            assert_eq!(result, completed(4));
            assert_eq!(executor.invocations.load(Ordering::Relaxed), 1);
        }
        host.drain(Duration::from_secs(1)).await?;
    }
    Ok(())
}

#[tokio::test]
async fn activation_fails_when_the_executor_does_not_load_the_assigned_actor() -> Result<()> {
    struct Unloaded;
    #[async_trait]
    impl ActorExecutor for Unloaded {
        fn supports(&self, _: &str) -> bool {
            false
        }
        async fn invoke(
            &self,
            _: ActorMethodInvocation,
            _: Arc<dyn ActorDatabase>,
        ) -> Result<ActorMethodOutcome> {
            anyhow::bail!("unused")
        }
    }
    let host = create_host(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host".into(),
        },
        Arc::new(Unloaded),
        Arc::new(FakeAuthority::default()),
        Arc::new(FakeStateTransport::default()),
        Arc::new(EmptySocketPublisher),
        replication().await,
    );
    let error = host.activate().await.unwrap_err();
    assert!(format!("{error:#}").contains("not loaded"));
    Ok(())
}
