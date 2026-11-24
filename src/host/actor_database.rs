use super::HostEndpoint;
use crate::payload::JsonText;
use crate::{
    actor::{
        ActorDatabase, ActorExecutionResult, ActorInvocation, DatabaseCall, DatabaseResult, RowSink,
    },
    litestream::Replicator,
    request_tracking::{RequestStage, RequestTracker},
    storage::{VerifiedSnapshot, WritePlan},
};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Instant,
};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
mod session;
mod sqlite;
use session::DatabaseSession;

#[async_trait]
pub(crate) trait ActorStorage: Send + Sync {
    async fn acquire_actor(&self) -> Result<ActorActivation>;
    async fn load_actor_state(&self, epoch: u64) -> Result<(u64, Option<VerifiedSnapshot>)>;
    async fn read_sqlite(
        &self,
        snapshot: &VerifiedSnapshot,
    ) -> Result<crate::bucket::RestoredSqlite>;
    async fn get_alarm(&self) -> Result<Option<i64>>;
    async fn update_alarm(&self, timestamp_ms: Option<i64>) -> Result<()>;
    fn ensure_authority(&self) -> Result<()>;
    async fn prepare_state_write(
        &self,
        owner_epoch: u64,
        expected_version: u64,
    ) -> Result<WritePlan>;
    /// How often the captured history needs a full copy; `None` when the store checkpoints.
    fn full_snapshots(&self) -> Option<std::time::Duration> {
        Some(crate::litestream::FULL_SNAPSHOT_INTERVAL)
    }
    /// The restored snapshot that the next write builds on, when the store keeps one history
    /// across activations; `None` starts a new history with a full copy.
    fn resume_parent(
        &self,
        _head: &crate::storage::SnapshotRef,
    ) -> Result<Option<crate::storage::SnapshotRef>> {
        Ok(None)
    }
    /// Whether sealing this activation found a new base due.
    fn base_due(&self) -> bool {
        false
    }
    /// What the node file service needs to restore this actor's database, for a store whose
    /// databases the node restores.
    fn slot_storage(&self) -> Option<crate::node_cache::SlotStorage> {
        None
    }
    /// Records what the node's restore found, which decides when this session's base is due.
    async fn restored_in_slot(
        &self,
        _snapshot: &VerifiedSnapshot,
        _restored: &crate::node_cache::SlotRestored,
    ) -> Result<()> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ActorActivation {
    pub owner_epoch: u64,
    pub state_version: u64,
    pub state: Option<VerifiedSnapshot>,
    /// The head's reference; without `state`, the node holds the head's closed database.
    pub head: Option<crate::storage::SnapshotRef>,
}

#[derive(Clone, Default)]
pub(super) struct CommitOrigin {
    pub connection_id: Option<String>,
    pub interleaved: bool,
}

impl CommitOrigin {
    pub(super) fn socket(event: &crate::actor::ActorSocketEvent) -> Self {
        use crate::actor::ActorSocketEvent;
        let connection = match event {
            ActorSocketEvent::Connect { connection }
            | ActorSocketEvent::Disconnect { connection, .. } => &connection.id,
            ActorSocketEvent::Message { connection_id, .. } => connection_id,
        };
        Self {
            connection_id: Some(connection.clone()),
            interleaved: false,
        }
    }
}

pub(crate) struct HostDatabase {
    commands: mpsc::Sender<Command>,
    version: Arc<AtomicU64>,
    pub(super) stopped: CancellationToken,
    storage: Arc<dyn ActorStorage>,
}

impl HostDatabase {
    pub(crate) fn new(
        endpoint: HostEndpoint,
        storage: Arc<dyn ActorStorage>,
        writer: Arc<dyn crate::state_transport::SnapshotWriter>,
        replication: Arc<dyn Replicator>,
        node_cache: Option<crate::node_cache::NodeCacheClient>,
    ) -> Arc<Self> {
        let (commands, incoming) = mpsc::channel(64);
        let version = Arc::new(AtomicU64::new(0));
        let stopped = CancellationToken::new();
        let service = DatabaseService {
            session: DatabaseSession::new(
                endpoint,
                storage.clone(),
                writer,
                replication,
                node_cache,
            ),
            version: version.clone(),
            stopped: stopped.clone(),
            ready: BTreeMap::new(),
            next_sequence: 1,
        };
        tokio::spawn(service.run(incoming));
        Arc::new(Self {
            commands,
            version,
            stopped,
            storage,
        })
    }

    pub(super) fn invocation(
        self: &Arc<Self>,
        invocation: ActorInvocation,
        owner_epoch: u64,
        origin: CommitOrigin,
    ) -> Arc<DatabaseInvocation> {
        self.scope(invocation, Some(owner_epoch), origin)
    }

    /// A scope for the whole activation: it never closes and follows the current owner epoch.
    pub(crate) fn background(
        self: &Arc<Self>,
        actor: crate::actor::ActorKey,
    ) -> Arc<dyn ActorDatabase> {
        let invocation = ActorInvocation {
            actor,
            request_id: "background".into(),
            method: "background".into(),
            args: Default::default(),
        };
        self.scope(invocation, None, CommitOrigin::default())
    }

    fn scope(
        self: &Arc<Self>,
        invocation: ActorInvocation,
        owner_epoch: Option<u64>,
        origin: CommitOrigin,
    ) -> Arc<DatabaseInvocation> {
        Arc::new(DatabaseInvocation {
            database: self.clone(),
            scope: Arc::new(InvocationScope {
                invocation,
                owner_epoch,
                origin,
                committed: AtomicU64::new(0),
                active: AtomicBool::new(true),
            }),
        })
    }

    pub(super) fn state_version(&self) -> Option<u64> {
        let version = self.version.load(Ordering::Acquire);
        (version > 0).then_some(version)
    }

    pub(super) fn ensure_authority(&self) -> Result<()> {
        self.storage.ensure_authority()
    }

    pub(super) async fn activate(&self) -> Result<PreparedActivation> {
        let (reply, result) = oneshot::channel();
        self.send(Command::Activate { reply }).await?;
        result
            .await
            .context("database service stopped during activation")?
    }

    pub(super) async fn hydrated(
        &self,
        activation: ActorActivation,
        result: Result<()>,
    ) -> Result<ActorActivation> {
        let (reply, response) = oneshot::channel();
        self.send(Command::Hydrated {
            activation,
            result,
            reply,
        })
        .await?;
        response
            .await
            .context("database service stopped during hydration")?
    }

    pub(super) async fn reset(&self) -> Result<()> {
        let (reply, result) = oneshot::channel();
        self.send(Command::Reset { reply }).await?;
        result
            .await
            .context("database service stopped during eviction")
    }

    /// Best effort within `budget`: an unparked database only means the next activation rebuilds
    /// it from its history.
    pub(super) async fn park(&self, budget: std::time::Duration) {
        let started = std::time::Instant::now();
        let parked = tokio::time::timeout(budget, async {
            let (reply, parked) = oneshot::channel();
            self.send(Command::Park { reply }).await?;
            parked
                .await
                .context("database service stopped while parking")?
        })
        .await
        .context("parking timed out")
        .and_then(|result| result);
        match parked {
            Ok(None) => {}
            Ok(Some(file)) => tracing::info!(
                event = "actor_sqlite_parked",
                bytes = file.bytes,
                device = file.device,
                inode = file.inode,
                duration_ms = started.elapsed().as_secs_f64() * 1000.0
            ),
            Err(error) => {
                tracing::warn!(error = %format!("{error:#}"), "database not parked")
            }
        }
    }

    async fn send(&self, command: Command) -> Result<()> {
        ensure!(!self.stopped.is_cancelled(), "database service stopped");
        self.commands
            .send(command)
            .await
            .map_err(|_| anyhow::anyhow!("database service stopped"))
    }
}

// Asked through the service queue like an actor call, so it never touches the database mid-call.
#[async_trait]
impl super::lifecycle::DatabaseSize for HostDatabase {
    async fn database_bytes(&self) -> Option<u64> {
        let (reply, size) = oneshot::channel();
        self.send(Command::DatabaseSize { reply }).await.ok()?;
        size.await.ok().flatten()
    }
}

pub(super) struct DatabaseInvocation {
    database: Arc<HostDatabase>,
    scope: Arc<InvocationScope>,
}

impl DatabaseInvocation {
    pub(super) async fn prepare(
        &self,
        replay: bool,
        tracker: &mut RequestTracker,
    ) -> Result<PreparedInvocation> {
        let (reply, result) = oneshot::channel();
        self.database
            .send(Command::Prepare {
                scope: self.scope.clone(),
                replay,
                reply,
            })
            .await?;
        let (result, timings) = result
            .await
            .context("database service stopped while preparing invocation")?;
        for (stage, at) in timings.0 {
            tracker.mark_at(stage, at);
        }
        result
    }

    pub(super) fn close(&self) {
        self.scope.active.store(false, Ordering::Release);
    }

    pub(super) fn wrote(&self) -> bool {
        self.scope.committed.load(Ordering::Acquire) > 0
    }

    /// `result` is `None` when it passes on as it arrives, which only a call that wrote nothing
    /// can do.
    pub(super) async fn complete(
        &self,
        result: Option<JsonText>,
        sequence: Option<u64>,
    ) -> Result<u64> {
        self.close();
        let (reply, response) = oneshot::channel();
        self.database
            .send(Command::Complete(PendingCompletion {
                scope: self.scope.clone(),
                result,
                sequence,
                reply,
            }))
            .await?;
        response
            .await
            .context("database service stopped before durable completion")?
    }
}

#[async_trait]
impl ActorDatabase for DatabaseInvocation {
    async fn call(&self, call: DatabaseCall, rows: Box<dyn RowSink>) -> Result<DatabaseResult> {
        ensure!(
            self.scope.active.load(Ordering::Acquire),
            "database call has no active invocation"
        );
        let (reply, result) = oneshot::channel();
        self.database
            .send(Command::Call {
                scope: self.scope.clone(),
                call,
                rows,
                reply,
            })
            .await?;
        result
            .await
            .context("database service stopped before database call completed")?
    }
}

struct InvocationScope {
    invocation: ActorInvocation,
    owner_epoch: Option<u64>,
    origin: CommitOrigin,
    committed: AtomicU64,
    active: AtomicBool,
}

#[derive(Default)]
struct DatabaseTimings(Vec<(RequestStage, Instant)>);
impl DatabaseTimings {
    fn mark(&mut self, stage: RequestStage) {
        self.0.push((stage, Instant::now()));
    }
}

pub(super) enum PreparedActivation {
    Completed(ActorActivation),
    Hydrate { activation: ActorActivation },
}
pub(super) enum PreparedInvocation {
    Execute,
    Completed(ActorExecutionResult),
}
enum DatabaseReply {
    Executed {
        result: DatabaseResult,
        committed: Option<u64>,
    },
    Rejected(anyhow::Error),
}

#[derive(Debug)]
pub(super) struct PublicationError;
impl std::fmt::Display for PublicationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("actor state publication could not be confirmed")
    }
}
impl std::error::Error for PublicationError {}

enum Command {
    Prepare {
        scope: Arc<InvocationScope>,
        replay: bool,
        reply: oneshot::Sender<(Result<PreparedInvocation>, DatabaseTimings)>,
    },
    Call {
        scope: Arc<InvocationScope>,
        call: DatabaseCall,
        rows: Box<dyn RowSink>,
        reply: oneshot::Sender<Result<DatabaseResult>>,
    },
    Activate {
        reply: oneshot::Sender<Result<PreparedActivation>>,
    },
    Hydrated {
        activation: ActorActivation,
        result: Result<()>,
        reply: oneshot::Sender<Result<ActorActivation>>,
    },
    Complete(PendingCompletion),
    Reset {
        reply: oneshot::Sender<()>,
    },
    Park {
        reply: oneshot::Sender<Result<Option<crate::node_cache::SlotFile>>>,
    },
    DatabaseSize {
        reply: oneshot::Sender<Option<u64>>,
    },
}

struct PendingCompletion {
    scope: Arc<InvocationScope>,
    result: Option<JsonText>,
    sequence: Option<u64>,
    reply: oneshot::Sender<Result<u64>>,
}

struct DatabaseService {
    session: DatabaseSession,
    version: Arc<AtomicU64>,
    stopped: CancellationToken,
    ready: BTreeMap<u64, PendingCompletion>,
    next_sequence: u64,
}

impl DatabaseService {
    async fn run(mut self, mut commands: mpsc::Receiver<Command>) {
        let _stopped = self.stopped.clone().drop_guard();
        loop {
            let command = tokio::select! {
                biased;
                _ = self.stopped.cancelled() => return,
                command = commands.recv() => match command { Some(command) => command, None => return },
            };
            if !self.handle(command).await {
                return;
            }
            self.version
                .store(self.session.state_version().unwrap_or(0), Ordering::Release);
        }
    }

    async fn handle(&mut self, command: Command) -> bool {
        match command {
            Command::Prepare {
                scope,
                replay,
                reply,
            } => {
                let mut timings = DatabaseTimings::default();
                let result = self
                    .session
                    .prepare_invocation(&scope.invocation, scope.owner_epoch, replay, &mut timings)
                    .await;
                let _ = reply.send((result, timings));
            }
            Command::Call {
                scope,
                call,
                rows,
                reply,
            } => return self.call(scope, call, rows, reply).await,
            Command::Activate { reply } => {
                let _ = reply.send(self.session.prepare_activation().await);
            }
            Command::Hydrated {
                activation,
                result,
                reply,
            } => {
                let _ = reply.send(self.session.finish_activation(activation, result));
            }
            Command::Complete(completion) => return self.complete(completion).await,
            Command::Reset { reply } => {
                self.session.reset();
                let _ = reply.send(());
            }
            Command::Park { reply } => {
                let _ = reply.send(self.session.park().await);
            }
            Command::DatabaseSize { reply } => {
                let _ = reply.send(self.session.database_bytes().await);
            }
        }
        true
    }

    async fn call(
        &mut self,
        scope: Arc<InvocationScope>,
        call: DatabaseCall,
        rows: Box<dyn RowSink>,
        reply: oneshot::Sender<Result<DatabaseResult>>,
    ) -> bool {
        if !scope.active.load(Ordering::Acquire) {
            let _ = reply.send(Err(anyhow::anyhow!(
                "database call has no active invocation"
            )));
            return true;
        }
        let result = self
            .session
            .database_call(
                &scope.invocation,
                scope.owner_epoch,
                scope.origin.clone(),
                call,
                rows,
            )
            .await;
        match result {
            Ok(DatabaseReply::Executed { result, committed }) => {
                if let Some(txid) = committed {
                    scope.committed.store(txid, Ordering::Release);
                }
                self.version
                    .store(self.session.state_version().unwrap_or(0), Ordering::Release);
                let _ = reply.send(Ok(result));
                true
            }
            Ok(DatabaseReply::Rejected(error)) => {
                let _ = reply.send(Err(error));
                true
            }
            Err(error) => {
                tracing::warn!(error = %format!("{error:#}"), "actor database write could not be confirmed");
                self.stopped.cancel();
                let _ = reply.send(Err(anyhow::anyhow!(
                    "actor database write could not be confirmed"
                )));
                false
            }
        }
    }

    async fn complete(&mut self, completion: PendingCompletion) -> bool {
        let Some(sequence) = completion.sequence else {
            self.publish(completion).await;
            return true;
        };
        if sequence < self.next_sequence || self.ready.contains_key(&sequence) {
            let _ = completion
                .reply
                .send(Err(anyhow::anyhow!("actor completion sequence repeated")));
            return false;
        }
        self.ready.insert(sequence, completion);
        while let Some(completion) = self.ready.remove(&self.next_sequence) {
            if !self.publish(completion).await {
                return false;
            }
            self.next_sequence += 1;
        }
        true
    }

    async fn publish(&mut self, completion: PendingCompletion) -> bool {
        let scope = completion.scope;
        let txid = scope.committed.load(Ordering::Acquire);
        let mut origin = scope.origin.clone();
        origin.interleaved = completion.sequence.is_some();
        let result = self
            .session
            .complete(
                &scope.invocation,
                scope.owner_epoch,
                completion.result,
                (txid > 0).then_some(txid),
                origin,
            )
            .await;
        let success = result.is_ok();
        self.version
            .store(self.session.state_version().unwrap_or(0), Ordering::Release);
        let _ = completion.reply.send(result);
        success
    }
}
