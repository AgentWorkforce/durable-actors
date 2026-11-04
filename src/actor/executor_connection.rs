use std::{
    collections::{HashMap, HashSet},
    os::unix::fs::FileTypeExt,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{
        UnixListener,
        unix::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::{mpsc, oneshot, watch},
    task::{JoinHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use super::{
    ActorCall, ActorDatabase, ActorInvocationFailure, ActorKey, ActorSocketSource, DatabaseCall,
    DatabaseResult,
};

const ACTOR_EXECUTOR_PROTOCOL_VERSION: u32 = 31;

#[derive(Debug, Serialize)]
pub struct ActorMethodInvocation {
    pub request_id: String,
    pub method: String,
    pub args: Vec<Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ActorSocketConnection {
    pub id: String,
    pub metadata: Value,
    pub tags: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ActorSocketMessage {
    Text { data: String },
    Binary { data: String },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ActorSocketEvent {
    Connect {
        connection: ActorSocketConnection,
    },
    Message {
        connection_id: String,
        message: ActorSocketMessage,
    },
    Disconnect {
        connection: ActorSocketConnection,
        code: u16,
        reason: String,
        was_clean: bool,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ActorSocketInvocation {
    pub request_id: String,
    pub actor: ActorKey,
    pub event: ActorSocketEvent,
    pub connections: Vec<ActorSocketConnection>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActorSocketTagMatch {
    #[default]
    All,
    Any,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ActorSocketEffect {
    SetAutoResponse {
        request: Option<String>,
        response: Option<String>,
    },
    StateSnapshot {
        connection_id: String,
        state: Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        version: Option<u64>,
    },
    StateUpdate {
        changes: Value,
        removed: Vec<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        except_connection_ids: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        version: Option<u64>,
    },
    Send {
        connection_id: String,
        message: ActorSocketMessage,
    },
    Broadcast {
        message: ActorSocketMessage,
        except_connection_ids: Vec<String>,
        tags: Vec<String>,
        #[serde(default)]
        tag_match: ActorSocketTagMatch,
    },
    Close {
        connection_id: String,
        code: u16,
        reason: String,
    },
    Reject {
        connection_id: String,
        code: u16,
        reason: String,
    },
    SetMetadata {
        connection_id: String,
        metadata: Value,
    },
    SetTags {
        connection_id: String,
        tags: Vec<String>,
    },
}

#[derive(Debug, PartialEq)]
pub enum ActorMethodOutcome {
    Interleaved(ActorInterleavedOutcome),
    Completed {
        result: Value,
        effects: Vec<ActorSocketEffect>,
    },
    Failed(ActorInvocationFailure),
}

#[derive(Debug, PartialEq)]
pub enum ActorSocketOutcome {
    Interleaved(ActorInterleavedOutcome),
    Handled { effects: Vec<ActorSocketEffect> },
    Failed(ActorInvocationFailure),
}

#[derive(Debug, PartialEq)]
pub struct ActorInterleavedOutcome {
    pub sequence: u64,
    pub result: Value,
    pub effects: Vec<ActorSocketEffect>,
}

#[async_trait]
pub trait ActorExecutor: Send + Sync {
    fn supports(&self, actor_name: &str) -> bool;

    fn invocation_admission(&self) -> Option<watch::Receiver<()>> {
        None
    }

    async fn hydrate(&self, _request_id: String, _database: Arc<dyn ActorDatabase>) -> Result<()> {
        Ok(())
    }

    fn resident(&self) -> Option<bool> {
        None
    }

    fn residency_changes(&self) -> Option<watch::Receiver<()>> {
        None
    }

    async fn invoke(
        &self,
        invocation: ActorMethodInvocation,
        database: Arc<dyn ActorDatabase>,
    ) -> Result<ActorMethodOutcome>;

    async fn handle_socket(
        &self,
        _invocation: ActorSocketInvocation,
        _database: Arc<dyn ActorDatabase>,
    ) -> Result<ActorSocketOutcome> {
        Ok(ActorSocketOutcome::Failed(ActorInvocationFailure {
            code: "socket_not_supported".into(),
            message: "actor executor does not support sockets".into(),
        }))
    }

    async fn evict(&self, _graceful: bool, _database: Arc<dyn ActorDatabase>) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
pub(crate) trait ActorSocketPublisher: Send + Sync {
    async fn publish(&self, effects: Vec<ActorSocketEffect>) -> Result<()>;
}

#[async_trait]
pub(crate) trait ActorCaller: Send + Sync {
    /// Returns the invocation outcome, reporting transport failures as failed outcomes.
    async fn call(&self, call: ActorCall) -> Value;
}

pub(crate) struct ActorExecutorListener {
    listener: UnixListener,
    socket_path: PathBuf,
}

impl ActorExecutorListener {
    pub(crate) async fn bind(socket_path: impl Into<PathBuf>) -> Result<Self> {
        let socket_path = socket_path.into();
        prepare_socket_path(&socket_path).await?;
        if let Some(parent) = socket_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("create actor executor directory {}", parent.display()))?;
        }
        let listener = UnixListener::bind(&socket_path)
            .with_context(|| format!("bind actor executor socket {}", socket_path.display()))?;
        Ok(Self {
            listener,
            socket_path,
        })
    }

    pub(crate) async fn accept(self) -> Result<ActorExecutorConnection> {
        let result = self.accept_connection().await;
        let cleanup = remove_socket(&self.socket_path).await;
        match (result, cleanup) {
            (Ok(connection), Ok(())) => Ok(connection),
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
        }
    }

    async fn accept_connection(&self) -> Result<ActorExecutorConnection> {
        let (stream, _) =
            self.listener.accept().await.with_context(|| {
                format!("accept actor executor at {}", self.socket_path.display())
            })?;
        let (reader, writer) = stream.into_split();
        let reader = BufReader::new(reader);
        attach_executor(reader, writer).await
    }

    pub(crate) async fn accept_warm(self) -> Result<WarmExecutor> {
        let (stream, _) = self.listener.accept().await?;
        remove_socket(&self.socket_path).await?;
        let (reader, writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        ensure!(
            matches!(
                read_client_message(&mut reader).await?,
                Some(ActorExecutorClientMessage::Warm {
                    protocol: ACTOR_EXECUTOR_PROTOCOL_VERSION
                })
            ),
            "generic executor did not finish warming"
        );
        Ok(WarmExecutor { reader, writer })
    }
}

pub(crate) struct WarmExecutor {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
}

impl WarmExecutor {
    pub(crate) async fn load(
        mut self,
        entrypoint: &str,
        environment: &HashMap<String, String>,
    ) -> Result<ActorExecutorConnection> {
        let mut bytes = serde_json::to_vec(&serde_json::json!({
            "type": "load", "entrypoint": entrypoint, "environment": environment
        }))?;
        bytes.push(b'\n');
        self.writer.write_all(&bytes).await?;
        attach_executor(self.reader, self.writer).await
    }
}

async fn attach_executor(
    mut reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
) -> Result<ActorExecutorConnection> {
    let actor_names = match read_client_message(&mut reader).await? {
        Some(ActorExecutorClientMessage::Attach {
            protocol,
            actor_names,
        }) => {
            ensure!(
                protocol == ACTOR_EXECUTOR_PROTOCOL_VERSION,
                "unsupported executor protocol {protocol}"
            );
            ensure!(
                !actor_names.is_empty(),
                "customer executor advertised no actor names"
            );
            actor_names
        }
        _ => anyhow::bail!("customer executor must attach after loading code"),
    };
    let (executor, task) = JsActorExecutor::start(reader, writer, actor_names);
    debug!(actor_names = ?executor.actor_names, "JavaScript executor connected");
    Ok(ActorExecutorConnection { executor, task })
}

pub(crate) struct ActorExecutorConnection {
    executor: Arc<JsActorExecutor>,
    task: JoinHandle<Result<()>>,
}

impl ActorExecutorConnection {
    pub(crate) fn executor(&self) -> Arc<dyn ActorExecutor> {
        self.executor.clone()
    }

    pub(crate) async fn mark_ready(
        &self,
        actor: ActorKey,
        publisher: Option<Arc<dyn ActorSocketPublisher>>,
        sockets: Option<Arc<dyn ActorSocketSource>>,
        database: Option<Arc<dyn ActorDatabase>>,
        calls: Option<Arc<dyn ActorCaller>>,
    ) -> Result<()> {
        self.executor
            .mark_ready(actor, publisher, sockets, database, calls)
            .await?;
        info!(
            actor_names = ?self.executor.actor_names,
            "customer JavaScript process attached to actor executor"
        );
        Ok(())
    }

    pub(crate) async fn run(mut self, shutdown: CancellationToken) -> Result<()> {
        tokio::select! {
            result = &mut self.task => {
                match result {
                    Ok(result) => result,
                    Err(error) => Err(error.into()),
                }
            }
            _ = shutdown.cancelled() => {
                self.task.abort();
                let _ = (&mut self.task).await;
                Ok(())
            }
        }
    }
}

impl Drop for ActorExecutorConnection {
    fn drop(&mut self) {
        self.task.abort();
    }
}

type Residency = Arc<Mutex<Option<(Instant, bool)>>>;

struct JsActorExecutor {
    changes: watch::Sender<()>,
    residency: Residency,
    actor_names: HashSet<String>,
    admission: watch::Sender<()>,
    commands: mpsc::UnboundedSender<ExecutorRequest>,
}

#[async_trait]
impl ActorExecutor for JsActorExecutor {
    fn invocation_admission(&self) -> Option<watch::Receiver<()>> {
        Some(self.admission.subscribe())
    }

    async fn hydrate(&self, request_id: String, database: Arc<dyn ActorDatabase>) -> Result<()> {
        match self
            .exchange(ExecutorCommand::Hydrate { request_id }, Some(database))
            .await?
        {
            ExecutorReply::Hydrated => Ok(()),
            ExecutorReply::Failed { code, message } => {
                anyhow::bail!("actor hydration failed ({code}): {message}")
            }
            reply => anyhow::bail!("unexpected actor hydration reply: {reply:?}"),
        }
    }

    fn residency_changes(&self) -> Option<watch::Receiver<()>> {
        Some(self.changes.subscribe())
    }
    fn resident(&self) -> Option<bool> {
        self.residency
            .lock()
            .unwrap()
            .as_ref()
            .filter(|(at, _)| at.elapsed() < Duration::from_secs(5))
            .map(|(_, resident)| *resident)
    }

    fn supports(&self, actor_name: &str) -> bool {
        self.actor_names.contains(actor_name)
    }

    async fn invoke(
        &self,
        invocation: ActorMethodInvocation,
        database: Arc<dyn ActorDatabase>,
    ) -> Result<ActorMethodOutcome> {
        match self
            .exchange(ExecutorCommand::Invoke(invocation), Some(database))
            .await?
        {
            ExecutorReply::Invoked {
                result,
                effects,
                sequence,
            } => Ok(match sequence {
                Some(sequence) => ActorMethodOutcome::Interleaved(ActorInterleavedOutcome {
                    sequence,
                    result,
                    effects,
                }),
                None => ActorMethodOutcome::Completed { result, effects },
            }),
            ExecutorReply::Failed { code, message } => {
                Ok(ActorMethodOutcome::Failed(ActorInvocationFailure {
                    code,
                    message,
                }))
            }
            reply => anyhow::bail!("unexpected actor invocation reply: {reply:?}"),
        }
    }

    async fn handle_socket(
        &self,
        invocation: ActorSocketInvocation,
        database: Arc<dyn ActorDatabase>,
    ) -> Result<ActorSocketOutcome> {
        let command = ExecutorCommand::WebsocketEvent {
            request_id: invocation.request_id,
            event: invocation.event,
            connections: invocation.connections,
        };
        match self.exchange(command, Some(database)).await? {
            ExecutorReply::WebsocketHandled { effects, sequence } => Ok(match sequence {
                Some(sequence) => ActorSocketOutcome::Interleaved(ActorInterleavedOutcome {
                    sequence,
                    result: Value::Null,
                    effects,
                }),
                None => ActorSocketOutcome::Handled { effects },
            }),
            ExecutorReply::Failed { code, message } => {
                Ok(ActorSocketOutcome::Failed(ActorInvocationFailure {
                    code,
                    message,
                }))
            }
            reply => anyhow::bail!("unexpected actor socket reply: {reply:?}"),
        }
    }

    async fn evict(&self, graceful: bool, database: Arc<dyn ActorDatabase>) -> Result<()> {
        match self
            .exchange(ExecutorCommand::Evict { graceful }, Some(database))
            .await?
        {
            ExecutorReply::Evicted => Ok(()),
            ExecutorReply::Failed { code, message } => {
                anyhow::bail!("actor executor rejected eviction ({code}): {message}")
            }
            reply => anyhow::bail!("unexpected actor eviction reply: {reply:?}"),
        }
    }
}

impl JsActorExecutor {
    fn start(
        reader: BufReader<OwnedReadHalf>,
        writer: OwnedWriteHalf,
        actor_names: Vec<String>,
    ) -> (Arc<Self>, JoinHandle<Result<()>>) {
        let (commands, incoming) = mpsc::unbounded_channel();
        let residency = Arc::new(Mutex::new(None));
        let (changes, _) = watch::channel(());
        let (admission, _) = watch::channel(());

        let executor = Arc::new(Self {
            changes: changes.clone(),
            residency: residency.clone(),
            actor_names: actor_names.into_iter().collect(),
            admission: admission.clone(),
            commands,
        });

        let task = tokio::spawn(async move {
            let result =
                run_executor_connection(reader, writer, incoming, residency, changes, admission)
                    .await;

            if let Err(error) = &result {
                tracing::error!(
                    error = %format!("{error:#}"),
                    "executor connection failed"
                );
            }

            result
        });

        (executor, task)
    }

    async fn mark_ready(
        &self,
        actor: ActorKey,
        publisher: Option<Arc<dyn ActorSocketPublisher>>,
        sockets: Option<Arc<dyn ActorSocketSource>>,
        database: Option<Arc<dyn ActorDatabase>>,
        calls: Option<Arc<dyn ActorCaller>>,
    ) -> Result<()> {
        let (reply, ready) = oneshot::channel();
        self.commands
            .send(ExecutorRequest::Ready {
                actor,
                written: reply,
                publisher,
                sockets,
                database,
                calls,
            })
            .context("actor executor stopped")?;
        ready
            .await
            .context("actor executor disconnected before readiness")?
    }

    async fn exchange(
        &self,
        command: ExecutorCommand,
        database: Option<Arc<dyn ActorDatabase>>,
    ) -> Result<ExecutorReply> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(ExecutorRequest::Exchange(Box::new(PendingCommand {
                command,
                database,
                reply,
                admission_granted: false,
            })))
            .context("actor executor stopped")?;
        response
            .await
            .context("customer actor executor disconnected before replying")?
    }
}

async fn run_executor_connection(
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
    commands: mpsc::UnboundedReceiver<ExecutorRequest>,
    residency: Residency,
    changes: watch::Sender<()>,
    admission: watch::Sender<()>,
) -> Result<()> {
    let (outbound, writes) = mpsc::unbounded_channel();
    let (inbound, replies) = mpsc::unbounded_channel();
    let driver = ExecutorDriver {
        admission,
        changes,
        residency,
        pending: HashMap::new(),
        next_message_id: 1,
        outbound,
        publisher: None,
        sockets: None,
        database: None,
        calls: None,
        loading_connections: JoinSet::new(),
        connection_lookup_ids: HashSet::new(),
        database_calls: JoinSet::new(),
        database_ids: HashSet::new(),
        actor_calls: JoinSet::new(),
    };
    tokio::try_join!(
        driver.run(commands, replies),
        read_executor_messages(reader, inbound),
        write_executor_messages(writer, writes)
    )?;
    Ok(())
}

struct ExecutorDriver {
    admission: watch::Sender<()>,
    changes: watch::Sender<()>,
    residency: Residency,
    pending: HashMap<u64, PendingCommand>,
    next_message_id: u64,
    outbound: mpsc::UnboundedSender<ExecutorWrite>,
    publisher: Option<Arc<dyn ActorSocketPublisher>>,
    sockets: Option<Arc<dyn ActorSocketSource>>,
    database: Option<Arc<dyn ActorDatabase>>,
    calls: Option<Arc<dyn ActorCaller>>,
    loading_connections: JoinSet<(u64, Result<super::SocketLookup>)>,
    connection_lookup_ids: HashSet<u64>,
    database_calls: JoinSet<(u64, Option<u64>, Result<DatabaseResult>)>,
    database_ids: HashSet<u64>,
    actor_calls: JoinSet<(u64, Value)>,
}

impl ExecutorDriver {
    async fn run(
        mut self,
        mut commands: mpsc::UnboundedReceiver<ExecutorRequest>,
        mut replies: mpsc::UnboundedReceiver<Result<ActorExecutorClientMessage>>,
    ) -> Result<()> {
        loop {
            tokio::select! {
                biased;
                reply = replies.recv() => {
                    match reply.context("actor executor reader stopped")?? {
                        ActorExecutorClientMessage::Residency { resident } => {
                            let mut current = self.residency.lock().unwrap();
                            let changed = current.as_ref().is_none_or(|(_, previous)| *previous != resident);
                            *current = Some((Instant::now(), resident));
                            if changed { self.changes.send_replace(()); }
                        }
                        ActorExecutorClientMessage::ReadyForInvocation { message_id } => self.allow_next_invocation(message_id)?,
                        ActorExecutorClientMessage::Reply { message_id, reply } => self.deliver(message_id, reply)?,
                        ActorExecutorClientMessage::SocketEffects { message_id, effects } => self.publish(message_id, effects)?,
                        ActorExecutorClientMessage::DatabaseCall { call_id, message_id, call } => self.call_database(call_id, message_id, call)?,
                        ActorExecutorClientMessage::GetConnections { message_id, query } => self.load_connections(message_id, query)?,
                        ActorExecutorClientMessage::ActorCall { call_id, call } => self.call_actor(call_id, call),
                        ActorExecutorClientMessage::Warm { .. } | ActorExecutorClientMessage::Attach { .. } => anyhow::bail!("customer actor executor attached more than once"),
                    }
                }
                called = self.database_calls.join_next(), if !self.database_calls.is_empty() => {
                    let (call_id, message_id, result) = called.context("actor database call stopped")??;
                    self.database_called(call_id, message_id, result)?;
                }
                called = self.actor_calls.join_next(), if !self.actor_calls.is_empty() => {
                    let (call_id, outcome) = called.context("actor call stopped")??;
                    self.write(encode_server_message(&ActorExecutorServerMessage::ActorResult { call_id, outcome })?)?;
                }
                loaded = self.loading_connections.join_next(), if !self.loading_connections.is_empty() => {
                    let (message_id, result) = loaded.context("connection lookup stopped")??;
                    self.connection_lookup_ids.remove(&message_id);
                    let (connections, error) = match result {
                        Ok(connections) => (connections, None),
                        Err(error) => (super::SocketLookup::Connections(Vec::new()), Some(format!("{error:#}"))),
                    };
                    self.write(encode_server_message(&ActorExecutorServerMessage::SocketConnections {
                        message_id, connections, error,
                    })?)?;
                }
                command = commands.recv() => match command {
                    Some(command) => self.handle_command(command)?,
                    None => return Ok(()),
                }
            }
        }
    }

    fn handle_command(&mut self, command: ExecutorRequest) -> Result<()> {
        match command {
            ExecutorRequest::Exchange(pending) => self.enqueue(*pending),
            ExecutorRequest::Ready {
                actor,
                written,
                publisher,
                sockets,
                database,
                calls,
            } => {
                self.publisher = publisher;
                self.sockets = sockets;
                self.database = database;
                self.calls = calls;
                self.outbound
                    .send(ExecutorWrite {
                        bytes: encode_server_message(&ActorExecutorServerMessage::Attached {
                            protocol: ACTOR_EXECUTOR_PROTOCOL_VERSION,
                            actor,
                        })?,
                        written: Some(written),
                    })
                    .map_err(|_| anyhow::anyhow!("actor executor writer stopped"))
            }
        }
    }

    fn allow_next_invocation(&mut self, message_id: u64) -> Result<()> {
        let pending = self
            .pending
            .get_mut(&message_id)
            .context("invocation admission has no active call")?;
        ensure!(
            matches!(
                pending.command,
                ExecutorCommand::Invoke(_) | ExecutorCommand::WebsocketEvent { .. }
            ),
            "only invocations can admit another call"
        );
        ensure!(
            !pending.admission_granted,
            "invocation admission was already granted"
        );
        pending.admission_granted = true;
        self.admission.send_replace(());
        Ok(())
    }

    fn call_database(
        &mut self,
        call_id: u64,
        message_id: Option<u64>,
        call: DatabaseCall,
    ) -> Result<()> {
        let message_id = message_id.filter(|id| self.pending.contains_key(id));
        let database = match message_id {
            Some(id) => self.pending[&id].database.clone(),
            None => self.database.clone(),
        };
        if let Some(id) = message_id {
            ensure!(
                self.database_ids.insert(id),
                "actor sent concurrent database calls"
            );
        }
        self.database_calls.spawn(async move {
            let result = match database {
                Some(database) => database.call(call).await,
                None => Err(anyhow::anyhow!("actor database is unavailable")),
            };
            (call_id, message_id, result)
        });
        Ok(())
    }

    fn database_called(
        &mut self,
        call_id: u64,
        message_id: Option<u64>,
        result: Result<DatabaseResult>,
    ) -> Result<()> {
        if let Some(id) = message_id {
            self.database_ids.remove(&id);
        }
        let (result, error) = match result {
            Ok(result) => (Some(result), None),
            Err(error) => (None, Some(format!("{error:#}"))),
        };
        self.write(encode_server_message(
            &ActorExecutorServerMessage::DatabaseResult {
                call_id,
                result,
                error,
            },
        )?)
    }

    fn call_actor(&mut self, call_id: u64, call: ActorCall) {
        let calls = self.calls.clone();
        self.actor_calls.spawn(async move {
            let outcome = match calls {
                Some(calls) => calls.call(call).await,
                None => serde_json::json!({
                    "type": "failed", "code": "unavailable", "message": "actor calls are unavailable"
                }),
            };
            (call_id, outcome)
        });
    }

    fn load_connections(&mut self, message_id: u64, query: super::SocketQuery) -> Result<()> {
        let pending = self
            .pending
            .get(&message_id)
            .context("connection lookup has no active actor invocation")?;
        ensure!(
            !matches!(pending.command, ExecutorCommand::Evict { .. }),
            "cannot load connections during eviction"
        );
        ensure!(
            self.connection_lookup_ids.insert(message_id),
            "actor sent concurrent connection lookups"
        );
        let sockets = self.sockets.clone();
        self.loading_connections.spawn(async move {
            let result = async {
                sockets
                    .context("actor connection lookup is unavailable")?
                    .query(query)
                    .await
            }
            .await;
            (message_id, result)
        });
        Ok(())
    }

    fn publish(&self, message_id: u64, effects: Vec<ActorSocketEffect>) -> Result<()> {
        let pending = self
            .pending
            .get(&message_id)
            .context("socket output has no active actor invocation")?;
        let connecting = matches!(
            &pending.command,
            ExecutorCommand::WebsocketEvent {
                event: ActorSocketEvent::Connect { .. },
                ..
            }
        );
        let publisher = self.publisher.clone();
        tokio::spawn(async move {
            let result = async {
                ensure!(
                    !connecting,
                    "socket output cannot precede connection acceptance"
                );
                super::validate_socket_effects(&effects)?;
                publisher
                    .context("actor socket publishing is unavailable")?
                    .publish(effects)
                    .await
            }
            .await;
            if let Err(error) = result {
                warn!(error = %format!("{error:#}"), "actor socket output was not published");
            }
        });
        Ok(())
    }

    fn enqueue(&mut self, pending: PendingCommand) -> Result<()> {
        let message_id = self.next_message_id;
        self.next_message_id = message_id
            .checked_add(1)
            .context("actor executor message ID overflow")?;
        let bytes = encode_server_message(&ActorExecutorServerMessage::Command {
            message_id,
            command: &pending.command,
        });
        match bytes {
            Ok(bytes) => {
                self.write(bytes)?;
                self.pending.insert(message_id, pending);
            }
            Err(error) => {
                let _ = pending.reply.send(Err(error));
            }
        }
        Ok(())
    }

    fn write(&self, bytes: Vec<u8>) -> Result<()> {
        self.outbound
            .send(ExecutorWrite {
                bytes,
                written: None,
            })
            .map_err(|_| anyhow::anyhow!("actor executor writer stopped"))
    }

    fn deliver(&mut self, message_id: u64, reply: ExecutorReply) -> Result<()> {
        ensure!(
            !self.database_ids.contains(&message_id),
            "actor replied before its database call completed"
        );
        ensure!(
            !self.connection_lookup_ids.contains(&message_id),
            "actor completed before connection lookup finished"
        );
        let pending = self
            .pending
            .remove(&message_id)
            .with_context(|| format!("actor executor replied to unknown message {message_id}"))?;
        let _ = pending.reply.send(Ok(reply));
        Ok(())
    }
}

async fn read_executor_messages(
    mut reader: BufReader<OwnedReadHalf>,
    inbound: mpsc::UnboundedSender<Result<ActorExecutorClientMessage>>,
) -> Result<()> {
    loop {
        let reply = match read_client_message(&mut reader).await {
            Ok(Some(message)) => Ok(message),
            Ok(None) => Err(anyhow::anyhow!(
                "customer JavaScript actor executor disconnected"
            )),
            Err(error) => Err(error),
        };
        let stopped = reply.is_err();
        inbound
            .send(reply)
            .context("actor executor driver stopped")?;
        if stopped {
            return Ok(());
        }
    }
}

async fn write_executor_messages(
    mut writer: OwnedWriteHalf,
    mut writes: mpsc::UnboundedReceiver<ExecutorWrite>,
) -> Result<()> {
    while let Some(write) = writes.recv().await {
        let result = writer
            .write_all(&write.bytes)
            .await
            .context("write actor executor command");
        if let Some(written) = write.written {
            let _ = written.send(
                result
                    .as_ref()
                    .map(|_| ())
                    .map_err(|error| anyhow::anyhow!("{error:#}")),
            );
        }
        result?;
    }
    Ok(())
}

enum ExecutorRequest {
    Exchange(Box<PendingCommand>),
    Ready {
        actor: ActorKey,
        written: oneshot::Sender<Result<()>>,
        publisher: Option<Arc<dyn ActorSocketPublisher>>,
        sockets: Option<Arc<dyn ActorSocketSource>>,
        database: Option<Arc<dyn ActorDatabase>>,
        calls: Option<Arc<dyn ActorCaller>>,
    },
}

struct PendingCommand {
    command: ExecutorCommand,
    database: Option<Arc<dyn ActorDatabase>>,
    admission_granted: bool,
    reply: oneshot::Sender<Result<ExecutorReply>>,
}

struct ExecutorWrite {
    bytes: Vec<u8>,
    written: Option<oneshot::Sender<Result<()>>>,
}

async fn read_client_message(
    reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
) -> Result<Option<ActorExecutorClientMessage>> {
    let mut document = Vec::new();
    let bytes = reader.read_until(b'\n', &mut document).await?;
    if bytes == 0 {
        return Ok(None);
    }
    serde_json::from_slice(trim_ascii_end(&document))
        .map(Some)
        .context("decode customer actor executor message")
}

fn trim_ascii_end(mut document: &[u8]) -> &[u8] {
    while document.last().is_some_and(u8::is_ascii_whitespace) {
        document = &document[..document.len() - 1];
    }
    document
}

fn encode_server_message(message: &ActorExecutorServerMessage<'_>) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec(message)?;
    bytes.push(b'\n');
    Ok(bytes)
}

async fn prepare_socket_path(path: &Path) -> Result<()> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) => {
            ensure!(
                metadata.file_type().is_socket(),
                "refusing to replace non-socket actor executor path {}",
                path.display()
            );
            tokio::fs::remove_file(path).await?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

async fn remove_socket(path: &Path) -> Result<()> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => {
            debug!(socket = %path.display(), "actor executor socket removed");
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ActorExecutorServerMessage<'a> {
    DatabaseResult {
        call_id: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        result: Option<DatabaseResult>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    ActorResult {
        call_id: u64,
        outcome: Value,
    },
    SocketConnections {
        message_id: u64,
        connections: super::SocketLookup,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    Attached {
        protocol: u32,
        actor: ActorKey,
    },
    Command {
        message_id: u64,
        command: &'a ExecutorCommand,
    },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ActorExecutorClientMessage {
    DatabaseCall {
        call_id: u64,
        #[serde(default)]
        message_id: Option<u64>,
        call: DatabaseCall,
    },
    ActorCall {
        call_id: u64,
        call: ActorCall,
    },
    Warm {
        protocol: u32,
    },
    Residency {
        resident: bool,
    },
    GetConnections {
        message_id: u64,
        #[serde(default)]
        query: super::SocketQuery,
    },
    ReadyForInvocation {
        message_id: u64,
    },
    SocketEffects {
        message_id: u64,
        effects: Vec<ActorSocketEffect>,
    },
    Attach {
        protocol: u32,
        actor_names: Vec<String>,
    },
    Reply {
        message_id: u64,
        reply: ExecutorReply,
    },
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ExecutorCommand {
    Hydrate {
        request_id: String,
    },
    Invoke(ActorMethodInvocation),
    WebsocketEvent {
        request_id: String,
        event: ActorSocketEvent,
        connections: Vec<ActorSocketConnection>,
    },
    Evict {
        graceful: bool,
    },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ExecutorReply {
    Hydrated,
    Invoked {
        result: Value,
        #[serde(default)]
        sequence: Option<u64>,
        #[serde(default)]
        effects: Vec<ActorSocketEffect>,
    },
    WebsocketHandled {
        #[serde(default)]
        sequence: Option<u64>,
        effects: Vec<ActorSocketEffect>,
    },
    Failed {
        code: String,
        message: String,
    },
    Evicted,
}

#[cfg(test)]
#[path = "../../tests/unit/actor/executor_connection.rs"]
mod tests;
