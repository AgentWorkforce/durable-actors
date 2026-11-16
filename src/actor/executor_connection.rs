use std::{
    collections::{HashMap, HashSet},
    os::{fd::OwnedFd, unix::fs::FileTypeExt},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use bytes::Bytes;
use command_fds::{CommandFdExt, FdMapping};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{
        UnixListener, UnixStream,
        unix::{OwnedReadHalf, OwnedWriteHalf},
    },
    process::Command,
    sync::{mpsc, oneshot, watch},
    task::{JoinHandle, JoinSet},
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use super::{
    ActorCall, ActorDatabase, ActorInvocationFailure, ActorKey, ActorLogRecord, ActorLogSink,
    ActorSocketSource, LogLevel, StdioLogSink,
    database_channel::{self, DatabaseScopes},
};
use crate::payload::{ARRIVING_BYTES, BUFFER_BYTES, JsonPayload, JsonStream, JsonText};

const ACTOR_EXECUTOR_PROTOCOL_VERSION: u32 = 33;
const DATABASE_FD: i32 = 3;
const NO_EFFECTS: &[u8] = b"[]";
const ARRIVING_RESULT_CHUNKS: usize = 16;
const ARRIVING_RESULT_STALL: Duration = Duration::from_secs(30);

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

impl ActorSocketEffect {
    /// The one socket this effect is for; `None` when it concerns every socket of the actor.
    pub(crate) fn connection_id(&self) -> Option<&str> {
        match self {
            Self::StateSnapshot { connection_id, .. }
            | Self::Send { connection_id, .. }
            | Self::Close { connection_id, .. }
            | Self::Reject { connection_id, .. }
            | Self::SetMetadata { connection_id, .. }
            | Self::SetTags { connection_id, .. } => Some(connection_id),
            Self::SetAutoResponse { .. } | Self::StateUpdate { .. } | Self::Broadcast { .. } => {
                None
            }
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum ActorMethodOutcome {
    Interleaved(ActorInterleavedOutcome),
    Completed {
        result: JsonPayload,
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
    pub result: JsonPayload,
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

/// The executor's first connection carries its commands. Each later one, and each channel made
/// for it to inherit, carries database calls.
pub(crate) struct ActorExecutorListener {
    listener: UnixListener,
    socket: SocketPath,
    inherited: Vec<UnixStream>,
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
            socket: SocketPath(socket_path),
            inherited: Vec::new(),
        })
    }

    pub(crate) fn socket_path(&self) -> &Path {
        &self.socket.0
    }

    /// Starts `command` holding a database channel, and tells it which descriptor that is. For
    /// a runtime that cannot open a socket its actor thread can block on.
    pub(crate) fn inherit_database_channel(&mut self, command: &mut Command) -> Result<()> {
        let (host, executor) = std::os::unix::net::UnixStream::pair()?;
        host.set_nonblocking(true)?;
        self.inherited.push(UnixStream::from_std(host)?);
        command
            .env("DURABLE_ACTORS_DATABASE_FD", DATABASE_FD.to_string())
            .fd_mappings(vec![FdMapping {
                parent_fd: OwnedFd::from(executor),
                child_fd: DATABASE_FD,
            }])?;
        Ok(())
    }

    pub(crate) async fn accept(self) -> Result<ActorExecutorConnection> {
        let (reader, writer) = self.accept_commands().await?;
        attach_executor(reader, writer, self.into()).await
    }

    pub(crate) async fn accept_warm(self) -> Result<WarmExecutor> {
        let (mut reader, writer) = self.accept_commands().await?;
        ensure!(
            matches!(
                read_client_message(&mut reader).await?,
                Some(ActorExecutorClientMessage::Warm {
                    protocol: ACTOR_EXECUTOR_PROTOCOL_VERSION
                })
            ),
            "generic executor did not finish warming"
        );
        Ok(WarmExecutor {
            reader,
            writer,
            channels: self.into(),
        })
    }

    async fn accept_commands(&self) -> Result<(BufReader<OwnedReadHalf>, OwnedWriteHalf)> {
        let (stream, _) = self
            .listener
            .accept()
            .await
            .with_context(|| format!("accept actor executor at {}", self.socket.0.display()))?;
        let (reader, writer) = stream.into_split();
        Ok((BufReader::new(reader), writer))
    }
}

pub(crate) struct WarmExecutor {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
    channels: DatabaseChannels,
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
        attach_executor(self.reader, self.writer, self.channels).await
    }
}

struct DatabaseChannels {
    listener: UnixListener,
    inherited: Vec<UnixStream>,
    _socket: SocketPath,
}

impl From<ActorExecutorListener> for DatabaseChannels {
    fn from(listener: ActorExecutorListener) -> Self {
        Self {
            listener: listener.listener,
            inherited: listener.inherited,
            _socket: listener.socket,
        }
    }
}

impl DatabaseChannels {
    async fn serve(self, scopes: Arc<DatabaseScopes>) {
        let mut serving = JoinSet::new();
        for stream in self.inherited {
            serving.spawn(database_channel::serve(stream, scopes.clone()));
        }
        loop {
            tokio::select! {
                accepted = self.listener.accept() => match accepted {
                    Ok((stream, _)) => {
                        serving.spawn(database_channel::serve(stream, scopes.clone()));
                    }
                    Err(error) => {
                        warn!(%error, "database channel was not accepted");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                },
                Some(served) = serving.join_next() => {
                    if let Ok(Err(error)) = served {
                        warn!(error = %format!("{error:#}"), "database channel failed");
                    }
                }
            }
        }
    }
}

struct SocketPath(PathBuf);

impl Drop for SocketPath {
    fn drop(&mut self) {
        match std::fs::remove_file(&self.0) {
            Ok(()) => debug!(socket = %self.0.display(), "actor executor socket removed"),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                warn!(%error, socket = %self.0.display(), "actor executor socket was not removed")
            }
        }
    }
}

async fn attach_executor(
    mut reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
    channels: DatabaseChannels,
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
    let scopes = Arc::new(DatabaseScopes::default());
    let (executor, task) = JsActorExecutor::start(reader, writer, actor_names, scopes.clone());
    debug!(actor_names = ?executor.actor_names, "JavaScript executor connected");
    Ok(ActorExecutorConnection {
        executor,
        task,
        channels: tokio::spawn(channels.serve(scopes)),
    })
}

pub(crate) struct ActorExecutorConnection {
    executor: Arc<JsActorExecutor>,
    task: JoinHandle<Result<()>>,
    channels: JoinHandle<()>,
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
        logs: Option<Arc<dyn ActorLogSink>>,
    ) -> Result<()> {
        self.executor
            .mark_ready(actor, publisher, sockets, database, calls, logs)
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
        self.channels.abort();
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
                    result: JsonPayload::default(),
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
        scopes: Arc<DatabaseScopes>,
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
            let result = run_executor_connection(
                reader, writer, incoming, residency, changes, admission, scopes,
            )
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
        logs: Option<Arc<dyn ActorLogSink>>,
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
                logs,
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
    scopes: Arc<DatabaseScopes>,
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
        scopes,
        calls: None,
        logs: Arc::new(StdioLogSink),
        loading_connections: JoinSet::new(),
        connection_lookup_ids: HashSet::new(),
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
    scopes: Arc<DatabaseScopes>,
    calls: Option<Arc<dyn ActorCaller>>,
    logs: Arc<dyn ActorLogSink>,
    loading_connections: JoinSet<(u64, Result<super::SocketLookup>)>,
    connection_lookup_ids: HashSet<u64>,
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
                        ActorExecutorClientMessage::Reply { message_id, reply, .. } => self.deliver(message_id, reply)?,
                        ActorExecutorClientMessage::SocketEffects { message_id, effects, .. } => self.publish(message_id, effects)?,
                        ActorExecutorClientMessage::GetConnections { message_id, query } => self.load_connections(message_id, query)?,
                        ActorExecutorClientMessage::ActorCall { call_id, call } => self.call_actor(call_id, call),
                        ActorExecutorClientMessage::Log { message_id, level, message } => self.log(message_id, level, message),
                        ActorExecutorClientMessage::Warm { .. } | ActorExecutorClientMessage::Attach { .. } => anyhow::bail!("customer actor executor attached more than once"),
                    }
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
                logs,
            } => {
                self.publisher = publisher;
                self.sockets = sockets;
                self.scopes.activate(database);
                self.calls = calls;
                if let Some(logs) = logs {
                    self.logs = logs;
                }
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

    fn log(&self, message_id: Option<u64>, level: LogLevel, message: String) {
        let request_id = message_id
            .and_then(|id| self.pending.get(&id))
            .and_then(|pending| pending.command.request_id())
            .map(str::to_owned);
        self.logs.emit(ActorLogRecord {
            request_id,
            level,
            message,
        });
    }

    fn enqueue(&mut self, mut pending: PendingCommand) -> Result<()> {
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
                // The command's database calls can arrive as soon as it is written.
                if let Some(database) = pending.database.take() {
                    self.scopes.open(message_id, database);
                }
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
            !self.connection_lookup_ids.contains(&message_id),
            "actor completed before connection lookup finished"
        );
        let pending = self
            .pending
            .remove(&message_id)
            .with_context(|| format!("actor executor replied to unknown message {message_id}"))?;
        self.scopes.close(message_id);
        let _ = pending.reply.send(Ok(reply));
        Ok(())
    }
}

async fn read_executor_messages(
    mut reader: BufReader<OwnedReadHalf>,
    inbound: mpsc::UnboundedSender<Result<ActorExecutorClientMessage>>,
) -> Result<()> {
    loop {
        if let Err(error) = forward_client_message(&mut reader, &inbound).await {
            inbound.send(Err(error)).map_err(|_| driver_stopped())?;
            return Ok(());
        }
    }
}

/// Sends the next message to the driver. A result passed on as it arrives follows its message.
async fn forward_client_message(
    reader: &mut BufReader<OwnedReadHalf>,
    inbound: &mpsc::UnboundedSender<Result<ActorExecutorClientMessage>>,
) -> Result<()> {
    let mut message = read_client_line(reader)
        .await?
        .context("customer JavaScript actor executor disconnected")?;
    let arriving = message.arriving_result();
    if arriving.is_none() {
        read_parts(reader, &mut message).await?;
    }
    inbound.send(Ok(message)).map_err(|_| driver_stopped())?;
    match arriving {
        Some(arriving) => arriving.forward(reader).await,
        None => Ok(()),
    }
}

fn driver_stopped() -> anyhow::Error {
    anyhow::anyhow!("actor executor driver stopped")
}

/// A result passed on as it arrives, followed by nothing but empty socket effects.
struct ArrivingResult {
    size: usize,
    chunks: mpsc::Sender<Bytes>,
}

impl ArrivingResult {
    /// Once nobody takes the result any more, the rest is still read, so the next message lines
    /// up.
    async fn forward(self, reader: &mut BufReader<OwnedReadHalf>) -> Result<()> {
        let mut chunks = Some(self.chunks);
        let mut remaining = self.size;
        while remaining > 0 {
            let chunk = read_part(reader, remaining.min(BUFFER_BYTES)).await?;
            remaining -= chunk.len();
            if let Some(taker) = &chunks
                && !pass_on(taker, chunk).await
            {
                chunks = None;
            }
        }
        ensure!(
            read_part(reader, NO_EFFECTS.len()).await? == NO_EFFECTS,
            "a result passed on as it arrives was followed by socket effects"
        );
        Ok(())
    }
}

async fn pass_on(chunks: &mpsc::Sender<Bytes>, chunk: Vec<u8>) -> bool {
    match tokio::time::timeout(ARRIVING_RESULT_STALL, chunks.send(chunk.into())).await {
        Ok(sent) => sent.is_ok(),
        Err(_) => {
            warn!("an actor result was not taken for 30 seconds, so the rest of it is dropped");
            false
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
        logs: Option<Arc<dyn ActorLogSink>>,
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

/// A message is a line of JSON, then the parts whose sizes the line announces.
async fn read_client_message(
    reader: &mut BufReader<OwnedReadHalf>,
) -> Result<Option<ActorExecutorClientMessage>> {
    let Some(mut message) = read_client_line(reader).await? else {
        return Ok(None);
    };
    read_parts(reader, &mut message).await?;
    Ok(Some(message))
}

async fn read_client_line(
    reader: &mut BufReader<OwnedReadHalf>,
) -> Result<Option<ActorExecutorClientMessage>> {
    let mut line = Vec::new();
    if reader.read_until(b'\n', &mut line).await? == 0 {
        return Ok(None);
    }
    serde_json::from_slice(trim_ascii_end(&line))
        .map(Some)
        .context("decode customer actor executor message")
}

async fn read_parts(
    reader: &mut BufReader<OwnedReadHalf>,
    message: &mut ActorExecutorClientMessage,
) -> Result<()> {
    let mut parts = Vec::new();
    for &size in message.part_sizes() {
        parts.push(read_part(reader, size).await?);
    }
    message
        .attach(parts)
        .context("decode customer actor executor message parts")
}

// The customer process chooses the size, so memory is claimed only as its bytes arrive.
async fn read_part(reader: &mut BufReader<OwnedReadHalf>, size: usize) -> Result<Vec<u8>> {
    let mut part = Vec::new();
    part.try_reserve_exact(size)
        .context("customer actor executor message part is too large")?;
    reader.take(size as u64).read_to_end(&mut part).await?;
    ensure!(
        part.len() == size,
        "customer actor executor disconnected inside a message"
    );
    Ok(part)
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

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ActorExecutorServerMessage<'a> {
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
        #[serde(default)]
        parts: Vec<usize>,
        #[serde(skip)]
        effects: Vec<ActorSocketEffect>,
    },
    Attach {
        protocol: u32,
        actor_names: Vec<String>,
    },
    Reply {
        message_id: u64,
        reply: ExecutorReply,
        #[serde(default)]
        parts: Vec<usize>,
    },
    Log {
        #[serde(default)]
        message_id: Option<u64>,
        level: LogLevel,
        message: String,
    },
}

impl ActorExecutorClientMessage {
    fn part_sizes(&self) -> &[usize] {
        match self {
            Self::Reply { parts, .. } | Self::SocketEffects { parts, .. } => parts,
            _ => &[],
        }
    }

    /// A large result of a call that is not interleaved, with no socket effects after it, is
    /// passed on as it arrives rather than read whole first.
    fn arriving_result(&mut self) -> Option<ArrivingResult> {
        let Self::Reply {
            reply:
                ExecutorReply::Invoked {
                    result,
                    sequence: None,
                    ..
                },
            parts,
            ..
        } = self
        else {
            return None;
        };
        let &[size, effects] = parts.as_slice() else {
            return None;
        };
        if size <= ARRIVING_BYTES || effects != NO_EFFECTS.len() {
            return None;
        }
        let (chunks, arriving) = mpsc::channel(ARRIVING_RESULT_CHUNKS);
        *result = JsonStream::new(ReceiverStream::new(arriving).map(Ok), size).into();
        Some(ArrivingResult { size, chunks })
    }

    /// Fills in what followed the line: the result of an invocation, then the socket effects.
    fn attach(&mut self, parts: Vec<Vec<u8>>) -> Result<()> {
        let mut parts = Parts(parts.into_iter());
        match self {
            Self::SocketEffects { effects, .. } => *effects = parts.effects()?,
            Self::Reply { reply, .. } => match reply {
                ExecutorReply::Invoked {
                    result, effects, ..
                } => {
                    *result = JsonText::from_vec(parts.next()?)?.into();
                    *effects = parts.effects()?;
                }
                ExecutorReply::WebsocketHandled { effects, .. } => *effects = parts.effects()?,
                _ => {}
            },
            _ => {}
        }
        parts.finish()
    }
}

struct Parts(std::vec::IntoIter<Vec<u8>>);

impl Parts {
    /// A text message is written without its text, which is the next part and so is never
    /// escaped into a JSON string.
    fn effects(&mut self) -> Result<Vec<ActorSocketEffect>> {
        let mut effects: Vec<Value> = self.json()?;
        for effect in &mut effects {
            let message = effect
                .get_mut("message")
                .filter(|message| message["type"] == "text");
            if let Some(message) = message {
                message["data"] = Value::String(self.text()?);
            }
        }
        Ok(serde_json::from_value(Value::Array(effects))?)
    }

    fn json<T: DeserializeOwned>(&mut self) -> Result<T> {
        Ok(serde_json::from_slice(&self.next()?)?)
    }

    fn text(&mut self) -> Result<String> {
        Ok(String::from_utf8(self.next()?)?)
    }

    fn next(&mut self) -> Result<Vec<u8>> {
        self.0.next().context("a part is missing")
    }

    fn finish(mut self) -> Result<()> {
        ensure!(self.0.next().is_none(), "a part is left over");
        Ok(())
    }
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

impl ExecutorCommand {
    fn request_id(&self) -> Option<&str> {
        match self {
            Self::Hydrate { request_id } | Self::WebsocketEvent { request_id, .. } => {
                Some(request_id)
            }
            Self::Invoke(invocation) => Some(&invocation.request_id),
            Self::Evict { .. } => None,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ExecutorReply {
    Hydrated,
    Invoked {
        #[serde(skip)]
        result: JsonPayload,
        #[serde(default)]
        sequence: Option<u64>,
        #[serde(skip)]
        effects: Vec<ActorSocketEffect>,
    },
    WebsocketHandled {
        #[serde(default)]
        sequence: Option<u64>,
        #[serde(skip)]
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
