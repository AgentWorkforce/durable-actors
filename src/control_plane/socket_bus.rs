use crate::{
    actor::{ActorKey, ActorSocketConnection, ActorSocketEffect},
    sockets::SocketRegistry,
};
use anyhow::{Context, Result, anyhow, ensure};
use async_trait::async_trait;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeSet, HashMap},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

#[async_trait]
pub(super) trait SocketBus: Send + Sync {
    fn registry(&self) -> &SocketRegistry;
    fn generation(&self) -> Option<u64>;
    /// Call before a socket for `actor` opens here.
    async fn watch(&self, actor: &ActorKey) -> Result<()>;
    async fn unwatch(&self, actor: &ActorKey);
    async fn publish(&self, actor: &ActorKey, effects: Vec<ActorSocketEffect>) -> Result<()>;
    async fn connections(
        &self,
        actor: &ActorKey,
        tag: Option<&str>,
    ) -> Result<Vec<ActorSocketConnection>>;
    async fn count(&self, actor: &ActorKey) -> Result<usize>;
}

#[derive(Clone, Default)]
pub(super) struct MemorySocketHub {
    gateways: Arc<Mutex<Vec<SocketRegistry>>>,
}

impl MemorySocketHub {
    pub(super) fn join(&self, max_connections: usize) -> Arc<dyn SocketBus> {
        let registry = SocketRegistry::with_max_connections(max_connections);
        self.gateways.lock().unwrap().push(registry.clone());
        Arc::new(MemorySocketBus {
            hub: self.clone(),
            registry,
        })
    }

    fn gateways(&self) -> Vec<SocketRegistry> {
        self.gateways.lock().unwrap().clone()
    }
}

struct MemorySocketBus {
    hub: MemorySocketHub,
    registry: SocketRegistry,
}

#[async_trait]
impl SocketBus for MemorySocketBus {
    fn registry(&self) -> &SocketRegistry {
        &self.registry
    }

    fn generation(&self) -> Option<u64> {
        Some(0)
    }

    async fn watch(&self, _: &ActorKey) -> Result<()> {
        Ok(())
    }

    async fn unwatch(&self, _: &ActorKey) {}

    async fn publish(&self, actor: &ActorKey, effects: Vec<ActorSocketEffect>) -> Result<()> {
        for gateway in self.hub.gateways() {
            gateway.apply(actor, effects.clone()).await;
        }
        Ok(())
    }

    async fn connections(
        &self,
        actor: &ActorKey,
        tag: Option<&str>,
    ) -> Result<Vec<ActorSocketConnection>> {
        let mut connections = Vec::new();
        for gateway in self.hub.gateways() {
            connections.extend(gateway.connections_with_tag(actor, tag).await);
        }
        Ok(connections)
    }

    async fn count(&self, actor: &ActorKey) -> Result<usize> {
        let mut count = 0;
        for gateway in self.hub.gateways() {
            count += gateway.count(actor).await;
        }
        Ok(count)
    }
}

const ANSWER_TIMEOUT: Duration = Duration::from_secs(2);
const SUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(5);
const AUTO_RESPONSE_TTL: Duration = Duration::from_secs(7 * 24 * 3600);
// Redis disconnects a subscriber once 8 MB waits for it, and still counts the 60 seconds it allows
// after the subscriber has caught up, so a second such message a minute later ends the subscription.
const PUBLISHED_EFFECTS_BYTES: usize = 64 * 1024;
const STORED_EFFECTS_TTL: Duration = Duration::from_secs(10);
// Shorter than SUBSCRIBE_TIMEOUT: the listener reads stored effects before it takes a new subscription.
const STORED_EFFECTS_TIMEOUT: Duration = Duration::from_secs(3);
const WAITING_EFFECTS_BYTES: usize = 64 * 1024 * 1024;

pub(super) struct RedisSocketBus {
    shared: Arc<Shared>,
    commands: mpsc::UnboundedSender<Command>,
    watched: Mutex<HashMap<ActorKey, usize>>,
}

struct Shared {
    redis: crate::redis_client::Redis,
    stored: StoredEffects,
    registry: SocketRegistry,
    namespace: String,
    replies: String,
    connected: AtomicBool,
    generation: AtomicU64,
    pending: Mutex<HashMap<String, mpsc::UnboundedSender<Answer>>>,
    subscribed: Mutex<Vec<ActorKey>>,
}

enum Command {
    Subscribe {
        actor: ActorKey,
        load: bool,
        done: oneshot::Sender<Result<()>>,
    },
    Unsubscribe(String),
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Envelope {
    Effects {
        actor: ActorKey,
        effects: Vec<ActorSocketEffect>,
    },
    StoredEffects {
        actor: ActorKey,
        key: String,
        /// The sockets the effects are for; `None` when they concern every socket of the actor.
        connection_ids: Option<BTreeSet<String>>,
    },
    Question {
        id: String,
        reply_to: String,
        question: Question,
    },
    Answer {
        id: String,
        answer: Answer,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Question {
    Count {
        actor: ActorKey,
    },
    Connections {
        actor: ActorKey,
        tag: Option<String>,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Answer {
    Count {
        count: usize,
    },
    Connections {
        connections: Vec<ActorSocketConnection>,
    },
}

#[derive(Serialize, Deserialize)]
struct AutoResponse {
    request: Option<String>,
    response: Option<String>,
}

impl RedisSocketBus {
    pub(super) fn start(
        redis: crate::redis_client::Redis,
        namespace: &str,
        max_connections: usize,
        accepts_sockets: bool,
        stop: CancellationToken,
    ) -> Result<Arc<dyn SocketBus>> {
        let shared = Arc::new(Shared {
            stored: StoredEffects::new(&redis, namespace)?,
            redis,
            registry: SocketRegistry::with_max_connections(max_connections),
            namespace: namespace.to_owned(),
            replies: format!("{namespace}:replies:{}", uuid::Uuid::new_v4()),
            connected: AtomicBool::new(false),
            generation: AtomicU64::new(0),
            pending: Mutex::default(),
            subscribed: Mutex::default(),
        });
        let (commands, receiver) = mpsc::unbounded_channel();
        tokio::spawn(shared.clone().listen(receiver, accepts_sockets, stop));
        Ok(Arc::new(Self {
            shared,
            commands,
            watched: Mutex::default(),
        }))
    }

    async fn ask(&self, channel: &str, question: Question) -> Result<Vec<Answer>> {
        let (answers, complete) = self.ask_partial(channel, question).await?;
        ensure!(complete, "a socket gateway did not answer");
        Ok(answers)
    }

    async fn ask_partial(&self, channel: &str, question: Question) -> Result<(Vec<Answer>, bool)> {
        ensure!(
            self.shared.connected.load(Ordering::SeqCst),
            "socket bus is reconnecting"
        );
        let id = uuid::Uuid::new_v4().to_string();
        let (sender, mut answers) = mpsc::unbounded_channel();
        self.shared
            .pending
            .lock()
            .unwrap()
            .insert(id.clone(), sender);
        let asked = async {
            let receivers = self
                .shared
                .publish(
                    channel,
                    &Envelope::Question {
                        id: id.clone(),
                        reply_to: self.shared.replies.clone(),
                        question,
                    },
                )
                .await?;
            let mut received = Vec::new();
            let deadline = tokio::time::Instant::now() + ANSWER_TIMEOUT;
            while received.len() < receivers {
                match tokio::time::timeout_at(deadline, answers.recv()).await {
                    Ok(Some(answer)) => received.push(answer),
                    _ => break,
                }
            }
            let complete = received.len() == receivers;
            Ok((received, complete))
        }
        .await;
        self.shared.pending.lock().unwrap().remove(&id);
        asked
    }
}

impl Shared {
    fn actor_channel(&self, actor: &ActorKey) -> String {
        format!("{}:actor:{}", self.namespace, actor.storage_key().as_str())
    }

    fn gateways_channel(&self) -> String {
        format!("{}:gateways", self.namespace)
    }

    fn auto_response_key(&self, actor: &ActorKey) -> String {
        format!(
            "{}:auto-response:{}",
            self.namespace,
            actor.storage_key().as_str()
        )
    }

    async fn publish_effects(
        &self,
        actor: &ActorKey,
        effects: Vec<ActorSocketEffect>,
    ) -> Result<()> {
        let channel = self.actor_channel(actor);
        let connection_ids = connection_ids(&effects);
        let message = serde_json::to_string(&Envelope::Effects {
            actor: actor.clone(),
            effects,
        })?;
        if message.len() <= PUBLISHED_EFFECTS_BYTES {
            self.send(&channel, message).await?;
            return Ok(());
        }
        let commands = self
            .stored
            .publication(&channel, actor, message, connection_ids)?;
        self.redis.run_all(commands).await
    }

    async fn publish(&self, channel: &str, envelope: &Envelope) -> Result<usize> {
        self.send(channel, serde_json::to_string(envelope)?).await
    }

    async fn send(&self, channel: &str, message: String) -> Result<usize> {
        let mut command = redis::cmd("PUBLISH");
        command.arg(channel).arg(message);
        self.redis.run::<usize>(command).await
    }

    // Runs on the listener after subscribing, so changes published meanwhile apply after it.
    async fn load_auto_response(&self, actor: &ActorKey) -> Result<()> {
        let mut get = redis::cmd("GET");
        get.arg(self.auto_response_key(actor));
        let AutoResponse { request, response } = match self.redis.run::<Option<String>>(get).await?
        {
            Some(stored) => serde_json::from_str(&stored)?,
            None => AutoResponse {
                request: None,
                response: None,
            },
        };
        self.registry
            .apply(
                actor,
                vec![ActorSocketEffect::SetAutoResponse { request, response }],
            )
            .await;
        Ok(())
    }

    /// Each reconnect starts a new generation, closing sockets that may have missed publishes.
    async fn listen(
        self: Arc<Self>,
        mut commands: mpsc::UnboundedReceiver<Command>,
        accepts_sockets: bool,
        stop: CancellationToken,
    ) {
        loop {
            let result = self
                .clone()
                .session(&mut commands, accepts_sockets, &stop)
                .await;
            self.connected.store(false, Ordering::SeqCst);
            match result {
                Ok(()) => return,
                Err(error) => tracing::warn!(error = %format!("{error:#}"),
                    "socket bus lost its Redis subscription; its sockets close and it reconnects"),
            }
            self.registry
                .close_all(1012, "socket gateway lost its subscription")
                .await;
            tokio::select! {
                _ = stop.cancelled() => return,
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
        }
    }

    async fn session(
        self: Arc<Self>,
        commands: &mut mpsc::UnboundedReceiver<Command>,
        accepts_sockets: bool,
        stop: &CancellationToken,
    ) -> Result<()> {
        let (mut sink, mut messages) = self.redis.pubsub().await?.split();
        sink.subscribe(&self.replies).await?;
        if accepts_sockets {
            sink.subscribe(self.gateways_channel()).await?;
        }
        let resubscribe = self.subscribed.lock().unwrap().clone();
        for actor in resubscribe {
            sink.subscribe(self.actor_channel(&actor)).await?;
            self.load_auto_response(&actor).await?;
        }
        self.generation.fetch_add(1, Ordering::SeqCst);
        self.connected.store(true, Ordering::SeqCst);
        loop {
            tokio::select! {
                _ = stop.cancelled() => return Ok(()),
                message = messages.next() => {
                    let message = message.context("the Redis pub/sub connection ended")?;
                    self.clone().receive(message.get_payload_bytes()).await;
                }
                command = commands.recv() => match command {
                    None => return Ok(()),
                    Some(Command::Subscribe { actor, load, done }) => {
                        let channel = self.actor_channel(&actor);
                        if let Err(error) = sink.subscribe(&channel).await {
                            let _ = done.send(Err(error.into()));
                            anyhow::bail!("subscribe to {channel} failed");
                        }
                        let loaded = match load {
                            true => self.load_auto_response(&actor).await,
                            false => Ok(()),
                        };
                        let _ = done.send(loaded);
                    }
                    Some(Command::Unsubscribe(channel)) => sink.unsubscribe(&channel).await?,
                },
            }
        }
    }

    async fn receive(self: Arc<Self>, payload: &[u8]) {
        let envelope = match serde_json::from_slice::<Envelope>(payload) {
            Ok(envelope) => envelope,
            Err(error) => {
                tracing::warn!(%error, "ignored an unreadable socket bus message");
                return;
            }
        };
        match envelope {
            // In order, on the listener: an actor's effects apply as published.
            Envelope::Effects { actor, effects } => self.registry.apply(&actor, effects).await,
            Envelope::StoredEffects {
                actor,
                key,
                connection_ids,
            } => self.receive_stored(&actor, &key, connection_ids).await,
            Envelope::Question {
                id,
                reply_to,
                question,
            } => {
                tokio::spawn(async move {
                    let answer = match question {
                        Question::Count { actor } => Answer::Count {
                            count: self.registry.count(&actor).await,
                        },
                        Question::Connections { actor, tag } => Answer::Connections {
                            connections: self
                                .registry
                                .connections_with_tag(&actor, tag.as_deref())
                                .await,
                        },
                    };
                    if let Err(error) = self
                        .publish(&reply_to, &Envelope::Answer { id, answer })
                        .await
                    {
                        tracing::warn!(error = %format!("{error:#}"), "socket bus answer failed");
                    }
                });
            }
            Envelope::Answer { id, answer } => {
                if let Some(waiting) = self.pending.lock().unwrap().get(&id) {
                    let _ = waiting.send(answer);
                }
            }
        }
    }

    async fn receive_stored(
        &self,
        actor: &ActorKey,
        key: &str,
        connection_ids: Option<BTreeSet<String>>,
    ) {
        if let Some(ids) = &connection_ids
            && !self.registry.holds_any(actor, ids).await
        {
            return;
        }
        match self.stored.read(key, read_once(&connection_ids)).await {
            Ok(effects) => self.registry.apply(actor, effects).await,
            Err(error) => {
                tracing::warn!(actor = %actor.storage_key().as_str(), error = %format!("{error:#}"),
                    "stored socket effects could not be read; the sockets they were for close");
                let reason = "socket gateway missed a message";
                self.registry
                    .close_actor(actor, connection_ids.as_ref(), 1012, reason)
                    .await;
            }
        }
    }
}

/// Effects too large to publish wait in Redis for the gateways that hold their sockets.
struct StoredEffects {
    redis: crate::redis_client::Redis,
    namespace: String,
    waiting: Arc<Semaphore>,
}

impl StoredEffects {
    fn new(redis: &crate::redis_client::Redis, namespace: &str) -> Result<Self> {
        Ok(Self {
            redis: redis.with_timeout(STORED_EFFECTS_TIMEOUT)?,
            namespace: namespace.to_owned(),
            waiting: Arc::new(Semaphore::new(WAITING_EFFECTS_BYTES / 1024)),
        })
    }

    /// The commands that store `message` and then publish where it is.
    fn publication(
        &self,
        channel: &str,
        actor: &ActorKey,
        message: String,
        connection_ids: Option<BTreeSet<String>>,
    ) -> Result<redis::Pipeline> {
        if !read_once(&connection_ids) {
            self.reserve(message.len())?;
        }
        let key = format!("{}:effects:{}", self.namespace, uuid::Uuid::new_v4());
        let stored = serde_json::to_string(&Envelope::StoredEffects {
            actor: actor.clone(),
            key: key.clone(),
            connection_ids,
        })?;
        let mut commands = redis::pipe();
        commands
            .cmd("SET")
            .arg(key)
            .arg(message)
            .arg("PX")
            .arg(STORED_EFFECTS_TTL.as_millis() as u64)
            .ignore();
        commands.cmd("PUBLISH").arg(channel).arg(stored).ignore();
        Ok(commands)
    }

    /// Effects read by several gateways stay until they expire, so a gateway limits how much it leaves.
    fn reserve(&self, bytes: usize) -> Result<()> {
        let kibibytes = u32::try_from(bytes.div_ceil(1024))?;
        let reserved = self
            .waiting
            .clone()
            .try_acquire_many_owned(kibibytes)
            .map_err(|_| anyhow!("too many large socket effects are waiting in Redis"))?;
        tokio::spawn(async move {
            tokio::time::sleep(STORED_EFFECTS_TTL).await;
            drop(reserved);
        });
        Ok(())
    }

    async fn read(&self, key: &str, once: bool) -> Result<Vec<ActorSocketEffect>> {
        let mut command = redis::cmd(if once { "GETDEL" } else { "GET" });
        command.arg(key);
        let stored = self
            .redis
            .run::<Option<Vec<u8>>>(command)
            .await?
            .context("the effects expired")?;
        match serde_json::from_slice(&stored)? {
            Envelope::Effects { effects, .. } => Ok(effects),
            _ => anyhow::bail!("the stored message is not socket effects"),
        }
    }
}

fn connection_ids(effects: &[ActorSocketEffect]) -> Option<BTreeSet<String>> {
    effects
        .iter()
        .map(|effect| effect.connection_id().map(str::to_owned))
        .collect()
}

/// One gateway holds a socket, so effects for a single socket are read once and deleted as they are read.
fn read_once(connection_ids: &Option<BTreeSet<String>>) -> bool {
    connection_ids.as_ref().is_some_and(|ids| ids.len() == 1)
}

#[async_trait]
impl SocketBus for RedisSocketBus {
    fn registry(&self) -> &SocketRegistry {
        &self.shared.registry
    }

    fn generation(&self) -> Option<u64> {
        self.shared
            .connected
            .load(Ordering::SeqCst)
            .then(|| self.shared.generation.load(Ordering::SeqCst))
    }

    async fn watch(&self, actor: &ActorKey) -> Result<()> {
        ensure!(
            self.shared.connected.load(Ordering::SeqCst),
            "socket bus is reconnecting"
        );
        let (done, subscribed) = oneshot::channel();
        {
            // Sent under the lock, so commands reach Redis in the order the counts changed.
            let mut watched = self.watched.lock().unwrap();
            let sockets = watched.entry(actor.clone()).or_default();
            *sockets += 1;
            let load = *sockets == 1;
            if load {
                self.shared.subscribed.lock().unwrap().push(actor.clone());
            }
            // Each watch waits for its own subscribe, even one that arrives while another is in flight.
            let _ = self.commands.send(Command::Subscribe {
                actor: actor.clone(),
                load,
                done,
            });
        }
        let subscribed = tokio::time::timeout(SUBSCRIBE_TIMEOUT, subscribed).await;
        if !matches!(subscribed, Ok(Ok(Ok(())))) {
            self.unwatch(actor).await;
            anyhow::bail!("socket bus could not subscribe to the actor");
        }
        Ok(())
    }

    async fn unwatch(&self, actor: &ActorKey) {
        let mut watched = self.watched.lock().unwrap();
        let Some(sockets) = watched.get_mut(actor) else {
            return;
        };
        *sockets -= 1;
        if *sockets == 0 {
            watched.remove(actor);
            self.shared
                .subscribed
                .lock()
                .unwrap()
                .retain(|subscribed| subscribed != actor);
            let _ = self
                .commands
                .send(Command::Unsubscribe(self.shared.actor_channel(actor)));
        }
    }

    async fn publish(&self, actor: &ActorKey, effects: Vec<ActorSocketEffect>) -> Result<()> {
        if let Some((request, response)) = effects.iter().rev().find_map(|effect| match effect {
            ActorSocketEffect::SetAutoResponse { request, response } => {
                Some((request.clone(), response.clone()))
            }
            _ => None,
        }) {
            let key = self.shared.auto_response_key(actor);
            let command = if request.is_some() && response.is_some() {
                let mut set = redis::cmd("SET");
                set.arg(key)
                    .arg(serde_json::to_string(&AutoResponse { request, response })?)
                    .arg("PX")
                    .arg(AUTO_RESPONSE_TTL.as_millis() as u64);
                set
            } else {
                let mut delete = redis::cmd("DEL");
                delete.arg(key);
                delete
            };
            self.shared.redis.run::<redis::Value>(command).await?;
        }
        self.shared.publish_effects(actor, effects).await
    }

    async fn connections(
        &self,
        actor: &ActorKey,
        tag: Option<&str>,
    ) -> Result<Vec<ActorSocketConnection>> {
        let answers = self
            .ask(
                &self.shared.actor_channel(actor),
                Question::Connections {
                    actor: actor.clone(),
                    tag: tag.map(str::to_owned),
                },
            )
            .await?;
        Ok(answers
            .into_iter()
            .flat_map(|answer| match answer {
                Answer::Connections { connections } => connections,
                _ => vec![],
            })
            .collect())
    }

    async fn count(&self, actor: &ActorKey) -> Result<usize> {
        let answers = self
            .ask(
                &self.shared.actor_channel(actor),
                Question::Count {
                    actor: actor.clone(),
                },
            )
            .await?;
        Ok(answers
            .into_iter()
            .map(|answer| match answer {
                Answer::Count { count } => count,
                _ => 0,
            })
            .sum())
    }
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/socket_bus.rs"]
mod tests;
