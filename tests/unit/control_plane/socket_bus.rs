use super::*;
use crate::{
    actor::{ActorSocketMessage, ActorSocketTagMatch},
    sockets::{OutboundMessage, SocketReceiver, socket_channel},
};

fn actor(id: &str) -> ActorKey {
    ActorKey {
        project_id: "p".into(),
        actor_name: "Room".into(),
        actor_id: id.into(),
    }
}

async fn open(
    registry: &SocketRegistry,
    actor: &ActorKey,
    id: &str,
    tags: &[&str],
) -> SocketReceiver {
    let (sender, receiver) = socket_channel();
    let connection = ActorSocketConnection {
        id: id.into(),
        metadata: serde_json::json!({ "socket": id }),
        tags: tags.iter().map(|tag| (*tag).to_owned()).collect(),
    };
    assert!(registry.insert(actor, connection, sender).await);
    registry.activate(actor, id).await;
    receiver
}

fn broadcast(data: &str) -> Vec<ActorSocketEffect> {
    vec![ActorSocketEffect::Broadcast {
        message: ActorSocketMessage::Text { data: data.into() },
        except_connection_ids: vec![],
        tags: vec![],
        tag_match: ActorSocketTagMatch::default(),
    }]
}

fn send(connection_id: &str, data: &str) -> Vec<ActorSocketEffect> {
    vec![ActorSocketEffect::Send {
        connection_id: connection_id.into(),
        message: ActorSocketMessage::Text { data: data.into() },
    }]
}

async fn outbound(receiver: &mut SocketReceiver) -> Result<OutboundMessage> {
    tokio::time::timeout(Duration::from_secs(5), receiver.recv())
        .await?
        .context("socket closed")
}

async fn received(receiver: &mut SocketReceiver) -> Result<String> {
    match outbound(receiver).await? {
        OutboundMessage::Message(ActorSocketMessage::Text { data }) => Ok(data),
        _ => anyhow::bail!("unexpected outbound message"),
    }
}

async fn closed(receiver: &mut SocketReceiver) -> Result<u16> {
    match outbound(receiver).await? {
        OutboundMessage::Close { code, .. } => Ok(code),
        _ => anyhow::bail!("unexpected outbound message"),
    }
}

#[tokio::test]
async fn memory_publishes_reach_every_gateway_holding_the_actors_sockets() -> Result<()> {
    let hub = MemorySocketHub::default();
    let (first, second) = (hub.join(10), hub.join(10));
    let room = actor("one");
    let mut a = open(first.registry(), &room, "a", &["red"]).await;
    let mut b = open(second.registry(), &room, "b", &[]).await;
    second.publish(&room, broadcast("hello")).await?;
    assert_eq!(received(&mut a).await?, "hello");
    assert_eq!(received(&mut b).await?, "hello");
    assert_eq!(first.count(&room).await?, 2);
    let red = first.connections(&room, Some("red")).await?;
    assert_eq!(red.iter().map(|c| c.id.as_str()).collect::<Vec<_>>(), ["a"]);
    Ok(())
}

async fn redis_bus(
    url: &str,
    namespace: &str,
    accepts_sockets: bool,
) -> Result<Arc<dyn SocketBus>> {
    let bus = RedisSocketBus::start(
        redis(url)?,
        namespace,
        10,
        accepts_sockets,
        CancellationToken::new(),
    )?;
    connected(&bus).await?;
    Ok(bus)
}

fn redis(url: &str) -> Result<crate::redis_client::Redis> {
    crate::redis_client::Redis::connect(&crate::redis_client::RedisConfig {
        url: url.to_owned(),
        ca_cert: None,
    })
}

fn actor_channel(namespace: &str, actor: &ActorKey) -> String {
    format!("{namespace}:actor:{}", actor.storage_key().as_str())
}

async fn connected(bus: &Arc<dyn SocketBus>) -> Result<u64> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(generation) = bus.generation() {
                return generation;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .context("socket bus did not connect")
}

fn test_redis() -> Option<(String, String)> {
    let url = std::env::var("DURABLE_ACTORS_TEST_REDIS_URL").ok()?;
    Some((url, format!("test:{}", uuid::Uuid::new_v4())))
}

#[tokio::test]
async fn redis_publishes_reach_every_gateway_holding_the_actors_sockets() -> Result<()> {
    let Some((url, namespace)) = test_redis() else {
        return Ok(());
    };
    let first = redis_bus(&url, &namespace, true).await?;
    let second = redis_bus(&url, &namespace, true).await?;
    let control = redis_bus(&url, &namespace, false).await?;
    let room = actor("one");
    first.watch(&room).await?;
    second.watch(&room).await?;
    let mut a = open(first.registry(), &room, "a", &["red"]).await;
    let mut b = open(second.registry(), &room, "b", &[]).await;

    control.publish(&room, broadcast("hello")).await?;
    assert_eq!(received(&mut a).await?, "hello");
    assert_eq!(received(&mut b).await?, "hello");
    assert_eq!(control.count(&room).await?, 2);
    let red = control.connections(&room, Some("red")).await?;
    assert_eq!(red.iter().map(|c| c.id.as_str()).collect::<Vec<_>>(), ["a"]);

    second.registry().remove(&room, "b").await;
    second.unwatch(&room).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(control.count(&room).await?, 1);
    assert_eq!(control.count(&actor("nobody")).await?, 0);
    Ok(())
}

#[tokio::test]
async fn redis_auto_responses_reach_gateways_that_open_sockets_later() -> Result<()> {
    let Some((url, namespace)) = test_redis() else {
        return Ok(());
    };
    let control = redis_bus(&url, &namespace, false).await?;
    let gateway = redis_bus(&url, &namespace, true).await?;
    let room = actor("one");
    control
        .publish(
            &room,
            vec![ActorSocketEffect::SetAutoResponse {
                request: Some("ping".into()),
                response: Some("pong".into()),
            }],
        )
        .await?;
    gateway.watch(&room).await?;
    assert_eq!(
        gateway
            .registry()
            .auto_response(&room, "ping")
            .await
            .as_deref(),
        Some("pong")
    );
    Ok(())
}

#[tokio::test]
async fn redis_auto_responses_cleared_while_unwatched_do_not_return() -> Result<()> {
    let Some((url, namespace)) = test_redis() else {
        return Ok(());
    };
    let control = redis_bus(&url, &namespace, false).await?;
    let gateway = redis_bus(&url, &namespace, true).await?;
    let room = actor("one");
    let set = |pair: Option<(&str, &str)>| {
        vec![ActorSocketEffect::SetAutoResponse {
            request: pair.map(|(request, _)| request.into()),
            response: pair.map(|(_, response)| response.into()),
        }]
    };
    control.publish(&room, set(Some(("ping", "pong")))).await?;
    gateway.watch(&room).await?;
    gateway.unwatch(&room).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    control.publish(&room, set(None)).await?;
    gateway.watch(&room).await?;
    assert_eq!(gateway.registry().auto_response(&room, "ping").await, None);
    Ok(())
}

#[tokio::test]
async fn redis_questions_fail_when_a_gateway_does_not_answer() -> Result<()> {
    let Some((url, namespace)) = test_redis() else {
        return Ok(());
    };
    let control = redis_bus(&url, &namespace, false).await?;
    let room = actor("one");
    let mut silent = redis(&url)?.pubsub().await?;
    silent.subscribe(format!("{namespace}:gateways")).await?;
    silent.subscribe(actor_channel(&namespace, &room)).await?;
    assert!(control.count(&room).await.is_err());
    Ok(())
}

#[tokio::test]
async fn redis_large_effects_reach_every_gateway_in_order_without_being_published() -> Result<()> {
    let Some((url, namespace)) = test_redis() else {
        return Ok(());
    };
    let first = redis_bus(&url, &namespace, true).await?;
    let second = redis_bus(&url, &namespace, true).await?;
    let room = actor("one");
    first.watch(&room).await?;
    second.watch(&room).await?;
    let mut a = open(first.registry(), &room, "a", &[]).await;
    let mut b = open(second.registry(), &room, "b", &[]).await;
    let mut published = redis(&url)?.pubsub().await?;
    published
        .subscribe(actor_channel(&namespace, &room))
        .await?;
    let mut published = published.into_on_message();
    let large = "x".repeat(9 * 1024 * 1024);

    for data in ["before", &large, "after"] {
        first.publish(&room, broadcast(data)).await?;
    }
    for socket in [&mut a, &mut b] {
        assert_eq!(received(socket).await?, "before");
        assert!(received(socket).await? == large);
        assert_eq!(received(socket).await?, "after");
    }
    for _ in 0..3 {
        let message = tokio::time::timeout(Duration::from_secs(5), published.next()).await?;
        let bytes = message.context("published")?.get_payload_bytes().len();
        assert!(bytes <= PUBLISHED_EFFECTS_BYTES, "published {bytes} bytes");
    }
    Ok(())
}

#[tokio::test]
async fn redis_large_effects_for_one_socket_are_read_once_by_the_gateway_holding_it() -> Result<()>
{
    let Some((url, namespace)) = test_redis() else {
        return Ok(());
    };
    let first = redis_bus(&url, &namespace, true).await?;
    let second = redis_bus(&url, &namespace, true).await?;
    let room = actor("one");
    first.watch(&room).await?;
    second.watch(&room).await?;
    let mut a = open(first.registry(), &room, "a", &[]).await;
    let mut b = open(second.registry(), &room, "b", &[]).await;
    let large = "x".repeat(PUBLISHED_EFFECTS_BYTES);

    second.publish(&room, send("a", &large)).await?;
    second.publish(&room, broadcast("after")).await?;
    assert!(received(&mut a).await? == large);
    assert_eq!(received(&mut a).await?, "after");
    assert_eq!(received(&mut b).await?, "after");
    let mut stored = redis::cmd("KEYS");
    stored.arg(format!("{namespace}:effects:*"));
    assert!(redis(&url)?.run::<Vec<String>>(stored).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn redis_stored_effects_that_are_gone_close_only_the_sockets_they_were_for() -> Result<()> {
    let Some((url, namespace)) = test_redis() else {
        return Ok(());
    };
    let gateway = redis_bus(&url, &namespace, true).await?;
    let (room, other) = (actor("one"), actor("two"));
    gateway.watch(&room).await?;
    gateway.watch(&other).await?;
    let mut a = open(gateway.registry(), &room, "a", &[]).await;
    let mut b = open(gateway.registry(), &room, "b", &[]).await;
    let mut c = open(gateway.registry(), &other, "c", &[]).await;
    let gone = |connection_ids: Option<&str>| {
        let mut publish = redis::cmd("PUBLISH");
        publish
            .arg(actor_channel(&namespace, &room))
            .arg(serde_json::to_string(&Envelope::StoredEffects {
                actor: room.clone(),
                key: format!("{namespace}:effects:gone"),
                connection_ids: connection_ids.map(|id| BTreeSet::from([id.to_owned()])),
            })?);
        anyhow::Ok(publish)
    };
    let redis = redis(&url)?;

    redis.run::<usize>(gone(Some("a"))?).await?;
    gateway.publish(&room, broadcast("still open")).await?;
    assert_eq!(closed(&mut a).await?, 1012);
    assert_eq!(received(&mut b).await?, "still open");

    redis.run::<usize>(gone(None)?).await?;
    gateway.publish(&other, broadcast("still open")).await?;
    assert_eq!(closed(&mut b).await?, 1012);
    assert_eq!(received(&mut c).await?, "still open");
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn stored_effects_for_several_gateways_are_limited_until_they_expire() -> Result<()> {
    let stored = StoredEffects::new(&redis("redis://127.0.0.1:1")?, "test")?;
    let half = "x".repeat(WAITING_EFFECTS_BYTES / 2);
    let store = |connection_ids| {
        stored
            .publication("channel", &actor("one"), half.clone(), connection_ids)
            .is_ok()
    };

    assert!(store(None) && store(None));
    assert!(!store(None));
    assert!(store(Some(BTreeSet::from(["a".to_owned()]))));
    tokio::time::sleep(STORED_EFFECTS_TTL + Duration::from_millis(1)).await;
    assert!(store(None));
    Ok(())
}

struct Proxy {
    address: std::net::SocketAddr,
    connections: Arc<Mutex<Vec<tokio::task::AbortHandle>>>,
}

impl Proxy {
    async fn start(target: String) -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let connections: Arc<Mutex<Vec<tokio::task::AbortHandle>>> = Arc::default();
        let tracked = connections.clone();
        tokio::spawn(async move {
            while let Ok((mut inbound, _)) = listener.accept().await {
                let target = target.clone();
                let task = tokio::spawn(async move {
                    if let Ok(mut outbound) = tokio::net::TcpStream::connect(target).await {
                        let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                    }
                });
                tracked.lock().unwrap().push(task.abort_handle());
            }
        });
        Ok(Self {
            address,
            connections,
        })
    }

    fn cut(&self) {
        for connection in self.connections.lock().unwrap().drain(..) {
            connection.abort();
        }
    }
}

#[tokio::test]
async fn redis_a_lost_subscription_starts_a_new_generation() -> Result<()> {
    let Some((url, namespace)) = test_redis() else {
        return Ok(());
    };
    let target = url
        .strip_prefix("redis://")
        .context("test Redis URL must be redis://host:port")?
        .trim_end_matches('/')
        .to_owned();
    let proxy = Proxy::start(target).await?;
    let gateway = redis_bus(&format!("redis://{}", proxy.address), &namespace, true).await?;
    let before = gateway.generation().context("connected")?;
    proxy.cut();
    tokio::time::timeout(Duration::from_secs(5), async {
        while gateway.generation() == Some(before) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .context("the bus did not notice its lost subscription")?;
    let after = connected(&gateway).await?;
    assert!(after > before);
    Ok(())
}
