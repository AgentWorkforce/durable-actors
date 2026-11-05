use std::{pin::Pin, sync::Arc, time::Duration};

use anyhow::Result;
use async_trait::async_trait;
use futures_util::{Stream, StreamExt, stream};
use tokio::sync::broadcast;

use crate::redis_client::Redis;

/// Each item is one OTLP export request (traces or logs) for the subscribed channel.
pub(crate) type LiveStream = Pin<Box<dyn Stream<Item = Vec<u8>> + Send>>;

#[async_trait]
pub(crate) trait LiveFeed: Send + Sync {
    async fn subscribe(&self, channel: String) -> Result<LiveStream>;
}

/// Receives what the collector's Redis exporter publishes.
pub(crate) struct RedisFeed {
    redis: Redis,
}

impl RedisFeed {
    pub(crate) fn new(redis: Redis) -> Self {
        Self { redis }
    }
}

#[async_trait]
impl LiveFeed for RedisFeed {
    async fn subscribe(&self, channel: String) -> Result<LiveStream> {
        let mut subscriber = self.redis.pubsub().await?;
        tokio::time::timeout(Duration::from_secs(5), subscriber.subscribe(&channel)).await??;
        Ok(Box::pin(
            subscriber
                .into_on_message()
                .map(|message| message.get_payload_bytes().to_vec()),
        ))
    }
}

#[derive(Clone)]
pub(crate) struct InProcessFeed {
    updates: broadcast::Sender<Arc<(String, Vec<u8>)>>,
}

impl Default for InProcessFeed {
    fn default() -> Self {
        Self {
            updates: broadcast::channel(256).0,
        }
    }
}

impl InProcessFeed {
    pub(crate) fn publish(&self, channel: String, payload: Vec<u8>) {
        let _ = self.updates.send(Arc::new((channel, payload)));
    }
}

#[async_trait]
impl LiveFeed for InProcessFeed {
    async fn subscribe(&self, channel: String) -> Result<LiveStream> {
        let updates = self.updates.subscribe();
        Ok(Box::pin(stream::unfold(
            (updates, channel),
            |(mut updates, channel)| async move {
                loop {
                    match updates.recv().await {
                        Ok(update) if update.0 == channel => {
                            let payload = update.1.clone();
                            return Some((payload, (updates, channel)));
                        }
                        Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => return None,
                    }
                }
            },
        )))
    }
}
