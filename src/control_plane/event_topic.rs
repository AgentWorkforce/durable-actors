//! Platform events published on a Pub/Sub topic for services outside the control plane, each in
//! the shape `observe/events` sends it.

use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result};
use async_trait::async_trait;
use aws_lc_rs::digest::{SHA256, digest};
use prost::Message;
use serde::Serialize;

use super::{event_history::RecordedEvent, observe_events::platform_event};
use crate::{
    platform_events::{PlatformEvent, PlatformEvents},
    pubsub::Topic,
    request_traces::otlp,
};

const PUBLISH_TIMEOUT: Duration = Duration::from_secs(5);

/// Also publishes what `recorded` records when a topic is named.
pub(super) async fn published<E: PlatformEvent>(
    recorded: Arc<dyn PlatformEvents<E>>,
    topic: Option<&str>,
) -> Result<Arc<dyn PlatformEvents<E>>> {
    let Some(topic) = topic else {
        return Ok(recorded);
    };
    let topic = Topic::connect(topic).await.context("events topic")?;
    Ok(Arc::new(PublishedEvents::new(recorded, Arc::new(topic))))
}

/// Records each event and publishes it. Like recording, publishing is best effort.
pub(super) struct PublishedEvents<E> {
    recorded: Arc<dyn PlatformEvents<E>>,
    topic: Arc<dyn EventTopic>,
}

#[async_trait]
pub(super) trait EventTopic: Send + Sync {
    async fn publish(&self, event: &TopicEvent) -> Result<()>;
}

/// One event as the topic carries it.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct TopicEvent {
    /// A hash of the stored record: the same each time one event is reported, so a subscriber
    /// can skip the repeats.
    pub event_id: String,
    pub project_id: String,
    #[serde(flatten)]
    pub event: RecordedEvent,
}

impl<E> PublishedEvents<E> {
    pub(super) fn new(recorded: Arc<dyn PlatformEvents<E>>, topic: Arc<dyn EventTopic>) -> Self {
        Self { recorded, topic }
    }

    fn publish(&self, event: TopicEvent) {
        let topic = self.topic.clone();
        tokio::spawn(async move {
            if let Err(error) = tokio::time::timeout(PUBLISH_TIMEOUT, topic.publish(&event))
                .await
                .map_err(anyhow::Error::from)
                .and_then(|result| result)
            {
                tracing::warn!(event_id = event.event_id, %error, "event publication failed");
            }
        });
    }
}

impl<E: PlatformEvent> PlatformEvents<E> for PublishedEvents<E> {
    fn record(&self, event: E) {
        if let Some(event) = TopicEvent::of(&event) {
            self.publish(event);
        }
        self.recorded.record(event);
    }
}

impl TopicEvent {
    /// The event as observers see it; none for a record the stream does not show.
    fn of(event: &impl PlatformEvent) -> Option<Self> {
        // No gateway: none of the events published here name the gateway that reports them.
        let record = event.to_record().export_request("");
        let event_id = otlp::hex(digest(&SHA256, &record.encode_to_vec()).as_ref());
        let logs = otlp::platform_logs(record);
        Some(Self {
            event_id,
            project_id: event.project_id().to_owned(),
            event: logs.iter().find_map(platform_event)?.into(),
        })
    }
}

#[async_trait]
impl EventTopic for Topic {
    async fn publish(&self, event: &TopicEvent) -> Result<()> {
        let attributes = [
            ("projectId", event.project_id.as_str()),
            ("eventId", event.event_id.as_str()),
            ("type", event.event.name),
        ];
        self.send(event, &attributes).await
    }
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/event_topic.rs"]
mod tests;
