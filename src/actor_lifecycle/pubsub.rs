use anyhow::Result;
use async_trait::async_trait;

use super::{UsagePublisher, UsageUpdate};
use crate::pubsub::Topic;

#[async_trait]
impl UsagePublisher for Topic {
    async fn publish(&self, event: &UsageUpdate) -> Result<()> {
        let mut attributes = vec![
            ("projectId", event.event.actor.project_id.as_str()),
            ("eventId", event.event.event_id.as_str()),
        ];
        if let Some(organization) = &event.event.organization_id {
            attributes.push(("organizationId", organization));
        }
        self.send(event, &attributes).await
    }
}
