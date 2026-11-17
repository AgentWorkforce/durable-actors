//! Pub/Sub topics the control plane publishes to, with Application Default Credentials.

use anyhow::{Result, ensure};
use google_cloud_pubsub::{client::Publisher, model::Message};
use serde::Serialize;

pub(crate) struct Topic(Publisher);

impl Topic {
    pub(crate) async fn connect(name: &str) -> Result<Self> {
        ensure!(
            matches!(name.split('/').collect::<Vec<_>>().as_slice(),
            ["projects", project, "topics", topic] if !project.is_empty() && !topic.is_empty()),
            "topic must be projects/<project>/topics/<topic>"
        );
        Ok(Self(Publisher::builder(name).build().await?))
    }

    /// Publishes the message as JSON.
    pub(crate) async fn send(
        &self,
        message: &impl Serialize,
        attributes: &[(&str, &str)],
    ) -> Result<()> {
        self.0
            .publish(
                Message::new()
                    .set_data(serde_json::to_vec(message)?)
                    .set_attributes(attributes.iter().copied()),
            )
            .await?;
        Ok(())
    }
}
