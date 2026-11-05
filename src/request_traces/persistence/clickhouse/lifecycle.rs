use super::*;
use crate::actor_lifecycle::{LifecycleEvent, LifecyclePersistence};

#[async_trait]
impl LifecyclePersistence for ClickHouseTracePersistence {
    async fn record(&self, event: &LifecycleEvent) -> Result<()> {
        self.send(event.clone())
    }

    async fn latest(&self, project: &str) -> Result<Vec<LifecycleEvent>> {
        let rows = tokio::time::timeout(
            Duration::from_secs(15),
            self.client
                .query(
                    "SELECT argMaxMerge(event) AS event_json FROM actor_inventory
             WHERE project_id = {project:String} GROUP BY actor_name, actor_id
             ORDER BY actor_name, actor_id",
                )
                .param("project", project)
                .fetch_all::<InventoryRow>(),
        )
        .await??;
        rows.into_iter()
            .map(|row| Ok(serde_json::from_str(&row.event_json)?))
            .collect()
    }

    async fn history(
        &self,
        project: &str,
        actor_name: &str,
        from: u64,
        to: u64,
    ) -> Result<Vec<LifecycleEvent>> {
        let rows = tokio::time::timeout(
            Duration::from_secs(15),
            self.client
                .query(include_str!("residency.sql"))
                .param("project", project)
                .param("actor_name", actor_name)
                .param("from", from)
                .param("to", to)
                .fetch_all::<InventoryRow>(),
        )
        .await??;
        rows.into_iter()
            .map(|row| Ok(serde_json::from_str(&row.event_json)?))
            .collect()
    }
}

#[derive(Row, Deserialize)]
struct InventoryRow {
    event_json: String,
}

#[derive(Row, Serialize)]
pub(super) struct LifecycleRow {
    project_id: String,
    actor_name: String,
    actor_id: String,
    event_id: String,
    owner_epoch: u64,
    sequence: u64,
    event_json: String,
}

impl LifecycleRow {
    pub(super) fn from_event(event: &LifecycleEvent) -> Result<Self> {
        Ok(Self {
            project_id: event.actor.project_id.clone(),
            actor_name: event.actor.actor_name.clone(),
            actor_id: event.actor.actor_id.clone(),
            event_id: event.event_id.clone(),
            owner_epoch: event.report.owner_epoch,
            sequence: event.report.sequence,
            event_json: serde_json::to_string(event)?,
        })
    }
}
