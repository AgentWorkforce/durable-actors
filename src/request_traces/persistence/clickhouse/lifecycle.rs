use super::*;
use crate::{
    actor_lifecycle::{
        LifecycleEvent, LifecyclePersistence, STALE_AFTER_MS,
        residency::{RUN_LIMIT, ResidencyRun},
    },
    placement::{ActorResidency, ResidencyQuery},
};

#[async_trait]
impl LifecyclePersistence for ClickHouseTracePersistence {
    /// The collector stores the reports the control plane emits.
    async fn record(&self, _event: &LifecycleEvent) -> Result<()> {
        Ok(())
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

    /// One more run than the limit keeps, so truncation shows.
    async fn residency_runs(
        &self,
        project: &str,
        query: &ResidencyQuery,
    ) -> Result<Vec<ResidencyRun>> {
        let rows = tokio::time::timeout(
            Duration::from_secs(15),
            self.client
                .query(include_str!("residency.sql"))
                .param("project", project)
                .param("actor_name", &query.actor_name)
                .param("actor_id", query.actor_id.as_deref().unwrap_or_default())
                .param("from", query.from_ms)
                .param("to", query.to_ms)
                .param("stale", STALE_AFTER_MS)
                .param("limit", RUN_LIMIT + 1)
                .fetch_all::<RunRow>(),
        )
        .await??;
        Ok(rows.into_iter().map(ResidencyRun::from).collect())
    }
}

#[derive(Row, Deserialize)]
struct InventoryRow {
    event_json: String,
}

#[derive(Row, Deserialize)]
struct RunRow {
    actor_id: String,
    first_epoch: u64,
    first_sequence: u64,
    run_status: u8,
    started_at_ms: u64,
    last_at_ms: u64,
    last_stopped: bool,
}

impl From<RunRow> for ResidencyRun {
    fn from(row: RunRow) -> Self {
        Self {
            actor_id: row.actor_id,
            owner_epoch: row.first_epoch,
            sequence: row.first_sequence,
            status: match row.run_status {
                1 => ActorResidency::Live,
                2 => ActorResidency::Dormant,
                _ => ActorResidency::Unknown,
            },
            started_at_ms: row.started_at_ms,
            last_at_ms: row.last_at_ms,
            stopped: row.last_stopped,
        }
    }
}
