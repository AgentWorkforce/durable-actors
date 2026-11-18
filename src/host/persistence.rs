use super::{actor_database::ActorStorage, storage::HostStorage};
use crate::{
    state_transport::{SnapshotWriter, StateWrite},
    storage::WritePlan,
};
use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;

pub(super) struct ActorPersistence {
    storage: Arc<HostStorage>,
}

impl ActorPersistence {
    pub fn new(storage: Arc<HostStorage>) -> Arc<Self> {
        Arc::new(Self { storage })
    }
}

#[async_trait]
impl SnapshotWriter for ActorPersistence {
    async fn write_snapshot(
        &self,
        plan: &WritePlan,
        bytes: crate::payload::Payload,
    ) -> Result<StateWrite> {
        self.storage.ensure_authority()?;
        let attempt = self.storage.stop.clone().drop_guard();
        let result = self.storage.runtime.write_snapshot(plan, bytes).await?;
        self.storage.ensure_authority()?;
        attempt.disarm();
        Ok(result)
    }
}
