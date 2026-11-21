use super::*;

impl RuntimeStorage {
    pub(crate) async fn drain_activation(
        &self,
        actor: &ActorKey,
        host: &HostId,
        session: &str,
        deadline: tokio::time::Instant,
    ) -> Result<SessionCheckpoint> {
        let record = self
            .owned
            .lock()
            .unwrap()
            .get(actor.storage_key().as_str())
            .cloned()
            .context("actor is not locally activated")?;
        ensure!(
            record.lease.id == *host && record.lease.session_id == session,
            "actor ownership changed"
        );
        let snapshot = self.uploaded_head(&record)?;
        let progress = self.snapshots.finish(&record.stream()?, deadline).await?;
        Ok(SessionCheckpoint { snapshot, progress })
    }

    fn uploaded_head(&self, record: &Ownership) -> Result<Option<SnapshotRef>> {
        let mut snapshot = record.base.clone();
        if let Some(uploaded) = self.uploaded.lock().unwrap().get(&record.lease.session_id) {
            ensure!(
                uploaded.started == uploaded.completed,
                "snapshot uploads did not complete successfully"
            );
            advance(&mut snapshot, uploaded.latest.clone())?;
        }
        Ok(snapshot)
    }
}
