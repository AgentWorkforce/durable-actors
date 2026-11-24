use futures_util::{StreamExt, stream};
use tokio_util::sync::CancellationToken;

use super::*;

/// Long enough that an actor with traffic is reactivated, and recovered, by its next host first.
const ABANDONED_AFTER_MS: u64 = 10 * 60 * 1000;
const RECOVERY_INTERVAL: Duration = Duration::from_secs(60 * 60);
const CONCURRENT_RECORDS: usize = 16;

impl RuntimeStorageReader {
    /// Recovers and seals activations whose hosts stopped without sealing them, which otherwise
    /// wait for the actor's next activation.
    pub(crate) fn start_abandoned_recovery(self: Arc<Self>, stop: CancellationToken) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(RECOVERY_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = stop.cancelled() => return,
                    _ = interval.tick() => {}
                }
                if let Err(error) = self.seal_abandoned().await {
                    tracing::warn!(%error, "abandoned activation recovery deferred");
                }
            }
        });
    }

    pub(crate) async fn seal_abandoned(&self) -> Result<()> {
        let started = std::time::Instant::now();
        let keys = self.authority.list(&crate::storage_paths::owners()).await?;
        let outcomes: Vec<_> = stream::iter(keys.iter().cloned())
            .map(|key| async move {
                let outcome = self.seal_if_abandoned(&key).await;
                (key, outcome)
            })
            .buffer_unordered(CONCURRENT_RECORDS)
            .collect()
            .await;
        let mut sealed = 0;
        for (key, outcome) in outcomes {
            match outcome {
                Ok(true) => sealed += 1,
                Ok(false) => {}
                Err(error) => {
                    tracing::warn!(error = %format!("{error:#}"), key, "abandoned activation recovery deferred")
                }
            }
        }
        tracing::info!(
            event = "abandoned_activations_sealed",
            records = keys.len(),
            sealed,
            duration_ms = started.elapsed().as_secs_f64() * 1000.0
        );
        Ok(())
    }

    async fn seal_if_abandoned(&self, key: &str) -> Result<bool> {
        let Some(stored) = self.authority.get(key).await? else {
            return Ok(false);
        };
        let mut record: Ownership = serde_json::from_reader(stored.bytes.reader())?;
        let abandoned_at = record
            .lease
            .expires_at_ms
            .saturating_add(ABANDONED_AFTER_MS);
        if record.sealed
            || !record.persistence.same_backend(&self.persistence)
            || abandoned_at > self.clock.now_ms()?
        {
            return Ok(false);
        }
        let recovered = self
            .snapshots
            .recover(&record.stream()?.prefix)
            .await?
            .map(|(object, bytes)| VerifiedSnapshot::named(object, bytes))
            .transpose()?;
        advance(
            &mut record.base,
            recovered.map(|snapshot| snapshot.reference),
        )?;
        record.sealed = true;
        self.replace_activation(&mut record, Some(stored.generation))
            .await
    }

    /// Ends every live lease, which lets a new host take over at once. Only for a store that no
    /// live host can be using.
    pub(crate) async fn release_leases(&self) -> Result<()> {
        for key in self.authority.list(&crate::storage_paths::owners()).await? {
            self.release_lease(&key).await?;
        }
        Ok(())
    }

    async fn release_lease(&self, key: &str) -> Result<()> {
        let Some(stored) = self.authority.get(key).await? else {
            return Ok(());
        };
        let mut record: Ownership = serde_json::from_reader(stored.bytes.reader())?;
        if record.lease.expires_at_ms <= self.clock.now_ms()? {
            return Ok(());
        }
        record.lease.expires_at_ms = 0;
        ensure!(
            self.replace_activation(&mut record, Some(stored.generation))
                .await?,
            "actor ownership changed while its lease was released"
        );
        Ok(())
    }
}
