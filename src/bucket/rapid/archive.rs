use super::*;

/// What a segment's `replicated` marker records: the archive holds the segment through
/// `end_offset`. Hosts wrote these before bases replaced the archive; restores still read them.
#[derive(Clone, Serialize, Deserialize)]
pub(super) struct ClosedSegment {
    pub last_version: u64,
    pub end_offset: u64,
}

impl ClosedSegment {
    pub fn validate(&self, manifest: &Manifest, end: u64, records: &[Record]) -> Result<()> {
        ensure!(
            self.end_offset == end
                && self.last_version >= manifest.first_version - 1
                && records
                    .last()
                    .is_none_or(|record| record.version == self.last_version),
            "incomplete replicated segment"
        );
        Ok(())
    }
}

impl LogStorage {
    pub async fn read_range(
        &self,
        manifest: &Manifest,
        start: u64,
        length: u64,
    ) -> Result<Payload> {
        for replica in &manifest.replicas {
            match bounded_transfer(
                length,
                self.zone(replica)?.read_range(replica, start, length),
            )
            .await
            {
                Ok(bytes) => return Ok(bytes),
                Err(error) => tracing::warn!(%error, "Rapid archive source unavailable"),
            }
        }
        anyhow::bail!("all Rapid archive sources unavailable")
    }
}

/// Archives segments as hosts did before bases, so tests can build legacy history.
#[cfg(test)]
impl LogStorage {
    pub async fn put_batch(&self, manifest: &Manifest, start: u64, bytes: Payload) -> Result<()> {
        let end = start
            .checked_add(bytes.len() as u64)
            .context("batch offset overflow")?;
        let key = format!(
            "{}~{start:020}~{end:020}.batch",
            manifest.key()?.trim_end_matches(".manifest")
        );
        let began = std::time::Instant::now();
        ensure!(
            bounded_transfer(
                bytes.len() as u64,
                replace(self.archive.as_ref(), &key, None, bytes.clone())
            )
            .await?,
            "conflicting archive batch"
        );
        self.index_batch(manifest, key, &bytes).await?;
        tracing::info!(
            event = "rapid_log_batch",
            start_offset = start,
            end_offset = end,
            bytes = bytes.len(),
            duration_ms = began.elapsed().as_secs_f64() * 1000.0
        );
        Ok(())
    }

    pub async fn mark_replicated(&self, manifest: &Manifest, closed: &ClosedSegment) -> Result<()> {
        ensure!(
            bounded(replace(
                self.archive.as_ref(),
                &manifest.marker("replicated")?,
                None,
                crate::payload::encode(closed)?
            ))
            .await?,
            "conflicting replication coverage"
        );
        Ok(())
    }
}
