use super::*;
use crate::bucket::{GcsBucket, SnapshotStoreConfig, gcs::GcsClients};
use futures_util::TryStreamExt;
use google_cloud_storage::{
    appendable_object_writer::AppendableObjectWriter, model_ext::ReadRange,
};

impl RapidSnapshots {
    pub(crate) async fn validate_gcs(
        config: &SnapshotStoreConfig,
        clients: GcsClients,
    ) -> Result<()> {
        let Some((buckets, archive_bucket)) = config.rapid_settings() else {
            anyhow::bail!("Rapid persistence configuration required");
        };
        GcsBucket::with_clients(archive_bucket, clients.clone())?
            .require_standard()
            .await?;
        for placement in buckets {
            let bucket = clients
                .control
                .get_bucket()
                .set_name(format!("projects/_/buckets/{}", placement.bucket))
                .send()
                .await?;
            ensure!(
                bucket.storage_class == "RAPID",
                "Rapid bucket has the wrong storage class"
            );
            validate_retention(&bucket)?;
            let locations = bucket
                .custom_placement_config
                .context("Rapid bucket zone missing")?
                .data_locations;
            ensure!(
                locations.len() == 1 && locations[0].eq_ignore_ascii_case(&placement.zone),
                "Rapid bucket does not occupy its configured zone"
            );
        }
        Ok(())
    }

    pub(crate) fn gcs(
        config: &SnapshotStoreConfig,
        clients: GcsClients,
        stop: CancellationToken,
        actor: Option<&crate::actor::ActorKey>,
    ) -> Result<Self> {
        config.validate()?;
        let SnapshotStoreConfig::Rapid {
            buckets,
            archive_bucket,
            archive_batch,
            placement,
        } = config
        else {
            anyhow::bail!("append-log persistence configuration required");
        };
        let archive = Arc::new(GcsBucket::with_clients(archive_bucket, clients.clone())?);
        let snapshots = Arc::new(super::standard::GcsSnapshots::new(
            archive_bucket,
            clients.clone(),
        )?);
        let zones = buckets
            .iter()
            .map(|placement| {
                Arc::new(GcsZone {
                    bucket: format!("projects/_/buckets/{}", placement.bucket),
                    clients: clients.clone(),
                }) as Arc<dyn LogZone>
            })
            .collect();
        let store = Self::new(
            archive,
            snapshots,
            zones,
            *placement,
            *archive_batch,
            Arc::new(crate::litestream::compaction::RustCompactor),
            stop,
        )?;
        if let Some(actor) = actor {
            store.prepare(actor)?;
        }
        Ok(store)
    }
}

struct GcsZone {
    bucket: String,
    clients: GcsClients,
}
struct GcsWriter(AppendableObjectWriter);

#[async_trait]
impl LogZone for GcsZone {
    fn bucket(&self) -> &str {
        &self.bucket
    }

    async fn open(&self, object: &str) -> Result<(Replica, Box<dyn LogWriter>)> {
        let writer = self
            .clients
            .storage
            .open_appendable_object(&self.bucket, object)
            .set_if_generation_match(0)
            .send()
            .await?;
        let replica = Replica {
            bucket: self.bucket.clone(),
            object: object.into(),
            generation: writer.generation(),
        };
        Ok((replica, Box::new(GcsWriter(writer))))
    }

    async fn read(&self, replica: &Replica, fence: bool) -> Result<Payload> {
        let _transfer = self.clients.transfers.acquire().await?;
        let mut writer = if fence {
            Some(
                self.clients
                    .storage
                    .reopen_appendable_object(&self.bucket, &replica.object, replica.generation)
                    .send()
                    .await?,
            )
        } else {
            None
        };
        let persisted = if let Some(writer) = &mut writer {
            Some(writer.flush().await?)
        } else {
            None
        };
        if persisted == Some(0) {
            return Ok(Payload::default());
        }
        let descriptor = self
            .clients
            .storage
            .open_object(&self.bucket, &replica.object)
            .set_generation(replica.generation)
            .send()
            .await?;
        let size = descriptor.object().size;
        ensure!(size >= 0, "invalid log segment size");
        if size == 0 {
            return Ok(Payload::default());
        }
        let mut reader = descriptor
            .read_range(ReadRange::segment(0, size as u64))
            .await;
        let mut download = crate::payload::Download::new();
        let mut length = 0usize;
        while let Some(chunk) = reader.next().await {
            let chunk = chunk?;
            length += chunk.len();
            ensure!(length <= size as usize, "log read exceeded persisted size");
            download = download.append(chunk).await?;
        }
        ensure!(
            length == size as usize && persisted.is_none_or(|size| size as usize == length),
            "incomplete fenced log read"
        );
        drop(writer);
        download.finish().await
    }

    async fn read_range(&self, replica: &Replica, start: u64, length: u64) -> Result<Payload> {
        let _transfer = self.clients.transfers.acquire().await?;
        let descriptor = self
            .clients
            .storage
            .open_object(&self.bucket, &replica.object)
            .set_generation(replica.generation)
            .send()
            .await?;
        let mut reader = descriptor
            .read_range(ReadRange::segment(start, length))
            .await;
        let mut download = crate::payload::Download::new();
        let mut received = 0u64;
        while let Some(chunk) = reader.next().await {
            let chunk = chunk?;
            received += chunk.len() as u64;
            ensure!(received <= length, "Rapid range exceeded requested length");
            download = download.append(chunk).await?;
        }
        ensure!(received == length, "incomplete Rapid range");
        download.finish().await
    }

    async fn delete(&self, replica: &Replica) -> Result<()> {
        match self
            .clients
            .control
            .delete_object()
            .set_bucket(&self.bucket)
            .set_object(&replica.object)
            .set_if_generation_match(replica.generation)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(error)
                if error.http_status_code() == Some(404)
                    || error.status().is_some_and(|s| s.code.name() == "NOT_FOUND") =>
            {
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }
}

#[async_trait]
impl LogWriter for GcsWriter {
    async fn append_and_flush(&mut self, header: Bytes, state: Payload) -> Result<u64> {
        self.0.append(header).await?;
        let mut chunks = std::pin::pin!(state.chunks());
        while let Some(chunk) = chunks.try_next().await? {
            self.0.append(chunk).await?;
        }
        Ok(self.0.flush().await?.try_into()?)
    }
}

/// The control plane archives abandoned logs within hours, so a week leaves ample margin.
pub(crate) fn validate_retention(bucket: &google_cloud_storage::model::Bucket) -> Result<()> {
    const PREFIX: &str = "durable-actors-v3-logs-";
    const MIN_AGE_DAYS: i32 = 7;
    if let Some(lifecycle) = &bucket.lifecycle {
        for rule in &lifecycle.rule {
            if rule
                .action
                .as_ref()
                .is_none_or(|action| action.r#type != "Delete")
            {
                continue;
            }
            let condition = rule.condition.as_ref();
            let prefixes = condition
                .map(|condition| condition.matches_prefix.as_slice())
                .unwrap_or_default();
            let covers_logs = prefixes.is_empty()
                || prefixes
                    .iter()
                    .any(|prefix| PREFIX.starts_with(prefix) || prefix.starts_with(PREFIX));
            ensure!(
                !covers_logs
                    || condition
                        .and_then(|condition| condition.age_days)
                        .is_some_and(|age| age >= MIN_AGE_DAYS),
                "Rapid log objects must stay at least {MIN_AGE_DAYS} days before lifecycle deletion"
            );
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "../../../tests/unit/bucket/rapid/gcs.rs"]
mod tests;
