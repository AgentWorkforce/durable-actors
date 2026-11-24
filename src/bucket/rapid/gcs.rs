use super::*;
use crate::bucket::{GcsBucket, SnapshotStoreConfig, gcs::GcsClients};
use futures_util::TryStreamExt;
use google_cloud_storage::{
    appendable_object_writer::AppendableObjectWriter, model_ext::ReadRange,
    object_descriptor::ObjectDescriptor,
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
        let store = Self::new(archive, snapshots, zones, *placement, *archive_batch, stop)?;
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

type OpenedRead = (
    Option<AppendableObjectWriter>,
    Option<i64>,
    Option<ObjectDescriptor>,
);

impl GcsZone {
    /// Fences the writer when asked, then opens the persisted bytes; `None` when nothing persisted.
    async fn open_read(&self, replica: &Replica, fence: bool) -> Result<OpenedRead> {
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
        let persisted = match &mut writer {
            Some(writer) => Some(writer.flush().await?),
            None => None,
        };
        if persisted == Some(0) {
            return Ok((writer, persisted, None));
        }
        let descriptor = self
            .clients
            .storage
            .open_object(&self.bucket, &replica.object)
            .set_generation(replica.generation)
            .send()
            .await?;
        Ok((writer, persisted, Some(descriptor)))
    }
}

impl GcsZone {
    /// A large segment downloads as ranges, each on its own descriptor: ranges on one descriptor
    /// share a stream, which carried ~90 MB/s.
    async fn download_segment(
        &self,
        replica: &Replica,
        descriptor: ObjectDescriptor,
        size: u64,
    ) -> Result<Payload> {
        if size < PARALLEL_SEGMENT_BYTES {
            return download(&descriptor, 0, size).await;
        }
        let file = Arc::new(
            tokio::task::spawn_blocking(move || -> Result<std::fs::File> {
                let file = tempfile::tempfile()?;
                file.set_len(size)?;
                Ok(file)
            })
            .await??,
        );
        let part = size.div_ceil(SEGMENT_PARTS);
        let mut ranges = Vec::new();
        for (index, offset) in (0..size).step_by(part as usize).enumerate() {
            let length = part.min(size - offset);
            let (file, descriptor) = (file.clone(), descriptor.clone());
            let (storage, bucket, replica) = (
                self.clients.storage.clone(),
                self.bucket.clone(),
                replica.clone(),
            );
            ranges.push(AbortOnDropHandle::new(tokio::spawn(async move {
                let descriptor = match index {
                    0 => descriptor,
                    _ => {
                        storage
                            .open_object(&bucket, &replica.object)
                            .set_generation(replica.generation)
                            .send()
                            .await?
                    }
                };
                let response = descriptor
                    .read_range(ReadRange::segment(offset, length))
                    .await;
                crate::bucket::gcs::write_part(file, offset, length, response, false).await
            })));
        }
        for range in ranges {
            range.await??;
        }
        Ok(Payload::from_file(file, usize::try_from(size)?))
    }
}

/// Segments at least this large download as parallel ranges.
const PARALLEL_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;
/// Parallel segment downloads at once in this process, however many restores run: each holds
/// `SEGMENT_PARTS` write buffers, so this bounds the node's memory.
static PARALLEL_SEGMENTS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(8);
const SEGMENT_PARTS: u64 = 8;

async fn download(descriptor: &ObjectDescriptor, start: u64, length: u64) -> Result<Payload> {
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
        let (writer, persisted, descriptor) = bounded(self.open_read(replica, fence)).await?;
        let Some(descriptor) = descriptor else {
            return Ok(Payload::default());
        };
        let size = descriptor.object().size;
        ensure!(size >= 0, "invalid log segment size");
        if size == 0 {
            return Ok(Payload::default());
        }
        // Queued outside the transfer's deadline, which only times the download itself.
        let _reading = match size as u64 >= PARALLEL_SEGMENT_BYTES {
            true => Some(PARALLEL_SEGMENTS.acquire().await?),
            false => None,
        };
        let bytes = bounded_transfer(
            size as u64,
            self.download_segment(replica, descriptor, size as u64),
        )
        .await?;
        ensure!(
            persisted.is_none_or(|size| size as usize == bytes.len()),
            "incomplete fenced log read"
        );
        drop(writer);
        Ok(bytes)
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
        download(&descriptor, start, length).await
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

/// Rapid holds every change no base covers yet, so only the history sweeper deletes log objects:
/// a lifecycle rule would delete them by age, covered or not.
pub(crate) fn validate_retention(bucket: &google_cloud_storage::model::Bucket) -> Result<()> {
    const PREFIX: &str = "durable-actors-v3-logs-";
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
                !covers_logs,
                "Rapid log objects must have no lifecycle delete rule; the history sweeper deletes them"
            );
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "../../../tests/unit/bucket/rapid/gcs.rs"]
mod tests;
