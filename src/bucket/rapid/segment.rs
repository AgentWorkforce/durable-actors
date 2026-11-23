use super::*;

pub(super) struct Segment {
    pub manifest: Manifest,
    pub zones: Vec<Arc<dyn LogZone>>,
    writers: Vec<Box<dyn LogWriter>>,
    pub offset: u64,
    healthy: bool,
}

impl Segment {
    /// Opens in `placed` zones when the session already chose them, otherwise by `storage`'s
    /// placement.
    pub async fn open(
        storage: &LogStorage,
        stream: StateStream,
        first_version: u64,
        placed: Option<Vec<Arc<dyn LogZone>>>,
    ) -> Result<Option<Self>> {
        let started = std::time::Instant::now();
        let actor = crate::storage_paths::actor_from_snapshot(&stream.object(first_version))?;
        let prefix = object_name(&crate::storage_paths::snapshots(&actor)?)?.replacen(
            "snapshots-",
            "logs-",
            1,
        );
        let prepared = storage.prepared.lock().unwrap().take();
        let prepared = match prepared {
            Some(prepared) => {
                ensure!(
                    prepared.prefix == prefix,
                    "prepared log belongs to another actor"
                );
                prepared
            }
            None => match placed {
                Some(zones) => Prepared::new(prefix, zones, crate::bucket::RapidPlacement::All),
                None => Prepared::new(prefix, storage.zones.clone(), storage.placement),
            },
        };
        let mut manifest = Manifest {
            format: 1,
            id: prepared.id.clone(),
            stream,
            first_version,
            replicas: Vec::new(),
        };
        let Some(opened) = prepared.take().await? else {
            return Ok(None);
        };
        let mut writers = Vec::new();
        for (replica, writer) in opened {
            manifest.replicas.push(replica);
            writers.push(writer);
        }
        let zones = storage.replica_zones(&manifest)?;
        let key = manifest.key()?;
        let open_ms = started.elapsed().as_secs_f64() * 1000.0;
        ensure!(
            bounded(replace(
                storage.archive.as_ref(),
                &key,
                None,
                crate::payload::encode(&manifest)?
            ))
            .await?,
            "conflicting log manifest"
        );
        tracing::info!(
            event = "rapid_log_open",
            owner_epoch = manifest.stream.owner_epoch,
            buckets = %manifest
                .replicas
                .iter()
                .map(|replica| replica.bucket.as_str())
                .collect::<Vec<_>>()
                .join(","),
            open_ms,
            manifest_ms = started.elapsed().as_secs_f64() * 1000.0 - open_ms,
            duration_ms = started.elapsed().as_secs_f64() * 1000.0
        );
        Ok(Some(Self {
            manifest,
            zones,
            writers,
            offset: 0,
            healthy: true,
        }))
    }

    pub async fn append(&mut self, version: u64, state: Payload) -> Result<Frame> {
        ensure!(
            self.healthy,
            "uncertain log append requires activation recovery"
        );
        let frame = Record { version, state }.frame()?;
        let expected = self
            .offset
            .checked_add(frame.len() as u64)
            .context("log offset overflow")?;
        // An interrupted flush may have persisted bytes; this pair must never be reused.
        self.healthy = false;
        let flushed = bounded_transfer(frame.len() as u64, async {
            Ok(futures_util::future::join_all(
                self.writers.iter_mut().map(|writer| {
                    writer.append_and_flush(frame.header.clone(), frame.state.clone())
                }),
            )
            .await)
        })
        .await?;
        for result in flushed {
            ensure!(result? == expected, "unexpected persisted log offset");
        }
        self.offset = expected;
        self.healthy = true;
        Ok(frame)
    }
}
