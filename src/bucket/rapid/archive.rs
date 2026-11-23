use super::*;
use std::sync::Mutex as StdMutex;
use tokio::{sync::Notify, time::Instant};

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

struct Pending {
    end: u64,
    version: u64,
    since: Option<Instant>,
    cache_start: u64,
    cache: Vec<Frame>,
    cache_bytes: usize,
    closed: bool,
}

pub(super) struct Archiver {
    pending: Arc<StdMutex<Pending>>,
    wake: Arc<Notify>,
    task: AbortOnDropHandle<Result<()>>,
    batch_bytes: usize,
}

impl Archiver {
    pub fn new(storage: Arc<LogStorage>, manifest: Manifest, version: u64) -> Self {
        let pending = Arc::new(StdMutex::new(Pending {
            end: 0,
            version,
            since: None,
            cache_start: 0,
            cache: Vec::new(),
            cache_bytes: 0,
            closed: false,
        }));
        let wake = Arc::new(Notify::new());
        let batch_bytes = storage.batch.bytes.min(1024 * 1024);
        let task = AbortOnDropHandle::new(tokio::spawn(run(
            storage,
            manifest,
            pending.clone(),
            wake.clone(),
        )));
        Self {
            pending,
            wake,
            task,
            batch_bytes,
        }
    }

    pub fn append(&self, frame: Frame, version: u64) {
        let mut pending = self.pending.lock().unwrap();
        if pending.cache_start + pending.cache_bytes as u64 == pending.end
            && pending.cache_bytes < self.batch_bytes
        {
            pending.cache_bytes += frame.len();
            pending.cache.push(frame.clone());
        }
        pending.end += frame.len() as u64;
        pending.version = version;
        pending.since.get_or_insert_with(Instant::now);
        self.wake.notify_one();
    }

    pub fn close(&self) {
        self.pending.lock().unwrap().closed = true;
        self.wake.notify_one();
    }

    pub fn is_finished(&self) -> bool {
        self.task.is_finished()
    }

    pub async fn finish(self) -> Result<()> {
        self.close();
        let pending = self.pending.lock().unwrap().end;
        bounded_transfer(pending, async {
            self.task.await.context("archive worker failed")?
        })
        .await
    }
}

async fn run(
    storage: Arc<LogStorage>,
    manifest: Manifest,
    pending: Arc<StdMutex<Pending>>,
    wake: Arc<Notify>,
) -> Result<()> {
    let mut through = 0;
    let mut retry = false;
    let interval = Duration::from_millis(storage.batch.interval_ms);
    loop {
        let (end, closed, due, last_version) = {
            let p = pending.lock().unwrap();
            (p.end, p.closed, p.since.map(|at| at + interval), p.version)
        };
        if through == end && closed {
            let coverage = ClosedSegment {
                last_version,
                end_offset: end,
            };
            // Readers need the Rapid copy until this marker lands, and the lifecycle rule
            // eventually deletes that copy.
            if let Err(error) = storage.mark_replicated(&manifest, &coverage).await {
                tracing::warn!(%error, "Rapid replication marker deferred");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
            tokio::spawn(async move {
                if let Err(error) = storage.delete_replicas(&manifest).await {
                    tracing::warn!(%error, "Rapid replicas left to the lifecycle rule");
                }
            });
            return Ok(());
        }
        if through < end
            && (closed
                || retry
                || end - through >= storage.batch.bytes as u64
                || due.is_some_and(|at| at <= Instant::now()))
        {
            let cached = take_cache(&pending, through)?;
            match upload(&storage, &manifest, through, end, cached).await {
                Ok(next) => {
                    retry = false;
                    through = next;
                    let mut p = pending.lock().unwrap();
                    if p.cache.is_empty() {
                        p.cache_start = through;
                    }
                    if through == p.end {
                        p.since = None;
                    } else {
                        p.since.get_or_insert_with(Instant::now);
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, "Standard batch upload deferred");
                    retry = true;
                    // Rapid holds the backlog; a failed upload must not grow the memory queue.
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
            continue;
        }
        match due {
            Some(at) => {
                tokio::select! { _ = wake.notified() => {}, _ = tokio::time::sleep_until(at) => {} }
            }
            None => wake.notified().await,
        }
    }
}

fn take_cache(pending: &StdMutex<Pending>, through: u64) -> Result<Option<Payload>> {
    let frames = {
        let mut p = pending.lock().unwrap();
        if p.cache_start != through || p.cache_bytes == 0 {
            return Ok(None);
        }
        p.cache_start += p.cache_bytes as u64;
        p.cache_bytes = 0;
        if p.cache_start == p.end {
            p.since = None;
        }
        std::mem::take(&mut p.cache)
    };
    let mut spool = crate::payload::Spool::new();
    for frame in frames {
        frame.write_to(&mut spool)?;
    }
    Ok(Some(spool.finish()?))
}

async fn upload(
    storage: &LogStorage,
    manifest: &Manifest,
    start: u64,
    end: u64,
    cached: Option<Payload>,
) -> Result<u64> {
    let bytes = match cached {
        Some(bytes) => bytes,
        None => {
            match read_batch(storage, manifest, start, end).await {
                Ok(bytes) => bytes,
                Err(error) => {
                    // Another worker may have archived and deleted the Rapid copies.
                    let (through, _) = storage.archived(manifest).await?;
                    if through > start {
                        return Ok(through.min(end));
                    }
                    return Err(error);
                }
            }
        }
    };
    let records = frame::decode(&bytes)?;
    let mut length = 0;
    for record in records {
        ensure!(
            manifest.stream.snapshot(&record.state)?.object
                == manifest.stream.object(record.version)
                && record.version >= manifest.first_version,
            "archive record identity mismatch"
        );
        length += frame::HEADER + record.state.len();
        if length >= storage.batch.bytes {
            break;
        }
    }
    ensure!(length > 0, "archive range has no complete record");
    storage
        .put_batch(manifest, start, bytes.slice(..length))
        .await?;
    Ok(start + length as u64)
}

async fn read_batch(
    storage: &LogStorage,
    manifest: &Manifest,
    start: u64,
    end: u64,
) -> Result<Payload> {
    let requested = (end - start).min(storage.batch.bytes.max(frame::HEADER) as u64);
    let bytes = storage.read_range(manifest, start, requested).await?;
    let length = frame::length(&bytes.read(0..frame::HEADER.min(bytes.len()))?)? as u64;
    if length <= requested {
        return Ok(bytes);
    }
    ensure!(length <= end - start, "incomplete committed record");
    let tail = storage
        .read_range(manifest, start + requested, length - requested)
        .await?;
    tokio::task::spawn_blocking(move || {
        use std::io::Read;
        crate::payload::copy(bytes.reader().chain(tail.reader()))
    })
    .await?
}

impl LogStorage {
    pub async fn put_batch(&self, manifest: &Manifest, start: u64, bytes: Payload) -> Result<()> {
        let end = start
            .checked_add(bytes.len() as u64)
            .context("batch offset overflow")?;
        let key = format!(
            "{}~{start:020}~{end:020}.batch",
            manifest.key()?.trim_end_matches(".manifest")
        );
        let began = Instant::now();
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

    /// Only called once the segment is replicated; a copy left behind expires under the Rapid
    /// lifecycle rule.
    async fn delete_replicas(&self, manifest: &Manifest) -> Result<()> {
        for replica in &manifest.replicas {
            bounded(self.zone(replica)?.delete(replica)).await?;
        }
        Ok(())
    }
}
