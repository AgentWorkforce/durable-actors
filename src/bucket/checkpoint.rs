//! Asynchronous checkpointing of a single-zone bucket (S3 Express One Zone) to a regional
//! S3 Standard bucket.
//!
//! An Express directory bucket stores its data in one Availability Zone, so losing that zone loses
//! every actor's state. [`CheckpointedBucket`] wraps the Express bucket: reads go to Express only,
//! and every successful write is queued for a background [`Checkpointer`] that copies the same
//! bytes and generation to the Standard bucket. The write path never waits on the copy.
//!
//! The copy keeps the bucket layout and the `da-generation` metadata, so the Standard bucket is
//! itself a valid durable-actors bucket. Recovery either points `DURABLE_ACTORS_BUCKET` at it
//! directly, or [`restore`]s it into a new Express bucket in another zone.
//!
//! Ordering: the queue is flushed oldest write first, and new objects (snapshots, first owner
//! records) are copied before replaced objects (owner records) in each batch. A replace is copied
//! only once every earlier create in its batch has been copied. So the copy never holds an owner
//! record that points at a snapshot it does not have.
//!
//! RPO: the copy trails Express by the time the queue takes to drain, normally one Standard PUT
//! (tens of milliseconds). `lag_ms` reports the age of the oldest write not yet copied. A warning
//! is logged when it exceeds `max_lag`. The queue is never allowed to slow writes. Writes still
//! queued when a host dies without a clean shutdown are lost from the copy (they stay on Express).

use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Result, ensure};
use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use tokio::{sync::Notify, time::Instant};
use tokio_util::sync::CancellationToken;

use super::{Bucket, BucketObject};

/// Writes an object at an exact generation, for copies whose generation must match the source.
#[async_trait]
pub trait Mirror: Bucket {
    /// Stores `bytes` as `key` at `generation`, unless the mirror already holds that generation or
    /// a later one, which keeps a delayed copy from overwriting a newer one.
    async fn mirror(&self, key: &str, generation: i64, bytes: bytes::Bytes) -> Result<()>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckpointConfig {
    /// The lag above which a warning is logged. It is an alert threshold, not backpressure.
    pub max_lag: Duration,
    /// Queued payload bytes kept in memory. Past this, queued writes keep only their key and the
    /// checkpointer reads the current object back from the source.
    pub max_pending_bytes: usize,
    /// Objects copied concurrently.
    pub concurrency: usize,
    /// Objects taken from the queue per batch.
    pub batch: usize,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            max_lag: Duration::from_secs(5),
            max_pending_bytes: 64 << 20,
            concurrency: 8,
            batch: 128,
        }
    }
}

/// Point-in-time checkpoint metrics, also logged as `event = "storage_checkpoint"`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CheckpointStats {
    /// Objects written to the source but not yet copied.
    pub pending: u64,
    pub pending_bytes: u64,
    /// Age of the oldest write not yet copied, or 0 when the copy is current.
    pub lag_ms: u64,
    pub copied_total: u64,
    pub failures_total: u64,
}

struct Pending {
    key: String,
    generation: i64,
    /// `None` when the payload was dropped to bound memory, or the write's outcome is unknown.
    bytes: Option<bytes::Bytes>,
    created: bool,
    since: Instant,
}

#[derive(Default)]
struct Queue {
    next: u64,
    entries: BTreeMap<u64, Pending>,
    keys: HashMap<String, u64>,
    bytes: usize,
    in_flight: usize,
    oldest_in_flight: Option<Instant>,
}

impl Queue {
    fn push(&mut self, mut entry: Pending, max_bytes: usize) {
        if let Some(sequence) = self.keys.remove(&entry.key)
            && let Some(previous) = self.entries.remove(&sequence)
        {
            // A newer write supersedes the queued one, but the copy has been stale since then.
            entry.since = entry.since.min(previous.since);
            entry.created &= previous.created;
            self.bytes -= previous.bytes.as_ref().map_or(0, bytes::Bytes::len);
        }
        if let Some(bytes) = &entry.bytes {
            if self.bytes + bytes.len() > max_bytes {
                entry.bytes = None;
            } else {
                self.bytes += bytes.len();
            }
        }
        let sequence = self.next;
        self.next += 1;
        self.keys.insert(entry.key.clone(), sequence);
        self.entries.insert(sequence, entry);
    }

    fn take(&mut self, limit: usize) -> Vec<(u64, Pending)> {
        let sequences: Vec<u64> = self.entries.keys().take(limit).copied().collect();
        let batch: Vec<_> = sequences
            .into_iter()
            .filter_map(|sequence| {
                let entry = self.entries.remove(&sequence)?;
                self.keys.remove(&entry.key);
                self.bytes -= entry.bytes.as_ref().map_or(0, bytes::Bytes::len);
                Some((sequence, entry))
            })
            .collect();
        self.in_flight = batch.len();
        self.oldest_in_flight = batch.iter().map(|(_, entry)| entry.since).min();
        batch
    }

    /// Puts back a write that was not copied, unless a newer write of the key is already queued.
    fn retry(&mut self, sequence: u64, entry: Pending) {
        if let Some(newer) = self
            .keys
            .get(&entry.key)
            .and_then(|sequence| self.entries.get_mut(sequence))
        {
            newer.since = newer.since.min(entry.since);
            newer.created &= entry.created;
            return;
        }
        self.bytes += entry.bytes.as_ref().map_or(0, bytes::Bytes::len);
        self.keys.insert(entry.key.clone(), sequence);
        self.entries.insert(sequence, entry);
    }

    fn oldest(&self) -> Option<Instant> {
        let queued = self.entries.values().map(|entry| entry.since).min();
        match (queued, self.oldest_in_flight) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    fn is_idle(&self) -> bool {
        self.entries.is_empty() && self.in_flight == 0
    }
}

struct Shared {
    source: Arc<dyn Bucket>,
    target: Arc<dyn Mirror>,
    config: CheckpointConfig,
    queue: Mutex<Queue>,
    queued: Notify,
    drained: Notify,
    copied: AtomicU64,
    failures: AtomicU64,
}

/// Copies writes from a source bucket to a mirror in the background.
pub struct Checkpointer {
    shared: Arc<Shared>,
    stop: CancellationToken,
}

impl Checkpointer {
    /// Starts the copy task; it runs until `stop` is cancelled or the checkpointer is dropped.
    pub fn spawn(
        source: Arc<dyn Bucket>,
        target: Arc<dyn Mirror>,
        config: CheckpointConfig,
        stop: &CancellationToken,
    ) -> Arc<Self> {
        let shared = Arc::new(Shared {
            source,
            target,
            config,
            queue: Mutex::new(Queue::default()),
            queued: Notify::new(),
            drained: Notify::new(),
            copied: AtomicU64::new(0),
            failures: AtomicU64::new(0),
        });
        let stop = stop.child_token();
        tokio::spawn(run(shared.clone(), stop.clone()));
        tokio::spawn(report(shared.clone(), stop.clone()));
        Arc::new(Self { shared, stop })
    }

    /// Queues a successful write. Never blocks on the mirror.
    pub fn record(&self, key: &str, generation: i64, bytes: bytes::Bytes, created: bool) {
        self.push(key, generation, Some(bytes), created);
    }

    /// Queues a key whose last write has an unknown outcome; the current object is copied.
    pub fn record_uncertain(&self, key: &str) {
        self.push(key, 0, None, false);
    }

    fn push(&self, key: &str, generation: i64, bytes: Option<bytes::Bytes>, created: bool) {
        self.shared.queue.lock().unwrap().push(
            Pending {
                key: key.into(),
                generation,
                bytes,
                created,
                since: Instant::now(),
            },
            self.shared.config.max_pending_bytes,
        );
        self.shared.queued.notify_one();
    }

    /// Queues every object under `prefix` that the source holds and the mirror lacks. A host that
    /// died before draining its queue leaves immutable snapshots that are never written again, so
    /// the next activation of the actor reconciles its snapshot prefix (one LIST on each side).
    pub async fn reconcile(&self, prefix: &str) -> Result<usize> {
        let (source, copied) = tokio::try_join!(
            self.shared.source.list(prefix),
            self.shared.target.list(prefix)
        )?;
        let copied: std::collections::HashSet<_> = copied.into_iter().collect();
        let mut missing = 0;
        for key in source.into_iter().filter(|key| !copied.contains(key)) {
            // Snapshots are created once, so they keep their place ahead of owner records.
            self.push(&key, 0, None, true);
            missing += 1;
        }
        if missing > 0 {
            tracing::info!(event = "storage_checkpoint_reconciled", prefix, missing,);
        }
        Ok(missing)
    }

    pub fn stats(&self) -> CheckpointStats {
        stats(&self.shared)
    }

    /// Waits until every write queued so far has been copied, or until `deadline`. Returns whether
    /// the copy is current.
    pub async fn flush(&self, deadline: Instant) -> bool {
        loop {
            let drained = self.shared.drained.notified();
            tokio::pin!(drained);
            drained.as_mut().enable();
            if self.shared.queue.lock().unwrap().is_idle() {
                return true;
            }
            if tokio::time::timeout_at(deadline, drained).await.is_err() {
                let stats = self.stats();
                tracing::warn!(
                    event = "storage_checkpoint_flush_incomplete",
                    pending = stats.pending,
                    lag_ms = stats.lag_ms,
                    "checkpoint copy not current at shutdown"
                );
                return false;
            }
        }
    }
}

impl Drop for Checkpointer {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

fn stats(shared: &Shared) -> CheckpointStats {
    let queue = shared.queue.lock().unwrap();
    CheckpointStats {
        pending: (queue.entries.len() + queue.in_flight) as u64,
        pending_bytes: queue.bytes as u64,
        lag_ms: queue
            .oldest()
            .map_or(0, |since| since.elapsed().as_millis() as u64),
        copied_total: shared.copied.load(Ordering::Relaxed),
        failures_total: shared.failures.load(Ordering::Relaxed),
    }
}

const RETRY_MIN: Duration = Duration::from_millis(100);
const RETRY_MAX: Duration = Duration::from_secs(5);

async fn run(shared: Arc<Shared>, stop: CancellationToken) {
    let mut backoff = RETRY_MIN;
    loop {
        let queued = shared.queued.notified();
        tokio::pin!(queued);
        queued.as_mut().enable();
        let batch = shared.queue.lock().unwrap().take(shared.config.batch);
        if batch.is_empty() {
            shared.drained.notify_waiters();
            tokio::select! {
                () = stop.cancelled() => return,
                () = &mut queued => continue,
            }
        }
        let failed = copy_batch(&shared, batch).await;
        let had_failures = !failed.is_empty();
        let idle = {
            let mut queue = shared.queue.lock().unwrap();
            for (sequence, entry) in failed {
                queue.retry(sequence, entry);
            }
            queue.in_flight = 0;
            queue.oldest_in_flight = None;
            queue.is_idle()
        };
        if idle {
            shared.drained.notify_waiters();
        }
        if had_failures {
            tokio::select! {
                () = stop.cancelled() => return,
                () = tokio::time::sleep(backoff) => {}
            }
            backoff = (backoff * 2).min(RETRY_MAX);
        } else {
            backoff = RETRY_MIN;
        }
    }
}

/// Copies creates, then replaces; replaces wait for the next batch when any create failed.
async fn copy_batch(shared: &Shared, batch: Vec<(u64, Pending)>) -> Vec<(u64, Pending)> {
    let (creates, replaces): (Vec<_>, Vec<_>) =
        batch.into_iter().partition(|(_, entry)| entry.created);
    let mut failed = copy_all(shared, creates).await;
    if failed.is_empty() {
        failed = copy_all(shared, replaces).await;
    } else {
        failed.extend(replaces);
    }
    failed
}

async fn copy_all(shared: &Shared, entries: Vec<(u64, Pending)>) -> Vec<(u64, Pending)> {
    stream::iter(entries)
        .map(|(sequence, entry)| async move {
            match copy(shared, &entry).await {
                Ok(()) => {
                    shared.copied.fetch_add(1, Ordering::Relaxed);
                    None
                }
                Err(error) => {
                    shared.failures.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        event = "storage_checkpoint_copy_failed",
                        key = entry.key,
                        error = format!("{error:#}"),
                    );
                    Some((sequence, entry))
                }
            }
        })
        .buffer_unordered(shared.config.concurrency.max(1))
        .filter_map(std::future::ready)
        .collect()
        .await
}

async fn copy(shared: &Shared, entry: &Pending) -> Result<()> {
    let (generation, bytes) = match &entry.bytes {
        Some(bytes) => (entry.generation, bytes.clone()),
        None => match shared.source.get(&entry.key).await? {
            Some(BucketObject { generation, bytes }) => (generation, bytes),
            // Nothing was written (an uncertain write that failed); nothing to copy.
            None => return Ok(()),
        },
    };
    shared.target.mirror(&entry.key, generation, bytes).await
}

async fn report(shared: Arc<Shared>, stop: CancellationToken) {
    const INTERVAL: Duration = Duration::from_secs(10);
    let mut ticker = tokio::time::interval(INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = stop.cancelled() => return,
            _ = ticker.tick() => {}
        }
        let stats = stats(&shared);
        let over = stats.lag_ms > shared.config.max_lag.as_millis() as u64;
        tracing::event!(
            target: "durable_actors::checkpoint",
            tracing::Level::INFO,
            event = "storage_checkpoint",
            pending = stats.pending,
            pending_bytes = stats.pending_bytes,
            lag_ms = stats.lag_ms,
            lag_exceeded = over,
            copied_total = stats.copied_total,
            failures_total = stats.failures_total,
        );
        if over {
            tracing::warn!(
                event = "storage_checkpoint_lag_exceeded",
                lag_ms = stats.lag_ms,
                max_lag_ms = shared.config.max_lag.as_millis() as u64,
                pending = stats.pending,
                "regional checkpoint copy is behind the single-zone bucket"
            );
        }
    }
}

/// A bucket whose writes are checkpointed to a mirror. The source must number generations like
/// [`super::S3Bucket`]: a create is generation 1 and each replace adds one.
pub struct CheckpointedBucket {
    source: Arc<dyn Bucket>,
    checkpointer: Arc<Checkpointer>,
}

impl CheckpointedBucket {
    pub fn new(source: Arc<dyn Bucket>, checkpointer: Arc<Checkpointer>) -> Self {
        Self {
            source,
            checkpointer,
        }
    }
}

#[async_trait]
impl Bucket for CheckpointedBucket {
    async fn get(&self, key: &str) -> Result<Option<BucketObject>> {
        self.source.get(key).await
    }

    async fn range(&self, key: &str, start: u64, length: u64) -> Result<Option<BucketObject>> {
        self.source.range(key, start, length).await
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        generation: Option<i64>,
        bytes: bytes::Bytes,
    ) -> Result<bool> {
        let result = self
            .source
            .compare_and_swap(key, generation, bytes.clone())
            .await;
        match (&result, generation) {
            (Ok(true), None) => self.checkpointer.record(key, 1, bytes, true),
            (Ok(true), Some(expected)) => match expected.checked_add(1) {
                Some(next) => self.checkpointer.record(key, next, bytes, false),
                None => self.checkpointer.record_uncertain(key),
            },
            // A retried request can report a lost precondition after the first one succeeded
            // (see `super::replace`), so copy whatever the source now holds.
            _ => self.checkpointer.record_uncertain(key),
        }
        result
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.source.list(prefix).await
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RestoreReport {
    pub listed: u64,
    pub copied: u64,
    pub missing: u64,
}

/// Copies every object under `prefix` from `source` into `target`, keeping generations. Objects
/// the target already holds at the same or a later generation are left alone, so a restore can
/// be re-run after an interruption.
pub async fn restore(
    source: &dyn Bucket,
    target: &dyn Mirror,
    prefix: &str,
    concurrency: usize,
) -> Result<RestoreReport> {
    ensure!(
        prefix.starts_with(crate::storage_paths::ROOT),
        "restore prefix must be under {}",
        crate::storage_paths::ROOT
    );
    let keys = source.list(prefix).await?;
    let listed = keys.len() as u64;
    let copied = AtomicU64::new(0);
    let missing = AtomicU64::new(0);
    stream::iter(keys)
        .map(|key| {
            let (copied, missing) = (&copied, &missing);
            async move {
                match source.get(&key).await? {
                    Some(object) => {
                        target.mirror(&key, object.generation, object.bytes).await?;
                        copied.fetch_add(1, Ordering::Relaxed);
                    }
                    None => {
                        missing.fetch_add(1, Ordering::Relaxed);
                    }
                }
                anyhow::Ok(())
            }
        })
        .buffer_unordered(concurrency.max(1))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    Ok(RestoreReport {
        listed,
        copied: copied.into_inner(),
        missing: missing.into_inner(),
    })
}

#[cfg(test)]
#[path = "../../tests/unit/bucket/checkpoint.rs"]
mod tests;
