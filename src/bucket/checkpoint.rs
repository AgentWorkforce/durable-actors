//! Asynchronous checkpointing of a single-zone bucket (S3 Express One Zone) to a regional
//! S3 Standard bucket.
//!
//! [`CheckpointedBucket`] wraps the Express bucket: reads go to Express only, and every successful
//! write is queued for a background [`Checkpointer`] that copies the same bytes and generation to
//! the Standard bucket. The write path never waits on the copy. The copy keeps the bucket layout
//! and `da-generation` metadata, so the Standard bucket is itself a valid durable-actors bucket.
//!
//! Consistency: each actor's writes are copied strictly in write order, one at a time; actors copy
//! in parallel. A copy holds an exact generation, and a read-back that finds a newer one goes to
//! the back of its lane. A host's startup reconciliation runs before anything else in its lane.
//! So the copy never holds a snapshot without its SQLite parent, or an owner record naming a
//! snapshot it lacks. [`verify`] proves this for a whole bucket.
//!
//! RPO: `lag_ms` is the age of the oldest write not yet copied. Writes still queued when a host
//! dies are recovered from Express by the next activation's reconciliation, unless the zone is
//! lost first.

mod queue;
mod restore;

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::Result;
use async_trait::async_trait;
use tokio::{sync::Notify, time::Instant};
use tokio_util::sync::CancellationToken;

use queue::{Expect, Job, Outcome, Pending, Queue};
pub use restore::{RestoreReport, VerifyReport, restore, verify};

use super::{Bucket, BucketObject};

/// Writes an object at an exact generation, for copies whose generation must match the source.
#[async_trait]
pub trait Mirror: Bucket {
    /// Stores `bytes` as `key` at `generation`, unless the mirror already holds that generation or
    /// a later one, which keeps a delayed copy from overwriting a newer one.
    async fn mirror(
        &self,
        key: &str,
        generation: i64,
        bytes: crate::payload::Payload,
    ) -> Result<()>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckpointConfig {
    /// The lag above which a warning is logged. It is an alert threshold, not backpressure.
    pub max_lag: Duration,
    /// Queued payload bytes kept in memory. Past this, queued writes keep only their key and
    /// generation, and the checkpointer reads the object back from the source.
    pub max_pending_bytes: usize,
    /// Lanes (actors) copied concurrently.
    pub concurrency: usize,
    /// Before copying an owner record, copy any of its actor's snapshots the mirror lacks. The
    /// control plane writes owner records (takeover, recovery) naming snapshots another process
    /// wrote, which its own queue never saw.
    pub reconcile_owners: bool,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            max_lag: Duration::from_secs(5),
            max_pending_bytes: 64 << 20,
            concurrency: 16,
            reconcile_owners: false,
        }
    }
}

/// Point-in-time checkpoint metrics, also logged as `event = "storage_checkpoint"`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CheckpointStats {
    /// Writes not yet copied.
    pub pending: u64,
    pub pending_bytes: u64,
    /// Age of the oldest write not yet copied, or 0 when the copy is current.
    pub lag_ms: u64,
    pub copied_total: u64,
    pub failures_total: u64,
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
    pub fn record(&self, key: &str, generation: i64, bytes: crate::payload::Payload) {
        self.push(key, Expect::Exact(generation), Some(bytes));
    }

    /// Queues a key whose last write has an unknown outcome.
    pub fn record_uncertain(&self, key: &str) {
        self.push(key, Expect::Unknown, None);
    }

    /// Holds `prefix`'s lane until every object under it that the mirror lacks is queued ahead of
    /// the lane's other writes. A host that died before draining its queue leaves snapshots that
    /// are never written again; its successor's owner record must not reach the mirror before
    /// them. Retries until it succeeds or the checkpointer stops.
    pub fn reconcile_in_background(self: &Arc<Self>, prefix: &str) {
        let lane = queue::lane_of(prefix);
        self.shared.queue.lock().unwrap().gate(&lane);
        let checkpointer = self.clone();
        let prefix = prefix.to_owned();
        tokio::spawn(async move { checkpointer.reconcile_until_done(&prefix, &lane).await });
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

    fn push(&self, key: &str, expect: Expect, bytes: Option<crate::payload::Payload>) {
        let entry = Pending {
            key: key.into(),
            expect,
            bytes,
            since: Instant::now(),
        };
        let max = self.shared.config.max_pending_bytes;
        self.shared.queue.lock().unwrap().push(entry, max);
        self.shared.queued.notify_one();
    }

    async fn reconcile_until_done(&self, prefix: &str, lane: &str) {
        let mut failures: u32 = 0;
        loop {
            match self.reconcile(prefix, lane).await {
                Ok(()) => break,
                Err(error) => {
                    failures += 1;
                    tracing::warn!(
                        event = "storage_checkpoint_reconcile_failed",
                        prefix,
                        failures,
                        error = format!("{error:#}"),
                    );
                    let pause = Duration::from_millis(250 * u64::from(failures.min(20)));
                    tokio::select! {
                        () = self.stop.cancelled() => return,
                        () = tokio::time::sleep(pause) => {}
                    }
                }
            }
        }
        self.shared.queue.lock().unwrap().ungate(lane);
        self.shared.queued.notify_one();
    }

    async fn reconcile(&self, prefix: &str, lane: &str) -> Result<()> {
        let (source, copied) = tokio::try_join!(
            self.shared.source.list(prefix),
            self.shared.target.list(prefix)
        )?;
        let copied: std::collections::HashSet<_> = copied.into_iter().collect();
        let mut missing: Vec<_> = source
            .into_iter()
            .filter(|key| !copied.contains(key))
            .collect();
        missing.sort();
        if missing.is_empty() {
            return Ok(());
        }
        tracing::info!(
            event = "storage_checkpoint_reconciled",
            prefix,
            missing = missing.len(),
        );
        let now = Instant::now();
        let entries = missing
            .into_iter()
            .map(|key| Pending {
                key,
                expect: Expect::Current,
                bytes: None,
                since: now,
            })
            .collect();
        self.shared.queue.lock().unwrap().push_front(lane, entries);
        Ok(())
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
        pending: queue.pending() as u64,
        pending_bytes: queue.bytes() as u64,
        lag_ms: queue
            .oldest()
            .map_or(0, |since| since.elapsed().as_millis() as u64),
        copied_total: shared.copied.load(Ordering::Relaxed),
        failures_total: shared.failures.load(Ordering::Relaxed),
    }
}

async fn run(shared: Arc<Shared>, stop: CancellationToken) {
    loop {
        let queued = shared.queued.notified();
        tokio::pin!(queued);
        queued.as_mut().enable();
        let (jobs, retry, idle) = {
            let mut queue = shared.queue.lock().unwrap();
            let jobs = queue.take(shared.config.concurrency.max(1), Instant::now());
            (jobs, queue.next_retry(), queue.is_idle())
        };
        if jobs.is_empty() {
            if idle {
                shared.drained.notify_waiters();
            }
            let wake = retry.unwrap_or_else(|| Instant::now() + Duration::from_secs(3600));
            tokio::select! {
                () = stop.cancelled() => return,
                () = &mut queued => {}
                () = tokio::time::sleep_until(wake) => {}
            }
            continue;
        }
        let outcomes =
            futures_util::future::join_all(jobs.iter().map(|job| copy(&shared, job))).await;
        let mut queue = shared.queue.lock().unwrap();
        let now = Instant::now();
        for (job, outcome) in jobs.iter().zip(outcomes) {
            queue.complete(job, outcome, now);
        }
    }
}

async fn copy(shared: &Shared, job: &Job) -> Outcome {
    match attempt(shared, job).await {
        Ok(outcome) => {
            if let Outcome::Copied = outcome {
                shared.copied.fetch_add(1, Ordering::Relaxed);
            }
            outcome
        }
        Err(error) => {
            shared.failures.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                event = "storage_checkpoint_copy_failed",
                key = job.key,
                error = format!("{error:#}"),
            );
            Outcome::Failed
        }
    }
}

async fn attempt(shared: &Shared, job: &Job) -> Result<Outcome> {
    if shared.config.reconcile_owners
        && let Some(prefix) = queue::owner_snapshots(&job.key)
    {
        copy_missing(shared, &prefix).await?;
    }
    if let (Expect::Exact(generation), Some(bytes)) = (job.expect, &job.bytes) {
        shared
            .target
            .mirror(&job.key, generation, bytes.clone())
            .await?;
        return Ok(Outcome::Copied);
    }
    let Some(BucketObject { generation, bytes }) = shared.source.get(&job.key).await? else {
        return Ok(Outcome::Dropped);
    };
    let copy = match job.expect {
        Expect::Current => true,
        Expect::Exact(expected) => generation == expected,
        Expect::Unknown => false,
    };
    if !copy {
        // A generation this entry did not record may depend on writes queued after it.
        return Ok(Outcome::Requeue(generation));
    }
    shared.target.mirror(&job.key, generation, bytes).await?;
    Ok(Outcome::Copied)
}

/// Copies, in key order, every object under `prefix` that the source holds and the mirror lacks.
async fn copy_missing(shared: &Shared, prefix: &str) -> Result<()> {
    let (source, copied) =
        tokio::try_join!(shared.source.list(prefix), shared.target.list(prefix))?;
    let copied: std::collections::HashSet<_> = copied.into_iter().collect();
    let mut missing: Vec<_> = source
        .into_iter()
        .filter(|key| !copied.contains(key))
        .collect();
    missing.sort();
    for key in missing {
        if let Some(object) = shared.source.get(&key).await? {
            shared
                .target
                .mirror(&key, object.generation, object.bytes)
                .await?;
        }
    }
    Ok(())
}

async fn report(shared: Arc<Shared>, stop: CancellationToken) {
    let mut ticker = tokio::time::interval(Duration::from_secs(10));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = stop.cancelled() => return,
            _ = ticker.tick() => {}
        }
        log_stats(&shared);
    }
}

fn log_stats(shared: &Shared) {
    let stats = stats(shared);
    let max_lag_ms = shared.config.max_lag.as_millis() as u64;
    let over = stats.lag_ms > max_lag_ms;
    tracing::info!(
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
            max_lag_ms,
            pending = stats.pending,
            "regional checkpoint copy is behind the single-zone bucket"
        );
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

    /// After a write with an unknown outcome, queues what the source now holds in the write's
    /// place: a retried request can report a lost precondition after the first one succeeded
    /// (see `super::replace`), and later writes may depend on it.
    async fn record_current(&self, key: &str) {
        match self.source.get(key).await {
            Ok(Some(object)) => self
                .checkpointer
                .record(key, object.generation, object.bytes),
            Ok(None) => {}
            Err(_) => self.checkpointer.record_uncertain(key),
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
        bytes: crate::payload::Payload,
    ) -> Result<bool> {
        let result = self
            .source
            .compare_and_swap(key, generation, bytes.clone())
            .await;
        let next = generation.map_or(Some(1), |expected| expected.checked_add(1));
        match (&result, next) {
            (Ok(true), Some(next)) => self.checkpointer.record(key, next, bytes),
            _ => self.record_current(key).await,
        }
        result
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.source.list(prefix).await
    }
}

#[cfg(test)]
#[path = "../../tests/unit/bucket/checkpoint.rs"]
mod tests;
