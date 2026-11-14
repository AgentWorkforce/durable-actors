use std::{
    collections::BTreeMap,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize},
    },
};

use bytes::Bytes;

use super::*;

/// An in-memory bucket numbering generations like S3Bucket: a create is 1, each replace adds one.
#[derive(Default)]
struct Memory {
    objects: Mutex<BTreeMap<String, (i64, Bytes)>>,
    /// Mirror writes, in the order they landed.
    log: Mutex<Vec<(String, i64)>>,
    /// While set, mirror writes fail.
    failing: AtomicBool,
    /// While set, mirror writes wait.
    gated: AtomicBool,
    mirrors: AtomicUsize,
}

impl Memory {
    fn object(&self, key: &str) -> Option<(i64, Bytes)> {
        self.objects.lock().unwrap().get(key).cloned()
    }

    fn put(&self, key: &str, generation: i64, bytes: &'static [u8]) {
        self.objects
            .lock()
            .unwrap()
            .insert(key.into(), (generation, Bytes::from_static(bytes)));
    }
}

#[async_trait]
impl Bucket for Memory {
    async fn get(&self, key: &str) -> Result<Option<BucketObject>> {
        Ok(self
            .object(key)
            .map(|(generation, bytes)| BucketObject { generation, bytes }))
    }

    async fn range(&self, key: &str, start: u64, length: u64) -> Result<Option<BucketObject>> {
        Ok(self.object(key).map(|(generation, bytes)| BucketObject {
            generation,
            bytes: bytes.slice(start as usize..(start + length) as usize),
        }))
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        generation: Option<i64>,
        bytes: Bytes,
    ) -> Result<bool> {
        let mut objects = self.objects.lock().unwrap();
        let current = objects.get(key).map(|(generation, _)| *generation);
        if current != generation {
            return Ok(false);
        }
        objects.insert(key.into(), (generation.map_or(1, |g| g + 1), bytes));
        Ok(true)
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        Ok(self
            .objects
            .lock()
            .unwrap()
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect())
    }
}

#[async_trait]
impl Mirror for Memory {
    async fn mirror(&self, key: &str, generation: i64, bytes: Bytes) -> Result<()> {
        self.mirrors.fetch_add(1, Ordering::SeqCst);
        while self.gated.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        ensure!(!self.failing.load(Ordering::SeqCst), "regional S3 is down");
        let mut objects = self.objects.lock().unwrap();
        if objects
            .get(key)
            .is_some_and(|(current, _)| *current >= generation)
        {
            return Ok(());
        }
        objects.insert(key.into(), (generation, bytes));
        self.log.lock().unwrap().push((key.into(), generation));
        Ok(())
    }
}

const SNAPSHOT: &str = "durable-actors/v3/snapshots/a/1";
const OWNER: &str = "durable-actors/v3/owners/a.json";

struct Fixture {
    source: Arc<Memory>,
    copy: Arc<Memory>,
    checkpointer: Arc<Checkpointer>,
    bucket: CheckpointedBucket,
    _stop: CancellationToken,
}

fn fixture(config: CheckpointConfig) -> Fixture {
    let source = Arc::new(Memory::default());
    let copy = Arc::new(Memory::default());
    let stop = CancellationToken::new();
    let checkpointer = Checkpointer::spawn(source.clone(), copy.clone(), config, &stop);
    let bucket = CheckpointedBucket::new(source.clone(), checkpointer.clone());
    Fixture {
        source,
        copy,
        checkpointer,
        bucket,
        _stop: stop,
    }
}

fn soon() -> Instant {
    Instant::now() + Duration::from_secs(5)
}

#[tokio::test]
async fn writes_are_copied_with_their_generation() -> Result<()> {
    let f = fixture(CheckpointConfig::default());
    assert!(
        f.bucket
            .compare_and_swap(OWNER, None, Bytes::from_static(b"v1"))
            .await?
    );
    assert!(
        f.bucket
            .compare_and_swap(OWNER, Some(1), Bytes::from_static(b"v2"))
            .await?
    );
    assert!(f.checkpointer.flush(soon()).await);
    assert_eq!(f.copy.object(OWNER), Some((2, Bytes::from_static(b"v2"))));
    assert_eq!(f.copy.object(OWNER), f.source.object(OWNER));
    let stats = f.checkpointer.stats();
    assert_eq!(
        (stats.pending, stats.lag_ms, stats.pending_bytes),
        (0, 0, 0)
    );
    Ok(())
}

#[tokio::test]
async fn a_lost_compare_and_swap_is_not_queued_as_a_write() -> Result<()> {
    let f = fixture(CheckpointConfig::default());
    f.source.put(OWNER, 3, b"theirs");
    assert!(
        !f.bucket
            .compare_and_swap(OWNER, Some(1), Bytes::from_static(b"mine"))
            .await?
    );
    assert!(f.checkpointer.flush(soon()).await);
    // The uncertain outcome copies whatever the source now holds, never the rejected bytes.
    assert_eq!(
        f.copy.object(OWNER),
        Some((3, Bytes::from_static(b"theirs")))
    );
    Ok(())
}

#[tokio::test]
async fn the_write_path_never_waits_for_the_copy() -> Result<()> {
    let f = fixture(CheckpointConfig::default());
    f.copy.gated.store(true, Ordering::SeqCst);
    let started = std::time::Instant::now();
    for i in 0..50 {
        let key = format!("durable-actors/v3/snapshots/a/{i}");
        assert!(
            f.bucket
                .compare_and_swap(&key, None, Bytes::from_static(b"s"))
                .await?
        );
    }
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(f.checkpointer.stats().pending >= 1);
    assert!(!f.checkpointer.flush(Instant::now()).await);
    f.copy.gated.store(false, Ordering::SeqCst);
    assert!(f.checkpointer.flush(soon()).await);
    assert_eq!(f.copy.list("durable-actors/v3/").await?.len(), 50);
    Ok(())
}

#[tokio::test]
async fn queued_writes_of_one_key_coalesce_to_the_latest() -> Result<()> {
    let f = fixture(CheckpointConfig::default());
    f.copy.gated.store(true, Ordering::SeqCst);
    f.bucket
        .compare_and_swap(SNAPSHOT, None, Bytes::from_static(b"blocker"))
        .await?;
    // Wait until the blocker is in flight, then queue three writes of the owner.
    while f.copy.mirrors.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    f.bucket
        .compare_and_swap(OWNER, None, Bytes::from_static(b"v1"))
        .await?;
    f.bucket
        .compare_and_swap(OWNER, Some(1), Bytes::from_static(b"v2"))
        .await?;
    f.bucket
        .compare_and_swap(OWNER, Some(2), Bytes::from_static(b"v3"))
        .await?;
    assert_eq!(f.checkpointer.stats().pending, 2);
    f.copy.gated.store(false, Ordering::SeqCst);
    assert!(f.checkpointer.flush(soon()).await);
    let log = f.copy.log.lock().unwrap().clone();
    assert_eq!(
        log,
        vec![(SNAPSHOT.into(), 1), (OWNER.into(), 3)],
        "one copy of the owner, at its latest generation"
    );
    Ok(())
}

#[tokio::test]
async fn creates_are_copied_before_the_replaces_that_follow_them() -> Result<()> {
    let f = fixture(CheckpointConfig::default());
    f.source.put(OWNER, 4, b"old");
    f.copy.put(OWNER, 4, b"old");
    f.copy.failing.store(true, Ordering::SeqCst);
    f.bucket
        .compare_and_swap(SNAPSHOT, None, Bytes::from_static(b"snapshot"))
        .await?;
    f.bucket
        .compare_and_swap(OWNER, Some(4), Bytes::from_static(b"points at snapshot"))
        .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    // The snapshot copy failed, so the owner record that references it was held back.
    assert_eq!(f.copy.object(OWNER), Some((4, Bytes::from_static(b"old"))));
    assert!(f.checkpointer.stats().failures_total >= 1);
    f.copy.failing.store(false, Ordering::SeqCst);
    assert!(f.checkpointer.flush(soon()).await);
    let log = f.copy.log.lock().unwrap().clone();
    assert_eq!(log, vec![(SNAPSHOT.into(), 1), (OWNER.into(), 5)]);
    Ok(())
}

#[tokio::test]
async fn lag_reports_the_oldest_uncopied_write() -> Result<()> {
    let f = fixture(CheckpointConfig::default());
    f.copy.failing.store(true, Ordering::SeqCst);
    f.bucket
        .compare_and_swap(OWNER, None, Bytes::from_static(b"v1"))
        .await?;
    tokio::time::sleep(Duration::from_millis(120)).await;
    // A newer write of the same key keeps the age of the first uncopied one.
    f.bucket
        .compare_and_swap(OWNER, Some(1), Bytes::from_static(b"v2"))
        .await?;
    let stats = f.checkpointer.stats();
    assert_eq!(stats.pending, 1);
    assert!(stats.lag_ms >= 100, "lag {} ms", stats.lag_ms);
    f.copy.failing.store(false, Ordering::SeqCst);
    assert!(f.checkpointer.flush(soon()).await);
    assert_eq!(f.checkpointer.stats().lag_ms, 0);
    assert_eq!(f.copy.object(OWNER).unwrap().0, 2);
    Ok(())
}

#[tokio::test]
async fn payloads_over_the_memory_bound_are_read_back_from_the_source() -> Result<()> {
    let f = fixture(CheckpointConfig {
        max_pending_bytes: 4,
        ..Default::default()
    });
    f.copy.gated.store(true, Ordering::SeqCst);
    f.bucket
        .compare_and_swap(SNAPSHOT, None, Bytes::from_static(b"blocker"))
        .await?;
    while f.copy.mirrors.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    f.bucket
        .compare_and_swap(OWNER, None, Bytes::from_static(b"far too large"))
        .await?;
    assert_eq!(f.checkpointer.stats().pending_bytes, 0);
    f.copy.gated.store(false, Ordering::SeqCst);
    assert!(f.checkpointer.flush(soon()).await);
    assert_eq!(f.copy.object(OWNER), f.source.object(OWNER));
    Ok(())
}

#[tokio::test]
async fn a_late_copy_never_overwrites_a_newer_one() -> Result<()> {
    let copy = Memory::default();
    copy.put(OWNER, 7, b"newer");
    copy.mirror(OWNER, 6, Bytes::from_static(b"older")).await?;
    assert_eq!(copy.object(OWNER), Some((7, Bytes::from_static(b"newer"))));
    Ok(())
}

#[tokio::test]
async fn restore_copies_a_prefix_keeping_generations_and_is_rerunnable() -> Result<()> {
    let standard = Memory::default();
    standard.put(OWNER, 9, b"owner");
    standard.put(SNAPSHOT, 1, b"snapshot");
    standard.put("durable-actors/v3/snapshots/b/1", 1, b"other");
    let express = Memory::default();
    let report = restore(&standard, &express, "durable-actors/v3/", 4).await?;
    assert_eq!(
        report,
        RestoreReport {
            listed: 3,
            copied: 3,
            missing: 0
        }
    );
    assert_eq!(
        express.object(OWNER),
        Some((9, Bytes::from_static(b"owner")))
    );
    // The restored bucket accepts the next compare-and-swap at the restored generation.
    assert!(
        express
            .compare_and_swap(OWNER, Some(9), Bytes::from_static(b"next"))
            .await?
    );
    let again = restore(&standard, &express, "durable-actors/v3/snapshots/", 4).await?;
    assert_eq!(again.copied, 2);
    assert_eq!(
        express.object(OWNER).unwrap().0,
        10,
        "re-run kept the newer owner"
    );
    assert!(restore(&standard, &express, "elsewhere/", 4).await.is_err());
    Ok(())
}

#[tokio::test]
async fn reconcile_copies_snapshots_a_crashed_host_never_copied() -> Result<()> {
    let f = fixture(CheckpointConfig::default());
    // Written to the source by a host that died before its queue drained.
    f.source.put(SNAPSHOT, 1, b"orphan");
    f.source
        .put("durable-actors/v3/snapshots/a/2", 1, b"copied");
    f.copy.put("durable-actors/v3/snapshots/a/2", 1, b"copied");
    f.source
        .put("durable-actors/v3/snapshots/b/1", 1, b"other actor");
    assert_eq!(
        f.checkpointer
            .reconcile("durable-actors/v3/snapshots/a/")
            .await?,
        1
    );
    assert!(f.checkpointer.flush(soon()).await);
    assert_eq!(
        f.copy.object(SNAPSHOT),
        Some((1, Bytes::from_static(b"orphan")))
    );
    assert_eq!(f.copy.object("durable-actors/v3/snapshots/b/1"), None);
    Ok(())
}
