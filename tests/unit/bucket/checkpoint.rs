use std::{
    collections::{BTreeMap, HashSet},
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize},
    },
};

use anyhow::ensure;
use bytes::Bytes;

use crate::payload::Payload;

use super::*;

/// An in-memory bucket numbering generations like S3Bucket: a create is 1, each replace adds one.
/// As a mirror it records the order copies land in, and counts copies that arrive before what
/// they depend on (see [`dependency`]).
#[derive(Default)]
struct Memory {
    objects: Mutex<BTreeMap<String, (i64, Bytes)>>,
    log: Mutex<Vec<(String, i64)>>,
    failing: AtomicBool,
    fail_keys: Mutex<HashSet<String>>,
    gated: AtomicBool,
    mirrors: AtomicUsize,
    list_failures: AtomicUsize,
    violations: AtomicUsize,
    /// Random failure rate in percent for mirror writes, and for source writes that succeed but
    /// report an error.
    chaos: AtomicU64,
    seed: AtomicU64,
}

impl Memory {
    fn object(&self, key: &str) -> Option<(i64, Bytes)> {
        self.objects.lock().unwrap().get(key).cloned()
    }

    fn put(&self, key: &str, generation: i64, bytes: impl Into<Bytes>) {
        self.objects
            .lock()
            .unwrap()
            .insert(key.into(), (generation, bytes.into()));
    }

    fn log(&self) -> Vec<(String, i64)> {
        self.log.lock().unwrap().clone()
    }

    fn unlucky(&self) -> bool {
        let percent = self.chaos.load(Ordering::Relaxed);
        if percent == 0 {
            return false;
        }
        let next = self
            .seed
            .fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed)
            .wrapping_mul(0xBF58_476D_1CE4_E5B9);
        (next >> 33) % 100 < percent
    }
}

#[async_trait]
impl Bucket for Memory {
    async fn get(&self, key: &str) -> Result<Option<BucketObject>> {
        Ok(self.object(key).map(|(generation, bytes)| BucketObject {
            generation,
            bytes: bytes.into(),
        }))
    }

    async fn range(&self, key: &str, start: u64, length: u64) -> Result<Option<BucketObject>> {
        Ok(self.object(key).map(|(generation, bytes)| BucketObject {
            generation,
            bytes: bytes
                .slice(start as usize..(start + length) as usize)
                .into(),
        }))
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        generation: Option<i64>,
        bytes: Payload,
    ) -> Result<bool> {
        let bytes = bytes.to_bytes()?;
        {
            let mut objects = self.objects.lock().unwrap();
            let current = objects.get(key).map(|(generation, _)| *generation);
            if current != generation {
                return Ok(false);
            }
            objects.insert(key.into(), (generation.map_or(1, |g| g + 1), bytes));
        }
        ensure!(!self.unlucky(), "reply lost after the write");
        Ok(true)
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        if self
            .list_failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            anyhow::bail!("list throttled");
        }
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
    async fn mirror(&self, key: &str, generation: i64, bytes: Payload) -> Result<()> {
        let bytes = bytes.to_bytes()?;
        self.mirrors.fetch_add(1, Ordering::SeqCst);
        while self.gated.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        ensure!(!self.failing.load(Ordering::SeqCst), "regional S3 is down");
        ensure!(
            !self.fail_keys.lock().unwrap().contains(key),
            "regional S3 refused {key}"
        );
        ensure!(!self.unlucky(), "regional S3 timed out");
        let mut objects = self.objects.lock().unwrap();
        if let Some(needed) = dependency(&bytes)
            && !objects.contains_key(&needed)
        {
            self.violations.fetch_add(1, Ordering::SeqCst);
        }
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

/// Test payloads name what they depend on: `after:<key>` (a SQLite parent, or the snapshot an
/// owner record points at).
fn dependency(bytes: &[u8]) -> Option<String> {
    std::str::from_utf8(bytes)
        .ok()?
        .strip_prefix("after:")
        .map(str::to_owned)
}

fn snapshot(actor: &str, n: u32) -> String {
    format!("durable-actors/v3/snapshots/00/p/n/{actor}/{n:04}.json")
}

fn owner(actor: &str) -> String {
    format!("durable-actors/v3/owners/00/p/n/{actor}.json")
}

fn snapshots(actor: &str) -> String {
    format!("durable-actors/v3/snapshots/00/p/n/{actor}/")
}

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
    Instant::now() + Duration::from_secs(10)
}

async fn create(bucket: &dyn Bucket, key: &str, bytes: impl Into<Bytes>) -> Result<()> {
    ensure!(
        bucket
            .compare_and_swap(key, None, Payload::from(bytes.into()))
            .await?
    );
    Ok(())
}

/// Blocks the mirror until its first copy is in flight, so later writes queue behind it.
async fn hold(copy: &Memory) {
    copy.gated.store(true, Ordering::SeqCst);
    while copy.mirrors.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn writes_are_copied_with_their_generation() -> Result<()> {
    let f = fixture(CheckpointConfig::default());
    let key = owner("a");
    create(&f.bucket, &key, "v1").await?;
    assert!(
        f.bucket
            .compare_and_swap(&key, Some(1), Bytes::from_static(b"v2").into())
            .await?
    );
    assert!(f.checkpointer.flush(soon()).await);
    assert_eq!(f.copy.object(&key), Some((2, Bytes::from_static(b"v2"))));
    let stats = f.checkpointer.stats();
    assert_eq!(
        (stats.pending, stats.lag_ms, stats.pending_bytes),
        (0, 0, 0)
    );
    Ok(())
}

#[tokio::test]
async fn a_lost_compare_and_swap_copies_what_the_source_holds() -> Result<()> {
    let f = fixture(CheckpointConfig::default());
    let key = owner("a");
    f.source.put(&key, 3, "theirs");
    assert!(
        !f.bucket
            .compare_and_swap(&key, Some(1), Bytes::from_static(b"mine").into())
            .await?
    );
    assert!(f.checkpointer.flush(soon()).await);
    assert_eq!(
        f.copy.object(&key),
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
        create(&f.bucket, &snapshot("a", i), "s").await?;
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
    create(&f.bucket, &snapshot("a", 1), "blocker").await?;
    hold(&f.copy).await;
    let key = owner("a");
    create(&f.bucket, &key, "v1").await?;
    for (generation, bytes) in [(1, "v2"), (2, "v3")] {
        f.bucket
            .compare_and_swap(&key, Some(generation), Bytes::from(bytes).into())
            .await?;
    }
    assert_eq!(f.checkpointer.stats().pending, 2);
    f.copy.gated.store(false, Ordering::SeqCst);
    assert!(f.checkpointer.flush(soon()).await);
    assert_eq!(f.copy.log(), vec![(snapshot("a", 1), 1), (key, 3)]);
    Ok(())
}

#[tokio::test]
async fn one_actors_failed_copy_holds_back_its_later_writes_but_not_other_actors() -> Result<()> {
    let f = fixture(CheckpointConfig::default());
    f.copy.fail_keys.lock().unwrap().insert(snapshot("a", 1));
    create(&f.bucket, &snapshot("a", 1), "parent").await?;
    create(
        &f.bucket,
        &snapshot("a", 2),
        format!("after:{}", snapshot("a", 1)),
    )
    .await?;
    create(
        &f.bucket,
        &owner("a"),
        format!("after:{}", snapshot("a", 2)),
    )
    .await?;
    create(&f.bucket, &snapshot("b", 1), "independent").await?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(f.copy.log(), vec![(snapshot("b", 1), 1)]);
    assert!(f.checkpointer.stats().failures_total >= 1);
    f.copy.fail_keys.lock().unwrap().clear();
    assert!(f.checkpointer.flush(soon()).await);
    let log = f.copy.log();
    assert_eq!(
        &log[1..],
        &[
            (snapshot("a", 1), 1),
            (snapshot("a", 2), 1),
            (owner("a"), 1)
        ]
    );
    assert_eq!(f.copy.violations.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn a_late_completion_never_replaces_a_newer_queued_write() -> Result<()> {
    let f = fixture(CheckpointConfig::default());
    create(&f.bucket, &snapshot("a", 1), "blocker").await?;
    hold(&f.copy).await;
    let key = owner("a");
    f.checkpointer
        .record(&key, 2, Bytes::from_static(b"two").into());
    f.checkpointer
        .record(&key, 1, Bytes::from_static(b"one").into());
    f.copy.gated.store(false, Ordering::SeqCst);
    assert!(f.checkpointer.flush(soon()).await);
    assert_eq!(f.copy.object(&key), Some((2, Bytes::from_static(b"two"))));
    Ok(())
}

#[tokio::test]
async fn reading_back_a_newer_owner_waits_for_the_snapshot_it_names() -> Result<()> {
    let f = fixture(CheckpointConfig {
        max_pending_bytes: 0,
        ..Default::default()
    });
    create(&f.bucket, &snapshot("a", 1), "blocker").await?;
    hold(&f.copy).await;
    let key = owner("a");
    create(&f.bucket, &key, format!("after:{}", snapshot("a", 1))).await?;
    // A newer owner record lands on the source before its write is recorded, and its snapshot is
    // recorded after the queued (payload-free) generation 1 entry.
    f.source.put(&key, 2, format!("after:{}", snapshot("a", 2)));
    create(
        &f.bucket,
        &snapshot("a", 2),
        format!("after:{}", snapshot("a", 1)),
    )
    .await?;
    f.copy.gated.store(false, Ordering::SeqCst);
    assert!(f.checkpointer.flush(soon()).await);
    assert_eq!(
        f.copy.log(),
        vec![(snapshot("a", 1), 1), (snapshot("a", 2), 1), (key, 2)]
    );
    assert_eq!(f.copy.violations.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn reconciliation_runs_first_and_retries_until_it_succeeds() -> Result<()> {
    let f = fixture(CheckpointConfig::default());
    // Left on the source by a host that died before copying it.
    f.source.put(&snapshot("a", 7), 1, "orphan");
    f.copy.list_failures.store(2, Ordering::SeqCst);
    f.checkpointer.reconcile_in_background(&snapshots("a"));
    create(
        &f.bucket,
        &owner("a"),
        format!("after:{}", snapshot("a", 7)),
    )
    .await?;
    create(&f.bucket, &snapshot("b", 1), "independent").await?;
    assert!(f.checkpointer.flush(soon()).await);
    let order: Vec<_> = f
        .copy
        .log()
        .into_iter()
        .map(|(key, _)| key)
        .filter(|key| key.contains("/a"))
        .collect();
    assert_eq!(order, vec![snapshot("a", 7), owner("a")]);
    assert_eq!(f.copy.violations.load(Ordering::SeqCst), 0);
    assert_eq!(f.copy.list_failures.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn payloads_over_the_memory_bound_are_read_back_from_the_source() -> Result<()> {
    let f = fixture(CheckpointConfig {
        max_pending_bytes: 4,
        ..Default::default()
    });
    create(&f.bucket, &snapshot("a", 1), "blocker").await?;
    hold(&f.copy).await;
    create(&f.bucket, &owner("a"), "far too large").await?;
    assert_eq!(f.checkpointer.stats().pending_bytes, 0);
    f.copy.gated.store(false, Ordering::SeqCst);
    assert!(f.checkpointer.flush(soon()).await);
    assert_eq!(f.copy.object(&owner("a")), f.source.object(&owner("a")));
    Ok(())
}

#[tokio::test]
async fn lag_reports_the_oldest_uncopied_write() -> Result<()> {
    let f = fixture(CheckpointConfig::default());
    f.copy.failing.store(true, Ordering::SeqCst);
    let key = owner("a");
    create(&f.bucket, &key, "v1").await?;
    tokio::time::sleep(Duration::from_millis(120)).await;
    f.bucket
        .compare_and_swap(&key, Some(1), Bytes::from_static(b"v2").into())
        .await?;
    let stats = f.checkpointer.stats();
    assert_eq!(stats.pending, 1);
    assert!(stats.lag_ms >= 100, "lag {} ms", stats.lag_ms);
    f.copy.failing.store(false, Ordering::SeqCst);
    assert!(f.checkpointer.flush(soon()).await);
    assert_eq!(f.checkpointer.stats().lag_ms, 0);
    assert_eq!(f.copy.object(&key).unwrap().0, 2);
    Ok(())
}

#[tokio::test]
async fn a_late_copy_never_overwrites_a_newer_one() -> Result<()> {
    let copy = Memory::default();
    copy.put(&owner("a"), 7, "newer");
    copy.mirror(&owner("a"), 6, Bytes::from_static(b"older").into())
        .await?;
    assert_eq!(
        copy.object(&owner("a")),
        Some((7, Bytes::from_static(b"newer")))
    );
    Ok(())
}

/// Several actors write snapshot chains and owner records concurrently while the mirror fails at
/// random, source replies are lost after successful writes, and payloads are dropped for memory.
/// No copy may ever land before what it depends on, and the copy must end identical.
#[tokio::test]
async fn random_failures_never_expose_a_copy_before_its_dependencies() -> Result<()> {
    for seed in 0..12u64 {
        let f = fixture(CheckpointConfig {
            max_pending_bytes: if seed % 2 == 0 { 64 } else { 64 << 20 },
            concurrency: 3,
            ..Default::default()
        });
        f.copy.seed.store(seed * 7919 + 1, Ordering::Relaxed);
        f.copy.chaos.store(30, Ordering::Relaxed);
        f.source.seed.store(seed * 104_729 + 3, Ordering::Relaxed);
        f.source.chaos.store(10, Ordering::Relaxed);
        let bucket = Arc::new(f.bucket);
        let writers = ["a", "b", "c", "d"].map(|actor| {
            let bucket = bucket.clone();
            tokio::spawn(async move { write_chain(bucket.as_ref(), actor, 25).await })
        });
        for writer in writers {
            writer.await??;
        }
        f.copy.chaos.store(0, Ordering::Relaxed);
        assert!(
            f.checkpointer.flush(soon()).await,
            "seed {seed} never drained"
        );
        assert_eq!(
            f.copy.violations.load(Ordering::SeqCst),
            0,
            "seed {seed}: a copy landed before its dependency"
        );
        assert_eq!(
            f.copy.objects.lock().unwrap().clone(),
            f.source.objects.lock().unwrap().clone(),
            "seed {seed}: copy differs from source"
        );
    }
    Ok(())
}

/// Like a host: each state write creates a snapshot naming its parent, then points the owner
/// record at it. Lost replies are treated as success, as `bucket::replace` would after re-reading.
async fn write_chain(bucket: &dyn Bucket, actor: &str, writes: u32) -> Result<()> {
    let key = owner(actor);
    let mut generation = None;
    for n in 1..=writes {
        let parent = if n == 1 {
            "root".to_owned()
        } else {
            format!("after:{}", snapshot(actor, n - 1))
        };
        let _ = bucket
            .compare_and_swap(&snapshot(actor, n), None, Bytes::from(parent).into())
            .await;
        let record = Bytes::from(format!("after:{}", snapshot(actor, n)));
        let _ = bucket
            .compare_and_swap(&key, generation, record.into())
            .await;
        generation = Some(generation.map_or(1, |g| g + 1));
        tokio::task::yield_now().await;
    }
    Ok(())
}

#[tokio::test]
async fn restore_copies_a_prefix_keeping_generations_and_is_rerunnable() -> Result<()> {
    let standard = Memory::default();
    standard.put(&owner("a"), 9, "owner");
    standard.put(&snapshot("a", 1), 1, "snapshot");
    standard.put(&snapshot("b", 1), 1, "other");
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
        express.object(&owner("a")),
        Some((9, Bytes::from_static(b"owner")))
    );
    assert!(
        express
            .compare_and_swap(&owner("a"), Some(9), Bytes::from_static(b"next").into())
            .await?
    );
    let again = restore(&standard, &express, "durable-actors/v3/snapshots/", 4).await?;
    assert_eq!(again.copied, 2);
    assert_eq!(express.object(&owner("a")).unwrap().0, 10);
    assert!(restore(&standard, &express, "elsewhere/", 4).await.is_err());
    Ok(())
}

#[tokio::test]
async fn control_plane_owner_records_copy_their_snapshots_first() -> Result<()> {
    let f = fixture(CheckpointConfig {
        reconcile_owners: true,
        ..Default::default()
    });
    // Written by a host that died before copying it; the control plane then takes over.
    f.source
        .put(&snapshot("a", 3), 1, format!("after:{}", snapshot("a", 2)));
    f.source.put(&snapshot("a", 2), 1, "root");
    create(
        &f.bucket,
        &owner("a"),
        format!("after:{}", snapshot("a", 3)),
    )
    .await?;
    assert!(f.checkpointer.flush(soon()).await);
    assert_eq!(
        f.copy.log(),
        vec![
            (snapshot("a", 2), 1),
            (snapshot("a", 3), 1),
            (owner("a"), 1)
        ]
    );
    assert_eq!(f.copy.violations.load(Ordering::SeqCst), 0);
    Ok(())
}
