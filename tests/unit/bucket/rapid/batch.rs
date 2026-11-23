use super::*;
use crate::bucket::{ArchiveBatchConfig, BucketObject};
use crate::storage::VerifiedSnapshot;

struct ArchiveBucket {
    inner: Arc<FileBucket>,
    stalled: AtomicBool,
    fail_batch: AtomicBool,
    batches: AtomicU64,
    read_requests: AtomicU64,
    read_batches: AtomicU64,
    version_reads: AtomicU64,
    /// Lists (`L`) and index reads (`I`) in the order they happen.
    ops: std::sync::Mutex<Vec<char>>,
    range_bytes: AtomicU64,
    index_failure: AtomicU64,
    fail_checkpoint: AtomicBool,
    stalled_checkpoint: AtomicBool,
    checkpoint_attempts: AtomicU64,
}

#[async_trait]
impl Bucket for ArchiveBucket {
    async fn range(
        &self,
        key: &str,
        start: u64,
        length: u64,
    ) -> Result<Option<crate::bucket::BucketObject>> {
        self.read_batches.fetch_add(1, Ordering::SeqCst);
        self.range_bytes.fetch_add(length, Ordering::SeqCst);
        self.inner.range(key, start, length).await
    }

    async fn get(&self, key: &str) -> Result<Option<BucketObject>> {
        if key.ends_with(".idx") {
            self.ops.lock().unwrap().push('I');
            match self.index_failure.load(Ordering::SeqCst) {
                1 => return Ok(None),
                2 => anyhow::bail!("injected index read failure"),
                _ => {}
            }
        }
        self.read_requests.fetch_add(1, Ordering::SeqCst);
        if key.ends_with(".batch") {
            self.read_batches.fetch_add(1, Ordering::SeqCst);
        }
        if key.ends_with(".json") {
            self.version_reads.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get(key).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        ensure!(
            !prefix.ends_with("index~") || self.index_failure.load(Ordering::SeqCst) != 3,
            "injected index list failure"
        );
        self.read_requests.fetch_add(1, Ordering::SeqCst);
        self.ops.lock().unwrap().push('L');
        self.inner.list(prefix).await
    }
    async fn compare_and_swap(
        &self,
        key: &str,
        generation: Option<i64>,
        bytes: Payload,
    ) -> Result<bool> {
        if key.ends_with(".checkpoint") {
            self.checkpoint_attempts.fetch_add(1, Ordering::SeqCst);
            while self.stalled_checkpoint.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
        ensure!(
            !key.ends_with(".checkpoint") || !self.fail_checkpoint.load(Ordering::SeqCst),
            "injected checkpoint failure"
        );
        if key.ends_with(".batch") {
            self.batches.fetch_add(1, Ordering::SeqCst);
            ensure!(
                !self.fail_batch.swap(false, Ordering::SeqCst),
                "injected archive failure"
            );
            while self.stalled.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
        self.inner.compare_and_swap(key, generation, bytes).await
    }
}

fn controlled(
    f: &Fixture,
    bytes: usize,
    interval_ms: u64,
) -> Result<(RapidSnapshots, Arc<ArchiveBucket>)> {
    let archive = Arc::new(ArchiveBucket {
        inner: f.archive.clone(),
        stalled: AtomicBool::new(false),
        fail_batch: AtomicBool::new(false),
        batches: AtomicU64::new(0),
        read_requests: AtomicU64::new(0),
        read_batches: AtomicU64::new(0),
        version_reads: AtomicU64::new(0),
        ops: std::sync::Mutex::new(Vec::new()),
        range_bytes: AtomicU64::new(0),
        index_failure: AtomicU64::new(0),
        fail_checkpoint: AtomicBool::new(false),
        stalled_checkpoint: AtomicBool::new(false),
        checkpoint_attempts: AtomicU64::new(0),
    });
    let store = RapidSnapshots::new(
        archive.clone(),
        Arc::new(BucketSnapshots(archive.clone())),
        f.zones
            .iter()
            .map(|z| z.clone() as Arc<dyn LogZone>)
            .collect(),
        f.placement,
        ArchiveBatchConfig { bytes, interval_ms },
        Arc::new(crate::litestream::compaction::RustCompactor),
        CancellationToken::new(),
    )?;
    Ok((store, archive))
}

async fn batches(f: &Fixture, count: usize) -> Result<Vec<String>> {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let keys = f
                .archive
                .list("")
                .await?
                .into_iter()
                .filter(|k| k.ends_with(".batch"))
                .collect::<Vec<_>>();
            if keys.len() >= count {
                return Ok(keys);
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await?
}

#[tokio::test]
async fn live_checkpoint_inputs_are_read_as_one_bounded_range() -> Result<()> {
    let f = Fixture::new()?;
    let (writer, _) = controlled(&f, 16 * 1024 * 1024, 60_000)?;
    let (replication, history) = write_history(&f, &writer, 31, None).await?;
    let sqlite = writer
        .restore_prefetched(
            VerifiedSnapshot::named(f.stream.object(31), history[30].clone())?,
            None,
        )
        .await?;
    assert_eq!(
        crate::litestream::storage::restored_fields(
            replication.as_ref(),
            &sqlite.files,
            sqlite.txid
        )
        .await?,
        serde_json::json!({"count":31})
    );
    assert_eq!(
        f.zones
            .iter()
            .map(|zone| zone.range_reads.load(Ordering::SeqCst))
            .sum::<u64>(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn checkpoints_compact_sqlite_history_for_cold_recovery() -> Result<()> {
    use crate::litestream::storage::{CaptureDirectory, SqliteCapture, restored_fields};
    let f = Fixture::new()?;
    let (writer, gate) = controlled(&f, 16 * 1024 * 1024, 10_000)?;
    gate.stalled_checkpoint.store(true, Ordering::SeqCst);
    let (replication, history) = write_history(&f, &writer, 35, Some(32)).await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let (progress, released) = tokio::join!(
        writer.finish(&f.stream, deadline),
        tokio::time::timeout_at(deadline, async {
            while f.archive.get(&f.stream.object(35)).await?.is_none() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            gate.stalled_checkpoint.store(false, Ordering::SeqCst);
            anyhow::Ok(())
        })
    );
    released??;
    let progress = progress?.unwrap();
    assert_eq!(progress.version, 32);
    assert_eq!(
        progress.log_bytes,
        history[32..].iter().map(|b| b.len() as u64).sum::<u64>()
    );
    assert_eq!(gate.checkpoint_attempts.load(Ordering::SeqCst), 1);
    assert!(
        f.archive
            .get(&format!("{}.checkpoint", f.stream.object(35)))
            .await?
            .is_none()
    );
    f.cleaned().await?;

    let (reader, archive) = controlled(&f, 16 * 1024 * 1024, 10_000)?;
    let sqlite = reader
        .restore_prefetched(
            VerifiedSnapshot::named(f.stream.object(32), history[31].clone())?,
            None,
        )
        .await?;
    assert_eq!(archive.read_requests.load(Ordering::SeqCst), 1);
    let source = f.stream.snapshot(&history[31])?;
    let prefetched = reader.prefetch_checkpoint(&source).await?;
    let requests = archive.read_requests.load(Ordering::SeqCst);
    let prepared = reader
        .restore_prefetched(
            VerifiedSnapshot::matching(source.clone(), history[31].clone())?,
            prefetched,
        )
        .await?;
    assert_eq!(archive.read_requests.load(Ordering::SeqCst), requests);
    assert_eq!(prepared.txid, sqlite.txid);
    assert_eq!(prepared.files.len(), 1);
    let mut corrupt = reader.prefetch_checkpoint(&source).await?.unwrap();
    let mut checkpoint = crate::bucket::recovery::Checkpoint {
        source: source.clone(),
        sqlite: crate::bucket::recovery::Checkpoint::decode(
            corrupt.bytes.as_ref().unwrap(),
            &source,
            sqlite.txid,
        )?,
    };
    checkpoint.source.digest = "incorrect".into();
    corrupt.bytes = Some(checkpoint.encode()?);
    assert!(
        reader
            .restore_prefetched(
                VerifiedSnapshot::matching(source.clone(), history[31].clone())?,
                Some(corrupt),
            )
            .await
            .is_err()
    );
    assert!(sqlite.parent.is_none());
    assert_eq!(sqlite.files.len(), 1);
    let original_size: usize = history
        .iter()
        .map(|bytes| {
            StateSnapshot::decode(bytes)
                .unwrap()
                .sqlite
                .files
                .iter()
                .map(|file| file.data.len())
                .sum::<usize>()
        })
        .sum();
    assert!(sqlite.files[0].data.len() < original_size);
    let latest = reader
        .restore_prefetched(
            VerifiedSnapshot::named(f.stream.object(35), history[34].clone())?,
            None,
        )
        .await?;
    let restored = SqliteCapture::restore(
        CaptureDirectory::temporary()?,
        replication.clone(),
        &latest.files,
        latest.txid,
        None,
        false,
    )
    .await?;
    let db = rusqlite::Connection::open(restored.path())?;
    assert_eq!(
        crate::litestream::storage::read_fields(&db)?,
        serde_json::json!({"count":35})
    );
    assert_eq!(
        db.query_row("SELECT count(*) FROM entries", [], |r| r.get::<_, i64>(0))?,
        35
    );

    archive.read_batches.store(0, Ordering::SeqCst);
    archive.range_bytes.store(0, Ordering::SeqCst);
    let tail = reader
        .restore_prefetched(
            VerifiedSnapshot::named(f.stream.object(34), history[33].clone())?,
            None,
        )
        .await?;
    assert_eq!(tail.files.len(), 3);
    assert_eq!(archive.read_batches.load(Ordering::SeqCst), 2);
    assert_eq!(
        archive.range_bytes.load(Ordering::SeqCst),
        (history[31].len() + history[32].len() + 2 * frame::HEADER) as u64
    );
    assert_eq!(
        restored_fields(replication.as_ref(), &tail.files, tail.txid).await?,
        serde_json::json!({"count":34})
    );
    archive.read_batches.store(0, Ordering::SeqCst);
    let older = reader
        .restore_prefetched(
            VerifiedSnapshot::named(f.stream.object(31), history[30].clone())?,
            None,
        )
        .await?;
    assert_eq!(
        restored_fields(replication.as_ref(), &older.files, older.txid).await?,
        serde_json::json!({"count":31})
    );
    assert_eq!(archive.read_batches.load(Ordering::SeqCst), 1);
    assert_eq!(
        reader.get(&f.stream.object(35)).await?,
        Some(history[34].clone())
    );
    let key = format!("{}.checkpoint", f.stream.object(32));
    let stored = f.archive.get(&key).await?.unwrap();
    let source = f.stream.snapshot(&history[31])?;
    let mut checkpoint = crate::bucket::recovery::Checkpoint {
        sqlite: crate::bucket::recovery::Checkpoint::decode(&stored.bytes, &source, sqlite.txid)?,
        source,
    };
    checkpoint.source.digest = "wrong-source".into();
    assert!(
        f.archive
            .compare_and_swap(
                &key,
                Some(stored.generation),
                crate::payload::encode(&checkpoint)?
            )
            .await?
    );
    assert!(
        reader
            .restore_prefetched(
                VerifiedSnapshot::named(f.stream.object(35), history[34].clone())?,
                None
            )
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn full_checkpoint_upload_allows_slow_transfers_and_bounds_stalls() -> Result<()> {
    let f = Fixture::new()?;
    let (writer, archive) = controlled(&f, 16 * 1024 * 1024, 10_000)?;
    let (_, history) = write_history(&f, &writer, 2, None).await?;
    let head = f.stream.snapshot(&history[1])?;
    writer
        .finish(
            &f.stream,
            tokio::time::Instant::now() + Duration::from_secs(5),
        )
        .await?;

    for (attempt, delay_secs) in [(1, 31), (2, 20)] {
        archive.stalled_checkpoint.store(true, Ordering::SeqCst);
        let upload = writer.storage.checkpoint(&head, Some(history[1].clone()));
        tokio::pin!(upload);
        tokio::select! {
            result = &mut upload => panic!("checkpoint finished before upload started: {result:?}"),
            _ = async {
                while archive.checkpoint_attempts.load(Ordering::SeqCst) < attempt {
                    tokio::task::yield_now().await;
                }
            } => {}
        }
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(delay_secs)).await;
        let result = futures_util::poll!(&mut upload);
        tokio::time::resume();
        if delay_secs == 31 {
            let std::task::Poll::Ready(result) = result else {
                panic!("stalled upload exceeded its deadline");
            };
            assert!(result.unwrap_err().is::<tokio::time::error::Elapsed>());
        } else {
            assert!(result.is_pending(), "slow upload expired: {result:?}");
            archive.stalled_checkpoint.store(false, Ordering::SeqCst);
            assert!(upload.await?.is_some());
        }
    }
    Ok(())
}
#[tokio::test]
async fn failed_checkpoint_publication_keeps_committed_state_recoverable() -> Result<()> {
    for stalled in [false, true] {
        let f = Fixture::new()?;
        let (writer, archive) = controlled(&f, 16 * 1024 * 1024, 10_000)?;
        archive.fail_checkpoint.store(!stalled, Ordering::SeqCst);
        archive.stalled_checkpoint.store(stalled, Ordering::SeqCst);
        let (replication, history) = write_history(&f, &writer, 32, Some(32)).await?;
        let progress = writer
            .finish(
                &f.stream,
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await?;
        let progress = progress.unwrap();
        assert_eq!(progress.version, 0);
        assert_eq!(
            progress.log_bytes,
            history.iter().map(|b| b.len() as u64).sum::<u64>()
        );
        assert!(archive.checkpoint_attempts.load(Ordering::SeqCst) > 0);
        f.cleaned().await?;
        let reader = f.store()?;
        assert_eq!(
            reader.get(&f.stream.object(32)).await?,
            Some(history[31].clone())
        );
        let sqlite = reader
            .restore_prefetched(
                VerifiedSnapshot::named(f.stream.object(32), history[31].clone())?,
                None,
            )
            .await?;
        assert_eq!(
            crate::litestream::storage::restored_fields(
                replication.as_ref(),
                &sqlite.files,
                sqlite.txid
            )
            .await?,
            serde_json::json!({"count":32})
        );
    }
    Ok(())
}

#[tokio::test]
async fn parked_wakes_checkpoint_at_the_version_cap_or_the_byte_threshold() -> Result<()> {
    let source = Fixture::new()?;
    let (_, history) = write_history(&source, &source.store()?, 259, None).await?;
    let f = Fixture::new()?;
    let actor = crate::storage_paths::actor_from_snapshot(&f.stream.object(1))?;
    let mut progress = None;
    let mut parent = None;
    let mut log_bytes = 0;
    let mut completed = 0;
    for bytes in history {
        let mut snapshot = StateSnapshot::decode(&bytes)?;
        let version = snapshot.state_version;
        snapshot.owner_epoch = version;
        snapshot.sqlite.parent = parent;
        if matches!(version, 257 | 258) {
            // Two individually sub-threshold records cross the 1 MiB byte floor on the second wake.
            snapshot.completion.as_mut().unwrap().result =
                serde_json::json!("x".repeat(600 * 1024)).into();
        }
        let key =
            crate::storage::snapshot_object_name(&actor, version, &format!("{version:032x}"))?;
        let stream = StateStream {
            prefix: format!("{}/", key.rsplit_once('/').unwrap().0),
            owner_epoch: version,
            base_version: version - 1,
            ..f.stream.clone()
        };
        let writer = f.store()?;
        writer
            .start(&stream, snapshot.sqlite.parent.as_ref().zip(progress))
            .await?;
        let bytes = snapshot.encode()?;
        parent = Some(stream.snapshot(&bytes)?);
        log_bytes += bytes.len() as u64;
        writer.put(&key, bytes).await?;
        progress = writer
            .finish(
                &stream,
                tokio::time::Instant::now() + Duration::from_secs(5),
            )
            .await?;
        // 256 tiny commits reach the version cap; then 257 and 258 together cross 1 MiB.
        if matches!(version, 256 | 258) {
            completed = version;
            log_bytes = 0;
        }
        let saved = progress.unwrap();
        assert_eq!(saved.version, completed);
        assert_eq!(saved.log_bytes, log_bytes);
        assert_eq!(saved.base_bytes > 0, version >= 256);
    }
    Ok(())
}

#[tokio::test]
async fn resumed_checkpoint_retries_without_new_writes() -> Result<()> {
    assert_resumed_checkpoint_retry(false).await
}

#[tokio::test]
async fn parked_checkpoint_retries_without_new_writes() -> Result<()> {
    assert_resumed_checkpoint_retry(true).await
}

async fn assert_resumed_checkpoint_retry(parked: bool) -> Result<()> {
    let f = Fixture::new()?;
    let (writer, archive) = controlled(&f, 16 * 1024 * 1024, 10_000)?;
    archive.fail_checkpoint.store(true, Ordering::SeqCst);
    let (_replication, history) = write_history(&f, &writer, 33, Some(32)).await?;
    let progress = writer
        .finish(
            &f.stream,
            tokio::time::Instant::now() + Duration::from_secs(5),
        )
        .await?
        .unwrap();
    let (resumed, archive) = controlled(&f, 16 * 1024 * 1024, 10_000)?;
    archive.fail_checkpoint.store(true, Ordering::SeqCst);
    let actor_prefix = f
        .stream
        .prefix
        .trim_end_matches('/')
        .rsplit_once('/')
        .unwrap()
        .0;
    let stream = StateStream {
        prefix: format!("{actor_prefix}/{:032x}/", 2),
        owner_epoch: 2,
        base_version: 33,
        ..f.stream.clone()
    };
    let source = VerifiedSnapshot::named(f.stream.object(33), history[32].clone())?;
    resumed
        .start(&stream, parked.then_some((&source.reference, progress)))
        .await?;
    if !parked {
        resumed.restore_prefetched(source, None).await?;
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        while archive.checkpoint_attempts.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    archive.fail_checkpoint.store(false, Ordering::SeqCst);
    let key = format!("{}.checkpoint", f.stream.object(33));
    tokio::time::timeout(Duration::from_secs(8), async {
        while f.archive.get(&key).await?.is_none() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        anyhow::Ok(())
    })
    .await??;
    assert!(archive.checkpoint_attempts.load(Ordering::SeqCst) >= 2);
    resumed
        .finish(
            &stream,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await?;
    Ok(())
}

async fn write_history(
    f: &Fixture,
    writer: &RapidSnapshots,
    count: u64,
    large_write_at: Option<u64>,
) -> Result<(Arc<crate::litestream::Litestream>, Vec<Payload>)> {
    use crate::litestream::{
        Litestream, Replicator,
        storage::{CaptureDirectory, SqliteCapture},
    };
    writer.start(&f.stream, None).await?;
    let replication = Arc::new(Litestream::default());
    let mut capture =
        SqliteCapture::new(CaptureDirectory::temporary()?, replication.clone(), None).await?;
    let db = rusqlite::Connection::open(capture.path())?;
    db.execute_batch(
        "CREATE TABLE entries(value INTEGER); INSERT INTO __terse_fields VALUES ('count', '0');",
    )?;
    let mut parent = None;
    let mut history = Vec::new();
    for version in 1..=count {
        if Some(version) == large_write_at {
            db.execute_batch("CREATE TABLE large_write AS SELECT randomblob(1048576) AS data")?;
        }
        db.execute("INSERT INTO entries VALUES (?)", [version as i64])?;
        db.execute("UPDATE __terse_fields SET value=?", [version.to_string()])?;
        let txid = replication.sync(&capture.path()).await?;
        let files = capture.capture(txid).await?;
        let snapshot = StateSnapshot::new(
            version,
            1,
            format!("request-{version}"),
            crate::state_log::SqliteSnapshot {
                txid,
                parent,
                files,
            },
            serde_json::json!(version),
        )?;
        let bytes = snapshot.encode()?;
        writer.put(&f.stream.object(version), bytes.clone()).await?;
        parent = Some(f.stream.snapshot(&bytes)?);
        history.push(bytes);
    }
    Ok((replication, history))
}

#[tokio::test]
async fn byte_threshold_combines_commits_without_rotating_rapid_streams() -> Result<()> {
    let f = Fixture::new()?;
    let threshold = [state(1, 1)?, state(2, 1)?]
        .iter()
        .map(|b| b.len() + frame::HEADER)
        .sum();
    let (store, archive) = controlled(&f, threshold, 60_000)?;
    store.start(&f.stream, None).await?;
    store.put(&f.stream.object(1), state(1, 1)?).await?;
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(archive.batches.load(Ordering::SeqCst), 0);
    store.put(&f.stream.object(2), state(2, 1)?).await?;
    let keys = batches(&f, 1).await?;
    let bytes = f.archive.get(&keys[0]).await?.unwrap().bytes;
    assert_eq!(frame::decode(&bytes)?.len(), 2);
    assert_eq!(archive.batches.load(Ordering::SeqCst), 1);
    assert!(f.zones.iter().all(|z| z.opens.load(Ordering::SeqCst) == 1));
    assert!(f.archive.get(&f.stream.object(1)).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn timer_flushes_partial_batches_and_never_uploads_empty_ones() -> Result<()> {
    let f = Fixture::new()?;
    let (store, archive) = controlled(&f, 16 * 1024 * 1024, 40)?;
    store.start(&f.stream, None).await?;
    tokio::time::sleep(Duration::from_millis(70)).await;
    assert_eq!(archive.batches.load(Ordering::SeqCst), 0);
    store.put(&f.stream.object(1), state(1, 1)?).await?;
    batches(&f, 1).await?;
    tokio::time::sleep(Duration::from_millis(70)).await;
    assert_eq!(archive.batches.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn failed_partial_batch_retries_without_another_write() -> Result<()> {
    let f = Fixture::new()?;
    let (store, archive) = controlled(&f, 16 * 1024 * 1024, 40)?;
    archive.fail_batch.store(true, Ordering::SeqCst);
    store.start(&f.stream, None).await?;
    store.put(&f.stream.object(1), state(1, 1)?).await?;
    let keys = batches(&f, 1).await?;
    let bytes = f.archive.get(&keys[0]).await?.unwrap().bytes;
    let records = frame::decode(&bytes)?;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].state, state(1, 1)?);
    assert_eq!(archive.batches.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn stalled_archive_does_not_block_writes_and_backlog_is_recovered_from_rapid() -> Result<()> {
    let f = Fixture::new()?;
    let threshold = state(1, 1)?.len() + frame::HEADER;
    let (store, archive) = controlled(&f, threshold, 60_000)?;
    archive.stalled.store(true, Ordering::SeqCst);
    store.start(&f.stream, None).await?;
    store.put(&f.stream.object(1), state(1, 1)?).await?;
    tokio::time::timeout(Duration::from_secs(1), async {
        while archive.batches.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    for version in 2..=20 {
        tokio::time::timeout(
            Duration::from_secs(1),
            store.put(&f.stream.object(version), state(version, 1)?),
        )
        .await??;
    }
    assert!(archive.batches.load(Ordering::SeqCst) > 0);
    assert!(
        f.zones
            .iter()
            .all(|z| !z.objects.lock().unwrap().is_empty())
    );
    archive.stalled.store(false, Ordering::SeqCst);
    store
        .finish(
            &f.stream,
            tokio::time::Instant::now() + Duration::from_secs(30),
        )
        .await?;
    f.cleaned().await?;
    let reader = f.store()?;
    for version in 1..=20 {
        assert_eq!(
            reader.get(&f.stream.object(version)).await?,
            Some(state(version, 1)?)
        );
    }
    Ok(())
}

#[tokio::test]
async fn crash_recovery_joins_standard_batches_and_unflushed_rapid_tail() -> Result<()> {
    let f = Fixture::new()?;
    let threshold = state(1, 1)?.len() + frame::HEADER;
    let (store, archive) = controlled(&f, threshold, 60_000)?;
    store.start(&f.stream, None).await?;
    store.put(&f.stream.object(1), state(1, 1)?).await?;
    batches(&f, 1).await?;
    archive.stalled.store(true, Ordering::SeqCst);
    store.put(&f.stream.object(2), state(2, 1)?).await?;
    drop(store);
    f.zones[1].offline.store(true, Ordering::SeqCst);
    let reader = f.store()?;
    assert_eq!(
        reader.recover(&f.stream.prefix).await?,
        Some((f.stream.object(2), state(2, 1)?))
    );
    f.zones[0].offline.store(true, Ordering::SeqCst);
    assert_eq!(reader.get(&f.stream.object(1)).await?, Some(state(1, 1)?));
    Ok(())
}

#[tokio::test]
async fn partial_flush_cannot_acknowledge_a_write_and_remains_recoverable() -> Result<()> {
    let f = Fixture::new()?;
    let store = f.store()?;
    store.start(&f.stream, None).await?;
    store.put(&f.stream.object(1), state(1, 1)?).await?;
    f.zones[1].stalled.store(true, Ordering::SeqCst);
    assert!(
        tokio::time::timeout(
            Duration::from_millis(30),
            store.put(&f.stream.object(2), state(2, 1)?)
        )
        .await
        .is_err()
    );
    f.zones[1].stalled.store(false, Ordering::SeqCst);
    assert!(store.put(&f.stream.object(2), state(2, 1)?).await.is_err());
    assert!(
        store
            .finish(
                &f.stream,
                tokio::time::Instant::now() + Duration::from_secs(30)
            )
            .await
            .is_err()
    );
    assert!(f.archive.get(&f.stream.object(2)).await?.is_none());
    assert_eq!(
        f.store()?.recover(&f.stream.prefix).await?,
        Some((f.stream.object(2), state(2, 1)?))
    );
    Ok(())
}

#[tokio::test]
async fn archived_sqlite_parent_and_rapid_tail_restore_together() -> Result<()> {
    use crate::{
        litestream::{
            Litestream, Replicator,
            storage::{CaptureDirectory, SqliteCapture},
        },
        state_log::SqliteSnapshot,
        storage::SnapshotRef,
    };
    let f = Fixture::new()?;
    let replication = Arc::new(Litestream::default());
    let mut capture =
        SqliteCapture::new(CaptureDirectory::temporary()?, replication.clone(), None).await?;
    let db = rusqlite::Connection::open(capture.path())?;
    db.execute_batch("INSERT INTO __terse_fields VALUES ('count','1'); CREATE TABLE entries(value INTEGER); INSERT INTO entries VALUES (1);")?;
    let txid = replication.sync(&capture.path()).await?;
    let first = StateSnapshot::new(
        1,
        1,
        "first".into(),
        SqliteSnapshot {
            txid,
            parent: None,
            files: capture.capture(txid).await?,
        },
        serde_json::json!(1),
    )?;
    let first_bytes = first.encode()?;
    db.execute_batch("UPDATE __terse_fields SET value='2'; INSERT INTO entries VALUES (2);")?;
    let txid = replication.sync(&capture.path()).await?;
    let second = StateSnapshot::new(
        2,
        1,
        "second".into(),
        SqliteSnapshot {
            txid,
            parent: Some(SnapshotRef::new(f.stream.object(1), &first, &first_bytes)?),
            files: capture.capture(txid).await?,
        },
        serde_json::json!(2),
    )?;
    assert!(second.sqlite.files[0].first > 1);
    let second_bytes = second.encode()?;
    let (store, archive) = controlled(&f, first_bytes.len() + frame::HEADER, 60_000)?;
    store.start(&f.stream, None).await?;
    store.put(&f.stream.object(1), first_bytes).await?;
    batches(&f, 1).await?;
    archive.stalled.store(true, Ordering::SeqCst);
    store.put(&f.stream.object(2), second_bytes.clone()).await?;
    drop(store);
    let reader = f.store()?;
    assert_eq!(
        reader.recover(&f.stream.prefix).await?,
        Some((f.stream.object(2), second_bytes))
    );
    let parent = second.sqlite.parent.as_ref().unwrap();
    let parent_bytes = reader.get(&parent.object).await?.unwrap();
    parent.verify(&parent_bytes)?;
    let mut files = StateSnapshot::decode(&parent_bytes)?.sqlite.files;
    files.extend(second.sqlite.files);
    let restored = SqliteCapture::restore(
        CaptureDirectory::temporary()?,
        replication,
        &files,
        txid,
        None,
        false,
    )
    .await?;
    let db = rusqlite::Connection::open(restored.path())?;
    assert_eq!(
        db.query_row(
            "SELECT value FROM __terse_fields WHERE name='count'",
            [],
            |r| r.get::<_, String>(0)
        )?,
        "2"
    );
    assert_eq!(
        db.query_row("SELECT sum(value) FROM entries", [], |r| r.get::<_, i64>(0))?,
        3
    );
    Ok(())
}

#[tokio::test]
async fn rapid_rotation_continues_writing_while_old_archive_is_stalled() -> Result<()> {
    let f = Fixture::new()?;
    let (store, archive) = controlled(&f, 16 * 1024 * 1024, 60_000)?;
    store.start(&f.stream, None).await?;
    store.put(&f.stream.object(1), state(1, 1)?).await?;
    archive.stalled.store(true, Ordering::SeqCst);
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(61)).await;
    tokio::time::resume();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            store
                .session
                .lock()
                .await
                .as_mut()
                .unwrap()
                .rotate(store.storage.clone())
                .await;
            if f.archive
                .list("")
                .await
                .unwrap()
                .iter()
                .filter(|key| key.ends_with(".manifest"))
                .count()
                == 2
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await?;
    let mut version = 1;
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            version += 1;
            store
                .put(&f.stream.object(version), state(version, 1)?)
                .await?;
            if f.zones.iter().all(|z| {
                z.objects
                    .lock()
                    .unwrap()
                    .values()
                    .filter(|o| !o.bytes.is_empty())
                    .count()
                    == 2
            }) {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await??;
    assert!(f.zones.iter().all(|z| z.opens.load(Ordering::SeqCst) == 2));
    archive.stalled.store(false, Ordering::SeqCst);
    store
        .finish(
            &f.stream,
            tokio::time::Instant::now() + Duration::from_secs(30),
        )
        .await?;
    f.cleaned().await?;
    assert_eq!(
        f.store()?.get(&f.stream.object(1)).await?,
        Some(state(1, 1)?)
    );
    Ok(())
}

#[tokio::test]
async fn conflicting_batches_never_allow_recovery_to_delete_rapid_data() -> Result<()> {
    let f = Fixture::new()?;
    let store = f.store()?;
    store.start(&f.stream, None).await?;
    let original = state(1, 1)?;
    store.put(&f.stream.object(1), original.clone()).await?;
    let key = f
        .archive
        .list("")
        .await?
        .into_iter()
        .find(|k| k.ends_with(".manifest"))
        .unwrap();
    let manifest: Manifest =
        serde_json::from_slice(&f.archive.get(&key).await?.unwrap().bytes.to_bytes()?)?;
    let mut snapshot = StateSnapshot::decode(&original)?;
    snapshot.completion.as_mut().unwrap().result = serde_json::json!(9).into();
    let mut frame = Vec::new();
    Record {
        version: 1,
        state: snapshot.encode()?,
    }
    .write_to(&mut frame)?;
    store.storage.put_batch(&manifest, 0, frame.into()).await?;
    assert!(f.store()?.recover(&f.stream.prefix).await.is_err());
    assert!(
        f.zones
            .iter()
            .all(|z| !z.objects.lock().unwrap().is_empty())
    );
    Ok(())
}

#[tokio::test]
async fn archiver_finishes_when_recovery_archived_its_backlog_and_rapid_expired() -> Result<()> {
    let f = Fixture::new()?;
    let threshold = state(1, 1)?.len() + frame::HEADER;
    let (store, archive) = controlled(&f, threshold, 60_000)?;
    let mut segment = Segment::open(&store.storage, f.stream.clone(), 1, None)
        .await?
        .unwrap();
    let manifest = segment.manifest.clone();
    let worker = archive::Archiver::new(store.storage.clone(), manifest.clone(), 0);
    archive.stalled.store(true, Ordering::SeqCst);
    worker.append(segment.append(1, state(1, 1)?).await?, 1);
    tokio::time::timeout(Duration::from_secs(2), async {
        while archive.batches.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    for version in 2..=4 {
        worker.append(segment.append(version, state(version, 1)?).await?, version);
    }
    worker.close();
    let rescue = f.store()?;
    rescue.storage.recover_segment(&manifest).await?;
    for zone in &f.zones {
        zone.objects.lock().unwrap().clear();
    }
    archive.stalled.store(false, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(2), worker.finish()).await??;
    assert_eq!(
        rescue.latest(&f.stream.prefix).await?,
        Some((f.stream.object(4), state(4, 1)?))
    );
    Ok(())
}

#[tokio::test]
async fn large_records_archive_after_retry_and_remain_indexed() -> Result<()> {
    let f = Fixture::new()?;
    let (writer, archive) = controlled(&f, 64 * 1024, 60_000)?;
    archive.fail_batch.store(true, Ordering::SeqCst);
    writer.start(&f.stream, None).await?;
    let mut snapshot = StateSnapshot::decode(&state(1, 1)?)?;
    snapshot.completion.as_mut().unwrap().result =
        serde_json::json!({"data": "x".repeat(5 * 1024 * 1024)}).into();
    let bytes = snapshot.encode()?;
    writer.put(&f.stream.object(1), bytes.clone()).await?;
    writer.put(&f.stream.object(2), state(2, 1)?).await?;
    writer
        .finish(
            &f.stream,
            tokio::time::Instant::now() + Duration::from_secs(10),
        )
        .await?;
    f.cleaned().await?;
    assert!(
        f.zones
            .iter()
            .any(|z| z.range_reads.load(Ordering::SeqCst) > 0)
    );
    let (reader, archive) = controlled(&f, 64 * 1024, 60_000)?;
    assert_eq!(reader.get(&f.stream.object(1)).await?, Some(bytes.clone()));
    assert_eq!(archive.read_batches.load(Ordering::SeqCst), 1);
    assert_eq!(
        archive.range_bytes.load(Ordering::SeqCst),
        (bytes.len() + frame::HEADER) as u64
    );
    assert_eq!(reader.get(&f.stream.object(2)).await?, Some(state(2, 1)?));
    Ok(())
}

#[tokio::test]
async fn restores_prefetch_archived_batches_without_reading_unlisted_versions() -> Result<()> {
    let f = Fixture::new()?;
    let (writer, _) = controlled(&f, 1, 10_000)?;
    let (replication, history) = write_history(&f, &writer, 20, None).await?;
    writer
        .finish(
            &f.stream,
            tokio::time::Instant::now() + Duration::from_secs(10),
        )
        .await?;
    f.cleaned().await?;
    let head = history.last().unwrap().clone();
    let snapshot = VerifiedSnapshot::matching(f.stream.snapshot(&head)?, head)?;

    let (reader, archive) = controlled(&f, 1, 10_000)?;
    let restored = reader.storage.restore(snapshot, None).await?;

    assert_eq!(restored.parents, 19);
    assert_eq!(archive.version_reads.load(Ordering::SeqCst), 0);
    assert_eq!(archive.read_batches.load(Ordering::SeqCst), 19);
    assert_eq!(
        crate::litestream::storage::restored_fields(
            replication.as_ref(),
            &restored.sqlite.files,
            restored.sqlite.txid
        )
        .await?,
        serde_json::json!({"count": 20})
    );
    Ok(())
}

#[tokio::test]
async fn restores_plan_history_spread_across_owner_epochs() -> Result<()> {
    let source = Fixture::new()?;
    let (replication, history) = write_history(&source, &source.store()?, 40, None).await?;
    let f = Fixture::new()?;
    let actor = crate::storage_paths::actor_from_snapshot(&f.stream.object(1))?;
    let (mut progress, mut parent, mut head) = (None, None, None);
    // Each version is its own activation, as for an actor that wakes, writes once and sleeps.
    for bytes in history {
        let mut snapshot = StateSnapshot::decode(&bytes)?;
        let version = snapshot.state_version;
        snapshot.owner_epoch = version;
        snapshot.sqlite.parent = parent;
        let key =
            crate::storage::snapshot_object_name(&actor, version, &format!("{version:032x}"))?;
        let stream = StateStream {
            prefix: format!("{}/", key.rsplit_once('/').unwrap().0),
            owner_epoch: version,
            base_version: version - 1,
            ..f.stream.clone()
        };
        let writer = f.store()?;
        writer
            .start(&stream, snapshot.sqlite.parent.as_ref().zip(progress))
            .await?;
        let bytes = snapshot.encode()?;
        let reference = stream.snapshot(&bytes)?;
        writer.put(&key, bytes.clone()).await?;
        progress = writer
            .finish(
                &stream,
                tokio::time::Instant::now() + Duration::from_secs(5),
            )
            .await?;
        parent = Some(reference.clone());
        head = Some(VerifiedSnapshot::matching(reference, bytes)?);
    }
    f.cleaned().await?;
    let (reader, archive) = controlled(&f, 16 * 1024 * 1024, 10_000)?;
    let restored = reader.storage.restore(head.unwrap(), None).await?;

    assert_eq!(restored.parents, 39);
    assert_eq!(archive.version_reads.load(Ordering::SeqCst), 0);
    let ops = archive.ops.lock().unwrap().clone();
    let last_list = ops.iter().rposition(|op| *op == 'L').unwrap();
    let first_index = ops.iter().position(|op| *op == 'I').unwrap();
    assert!(
        last_list < first_index,
        "every epoch is listed before any batch is read: {ops:?}"
    );
    assert_eq!(
        crate::litestream::storage::restored_fields(
            replication.as_ref(),
            &restored.sqlite.files,
            restored.sqlite.txid
        )
        .await?,
        serde_json::json!({"count": 40})
    );
    Ok(())
}

#[test]
fn checkpoints_allow_more_versions_as_the_database_grows() {
    use super::super::checkpoint::max_log_versions;
    assert_eq!(max_log_versions(0), 256);
    assert_eq!(max_log_versions(128 << 20), 256);
    assert_eq!(max_log_versions(1 << 30), 2048);
    assert_eq!(max_log_versions(4 << 30), 4096);
}

#[test]
fn bounded_transfers_get_time_in_proportion_to_their_size() {
    let base = Duration::from_secs(10);
    assert_eq!(super::super::transfer_timeout(base, 0), base);
    assert_eq!(
        super::super::transfer_timeout(base, 64 * 1024 * 1024),
        Duration::from_secs(12)
    );
}

#[tokio::test]
async fn indexed_reads_fetch_only_the_requested_archived_record() -> Result<()> {
    let f = Fixture::new()?;
    let (writer, _) = controlled(&f, 1, 10_000)?;
    writer.start(&f.stream, None).await?;
    for version in 1..=5 {
        writer
            .put(&f.stream.object(version), state(version, 1)?)
            .await?;
    }
    writer
        .finish(
            &f.stream,
            tokio::time::Instant::now() + Duration::from_secs(10),
        )
        .await?;
    f.cleaned().await?;
    let (reader, archive) = controlled(&f, 1, 10_000)?;
    let result = reader.get(&f.stream.object(3)).await?;
    assert_eq!(result, Some(state(3, 1)?));
    assert_eq!(archive.read_batches.load(Ordering::SeqCst), 1);
    assert_eq!(
        archive.range_bytes.load(Ordering::SeqCst),
        (state(3, 1)?.len() + frame::HEADER) as u64
    );
    Ok(())
}

#[tokio::test]
async fn unavailable_or_invalid_indexes_preserve_archive_recovery() -> Result<()> {
    let f = Fixture::new()?;
    let writer = f.store()?;
    writer.start(&f.stream, None).await?;
    for version in 1..=3 {
        writer
            .put(&f.stream.object(version), state(version, 1)?)
            .await?;
    }
    writer
        .finish(
            &f.stream,
            tokio::time::Instant::now() + Duration::from_secs(10),
        )
        .await?;
    f.cleaned().await?;
    let (reader, archive) = controlled(&f, 16 * 1024 * 1024, 10_000)?;
    for failure in 1..=3 {
        archive.index_failure.store(failure, Ordering::SeqCst);
        assert_eq!(reader.get(&f.stream.object(2)).await?, Some(state(2, 1)?));
    }
    archive.index_failure.store(0, Ordering::SeqCst);
    let key = f
        .archive
        .list("")
        .await?
        .into_iter()
        .find(|key| key.ends_with(".idx"))
        .unwrap();
    let original = f.archive.get(&key).await?.unwrap();
    let mut wrong: serde_json::Value = serde_json::from_slice(&original.bytes.to_bytes()?)?;
    wrong["records"][1]["start"] = 1.into();
    for bytes in [
        Payload::from(&b"invalid json"[..]),
        serde_json::to_vec(&wrong)?.into(),
    ] {
        let stored = f.archive.get(&key).await?.unwrap();
        assert!(
            f.archive
                .compare_and_swap(&key, Some(stored.generation), bytes)
                .await?
        );
        assert_eq!(reader.get(&f.stream.object(2)).await?, Some(state(2, 1)?));
    }
    Ok(())
}
