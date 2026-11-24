use super::*;
use crate::bucket::{ArchiveBatchConfig, Bases, BucketObject};
use crate::storage::VerifiedSnapshot;

struct ArchiveBucket {
    inner: Arc<FileBucket>,
    read_requests: AtomicU64,
    read_batches: AtomicU64,
    version_reads: AtomicU64,
    /// Lists (`L`) and index reads (`I`) in the order they happen.
    ops: std::sync::Mutex<Vec<char>>,
    range_bytes: AtomicU64,
    index_failure: AtomicU64,
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
        read_requests: AtomicU64::new(0),
        read_batches: AtomicU64::new(0),
        version_reads: AtomicU64::new(0),
        ops: std::sync::Mutex::new(Vec::new()),
        range_bytes: AtomicU64::new(0),
        index_failure: AtomicU64::new(0),
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
        CancellationToken::new(),
    )?;
    Ok((store, archive))
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
async fn crash_recovery_joins_standard_batches_and_unflushed_rapid_tail() -> Result<()> {
    let f = Fixture::new()?;
    let store = f.store()?;
    store.start(&f.stream, None).await?;
    store.put(&f.stream.object(1), state(1, 1)?).await?;
    store.put(&f.stream.object(2), state(2, 1)?).await?;
    archive_first_record(&store, &f).await?;
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
    let store = f.store()?;
    store.start(&f.stream, None).await?;
    store.put(&f.stream.object(1), first_bytes).await?;
    store.put(&f.stream.object(2), second_bytes.clone()).await?;
    archive_first_record(&store, &f).await?;
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
    archive_legacy(&writer, &f).await?;
    let head = history.last().unwrap().clone();
    let snapshot = VerifiedSnapshot::matching(f.stream.snapshot(&head)?, head)?;

    let (reader, archive) = controlled(&f, 1, 10_000)?;
    let restored = reader
        .storage
        .restore(snapshot, Bases::Newest(None), None)
        .await?;

    assert_eq!(restored.parents, 19);
    assert_eq!(archive.version_reads.load(Ordering::SeqCst), 0);
    assert_eq!(archive.read_batches.load(Ordering::SeqCst), 19);
    assert_eq!(
        crate::litestream::storage::restored_fields(
            tempfile::tempdir()?,
            replication.as_ref(),
            None,
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
    archive_legacy(&f.store()?, &f).await?;
    let (reader, archive) = controlled(&f, 16 * 1024 * 1024, 10_000)?;
    let restored = reader
        .storage
        .restore(head.unwrap(), Bases::Newest(None), None)
        .await?;

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
            tempfile::tempdir()?,
            replication.as_ref(),
            None,
            &restored.sqlite.files,
            restored.sqlite.txid
        )
        .await?,
        serde_json::json!({"count": 40})
    );
    Ok(())
}

#[test]
fn bases_allow_more_versions_as_the_database_grows() {
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
    archive_legacy(&writer, &f).await?;
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
    archive_legacy(&writer, &f).await?;
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

/// Stores the database at `version` as a base, claiming SQLite transaction `txid` when given.
async fn store_base(
    f: &Fixture,
    store: &RapidSnapshots,
    replication: &crate::litestream::Litestream,
    history: &[Payload],
    version: u64,
    txid: Option<u64>,
) -> Result<u64> {
    use crate::litestream::DatabaseRestore;
    let snapshot = VerifiedSnapshot::named(
        f.stream.object(version),
        history[version as usize - 1].clone(),
    )?;
    let crate::bucket::RestoredSqlite {
        sqlite: restored,
        base: previous,
        ..
    } = store
        .restore_sqlite(snapshot, Bases::Newest(None), None)
        .await?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("base.sqlite");
    match previous {
        Some(previous) => {
            previous.download_to(&path).await?;
            crate::litestream::apply::apply(
                &path,
                Some(previous.name.txid),
                &restored.files,
                restored.txid,
            )?;
        }
        None => {
            replication
                .restore(&restored.files, &path, restored.txid)
                .await?
        }
    }
    let bytes = std::fs::read(&path)?;
    let reference = f.stream.snapshot(&history[version as usize - 1])?;
    let key = base::key(
        &reference,
        txid.unwrap_or(restored.txid),
        bytes.len() as u64,
    )?;
    assert!(f.archive.compare_and_swap(&key, None, bytes.into()).await?);
    Ok(restored.txid)
}

async fn restore_version(
    store: &RapidSnapshots,
    f: &Fixture,
    history: &[Payload],
    version: u64,
) -> Result<crate::bucket::RestoredSqlite> {
    store
        .restore_sqlite(
            VerifiedSnapshot::named(
                f.stream.object(version),
                history[version as usize - 1].clone(),
            )?,
            Bases::Newest(None),
            None,
        )
        .await
}

#[tokio::test]
async fn restores_from_the_newest_base_and_the_history_after_it() -> Result<()> {
    let f = Fixture::new()?;
    let (writer, _) = controlled(&f, 16 * 1024 * 1024, 60_000)?;
    let (replication, history) = write_history(&f, &writer, 40, None).await?;
    store_base(&f, &writer, &replication, &history, 20, None).await?;
    let txid = store_base(&f, &writer, &replication, &history, 30, None).await?;
    let (reader, _) = controlled(&f, 16 * 1024 * 1024, 10_000)?;
    let restored = restore_version(&reader, &f, &history, 40).await?;
    assert_eq!(
        restored.base.as_ref().map(|base| base.name.version),
        Some(30)
    );
    assert_eq!(restored.sqlite.files[0].first, txid + 1);
    assert_eq!(
        crate::litestream::storage::restored_fields(
            tempfile::tempdir()?,
            replication.as_ref(),
            restored.base.as_ref(),
            &restored.sqlite.files,
            restored.sqlite.txid
        )
        .await?,
        serde_json::json!({"count":40})
    );
    let at_base = restore_version(&reader, &f, &history, 30).await?;
    assert!(at_base.sqlite.files.is_empty());
    assert_eq!(
        crate::litestream::storage::restored_fields(
            tempfile::tempdir()?,
            replication.as_ref(),
            at_base.base.as_ref(),
            &at_base.sqlite.files,
            at_base.sqlite.txid
        )
        .await?,
        serde_json::json!({"count":30})
    );
    Ok(())
}

#[tokio::test]
async fn ignores_a_base_whose_transaction_disagrees_with_the_history() -> Result<()> {
    let f = Fixture::new()?;
    let (writer, _) = controlled(&f, 16 * 1024 * 1024, 60_000)?;
    let (replication, history) = write_history(&f, &writer, 12, None).await?;
    store_base(&f, &writer, &replication, &history, 8, Some(999)).await?;
    let (reader, _) = controlled(&f, 16 * 1024 * 1024, 10_000)?;
    let directory = tempfile::tempdir()?;
    let download = crate::litestream::storage::restoring(directory.path());
    let restored = reader
        .restore_sqlite(
            VerifiedSnapshot::named(f.stream.object(12), history[11].clone())?,
            Bases::Newest(None),
            Some(&download),
        )
        .await?;
    assert!(restored.base.is_none());
    assert!(!download.exists());
    assert_eq!(
        crate::litestream::storage::restored_fields(
            tempfile::tempdir()?,
            replication.as_ref(),
            None,
            &restored.sqlite.files,
            restored.sqlite.txid
        )
        .await?,
        serde_json::json!({"count":12})
    );
    Ok(())
}

#[tokio::test]
async fn restores_the_same_rows_from_an_older_base_when_the_newest_is_corrupt() -> Result<()> {
    let f = Fixture::new()?;
    let writer = f.store()?;
    let (replication, history) = write_history(&f, &writer, 12, None).await?;
    store_base(&f, &writer, &replication, &history, 4, None).await?;
    store_base(&f, &writer, &replication, &history, 8, None).await?;
    let head = VerifiedSnapshot::named(f.stream.object(12), history[11].clone())?;
    let reader = f.store()?;
    let newest = reader.storage.newest_base(&head.reference).await?.unwrap();
    let object = f.archive.get(&newest.key).await?.unwrap();
    // The file bucket keeps no checksum, so the corruption it can detect is a short base.
    let mut bytes = object.bytes.to_bytes()?.to_vec();
    bytes.pop();
    f.archive
        .compare_and_swap(&newest.key, Some(object.generation), bytes.into())
        .await?;
    let dir = tempfile::tempdir()?;
    let path = crate::litestream::storage::restoring(dir.path());
    let restored = reader
        .restore_sqlite(head, Bases::Newest(None), Some(&path))
        .await?;
    assert_eq!(restored.base.as_ref().unwrap().name.version, 4);
    let fields = crate::litestream::storage::restored_fields(
        dir,
        replication.as_ref(),
        restored.base.as_ref(),
        &restored.sqlite.files,
        restored.sqlite.txid,
    )
    .await?;
    assert_eq!(fields, serde_json::json!({"count":12}));
    Ok(())
}

#[tokio::test]
async fn downloads_the_base_while_reading_the_history_after_it() -> Result<()> {
    use crate::litestream::storage::{read_fields, restore_database, restoring};
    let f = Fixture::new()?;
    let (writer, _) = controlled(&f, 16 * 1024 * 1024, 60_000)?;
    let (replication, history) = write_history(&f, &writer, 12, None).await?;
    store_base(&f, &writer, &replication, &history, 8, None).await?;
    let (reader, _) = controlled(&f, 16 * 1024 * 1024, 10_000)?;
    let directory = tempfile::tempdir()?;
    let download = restoring(directory.path());
    let restored = reader
        .restore_sqlite(
            VerifiedSnapshot::named(f.stream.object(12), history[11].clone())?,
            Bases::Newest(None),
            Some(&download),
        )
        .await?;
    let base = restored.base.as_ref().context("restore ignored the base")?;
    assert_eq!(base.downloaded.as_deref(), Some(download.as_path()));
    assert_eq!(std::fs::metadata(&download)?.len(), base.name.bytes);
    restore_database(
        replication.as_ref(),
        directory.path(),
        Some(base),
        &restored.sqlite.files,
        restored.sqlite.txid,
    )
    .await?;
    let db = rusqlite::Connection::open(directory.path().join("actor.sqlite"))?;
    assert_eq!(read_fields(&db)?, serde_json::json!({"count":12}));
    assert!(!download.exists());
    Ok(())
}

async fn manifests(f: &Fixture) -> Result<Vec<Manifest>> {
    let mut manifests = Vec::new();
    for key in f.archive.list("").await? {
        if key.ends_with(".manifest") {
            let stored = f.archive.get(&key).await?.context("manifest disappeared")?;
            manifests.push(serde_json::from_reader(stored.bytes.reader())?);
        }
    }
    Ok(manifests)
}

/// Archives every segment as hosts did before bases, batches, indexes and a `replicated` marker,
/// then deletes the Rapid copies, leaving the history only in the archive.
async fn archive_legacy(store: &RapidSnapshots, f: &Fixture) -> Result<()> {
    for manifest in manifests(f).await? {
        store.storage.archive_segment(&manifest).await?;
        for replica in &manifest.replicas {
            store.storage.zone(replica)?.delete(replica).await?;
        }
    }
    f.cleaned().await
}

/// Archives only the first record of every segment, with no marker, as an archiver cut off
/// mid-segment left it before bases.
async fn archive_first_record(store: &RapidSnapshots, f: &Fixture) -> Result<()> {
    for manifest in manifests(f).await? {
        let records = store.storage.read_replicas(&manifest, false).await?;
        let mut batch = crate::payload::Spool::new();
        records[0].write_to(&mut batch)?;
        store
            .storage
            .put_batch(&manifest, 0, batch.finish()?)
            .await?;
    }
    Ok(())
}

#[tokio::test]
async fn restores_rapid_history_spread_across_owner_epochs() -> Result<()> {
    let source = Fixture::new()?;
    let (replication, history) = write_history(&source, &source.store()?, 40, None).await?;
    let f = Fixture::new()?;
    let actor = crate::storage_paths::actor_from_snapshot(&f.stream.object(1))?;
    let (mut progress, mut parent, mut head) = (None, None, None);
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
    let (reader, archive) = controlled(&f, 16 * 1024 * 1024, 10_000)?;
    let restored = reader
        .storage
        .restore(head.unwrap(), Bases::Newest(None), None)
        .await?;

    assert_eq!(restored.parents, 39);
    // The planner read every epoch's segments at once, so the walk read no single version.
    assert_eq!(archive.version_reads.load(Ordering::SeqCst), 0);
    assert_eq!(
        crate::litestream::storage::restored_fields(
            tempfile::tempdir()?,
            replication.as_ref(),
            None,
            &restored.sqlite.files,
            restored.sqlite.txid
        )
        .await?,
        serde_json::json!({"count": 40})
    );
    Ok(())
}

#[tokio::test]
async fn restores_from_a_legacy_checkpoint_and_refuses_a_corrupt_one() -> Result<()> {
    use crate::bucket::recovery::Checkpoint;
    let f = Fixture::new()?;
    let (writer, _) = controlled(&f, 16 * 1024 * 1024, 60_000)?;
    let (replication, history) = write_history(&f, &writer, 40, None).await?;
    let source = f.stream.snapshot(&history[29])?;
    let sqlite = restore_version(&writer, &f, &history, 30).await?.sqlite;
    let mut data = Vec::new();
    terse_ltx::compact(
        sqlite.files.iter().map(|file| file.data.reader()).collect(),
        &mut data,
    )?;
    let checkpoint = Checkpoint {
        source: source.clone(),
        sqlite: crate::state_log::SqliteSnapshot {
            txid: sqlite.txid,
            parent: None,
            files: vec![crate::litestream::storage::LtxFile {
                level: 9,
                first: 1,
                last: sqlite.txid,
                data: data.into(),
            }],
        },
    };
    BucketSnapshots(f.archive.clone())
        .put(
            &format!("{}.checkpoint", source.object),
            checkpoint.encode()?,
        )
        .await?;

    let (reader, _) = controlled(&f, 16 * 1024 * 1024, 10_000)?;
    let restored = restore_version(&reader, &f, &history, 40).await?;
    assert!(restored.base.is_none());
    assert_eq!(restored.sqlite.files[0].level, 9);
    assert_eq!(restored.sqlite.files.len(), 11);
    assert_eq!(
        crate::litestream::storage::restored_fields(
            tempfile::tempdir()?,
            replication.as_ref(),
            None,
            &restored.sqlite.files,
            restored.sqlite.txid
        )
        .await?,
        serde_json::json!({"count": 40})
    );
    let corrupt = f.stream.snapshot(&history[34])?;
    BucketSnapshots(f.archive.clone())
        .put(
            &format!("{}.checkpoint", corrupt.object),
            Payload::from(&b"not a checkpoint"[..]),
        )
        .await?;
    let head = VerifiedSnapshot::matching(corrupt, history[34].clone())?;
    assert!(
        reader
            .restore_sqlite(head, Bases::Newest(None), None)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn parked_reopen_finds_the_previous_upload_and_counts_only_later_writes() -> Result<()> {
    let f = Fixture::new()?;
    let writer = f.store()?;
    let (replication, history) = write_history(&f, &writer, 2, None).await?;
    let head = f.stream.snapshot(&history[1])?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let sealed = writer.finish(&f.stream, deadline).await?.unwrap();
    assert!(sealed.base_due);
    store_base(&f, &f.store()?, &replication, &history, 2, None).await?;

    let actor = crate::storage_paths::actor_from_snapshot(&head.object)?;
    let object = crate::storage::snapshot_object_name(&actor, 3, &format!("{:032x}", 2))?;
    let stream = StateStream {
        owner_epoch: 2,
        base_version: 2,
        prefix: format!("{}/", object.rsplit_once('/').unwrap().0),
        ..f.stream.clone()
    };
    let parked = f.store()?;
    parked.start(&stream, Some((&head, sealed))).await?;
    let bytes = state(3, 2)?;
    parked.put(&object, bytes.clone()).await?;
    let sealed = parked.finish(&stream, deadline).await?.unwrap();
    assert_eq!(sealed.version, 2);
    assert_eq!(sealed.log_bytes, bytes.len() as u64);
    assert!(!sealed.base_due);
    Ok(())
}

#[tokio::test]
async fn a_session_the_node_restored_after_a_crash_counts_the_history_since_the_base() -> Result<()>
{
    let f = Fixture::new()?;
    let writer = f.store()?;
    let (replication, history) = write_history(&f, &writer, 12, None).await?;
    store_base(&f, &f.store()?, &replication, &history, 8, None).await?;
    let head = VerifiedSnapshot::named(f.stream.object(12), history[11].clone())?;
    let restored = f
        .store()?
        .restore_sqlite(head.clone(), Bases::Newest(None), None)
        .await?;

    let actor = crate::storage_paths::actor_from_snapshot(&head.reference.object)?;
    let object = crate::storage::snapshot_object_name(&actor, 13, &format!("{:032x}", 2))?;
    let stream = StateStream {
        owner_epoch: 2,
        base_version: 12,
        prefix: format!("{}/", object.rsplit_once('/').unwrap().0),
        ..f.stream.clone()
    };
    // The crashed owner sealed nothing, so this owner learns the history only from the restore.
    let owner = f.store()?;
    owner.start(&stream, None).await?;
    let base = restored.base.as_ref().map(|base| base.name);
    owner
        .restored_elsewhere(&head, restored.progress, base)
        .await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let sealed = owner.finish(&stream, deadline).await?.unwrap();
    assert_eq!(sealed.version, 8);
    assert!(sealed.log_bytes > 0);
    assert!(!sealed.base_due);
    Ok(())
}

#[test]
fn a_base_is_due_when_none_exists_or_the_changes_outgrow_the_newest() {
    use super::super::{base::BaseName, checkpoint::Progress};
    let base = |version, bytes| {
        Some(BaseName {
            owner_epoch: 1,
            version,
            txid: version,
            bytes,
        })
    };
    let sealed = CheckpointProgress {
        version: 10,
        base_bytes: 4 << 20,
        log_bytes: 0,
        base_due: false,
    };
    assert!(Progress::resume(sealed, 10).seal(10).base_due);

    let mut progress = Progress::resume(sealed, 10);
    progress.found(base(10, 4 << 20));
    assert!(!progress.seal(10).base_due);
    progress.logged(1 << 20);
    assert!(!progress.seal(11).base_due);
    progress.logged(3 << 20);
    assert!(progress.seal(12).base_due);

    let mut tiny = Progress::resume(sealed, 10);
    tiny.found(base(10, 4 << 20));
    tiny.logged(10);
    assert!(!tiny.seal(10 + 255).base_due);
    assert!(tiny.seal(10 + 256).base_due);

    // A node stored a newer base after the last owner sealed.
    let mut newer = Progress::resume(sealed, 20);
    newer.found(base(20, 8 << 20));
    let newer = newer.seal(20);
    assert_eq!(
        (newer.version, newer.base_bytes, newer.log_bytes),
        (20, 8 << 20, 0)
    );
    assert!(!newer.base_due);
}
