use super::*;
use crate::{
    bucket::{BaseName, Bucket, BucketObject, FileBucket, RestoredSqlite},
    payload::Payload,
    state_log::{SqliteSnapshot, StateSnapshot},
};
use serde_json::json;

/// A store whose history starts from one base, with no changes after it.
struct FromBase(SqliteBase);

#[async_trait::async_trait]
impl crate::bucket::SnapshotStore for FromBase {
    async fn get(&self, _: &str) -> Result<Option<Payload>> {
        Ok(None)
    }
    async fn restore_sqlite(
        &self,
        _: VerifiedSnapshot,
        _: crate::bucket::Bases,
        _: Option<&Path>,
    ) -> Result<RestoredSqlite> {
        Ok(RestoredSqlite {
            sqlite: SqliteSnapshot {
                txid: self.0.name.txid,
                parent: None,
                files: Vec::new(),
            },
            base: Some(self.0.clone()),
            progress: Default::default(),
        })
    }
    async fn list(&self, _: &str) -> Result<Vec<String>> {
        Ok(Vec::new())
    }
    async fn latest(&self, _: &str) -> Result<Option<(String, Payload)>> {
        Ok(None)
    }
    async fn put(&self, _: &str, _: Payload) -> Result<()> {
        Ok(())
    }
}

/// A bucket that holds nothing, so any download from it fails.
struct Empty;

#[async_trait::async_trait]
impl Bucket for Empty {
    async fn get(&self, _: &str) -> Result<Option<BucketObject>> {
        Ok(None)
    }
    async fn range(&self, _: &str, _: u64, _: u64) -> Result<Option<BucketObject>> {
        Ok(None)
    }
    async fn compare_and_swap(&self, _: &str, _: Option<i64>, _: Payload) -> Result<bool> {
        Ok(false)
    }
    async fn list(&self, _: &str) -> Result<Vec<String>> {
        Ok(Vec::new())
    }
}

fn database(directory: &Path, rows: i64) -> Result<Vec<u8>> {
    let path = directory.join("source.sqlite");
    let db = rusqlite::Connection::open(&path)?;
    db.execute_batch("PRAGMA journal_mode = DELETE; CREATE TABLE t(v INTEGER);")?;
    for row in 0..rows {
        db.execute("INSERT INTO t VALUES (?)", [row])?;
    }
    drop(db);
    let bytes = std::fs::read(&path)?;
    std::fs::remove_file(path)?;
    Ok(bytes)
}

fn base(key: &str, bytes: u64, bucket: Arc<dyn Bucket>) -> SqliteBase {
    SqliteBase {
        key: key.into(),
        name: BaseName {
            owner_epoch: 1,
            version: 1,
            txid: 1,
            bytes,
        },
        bucket,
        downloaded: None,
    }
}

fn head() -> Result<VerifiedSnapshot> {
    let sqlite = crate::test_sqlite::snapshot(json!({}))?;
    let bytes = StateSnapshot::new(1, 1, "request".into(), sqlite, json!(1))?.encode()?;
    let actor = ActorKey {
        project_id: "project".into(),
        actor_name: "Counter".into(),
        actor_id: "id".into(),
    };
    let object = crate::storage::snapshot_object_name(&actor, 1, &format!("{:032x}", 1))?;
    VerifiedSnapshot::named(object, bytes)
}

/// A prepared download of the base named `key` that has already finished with `bytes`.
fn prepared(
    key: &str,
    bytes: &[u8],
    store: Arc<dyn crate::bucket::SnapshotStore>,
) -> Result<Prepared> {
    let directory = tempfile::tempdir()?;
    std::fs::write(directory.path().join(PREPARED_FILE), bytes)?;
    let key = key.to_owned();
    Ok(Prepared {
        actor: ActorKey {
            project_id: "project".into(),
            actor_name: "Counter".into(),
            actor_id: "id".into(),
        },
        store: futures_util::future::ready(Ok(store)).boxed().shared(),
        listed: Arc::new(OnceLock::new()),
        directory,
        started: tokio::time::Instant::now(),
        task: AbortOnDropHandle::new(tokio::spawn(async move { Ok(Some(key)) })),
    })
}

#[tokio::test]
async fn restores_onto_the_prepared_base_without_downloading_it_again() -> Result<()> {
    let slot = tempfile::tempdir()?;
    let bytes = database(slot.path(), 3)?;
    let store = Arc::new(FromBase(base("base", bytes.len() as u64, Arc::new(Empty))));
    let database = slot.path().join(DATABASE);
    let prepared = prepared("base", &bytes, store.clone())?;
    let file = prepared.directory.path().join(PREPARED_FILE);
    let restored = restore_into(store.as_ref(), head()?, Some(prepared), &database).await?;
    assert_eq!(
        (restored.txid, restored.base_bytes),
        (1, Some(bytes.len() as u64))
    );
    assert_eq!(std::fs::read(database.join(DATABASE_FILE))?, bytes);
    assert!(!file.exists());
    Ok(())
}

#[tokio::test]
async fn downloads_another_base_when_the_history_starts_from_it() -> Result<()> {
    let slot = tempfile::tempdir()?;
    let bucket = Arc::new(FileBucket::new(slot.path().join("bucket"))?);
    let bytes = database(slot.path(), 5)?;
    assert!(
        bucket
            .compare_and_swap("newer", None, bytes.clone().into())
            .await?
    );
    let store = Arc::new(FromBase(base("newer", bytes.len() as u64, bucket)));
    let database = slot.path().join(DATABASE);
    let prepared = prepared("older", b"an older base", store.clone())?;
    restore_into(store.as_ref(), head()?, Some(prepared), &database).await?;
    assert_eq!(std::fs::read(database.join(DATABASE_FILE))?, bytes);
    Ok(())
}

#[tokio::test]
async fn prepares_the_newest_base_through_the_nodes_own_bucket() -> Result<()> {
    let slot = tempfile::tempdir()?;
    let bases: Arc<dyn Bucket> = Arc::new(FileBucket::new(slot.path().join("bucket"))?);
    let bytes = database(slot.path(), 4)?;
    let key = crate::bucket::base_key(&head()?.reference, 1, bytes.len() as u64)?;
    assert!(
        bases
            .compare_and_swap(&key, None, bytes.clone().into())
            .await?
    );
    let store: Arc<dyn crate::bucket::SnapshotStore> =
        Arc::new(FromBase(base("unused", 0, Arc::new(Empty))));
    let listed = Arc::new(OnceLock::new());
    let file = slot.path().join(PREPARED_FILE);
    let actor = ActorKey {
        project_id: "project".into(),
        actor_name: "Counter".into(),
        actor_id: "id".into(),
    };
    let prepared = download_newest(
        Some(bases),
        futures_util::future::ready(Ok(store)).boxed().shared(),
        actor,
        listed.clone(),
        file.clone(),
        Arc::new(tokio::sync::Semaphore::new(1)),
    )
    .await?;
    assert_eq!(prepared.as_deref(), Some(key.as_str()));
    assert_eq!(
        listed.get().map(|base| base.key.as_str()),
        Some(key.as_str())
    );
    assert_eq!(std::fs::read(file)?, bytes);
    Ok(())
}

/// A store whose history starts from the base `Bases` picks among these, oldest first, with no
/// changes after it.
struct Layered(Vec<SqliteBase>);

#[async_trait::async_trait]
impl crate::bucket::SnapshotStore for Layered {
    async fn get(&self, _: &str) -> Result<Option<Payload>> {
        Ok(None)
    }
    async fn restore_sqlite(
        &self,
        _: VerifiedSnapshot,
        bases: crate::bucket::Bases,
        _: Option<&Path>,
    ) -> Result<RestoredSqlite> {
        let below = match bases {
            crate::bucket::Bases::Newest(_) => u64::MAX,
            crate::bucket::Bases::Below(version) => version,
        };
        let base = self
            .0
            .iter()
            .rev()
            .find(|base| base.name.version < below)
            .context("no history")?;
        FromBase(base.clone())
            .restore_sqlite(head()?, crate::bucket::Bases::Newest(None), None)
            .await
    }
    async fn list(&self, _: &str) -> Result<Vec<String>> {
        Ok(Vec::new())
    }
    async fn latest(&self, _: &str) -> Result<Option<(String, Payload)>> {
        Ok(None)
    }
    async fn put(&self, _: &str, _: Payload) -> Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn restores_from_an_older_base_when_the_newest_fails() -> Result<()> {
    let slot = tempfile::tempdir()?;
    let bucket = Arc::new(FileBucket::new(slot.path().join("bucket"))?);
    let bytes = database(slot.path(), 2)?;
    assert!(
        bucket
            .compare_and_swap("older", None, bytes.clone().into())
            .await?
    );
    let mut newest = base("newest", bytes.len() as u64, Arc::new(Empty));
    newest.name.version = 2;
    let store = Layered(vec![base("older", bytes.len() as u64, bucket), newest]);
    let database = slot.path().join(DATABASE);
    let restored = restore_into(&store, head()?, None, &database).await?;
    assert_eq!(restored.base.map(|base| base.version), Some(1));
    assert_eq!(std::fs::read(database.join(DATABASE_FILE))?, bytes);
    Ok(())
}

#[tokio::test]
async fn hands_the_database_over_replacing_an_earlier_attempt() -> Result<()> {
    let (staged, slot) = (tempfile::tempdir()?, tempfile::tempdir()?);
    std::fs::create_dir(slot.path().join(DATABASE))?;
    std::fs::write(slot.path().join(DATABASE).join(DATABASE_FILE), b"earlier")?;
    std::fs::write(staged.path().join(DATABASE_FILE), b"pages")?;

    hand_over(staged.path(), slot.path()).await?;

    let file = slot.path().join(DATABASE).join(DATABASE_FILE);
    assert_eq!(std::fs::read(&file)?, b"pages");
    assert_eq!(
        std::os::unix::fs::PermissionsExt::mode(&std::fs::metadata(file)?.permissions()) & 0o777,
        0o666
    );
    Ok(())
}

#[tokio::test]
async fn refuses_a_symlinked_database_directory() -> Result<()> {
    let (staged, slot, elsewhere) = (
        tempfile::tempdir()?,
        tempfile::tempdir()?,
        tempfile::tempdir()?,
    );
    std::fs::write(
        elsewhere.path().join(DATABASE_FILE),
        b"another actor's pages",
    )?;
    std::os::unix::fs::symlink(elsewhere.path(), slot.path().join(DATABASE))?;
    std::fs::write(staged.path().join(DATABASE_FILE), b"pages")?;

    assert!(hand_over(staged.path(), slot.path()).await.is_err());

    assert_eq!(
        std::fs::read(elsewhere.path().join(DATABASE_FILE))?,
        b"another actor's pages"
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn drops_an_unclaimed_prepared_base_without_another_prepare() -> Result<()> {
    let staging = tempfile::tempdir()?;
    let restores = Restores::new(None, staging.path().to_owned());
    let store = Arc::new(FromBase(base("base", 5, Arc::new(Empty))));
    let prepared = prepared("base", b"pages", store)?;
    restores
        .prepared
        .lock()
        .await
        .insert("dead host".into(), prepared);

    tokio::time::sleep(PREPARED_TTL * 2).await;

    assert!(restores.prepared.lock().await.is_empty());
    Ok(())
}
