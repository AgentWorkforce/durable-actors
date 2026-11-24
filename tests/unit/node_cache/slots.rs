use super::*;

#[tokio::test]
async fn a_parked_database_moves_to_the_next_spare_with_its_inode() -> Result<()> {
    let node = Node::start().await?;
    let (first, second) = (node.spare()?, node.spare()?);
    assert!(
        node.slots
            .check_out(&actor("one"), &first, None)
            .await?
            .is_none()
    );
    write_value(&node.database(&first), 89)?;

    let parked = node
        .slots
        .park(&actor("one"), &first, snapshot(1), 7, false)
        .await?;
    assert!(!node.database(&first).exists());
    assert!(node.slots.is_parked(&actor("one"), "d1").await);
    let reused = node
        .slots
        .check_out(&actor("one"), &second, Some(snapshot(1)))
        .await?
        .unwrap();

    assert_eq!(
        (reused.device, reused.inode, reused.txid),
        (parked.device, parked.inode, 7)
    );
    assert_eq!(read_value(&node.database(&second))?, 89);
    assert!(!node.slots.is_parked(&actor("one"), "d1").await);
    Ok(())
}

#[tokio::test]
async fn a_database_parked_at_another_snapshot_is_dropped() -> Result<()> {
    let node = Node::start().await?;
    let (first, second) = (node.spare()?, node.spare()?);
    node.slots.check_out(&actor("one"), &first, None).await?;
    write_value(&node.database(&first), 89)?;
    node.slots
        .park(&actor("one"), &first, snapshot(1), 7, false)
        .await?;

    assert!(!node.slots.is_parked(&actor("two"), "d1").await);
    let checked_out = node
        .slots
        .check_out(&actor("one"), &second, Some(snapshot(2)))
        .await?;

    assert!(checked_out.is_none());
    assert!(!node.database(&second).exists());
    assert!(!node.slots.is_parked(&actor("one"), "d1").await);
    Ok(())
}

#[tokio::test]
async fn only_a_closed_database_of_plain_files_is_parked() -> Result<()> {
    let node = Node::start().await?;
    let slot = node.spare()?;
    node.slots.check_out(&actor("one"), &slot, None).await?;
    let database = node.database(&slot);
    write_value(&database, 89)?;
    std::fs::write(database.join("actor.sqlite-wal"), b"frames")?;
    assert!(
        node.slots
            .park(&actor("one"), &slot, snapshot(1), 7, false)
            .await
            .is_err()
    );

    assert!(!database.exists());

    write_value(&database, 89)?;
    std::os::unix::fs::symlink("/etc/passwd", database.join("link"))?;
    assert!(
        node.slots
            .park(&actor("one"), &slot, snapshot(1), 7, false)
            .await
            .is_err()
    );
    assert!(!node.slots.is_parked(&actor("one"), "d1").await);
    Ok(())
}

#[tokio::test]
async fn a_host_parks_only_at_its_own_actors_snapshot() -> Result<()> {
    let node = Node::start().await?;
    let slot = node.spare()?;
    write_value(&node.database(&slot), 89)?;

    let parked = node
        .slots
        .park(&actor("two"), &slot, snapshot(1), 7, true)
        .await;

    assert!(parked.is_err());
    assert!(node.database(&slot).join(DATABASE_FILE).exists());
    Ok(())
}

#[tokio::test]
async fn parking_moves_a_symlinked_database_directory_without_following_it() -> Result<()> {
    let node = Node::start().await?;
    let slot = node.spare()?;
    let elsewhere = node.root.path().join("elsewhere");
    write_value(&elsewhere, 89)?;
    std::os::unix::fs::symlink(&elsewhere, node.database(&slot))?;

    let parked = node
        .slots
        .park(&actor("one"), &slot, snapshot(1), 7, false)
        .await;

    assert!(parked.is_err());
    assert!(!node.slots.is_parked(&actor("one"), "d1").await);
    assert_eq!(read_value(&elsewhere)?, 89);
    Ok(())
}

struct Node {
    root: tempfile::TempDir,
    slots: SqliteSlots,
}

impl Node {
    async fn start() -> Result<Self> {
        Self::uploading(None).await
    }

    async fn uploading(bucket: Option<Arc<dyn crate::bucket::Bucket>>) -> Result<Self> {
        let root = tempfile::tempdir()?;
        let cache = root.path().join("cache");
        let store = Arc::new(DiskStore::open(cache.clone(), 1 << 30, 1 << 10).await?);
        let layout = SlotLayout::Kubelet(root.path().join("pods"));
        let uploads = BaseUploads::new(bucket, store.staging().join("bases"));
        let slots = SqliteSlots::open(cache, Some(layout), store, uploads).await?;
        Ok(Self { root, slots })
    }

    /// What kubelet creates for a spare's `slot` emptyDir; the slot is the pod's UID.
    fn spare(&self) -> Result<String> {
        let uid = uuid::Uuid::new_v4().to_string();
        std::fs::create_dir_all(self.slot(&uid))?;
        Ok(uid)
    }

    fn slot(&self, uid: &str) -> PathBuf {
        self.root
            .path()
            .join("pods")
            .join(uid)
            .join("volumes/kubernetes.io~empty-dir/slot")
    }

    fn database(&self, uid: &str) -> PathBuf {
        self.slot(uid).join(DATABASE)
    }
}

fn actor(id: &str) -> ActorKey {
    ActorKey {
        project_id: "project".into(),
        actor_name: "Counter".into(),
        actor_id: id.into(),
    }
}

/// A snapshot of actor `one`.
fn snapshot(version: u64) -> SnapshotRef {
    SnapshotRef {
        object: crate::storage::snapshot_object_name(
            &actor("one"),
            version,
            &format!("{:032x}", 1),
        )
        .unwrap(),
        state_version: version,
        request_id: format!("request-{version}"),
        digest: format!("d{version}"),
    }
}

/// What a host does in its slot: create the database directory and write the database.
fn write_value(directory: &Path, value: i64) -> Result<()> {
    std::fs::create_dir_all(directory)?;
    let db = rusqlite::Connection::open(directory.join(DATABASE_FILE))?;
    db.execute_batch("PRAGMA journal_mode=WAL; CREATE TABLE marker(v INTEGER)")?;
    db.execute("INSERT INTO marker VALUES (?1)", [value])?;
    db.close().map_err(|(_, error)| error)?;
    Ok(())
}

fn read_value(directory: &Path) -> Result<i64> {
    let db = rusqlite::Connection::open(directory.join(DATABASE_FILE))?;
    Ok(db.query_row("SELECT v FROM marker", [], |row| row.get(0))?)
}

#[tokio::test]
async fn parking_with_a_due_base_uploads_the_parked_file() -> Result<()> {
    let bucket_dir = tempfile::tempdir()?;
    let bucket = Arc::new(crate::bucket::FileBucket::new(bucket_dir.path().into())?);
    let node = Node::uploading(Some(bucket.clone())).await?;
    let slot = node.spare()?;
    write_value(&node.database(&slot), 5)?;
    let snapshot = snapshot(5);
    let parked = node
        .slots
        .park(&actor("one"), &slot, snapshot.clone(), 9, true)
        .await?;
    let bytes = std::fs::metadata(
        node.slots
            .parked(&actor("one"))
            .join(DATABASE)
            .join(DATABASE_FILE),
    )?
    .len();
    assert_eq!(parked.bytes, bytes);
    let key = crate::bucket::base_key(&snapshot, 9, bytes)?;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while crate::bucket::Bucket::get(bucket.as_ref(), &key)
            .await?
            .is_none()
        {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        anyhow::Ok(())
    })
    .await??;
    Ok(())
}
