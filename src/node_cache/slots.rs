use std::{
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};

use super::{
    bases::{BaseUploads, Stage},
    store::{DiskStore, PARKED},
};
use crate::{actor::ActorKey, control_plane::ActorPrincipal, storage::SnapshotRef};

pub(super) const DATABASE: &str = "db";
pub(super) const DATABASE_FILE: &str = "actor.sqlite";
const RECORD: &str = "clean.json";

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SlotFile {
    pub bytes: u64,
    pub device: u64,
    pub inode: u64,
    /// The SQLite transaction the database was closed at, so a host can reopen it without the
    /// snapshot.
    pub txid: u64,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct SlotCheckout {
    pub snapshot: Option<SnapshotRef>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct SlotPark {
    pub snapshot: SnapshotRef,
    pub txid: u64,
    /// Whether the node should also upload the parked database as the actor's base.
    #[serde(default)]
    pub base: bool,
}

/// The actor a parked database belongs to and the committed snapshot and transaction it was
/// closed at. Only the node writes it, outside any slot; the transaction is the parking host's.
#[derive(Serialize, Deserialize)]
struct CleanRecord {
    actor: String,
    snapshot: SnapshotRef,
    txid: u64,
}

/// Moves actor databases between hosts' slots and parked directories (`actors/<actor>/db`) by
/// renaming them on one filesystem. Whoever owns the slots deletes whatever a dead host left in
/// one. One lock orders every move, so a database is only ever in one place.
pub(super) struct SqliteSlots {
    root: PathBuf,
    layout: Option<SlotLayout>,
    store: Arc<DiskStore>,
    uploads: BaseUploads,
    lock: tokio::sync::Mutex<()>,
}

/// Where a host's slot is, and which identity in its signed token names it.
pub(super) enum SlotLayout {
    /// A spare's `slot` emptyDir under kubelet's directory for the pod, named by the token's pod
    /// UID. Kubelet caps it and deletes it with the pod.
    Kubelet(PathBuf),
    /// A directory per local host, named by the token's host ID. The local runtime creates it and
    /// deletes it when the host exits.
    Hosts(PathBuf),
}

impl SqliteSlots {
    pub async fn open(
        root: PathBuf,
        layout: Option<SlotLayout>,
        store: Arc<DiskStore>,
        uploads: BaseUploads,
    ) -> Result<Self> {
        let slots = Self {
            root,
            layout,
            store,
            uploads,
            lock: tokio::sync::Mutex::new(()),
        };
        let parked = slots.root.join(PARKED);
        for (name, bytes) in blocking(move || keep_clean(&parked)).await? {
            slots.store.track(&format!("{PARKED}/{name}"), bytes).await;
        }
        Ok(slots)
    }

    pub async fn is_parked(&self, actor: &ActorKey, digest: &str) -> bool {
        let (parked, actor) = (self.parked(actor), actor_name(actor));
        blocking(move || Ok(read_record(&parked)))
            .await
            .ok()
            .flatten()
            .is_some_and(|record| record.actor == actor && record.snapshot.digest == digest)
    }

    pub async fn check_out(
        &self,
        actor: &ActorKey,
        slot: &str,
        snapshot: Option<SnapshotRef>,
    ) -> Result<Option<SlotFile>> {
        let target = self.slot(slot)?.join(DATABASE);
        let (key, parked, name) = (self.key(actor), self.parked(actor), actor_name(actor));
        let _moving = self.moving(actor).await;
        let file = blocking(move || check_out(&parked, &target, &name, snapshot.as_ref())).await;
        self.store.forget(&key).await;
        file
    }

    pub async fn park(
        &self,
        actor: &ActorKey,
        slot: &str,
        snapshot: SnapshotRef,
        txid: u64,
        base: bool,
    ) -> Result<SlotFile> {
        ensure!(
            crate::storage_paths::actor_from_snapshot(&snapshot.object)? == *actor,
            "snapshot belongs to another actor"
        );
        let source = self.slot(slot)?.join(DATABASE);
        let (key, parked) = (self.key(actor), self.parked(actor));
        let database = parked.join(DATABASE).join(DATABASE_FILE);
        let record = CleanRecord {
            actor: actor_name(actor),
            snapshot: snapshot.clone(),
            txid,
        };
        let _moving = self.moving(actor).await;
        self.store.forget(&key).await;
        let file = blocking(move || park(&source, &parked, &record)).await?;
        self.store.track(&key, file.bytes).await;
        if base {
            self.uploads.start(actor, database, snapshot, txid).await;
        }
        Ok(file)
    }

    /// The slot a host's token names; a host without one cannot move databases.
    pub fn slot_of<'a>(&self, host: &'a ActorPrincipal) -> Option<&'a str> {
        match self.layout.as_ref()? {
            SlotLayout::Kubelet(_) => host.slot.as_deref(),
            SlotLayout::Hosts(_) => Some(host.host_id.as_str()),
        }
    }

    /// The lock every move holds, taken once no upload is copying the actor's parked file.
    async fn moving(&self, actor: &ActorKey) -> tokio::sync::MutexGuard<'_, ()> {
        loop {
            let moving = self.lock.lock().await;
            let Some(mut copying) = self.uploads.release(actor).await else {
                return moving;
            };
            // Other actors park and check out while this one's base finishes its copy.
            drop(moving);
            let _ = copying.wait_for(|stage| *stage != Stage::Copying).await;
        }
    }

    pub(super) fn slot(&self, name: &str) -> Result<PathBuf> {
        match self.layout.as_ref().context("SQLite slots are off here")? {
            SlotLayout::Kubelet(pods) => {
                let uid = uuid::Uuid::parse_str(name).context("invalid slot")?;
                ensure!(uid.hyphenated().to_string() == name, "invalid slot");
                Ok(pods.join(name).join("volumes/kubernetes.io~empty-dir/slot"))
            }
            SlotLayout::Hosts(hosts) => {
                ensure!(
                    !name.is_empty()
                        && !name.starts_with('.')
                        && name.bytes().all(|byte| byte.is_ascii_alphanumeric()
                            || matches!(byte, b'.' | b'-' | b'_')),
                    "invalid slot"
                );
                Ok(hosts.join(name))
            }
        }
    }

    fn key(&self, actor: &ActorKey) -> String {
        format!("{PARKED}/{}", super::actor_digest(actor))
    }

    fn parked(&self, actor: &ActorKey) -> PathBuf {
        self.root.join(self.key(actor))
    }
}

fn check_out(
    parked: &Path,
    target: &Path,
    actor: &str,
    snapshot: Option<&SnapshotRef>,
) -> Result<Option<SlotFile>> {
    ensure_directory(target.parent().context("slot database has no slot")?)?;
    ensure!(!present(target)?, "slot already holds a database");
    let clean = read_record(parked)
        .filter(|record| record.actor == actor && Some(&record.snapshot) == snapshot);
    // Described before the move: once in the slot, the host controls what its path names.
    if let Some(record) = clean
        && let Ok(file) = describe(&parked.join(DATABASE), record.txid)
        && take(parked, target).is_ok()
    {
        let _ = std::fs::remove_dir_all(parked);
        return Ok(Some(file));
    }
    if present(parked)? {
        std::fs::remove_dir_all(parked)?;
    }
    Ok(None)
}

/// Removes the record before the move, so a crash between the two leaves a database nothing trusts.
fn take(parked: &Path, target: &Path) -> Result<()> {
    std::fs::remove_file(parked.join(RECORD))?;
    std::fs::rename(parked.join(DATABASE), target)?;
    sync_parent(target)
}

/// Moves the slot's database directory out before describing it: renaming moves whatever entry
/// the host left there without following it, and the host cannot swap it once outside the slot.
fn park(source: &Path, parked: &Path, record: &CleanRecord) -> Result<SlotFile> {
    if present(parked)? {
        std::fs::remove_dir_all(parked)?;
    }
    std::fs::create_dir_all(parked)?;
    std::fs::rename(source, parked.join(DATABASE))?;
    let file = match describe(&parked.join(DATABASE), record.txid) {
        Ok(file) => file,
        Err(error) => {
            std::fs::remove_dir_all(parked)?;
            return Err(error);
        }
    };
    write_record(parked, record)?;
    sync_parent(parked)?;
    Ok(file)
}

/// A closed database: a real directory of regular files whose WAL holds no frames.
fn describe(directory: &Path, txid: u64) -> Result<SlotFile> {
    ensure_directory(directory)?;
    let mut bytes = 0;
    for entry in std::fs::read_dir(directory)? {
        let metadata = entry?.metadata()?;
        ensure!(
            metadata.is_file(),
            "slot database holds something other than files"
        );
        bytes += metadata.len();
    }
    let database = std::fs::symlink_metadata(directory.join(DATABASE_FILE))?;
    ensure!(database.is_file(), "slot database is not a file");
    let wal = directory.join(format!("{DATABASE_FILE}-wal"));
    ensure!(
        !present(&wal)? || std::fs::symlink_metadata(&wal)?.len() == 0,
        "slot database has WAL frames"
    );
    Ok(SlotFile {
        bytes,
        device: database.dev(),
        inode: database.ino(),
        txid,
    })
}

/// Parked databases a previous process left, by name and size; it drops any without a record.
fn keep_clean(parked: &Path) -> Result<Vec<(String, u64)>> {
    std::fs::create_dir_all(parked)?;
    let mut kept = Vec::new();
    for entry in std::fs::read_dir(parked)? {
        let path = entry?.path();
        let file =
            read_record(&path).and_then(|record| describe(&path.join(DATABASE), record.txid).ok());
        match (path.file_name().and_then(|name| name.to_str()), file) {
            (Some(name), Some(file)) => kept.push((name.to_owned(), file.bytes)),
            _ if path.is_dir() => std::fs::remove_dir_all(&path)?,
            _ => std::fs::remove_file(&path)?,
        }
    }
    Ok(kept)
}

fn read_record(parked: &Path) -> Option<CleanRecord> {
    serde_json::from_slice(&std::fs::read(parked.join(RECORD)).ok()?).ok()
}

fn write_record(parked: &Path, record: &CleanRecord) -> Result<()> {
    let staged = parked.join(format!("{RECORD}.tmp"));
    let mut file = std::fs::File::create(&staged)?;
    serde_json::to_writer(&mut file, record)?;
    file.sync_all()?;
    std::fs::rename(staged, parked.join(RECORD))?;
    std::fs::File::open(parked)?.sync_all()?;
    Ok(())
}

fn ensure_directory(path: &Path) -> Result<()> {
    ensure!(
        std::fs::symlink_metadata(path)?.is_dir(),
        "{} is not a directory",
        path.display()
    );
    Ok(())
}

fn present(path: &Path) -> Result<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn sync_parent(path: &Path) -> Result<()> {
    let parent = path.parent().context("path has no parent")?;
    std::fs::File::open(parent)?.sync_all()?;
    Ok(())
}

fn actor_name(actor: &ActorKey) -> String {
    actor.storage_key().as_str().to_owned()
}

async fn blocking<T: Send + 'static>(
    operation: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    tokio::task::spawn_blocking(operation).await?
}

#[cfg(test)]
#[path = "../../tests/unit/node_cache/slots.rs"]
mod tests;
