//! Rebuilds hosts' Rapid databases outside the sandbox. A host prepares as its assignment arrives,
//! so the newest base downloads while it activates, then asks for the database at its verified
//! head. The node finds and downloads bases with its own warm client; the history after a base is
//! read with the host's forwarded token. Everything is built in the node's own staging directory
//! and only the finished file enters the slot, whose contents the host controls.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use futures_util::{
    FutureExt,
    future::{BoxFuture, Shared},
};
use serde::{Deserialize, Serialize};
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};

use super::slots::{DATABASE, DATABASE_FILE};
use crate::{
    actor::ActorKey,
    bucket::{BaseName, Bucket, SnapshotStoreConfig, SqliteBase, recovery::CheckpointProgress},
    litestream::storage::{restore_database, restoring},
    storage::{SnapshotRef, VerifiedSnapshot},
};

/// Downloads and restores at once per node, each with its own budget: a restore can wait for its
/// prepared download, so they must not share permits.
const CONCURRENT_DOWNLOADS: usize = 4;
const CONCURRENT_RESTORES: usize = 4;
/// A prepared download no restore claims within this long is dropped, checked this often.
const PREPARED_TTL: Duration = Duration::from_secs(60);
const PREPARED_FILE: &str = "base.sqlite";

/// What a host forwards so the node reads its actor's history with the host's own scope.
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct SlotStorage {
    pub bucket: String,
    pub persistence: SnapshotStoreConfig,
    pub access_token: String,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct SlotPrepare {
    pub storage: SlotStorage,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct SlotRestore {
    pub storage: SlotStorage,
    pub head: SnapshotRef,
    /// The head's bytes, base64, when the host sends them so the node need not read them again.
    #[serde(default)]
    pub head_bytes: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SlotRestored {
    pub txid: u64,
    /// `base`, or `history` when no base applied.
    pub source: String,
    pub base_bytes: Option<u64>,
    pub duration_ms: f64,
    /// Opening the actor's store, reading its head, and waiting for the prepared download.
    #[serde(default)]
    pub open_ms: f64,
    #[serde(default)]
    pub head_ms: f64,
    #[serde(default)]
    pub wait_ms: f64,
    /// What the restored history holds since its base, for the host's next due-base decision.
    #[serde(default)]
    pub progress: CheckpointProgress,
    #[serde(default)]
    pub base: Option<BaseName>,
}

pub(super) struct Restores {
    /// The archive as the node itself reads it; none means no prepared downloads.
    bases: Option<Arc<dyn Bucket>>,
    /// On the slots' filesystem, so a finished database moves into its slot by renaming.
    staging: PathBuf,
    prepared: Arc<tokio::sync::Mutex<HashMap<String, Prepared>>>,
    downloads: Arc<tokio::sync::Semaphore>,
    restores: tokio::sync::Semaphore,
    _expiry: AbortOnDropHandle<()>,
}

/// The actor's store, opened once for its prepare and restore.
type Store =
    Shared<BoxFuture<'static, Result<Arc<dyn crate::bucket::SnapshotStore>, Arc<anyhow::Error>>>>;

/// The newest base, downloading into a slot before its host knows its head. `listed` holds that
/// base once listed; the task returns its key, `None` when the actor has no base.
struct Prepared {
    actor: ActorKey,
    store: Store,
    listed: Arc<OnceLock<SqliteBase>>,
    /// Holds the download as `PREPARED_FILE`, and goes with it.
    directory: tempfile::TempDir,
    started: tokio::time::Instant,
    task: AbortOnDropHandle<Result<Option<String>>>,
}

impl Restores {
    pub fn new(bases: Option<Arc<dyn Bucket>>, staging: PathBuf) -> Self {
        let prepared = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        Self {
            bases,
            staging,
            _expiry: AbortOnDropHandle::new(tokio::spawn(expire(prepared.clone()))),
            prepared,
            downloads: Arc::new(tokio::sync::Semaphore::new(CONCURRENT_DOWNLOADS)),
            restores: tokio::sync::Semaphore::new(CONCURRENT_RESTORES),
        }
    }

    /// Starts downloading `actor`'s newest base for `slot`'s restore, unless one is already coming.
    pub async fn prepare(&self, actor: &ActorKey, slot: &str, storage: SlotStorage) -> Result<()> {
        let mut prepared = self.prepared.lock().await;
        if prepared.contains_key(slot) {
            return Ok(());
        }
        let directory = self.staged().await?;
        let store = open(storage).boxed().shared();
        let listed = Arc::new(OnceLock::new());
        let task = AbortOnDropHandle::new(tokio::spawn(download_newest(
            self.bases.clone(),
            store.clone(),
            actor.clone(),
            listed.clone(),
            directory.path().join(PREPARED_FILE),
            self.downloads.clone(),
        )));
        prepared.insert(
            slot.to_owned(),
            Prepared {
                actor: actor.clone(),
                store,
                listed,
                directory,
                started: tokio::time::Instant::now(),
                task,
            },
        );
        Ok(())
    }

    /// Writes `actor`'s database at `head` into the database directory of the slot at `slot_path`
    /// and returns its SQLite transaction. A retry replaces what an earlier attempt handed over.
    pub async fn restore(
        &self,
        actor: &ActorKey,
        slot: &str,
        slot_path: &Path,
        request: SlotRestore,
    ) -> Result<SlotRestored> {
        let started = Instant::now();
        let prepared = self.take(slot, actor).await;
        let _permit = self.restores.acquire().await?;
        let store = match &prepared {
            Some(prepared) => prepared.store.clone().await,
            None => open(request.storage.clone()).await,
        }
        .map_err(|error| anyhow::anyhow!("{error:#}"))?;
        let open_ms = elapsed_ms(started);
        let head = read_head(store.as_ref(), request.head, request.head_bytes).await?;
        let head_ms = elapsed_ms(started) - open_ms;
        let database = self.staged().await?;
        let restored = async {
            let restored = restore_into(store.as_ref(), head, prepared, database.path()).await?;
            hand_over(database.path(), slot_path).await?;
            anyhow::Ok(restored)
        }
        .await;
        discard(database);
        let restored = restored?;
        Ok(SlotRestored {
            txid: restored.txid,
            source: if restored.base_bytes.is_some() {
                "base"
            } else {
                "history"
            }
            .into(),
            base_bytes: restored.base_bytes,
            duration_ms: elapsed_ms(started),
            open_ms,
            head_ms,
            wait_ms: restored.wait_ms,
            progress: restored.progress,
            base: restored.base,
        })
    }

    /// Drops the slot's prepared download, when its host reopened a parked database or left.
    pub async fn cancel(&self, slot: &str) {
        let cancelled = self.prepared.lock().await.remove(slot);
        if let Some(prepared) = cancelled {
            discard(prepared);
        }
    }

    async fn take(&self, slot: &str, actor: &ActorKey) -> Option<Prepared> {
        let prepared = self.prepared.lock().await.remove(slot)?;
        if &prepared.actor == actor {
            return Some(prepared);
        }
        discard(prepared);
        None
    }

    /// A new directory of the node's own, deleted when dropped.
    async fn staged(&self) -> Result<tempfile::TempDir> {
        tokio::fs::create_dir_all(&self.staging).await?;
        Ok(tempfile::TempDir::new_in(&self.staging)?)
    }
}

/// Drops prepared downloads nobody claimed, such as a dead host's, whether or not other hosts
/// prepare: their files are the node's own, so nothing else deletes them.
async fn expire(prepared: Arc<tokio::sync::Mutex<HashMap<String, Prepared>>>) {
    let mut ticks = tokio::time::interval(PREPARED_TTL);
    loop {
        ticks.tick().await;
        let mut prepared = prepared.lock().await;
        let expired: Vec<String> = prepared
            .iter()
            .filter(|(_, prepared)| prepared.started.elapsed() >= PREPARED_TTL)
            .map(|(slot, _)| slot.clone())
            .collect();
        for slot in expired {
            discard(prepared.remove(&slot));
        }
    }
}

/// The head the host verified, from the bytes it sent or else read again.
async fn read_head(
    store: &dyn crate::bucket::SnapshotStore,
    head: SnapshotRef,
    sent: Option<String>,
) -> Result<VerifiedSnapshot> {
    use base64::{Engine, engine::general_purpose::STANDARD};
    let bytes = match sent {
        Some(sent) => STANDARD.decode(sent)?.into(),
        None => store
            .get(&head.object)
            .await?
            .context("actor head is missing")?,
    };
    VerifiedSnapshot::matching(head, bytes)
}

struct Restored {
    txid: u64,
    base_bytes: Option<u64>,
    wait_ms: f64,
    progress: CheckpointProgress,
    base: Option<BaseName>,
}

/// Starts from the newest base, using the prepared download when the head's history starts from
/// it. A base that fails to download or apply gives way to the next older one, then to history
/// alone.
async fn restore_into(
    store: &dyn crate::bucket::SnapshotStore,
    head: VerifiedSnapshot,
    mut prepared: Option<Prepared>,
    database: &Path,
) -> Result<Restored> {
    tokio::fs::create_dir_all(database).await?;
    let listed = prepared
        .as_ref()
        .and_then(|prepared| prepared.listed.get().cloned());
    let mut bases = crate::bucket::Bases::Newest(listed);
    loop {
        let mut restored = store.restore_sqlite(head.clone(), bases, None).await?;
        let wait_ms = use_prepared(&mut restored, prepared.take(), database).await;
        let failed = restored.base.as_ref().map(|base| base.name.version);
        match finish(database, restored, wait_ms).await {
            Ok(restored) => return Ok(restored),
            Err(error) => {
                let Some(version) = failed else {
                    return Err(error);
                };
                tracing::warn!(base_version = version, error = %format!("{error:#}"), "SQLite base failed to restore, trying an older one");
                clear(database).await?;
                bases = crate::bucket::Bases::Below(version);
            }
        }
    }
}

/// Moves the prepared download into place when it is the base `restored` starts from, and
/// returns how long that waited for it.
async fn use_prepared(
    restored: &mut crate::bucket::RestoredSqlite,
    prepared: Option<Prepared>,
    database: &Path,
) -> f64 {
    let waited = Instant::now();
    let (Some(base), Some(prepared)) = (restored.base.as_mut(), prepared) else {
        return 0.0;
    };
    let (directory, task) = (prepared.directory, prepared.task);
    let downloaded = matches!(task.await, Ok(Ok(Some(key))) if key == base.key);
    let partial = restoring(database);
    if downloaded
        && tokio::fs::rename(directory.path().join(PREPARED_FILE), &partial)
            .await
            .is_ok()
    {
        base.downloaded = Some(partial);
    }
    discard(directory);
    elapsed_ms(waited)
}

async fn finish(
    database: &Path,
    restored: crate::bucket::RestoredSqlite,
    wait_ms: f64,
) -> Result<Restored> {
    restore_database(
        &crate::litestream::EmbeddedRestore,
        database,
        restored.base.as_ref(),
        &restored.sqlite.files,
        restored.sqlite.txid,
    )
    .await?;
    Ok(Restored {
        txid: restored.sqlite.txid,
        base_bytes: restored.base.as_ref().map(|base| base.name.bytes),
        wait_ms,
        progress: restored.progress,
        base: restored.base.map(|base| base.name),
    })
}

/// Opens the actor's store beside finding and downloading its newest base, so the restore finds
/// both ready. The store's own errors surface when the restore awaits it.
async fn download_newest(
    bases: Option<Arc<dyn Bucket>>,
    store: Store,
    actor: ActorKey,
    listed: Arc<OnceLock<SqliteBase>>,
    file: PathBuf,
    permits: Arc<tokio::sync::Semaphore>,
) -> Result<Option<String>> {
    let newest = async {
        let Some(bases) = bases else {
            return Ok(None);
        };
        let Some(base) = crate::bucket::newest_base_of(&bases, &actor).await? else {
            return Ok(None);
        };
        let _ = listed.set(base.clone());
        download(base.clone(), file, permits).await?;
        anyhow::Ok(Some(base.key))
    };
    let (_, newest) = tokio::join!(store, newest);
    newest
}

async fn download(
    base: SqliteBase,
    file: PathBuf,
    permits: Arc<tokio::sync::Semaphore>,
) -> Result<()> {
    let _permit = permits.acquire_owned().await?;
    let directory = file.parent().context("prepared base has no directory")?;
    tokio::fs::create_dir_all(directory).await?;
    if tokio::fs::try_exists(&file).await? {
        tokio::fs::remove_file(&file).await?;
    }
    let started = Instant::now();
    base.download_to(&file).await?;
    tracing::info!(
        event = "sqlite_base_prepared",
        bytes = base.name.bytes,
        duration_ms = started.elapsed().as_secs_f64() * 1000.0
    );
    Ok(())
}

/// The actor's Rapid store, read with the token its host forwarded. It names no actor, so it
/// opens no write segment: only the host writes.
async fn open(
    storage: SlotStorage,
) -> Result<Arc<dyn crate::bucket::SnapshotStore>, Arc<anyhow::Error>> {
    let opened = async {
        let state = crate::bucket::GcsBucket::for_restore(
            &storage.bucket,
            crate::bucket::forwarded_token(storage.access_token.clone()),
        )
        .await?;
        let store: Arc<dyn crate::bucket::SnapshotStore> =
            Arc::new(crate::bucket::RapidSnapshots::gcs(
                &storage.persistence,
                state.clients(),
                CancellationToken::new(),
                None,
            )?);
        anyhow::Ok(store)
    };
    opened.await.map_err(Arc::new)
}

/// Moves the database built in `staged` into the database directory the host made in its slot,
/// replacing any earlier attempt's. The host controls the slot's contents, so the move resolves
/// that directory once, refusing a symlink, and never names a path inside the slot again.
async fn hand_over(staged: &Path, slot: &Path) -> Result<()> {
    use nix::{
        fcntl::{AT_FDCWD, OFlag, open, openat, renameat},
        sys::stat::Mode,
    };
    use std::os::unix::fs::PermissionsExt;
    let (file, slot) = (staged.join(DATABASE_FILE), slot.to_owned());
    tokio::task::spawn_blocking(move || {
        // The node writes as another user, so the host gets read and write access to the file.
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o666))?;
        let directory = OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC;
        let slot = open(&slot, directory, Mode::empty()).context("open the slot")?;
        let database = openat(&slot, DATABASE, directory, Mode::empty())
            .context("open the slot's database directory")?;
        renameat(AT_FDCWD, &file, &database, DATABASE_FILE)
            .context("move the database into the slot")?;
        anyhow::Ok(())
    })
    .await?
}

fn elapsed_ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1000.0
}

async fn clear(database: &Path) -> Result<()> {
    for file in [restoring(database), database.join(DATABASE_FILE)] {
        if tokio::fs::try_exists(&file).await? {
            tokio::fs::remove_file(&file).await?;
        }
    }
    Ok(())
}

/// Drops `value` off the async workers: it may delete a downloaded base.
fn discard<T: Send + 'static>(value: T) {
    tokio::task::spawn_blocking(move || drop(value));
}

#[cfg(test)]
#[path = "../../tests/unit/node_cache/restores.rs"]
mod tests;
