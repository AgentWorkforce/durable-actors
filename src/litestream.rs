use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use terse_litestream::{
    CheckpointMode, Database, Durability, FileStore, Options, ReplicaStore, Segment, SegmentWriter,
};
use tokio::sync::watch;
use tokio_util::task::AbortOnDropHandle;

use storage::LtxFile;

pub(crate) mod compaction;
pub(crate) mod storage;

/// How often a database whose store has no checkpoints of its own writes a full copy, which
/// keeps restores from walking an ever longer chain of deltas.
pub(crate) const FULL_SNAPSHOT_INTERVAL: Duration = Duration::from_secs(60);
const CHECKPOINT_IDLE: Duration = Duration::from_secs(1);
const CHECKPOINT_MAX_DELAY: Duration = Duration::from_secs(10);

#[async_trait]
pub(crate) trait Replicator: DatabaseRestore {
    /// Starts capture and returns its position: `resume_at` continues a restored history, or
    /// 0 when capture starts a new one.
    async fn register(
        &self,
        path: &Path,
        replica: &Path,
        full_snapshots: Option<Duration>,
        resume_at: Option<u64>,
    ) -> Result<u64>;
    async fn sync(&self, path: &Path) -> Result<u64>;
    async fn unregister(&self, path: &Path) -> Result<()>;
}

#[async_trait]
pub(crate) trait DatabaseRestore: Send + Sync {
    async fn restore(&self, files: &[LtxFile], path: &Path, txid: u64) -> Result<()>;
}

pub(crate) struct EmbeddedRestore;

#[derive(Default)]
pub(crate) struct Litestream {
    registry: Arc<Registry>,
    checkpoints: Mutex<HashMap<PathBuf, IdleCheckpoint>>,
}

#[async_trait]
impl Replicator for Litestream {
    async fn register(
        &self,
        path: &Path,
        replica: &Path,
        full_snapshots: Option<Duration>,
        resume_at: Option<u64>,
    ) -> Result<u64> {
        let (registry, owned, replica) =
            (self.registry.clone(), path.to_owned(), replica.to_owned());
        let position = tokio::task::spawn_blocking(move || {
            registry.register(owned, replica, full_snapshots, resume_at)
        })
        .await??;
        let checkpoint = IdleCheckpoint::start(self.registry.clone(), path.to_owned());
        self.checkpoints()?.insert(path.to_owned(), checkpoint);
        Ok(position)
    }

    async fn sync(&self, path: &Path) -> Result<u64> {
        let (registry, owned) = (self.registry.clone(), path.to_owned());
        let (position, advanced) =
            tokio::task::spawn_blocking(move || registry.sync(&owned)).await??;
        if advanced && let Some(checkpoint) = self.checkpoints()?.get(path) {
            checkpoint.committed();
        }
        Ok(position)
    }

    async fn unregister(&self, path: &Path) -> Result<()> {
        self.checkpoints()?.remove(path);
        let (registry, path) = (self.registry.clone(), path.to_owned());
        tokio::task::spawn_blocking(move || registry.unregister(&path)).await?
    }
}

impl Litestream {
    fn checkpoints(&self) -> Result<std::sync::MutexGuard<'_, HashMap<PathBuf, IdleCheckpoint>>> {
        self.checkpoints
            .lock()
            .map_err(|_| anyhow::anyhow!("SQLite checkpoints poisoned"))
    }
}

/// Runs the WAL checkpoint once commits pause, so commits rarely wait on one.
struct IdleCheckpoint {
    commits: watch::Sender<()>,
    _task: AbortOnDropHandle<()>,
}

impl IdleCheckpoint {
    fn start(registry: Arc<Registry>, path: PathBuf) -> Self {
        let (commits, receiver) = watch::channel(());
        Self {
            commits,
            _task: AbortOnDropHandle::new(tokio::spawn(checkpoint_when_idle(
                registry, path, receiver,
            ))),
        }
    }

    fn committed(&self) {
        self.commits.send_replace(());
    }
}

async fn checkpoint_when_idle(
    registry: Arc<Registry>,
    path: PathBuf,
    mut commits: watch::Receiver<()>,
) {
    while commits.changed().await.is_ok() {
        if !wait_for_idle(&mut commits).await {
            return;
        }
        let (registry, owned) = (registry.clone(), path.clone());
        let started = Instant::now();
        let result = tokio::task::spawn_blocking(move || registry.checkpoint(&owned))
            .await
            .map_err(anyhow::Error::from)
            .and_then(|result| result);
        let duration_ms = started.elapsed().as_secs_f64() * 1_000.0;
        match result {
            Ok(()) => tracing::debug!(event = "sqlite_wal_checkpoint", duration_ms),
            Err(error) => tracing::warn!(event = "sqlite_wal_checkpoint_failed", duration_ms,
                error = %format!("{error:#}")),
        }
    }
}

/// Waits until no commit arrives for `CHECKPOINT_IDLE`, or `CHECKPOINT_MAX_DELAY` passes.
async fn wait_for_idle(commits: &mut watch::Receiver<()>) -> bool {
    let deadline = tokio::time::Instant::now() + CHECKPOINT_MAX_DELAY;
    loop {
        tokio::select! {
            () = tokio::time::sleep(CHECKPOINT_IDLE) => return true,
            () = tokio::time::sleep_until(deadline) => return true,
            changed = commits.changed() => if changed.is_err() { return false },
        }
    }
}

#[async_trait]
impl DatabaseRestore for Litestream {
    async fn restore(&self, files: &[LtxFile], path: &Path, txid: u64) -> Result<()> {
        EmbeddedRestore.restore(files, path, txid).await
    }
}

#[async_trait]
impl DatabaseRestore for EmbeddedRestore {
    async fn restore(&self, files: &[LtxFile], path: &Path, txid: u64) -> Result<()> {
        ensure!(txid > 0, "cannot restore an empty SQLite position");
        let (files, path) = (files.to_vec(), path.to_owned());
        tokio::task::spawn_blocking(move || {
            let replica = MemoryReplica::decode(&files)?;
            terse_litestream::restore_with(&replica, path, Some(txid), Durability::Scratch)?;
            anyhow::Ok(())
        })
        .await?
    }
}

/// Restores read the LTX files from memory instead of writing them to disk first.
struct MemoryReplica(Vec<(Segment, bytes::Bytes)>);

impl MemoryReplica {
    fn decode(files: &[LtxFile]) -> Result<Self> {
        files
            .iter()
            .map(|file| {
                file.validate()?;
                let segment = Segment::new(file.level, file.first, file.last)?;
                Ok((segment, file.data.clone()))
            })
            .collect::<Result<_>>()
            .map(Self)
    }
}

impl ReplicaStore for MemoryReplica {
    fn list(&self, level: u8) -> Result<Vec<Segment>> {
        Ok(self
            .0
            .iter()
            .map(|(segment, _)| *segment)
            .filter(|segment| segment.level == level)
            .collect())
    }

    fn open(&self, segment: &Segment) -> Result<Box<dyn std::io::Read + Send>> {
        let (_, bytes) = self
            .0
            .iter()
            .find(|(candidate, _)| candidate == segment)
            .context("LTX file missing from restore")?;
        Ok(Box::new(std::io::Cursor::new(bytes.clone())))
    }

    fn create(&self, _: &Segment) -> Result<Box<dyn SegmentWriter>> {
        anyhow::bail!("restore replicas are read-only")
    }

    fn remove(&self, _: &Segment) -> Result<()> {
        anyhow::bail!("restore replicas are read-only")
    }
}

#[derive(Default)]
struct Registry {
    databases: Mutex<HashMap<PathBuf, Arc<Mutex<Option<Capture>>>>>,
}

impl Registry {
    fn register(
        &self,
        path: PathBuf,
        replica: PathBuf,
        full_snapshots: Option<Duration>,
        resume_at: Option<u64>,
    ) -> Result<u64> {
        let mut databases = self
            .databases
            .lock()
            .map_err(|_| anyhow::anyhow!("SQLite registry poisoned"))?;
        ensure!(
            !databases.contains_key(&path),
            "SQLite database already registered"
        );
        let database = open_capture(&path, replica, resume_at)?;
        let position = database.position();
        databases.insert(
            path,
            Arc::new(Mutex::new(Some(Capture {
                database,
                full_snapshots,
                snapshot_at: Instant::now(),
            }))),
        );
        Ok(position)
    }

    /// Returns the capture position and whether this sync captured anything.
    fn sync(&self, path: &Path) -> Result<(u64, bool)> {
        self.with_capture(path, |capture| {
            let before = capture.database.position();
            let position = capture.sync()?;
            Ok((position, position != before))
        })
    }

    fn checkpoint(&self, path: &Path) -> Result<()> {
        self.with_capture(path, |capture| {
            capture.database.checkpoint(CheckpointMode::Passive)?;
            Ok(())
        })
    }

    fn with_capture<T>(
        &self,
        path: &Path,
        run: impl FnOnce(&mut Capture) -> Result<T>,
    ) -> Result<T> {
        let database = self
            .databases
            .lock()
            .map_err(|_| anyhow::anyhow!("SQLite registry poisoned"))?
            .get(path)
            .cloned()
            .context("SQLite database is not registered")?;
        let mut capture = database
            .lock()
            .map_err(|_| anyhow::anyhow!("SQLite capture poisoned"))?;
        run(capture
            .as_mut()
            .context("SQLite database was unregistered")?)
    }

    fn unregister(&self, path: &Path) -> Result<()> {
        let database = self
            .databases
            .lock()
            .map_err(|_| anyhow::anyhow!("SQLite registry poisoned"))?
            .remove(path)
            .context("SQLite database is not registered")?;
        // Dropped rather than closed: closing syncs, and the local replica is thrown away with it.
        database
            .lock()
            .map_err(|_| anyhow::anyhow!("SQLite capture poisoned"))?
            .take()
            .context("SQLite database was unregistered")?;
        Ok(())
    }
}

/// The local replica only stages files until their commit is uploaded, so it skips fsync. A new
/// history's base snapshot waits for the first write, and WAL checkpoints run between commits.
fn open_capture(path: &Path, replica: PathBuf, resume_at: Option<u64>) -> Result<Database> {
    let options = Options {
        defer_base_snapshot: true,
        min_checkpoint_pages: 0,
        checkpoint_interval: Duration::ZERO,
        ..Options::default()
    };
    let store = Arc::new(FileStore::scratch(replica));
    if let Some(txid) = resume_at {
        match Database::resume(path, options.clone(), store.clone(), txid) {
            Ok(database) => return Ok(database),
            Err(error) => tracing::warn!(event = "sqlite_resume_failed", txid,
                error = %format!("{error:#}"), "SQLite capture starts a new history"),
        }
    }
    Database::with_store(path, options, store)
}

struct Capture {
    database: Database,
    full_snapshots: Option<Duration>,
    snapshot_at: Instant,
}

impl Capture {
    fn sync(&mut self) -> Result<u64> {
        let captured = self.database.sync()?;
        if captured.changed
            && self
                .full_snapshots
                .is_some_and(|interval| self.snapshot_at.elapsed() >= interval)
        {
            self.database.snapshot()?;
            self.snapshot_at = Instant::now();
        }
        Ok(self.database.position())
    }
}

#[cfg(test)]
#[path = "../tests/unit/litestream.rs"]
mod tests;
