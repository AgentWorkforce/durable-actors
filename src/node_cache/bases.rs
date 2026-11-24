use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use anyhow::{Result, ensure};
use tokio_util::task::AbortOnDropHandle;

use crate::{actor::ActorKey, bucket::Bucket, storage::SnapshotRef};

/// Uploads at once per node, so a wave of sleeping actors shares the node's network.
const CONCURRENT_UPLOADS: usize = 4;

/// Uploads parked databases as bases, after their hosts have exited and outside any sandbox.
/// Each upload reads its own copy, made once it has an upload permit so waiting uploads hold no
/// disk. A host can take its file back once the copy is made without stopping the upload: an
/// actor that wakes more often than its base takes to upload would otherwise never get one.
pub(super) struct BaseUploads {
    bucket: Option<Arc<dyn Bucket>>,
    staging: PathBuf,
    running: tokio::sync::Mutex<HashMap<String, Upload>>,
    permits: Arc<tokio::sync::Semaphore>,
}

struct Upload {
    stage: tokio::sync::watch::Receiver<Stage>,
    task: AbortOnDropHandle<()>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Stage {
    Waiting,
    Copying,
    Copied,
}

impl BaseUploads {
    /// Uploads to `bucket`, copying parked databases into `staging` first.
    pub fn new(bucket: Option<Arc<dyn Bucket>>, staging: PathBuf) -> Self {
        Self {
            bucket,
            staging,
            running: tokio::sync::Mutex::new(HashMap::new()),
            permits: Arc::new(tokio::sync::Semaphore::new(CONCURRENT_UPLOADS)),
        }
    }

    /// Uploads the database parked at `path` as the actor's base at `snapshot`, unless an earlier
    /// base of the actor's is still uploading from its copy.
    pub async fn start(&self, actor: &ActorKey, path: PathBuf, snapshot: SnapshotRef, txid: u64) {
        let Some(bucket) = self.bucket.clone() else {
            tracing::warn!(event = "sqlite_base_skipped", "no base bucket configured");
            return;
        };
        let key = super::actor_digest(actor);
        let mut running = self.running.lock().await;
        running.retain(|_, upload| !upload.task.is_finished());
        if running
            .get(&key)
            .is_some_and(|upload| *upload.stage.borrow() == Stage::Copied)
        {
            tracing::info!(
                event = "sqlite_base_skipped",
                "an earlier base is still uploading"
            );
            return;
        }
        let (stage, watching) = tokio::sync::watch::channel(Stage::Waiting);
        let (staging, permits) = (self.staging.clone(), self.permits.clone());
        let task = AbortOnDropHandle::new(tokio::spawn(async move {
            let Ok(_permit) = permits.acquire_owned().await else {
                return;
            };
            stage.send_replace(Stage::Copying);
            let copy = copy(&path, &staging).await;
            stage.send_replace(Stage::Copied);
            match copy {
                Ok(copy) => upload(bucket.as_ref(), &copy, &snapshot, txid).await,
                Err(error) => {
                    tracing::warn!(event = "sqlite_base_upload_failed", error = %format!("{error:#}"));
                }
            }
        }));
        running.insert(
            key,
            Upload {
                stage: watching,
                task,
            },
        );
    }

    /// Frees the actor's parked file to move: drops an upload still waiting for a permit, which
    /// has not read it, and returns one still copying it, to wait for.
    pub async fn release(&self, actor: &ActorKey) -> Option<tokio::sync::watch::Receiver<Stage>> {
        let key = super::actor_digest(actor);
        let mut running = self.running.lock().await;
        let stage = running.get(&key).map(|upload| *upload.stage.borrow());
        match stage {
            Some(Stage::Waiting) => {
                running.remove(&key);
                None
            }
            Some(Stage::Copying) => running.get(&key).map(|upload| upload.stage.clone()),
            Some(Stage::Copied) | None => None,
        }
    }
}

/// A copy of the parked database in `staging`, deleted when dropped. A host may have left the
/// parked directory holding a symlink, so the copy reads only a regular file.
async fn copy(path: &Path, staging: &Path) -> Result<tempfile::TempPath> {
    use std::os::unix::fs::OpenOptionsExt;
    let (path, staging) = (path.to_owned(), staging.to_owned());
    tokio::task::spawn_blocking(move || {
        let mut source = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(nix::libc::O_NOFOLLOW)
            .open(&path)?;
        ensure!(
            source.metadata()?.is_file(),
            "parked database is not a file"
        );
        std::fs::create_dir_all(&staging)?;
        let mut copy = tempfile::NamedTempFile::new_in(&staging)?;
        std::io::copy(&mut source, copy.as_file_mut())?;
        Ok(copy.into_temp_path())
    })
    .await?
}

async fn upload(bucket: &dyn Bucket, copy: &Path, snapshot: &SnapshotRef, txid: u64) {
    let started = Instant::now();
    let (uploaded, resources) = crate::process_usage::measure(crate::bucket::upload_sqlite_base(
        bucket, snapshot, txid, copy,
    ))
    .await;
    let duration_ms = started.elapsed().as_secs_f64() * 1000.0;
    match uploaded {
        Ok(bytes) => tracing::info!(
            event = "sqlite_base_uploaded",
            state_version = snapshot.state_version,
            bytes,
            cpu_ms = resources.cpu_ms,
            peak_rss_bytes = resources.peak_rss_bytes,
            duration_ms
        ),
        Err(error) => tracing::warn!(
            event = "sqlite_base_upload_failed",
            state_version = snapshot.state_version,
            error = %format!("{error:#}"),
            cpu_ms = resources.cpu_ms, peak_rss_bytes = resources.peak_rss_bytes,
            duration_ms
        ),
    }
}

#[cfg(test)]
#[path = "../../tests/unit/node_cache/bases.rs"]
mod tests;
