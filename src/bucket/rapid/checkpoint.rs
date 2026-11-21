use super::*;
use crate::bucket::recovery::{Checkpoint, ResolvedSqlite, SnapshotHistory, resolve};
use crate::state_log::SqliteSnapshot;
use crate::storage::VerifiedSnapshot;

// Below this a checkpoint saves less than its own upload costs.
const MIN_LOG_BYTES: u64 = 1024 * 1024; // 1 MB
const MAX_LOG_BYTES: u64 = 32 * 1024 * 1024; // 32 MB
// Bounds the history a restore replays when commits are tiny. Tests write tiny commits, so they
// exercise this bound.
const MAX_LOG_VERSIONS: u64 = if cfg!(test) { 32 } else { 256 };

impl LogStorage {
    pub async fn restore(
        &self,
        snapshot: VerifiedSnapshot,
        checkpoint: Option<crate::bucket::recovery::PrefetchedCheckpoint>,
    ) -> Result<ResolvedSqlite> {
        let started = std::time::Instant::now();
        let object = snapshot.reference.object.clone();
        tracing::info!(event = "sqlite_history_started", object);
        let mut history = LogHistory {
            storage: self,
            epochs: BTreeMap::new(),
            reads: 0,
            probes: 0,
        };
        let resolved = resolve(&mut history, snapshot, checkpoint).await?;
        tracing::info!(
            event = "sqlite_history_restored",
            object,
            checkpoint = resolved.checkpoint,
            checkpoint_version = resolved.progress.version,
            parents = resolved.parents,
            archive_epochs = history
                .epochs
                .values()
                .filter(|epoch| epoch.records.is_some())
                .count(),
            dependency_gets = history.reads,
            checkpoint_gets = history.probes,
            files = resolved.sqlite.files.len(),
            ltx_bytes = resolved
                .sqlite
                .files
                .iter()
                .map(|file| file.data.len())
                .sum::<usize>(),
            duration_ms = started.elapsed().as_secs_f64() * 1000.0
        );
        Ok(resolved)
    }

    /// Returns the LTX bytes of the full copy that now ends the history at `object`, if any.
    pub async fn checkpoint(
        &self,
        head: &SnapshotRef,
        bytes: Option<Payload>,
    ) -> Result<Option<u64>> {
        let started = std::time::Instant::now();
        let object = head.object.as_str();
        let bytes = match bytes {
            Some(bytes) => bytes,
            None => self
                .snapshots
                .get(object)
                .await?
                .context("sealed checkpoint head missing")?,
        };
        let snapshot = VerifiedSnapshot::matching(head.clone(), bytes)?;
        let state_version = snapshot.reference.state_version;
        if snapshot.snapshot.sqlite.parent.is_none() {
            tracing::info!(
                event = "sqlite_checkpoint",
                object,
                state_version,
                outcome = "full_snapshot",
                duration_ms = started.elapsed().as_secs_f64() * 1000.0
            );
            return Ok(Some(sqlite_bytes(&snapshot.snapshot.sqlite.files)));
        }
        let source = snapshot.reference.clone();
        let key = format!("{object}.checkpoint");
        if let Some(stored) = self.snapshots.get(&key).await? {
            let sqlite = Checkpoint::decode(&stored, &source, snapshot.snapshot.sqlite.txid)?;
            tracing::info!(
                event = "sqlite_checkpoint",
                object,
                state_version,
                outcome = "existing",
                duration_ms = started.elapsed().as_secs_f64() * 1000.0
            );
            return Ok(Some(sqlite_bytes(&sqlite.files)));
        }
        let sqlite = self
            .restore(snapshot, None)
            .await
            .context("resolve checkpoint inputs")?
            .sqlite;
        let history_ms = started.elapsed().as_secs_f64() * 1000.0;
        let input_files = sqlite.files.len();
        let compaction_started = std::time::Instant::now();
        let file = match sqlite.files.as_slice() {
            [file] if file.level == 9 => file.clone(),
            files => self
                .compactor
                .compact(files)
                .await
                .context("compact SQLite checkpoint")?,
        };
        let compaction_ms = compaction_started.elapsed().as_secs_f64() * 1000.0;
        let checkpoint_files = vec![file];
        let checkpoint = Checkpoint {
            source,
            sqlite: SqliteSnapshot {
                txid: sqlite.txid,
                parent: None,
                files: checkpoint_files.clone(),
            },
        };
        let bytes = tokio::task::spawn_blocking(move || checkpoint.encode()).await??;
        let size = bytes.len();
        let checkpoint_bytes = sqlite_bytes(&checkpoint_files);
        let upload_started = std::time::Instant::now();
        bounded(self.snapshots.put(&key, bytes))
            .await
            .context("upload SQLite checkpoint")?;
        tracing::info!(
            event = "sqlite_checkpoint",
            object,
            outcome = "published",
            history_ms,
            compaction_ms,
            upload_ms = upload_started.elapsed().as_secs_f64() * 1000.0,
            state_version,
            input_files,
            bytes = size,
            duration_ms = started.elapsed().as_secs_f64() * 1000.0
        );
        Ok(Some(checkpoint_bytes))
    }
}

fn sqlite_bytes(files: &[crate::litestream::storage::LtxFile]) -> u64 {
    files.iter().map(|file| file.data.len() as u64).sum()
}

struct LogHistory<'a> {
    storage: &'a LogStorage,
    epochs: BTreeMap<String, EpochHistory>,
    reads: usize,
    probes: usize,
}

struct EpochHistory {
    records: Option<BTreeMap<String, Payload>>,
    indexes: Vec<String>,
    indexed: BTreeMap<String, Payload>,
    checkpoints: std::collections::BTreeSet<String>,
}

#[async_trait]
impl SnapshotHistory for LogHistory<'_> {
    async fn read(&mut self, object: &str) -> Result<Payload> {
        let (prefix, _) = object.rsplit_once('/').context("invalid snapshot name")?;
        if !self.epochs.contains_key(prefix) {
            let prefix_key = format!("{prefix}/");
            let (keys, indexes) = tokio::try_join!(
                self.storage.snapshots.list(&prefix_key),
                self.storage.index_keys(&prefix_key)
            )?;
            self.epochs.insert(
                prefix.into(),
                EpochHistory {
                    records: None,
                    indexes,
                    indexed: BTreeMap::new(),
                    checkpoints: keys
                        .into_iter()
                        .filter(|key| key.ends_with(".checkpoint"))
                        .collect(),
                },
            );
        }
        let epoch = self.epochs.get_mut(prefix).unwrap();
        if let Some(bytes) = epoch
            .records
            .as_ref()
            .and_then(|records| records.get(object))
        {
            return Ok(bytes.clone());
        }
        if let Some(bytes) = epoch.indexed.remove(object) {
            return Ok(bytes);
        }
        let version =
            super::super::snapshots::version(object).context("invalid snapshot version")?;
        let first = epoch
            .checkpoints
            .iter()
            .filter_map(|key| key.strip_suffix(".checkpoint"))
            .filter_map(super::super::snapshots::version)
            .filter(|v| *v <= version)
            .max()
            .unwrap_or(0);
        if let Some(records) = self.storage.live_records(object, first).await? {
            self.reads += 1;
            epoch.indexed = records;
            return epoch
                .indexed
                .remove(object)
                .context("live SQLite dependency missing");
        }
        self.reads += 1;
        if let Some(bytes) = self.storage.snapshots.get(object).await? {
            return Ok(bytes);
        }
        if let Some(records) = self
            .storage
            .indexed_records(object, first, &epoch.indexes)
            .await
        {
            epoch.indexed = records;
            return epoch
                .indexed
                .remove(object)
                .context("indexed SQLite dependency missing");
        }
        if epoch.records.is_none() {
            epoch.records = Some(self.storage.records(&format!("{prefix}/"), false).await?);
        }
        epoch
            .records
            .as_ref()
            .and_then(|records| records.get(object))
            .cloned()
            .context("SQLite dependency missing")
    }

    async fn checkpoint(&mut self, object: &str) -> Result<Option<Payload>> {
        let key = format!("{object}.checkpoint");
        let (prefix, _) = object.rsplit_once('/').context("invalid snapshot name")?;
        if self
            .epochs
            .get(prefix)
            .is_some_and(|epoch| !epoch.checkpoints.contains(&key))
        {
            return Ok(None);
        }
        self.probes += 1;
        self.storage.snapshots.get(&key).await
    }
}

/// Writes a checkpoint once the deltas since the last one outweigh the full copy it would write.
#[derive(Default)]
pub(super) struct Progress {
    completed: u64,
    base_bytes: u64,
    /// LTX bytes of each version logged after `completed`.
    logged: std::collections::VecDeque<(u64, u64)>,
    task: Option<(u64, AbortOnDropHandle<Result<Option<u64>>>)>,
    retry_at: Option<tokio::time::Instant>,
}

impl Progress {
    pub fn resume(progress: CheckpointProgress, version: u64) -> Self {
        let mut state = Self {
            completed: progress.version,
            base_bytes: progress.base_bytes,
            ..Self::default()
        };
        if version > state.completed {
            state.logged(version, progress.log_bytes);
        }
        state
    }

    pub fn restored(&mut self, progress: CheckpointProgress, version: u64) {
        if self.task.is_none() {
            *self = Self::resume(progress, version);
        }
    }

    pub fn logged(&mut self, version: u64, bytes: u64) {
        self.logged.push_back((version, bytes));
    }

    pub async fn schedule(
        &mut self,
        storage: Arc<LogStorage>,
        latest: Option<&(SnapshotRef, Option<Payload>)>,
    ) {
        self.collect().await;
        let Some((head, bytes)) = latest else { return };
        if self.task.is_some()
            || !self.due(head.state_version)
            || self
                .retry_at
                .is_some_and(|retry| tokio::time::Instant::now() < retry)
        {
            return;
        }
        self.start(storage, head.clone(), bytes.clone());
    }

    fn due(&self, version: u64) -> bool {
        let logged = self.log_bytes();
        let max_ltx_files_reached = version.saturating_sub(self.completed) >= MAX_LOG_VERSIONS;
        let max_log_bytes_reached = version > self.completed
            && logged >= self.base_bytes.clamp(MIN_LOG_BYTES, MAX_LOG_BYTES);

        // Trigger compaction if we have max # of ltx files, or total bytes exceeds MAX_LOG_BYTES.
        max_ltx_files_reached || max_log_bytes_reached
    }

    fn completed(&mut self, version: u64, base_bytes: Option<u64>) {
        if version <= self.completed {
            return;
        }
        self.completed = version;
        if let Some(bytes) = base_bytes {
            self.base_bytes = bytes;
        }
        while self
            .logged
            .front()
            .is_some_and(|(logged, _)| *logged <= version)
        {
            self.logged.pop_front();
        }
    }

    pub async fn finish(&mut self, deadline: tokio::time::Instant) -> CheckpointProgress {
        if let Some((version, task)) = self.task.take() {
            match tokio::time::timeout_at(deadline, task).await {
                Ok(Ok(Ok(base_bytes))) => self.completed(version, base_bytes),
                result => {
                    tracing::warn!(event = "sqlite_checkpoint_deferred", state_version = version,
                    error = ?result, "checkpoint incomplete; archived history remains recoverable")
                }
            }
        }
        CheckpointProgress {
            version: self.completed,
            base_bytes: self.base_bytes,
            log_bytes: self.log_bytes(),
        }
    }

    fn log_bytes(&self) -> u64 {
        self.logged.iter().map(|(_, bytes)| bytes).sum()
    }

    fn start(&mut self, storage: Arc<LogStorage>, head: SnapshotRef, bytes: Option<Payload>) {
        let version = head.state_version;
        tracing::info!(
            event = "sqlite_checkpoint_started",
            object = %head.object,
            state_version = version,
            trigger = "background"
        );
        self.task = Some((
            version,
            AbortOnDropHandle::new(tokio::spawn(async move {
                let started = std::time::Instant::now();
                let result = tokio::time::timeout(
                    Duration::from_secs(120),
                    storage.checkpoint(&head, bytes),
                )
                .await
                .context("checkpoint timed out")
                .and_then(|result| result);
                if let Err(error) = &result {
                    tracing::warn!(event = "sqlite_checkpoint_failed", object = %head.object, state_version = version,
                        error = %format!("{error:#}"), duration_ms = started.elapsed().as_secs_f64() * 1000.0);
                }
                result
            })),
        ));
    }

    async fn collect(&mut self) {
        if !self
            .task
            .as_ref()
            .is_some_and(|(_, task)| task.is_finished())
        {
            return;
        }
        let (version, task) = self.task.take().unwrap();
        match task.await {
            Ok(Ok(base_bytes)) => {
                self.completed(version, base_bytes);
                self.retry_at = None;
            }
            error => {
                self.retry_at = Some(tokio::time::Instant::now() + Duration::from_secs(5));
                tracing::warn!(event = "sqlite_checkpoint_deferred", state_version = version,
                    error = ?error, "SQLite checkpoint will retry");
            }
        }
    }
}
