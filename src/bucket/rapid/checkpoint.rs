use super::base::BaseName;
use super::*;
use crate::bucket::recovery::{ResolvedSqlite, SnapshotHistory, resolve};
use crate::storage::VerifiedSnapshot;
use futures_util::{StreamExt, TryStreamExt, stream};

// Below this a base saves less than its own upload costs.
const MIN_LOG_BYTES: u64 = 1024 * 1024;
// Each version costs a restore about 0.2 ms however small it is, so tiny commits also need a
// bound. Larger databases restore slowly anyway and checkpoint expensively, so they allow more.
const MIN_LOG_VERSIONS: u64 = 256;
const MAX_LOG_VERSIONS: u64 = 4096;
const BASE_BYTES_PER_LOG_VERSION: u64 = 512 * 1024;

/// Versions logged after a base before the next is due: 256 up to 128 MiB, 4096 from 2 GiB.
pub(super) fn max_log_versions(base_bytes: u64) -> u64 {
    (base_bytes / BASE_BYTES_PER_LOG_VERSION).clamp(MIN_LOG_VERSIONS, MAX_LOG_VERSIONS)
}

impl LogStorage {
    /// Resolves the history of `snapshot`'s database. Given `download`, the base it starts from
    /// downloads there while the changes since that base are read.
    pub async fn restore(
        &self,
        snapshot: VerifiedSnapshot,
        bases: crate::bucket::Bases,
        download: Option<&std::path::Path>,
    ) -> Result<ResolvedSqlite> {
        let reference = &snapshot.reference;
        tracing::info!(event = "sqlite_history_started", object = reference.object);
        let base = match bases {
            crate::bucket::Bases::Newest(Some(base))
                if base.name.version <= reference.state_version =>
            {
                Some(base)
            }
            crate::bucket::Bases::Newest(_) => {
                self.newest_base(reference).await.unwrap_or_else(|error| {
                    tracing::warn!(error = %format!("{error:#}"), "SQLite bases unavailable; restoring from history");
                    None
                })
            }
            crate::bucket::Bases::Below(version) => match version.checked_sub(1) {
                Some(version) => self.base_at_or_below(reference, version).await?,
                None => None,
            },
        };
        let Some(newest) = base else {
            return self.restore_from(snapshot, None, download).await;
        };
        match self
            .restore_from(snapshot.clone(), Some(newest.clone()), download)
            .await
        {
            Ok(result) => return Ok(result),
            Err(error) => {
                tracing::warn!(base = newest.key, error = %format!("{error:#}"), "SQLite restore from newest base failed")
            }
        }
        if let Some(version) = newest.name.version.checked_sub(1)
            && let Some(older) = self.base_at_or_below(reference, version).await?
        {
            match self
                .restore_from(snapshot.clone(), Some(older.clone()), download)
                .await
            {
                Ok(result) => return Ok(result),
                Err(error) => {
                    tracing::warn!(base = older.key, error = %format!("{error:#}"), "SQLite restore from previous base failed")
                }
            }
        }
        self.restore_from(snapshot, None, download)
            .await
            .context("restore from history after base recovery failed")
    }

    async fn restore_from(
        &self,
        snapshot: VerifiedSnapshot,
        base: Option<super::base::SqliteBase>,
        download: Option<&std::path::Path>,
    ) -> Result<ResolvedSqlite> {
        let started = std::time::Instant::now();
        let object = snapshot.reference.object.clone();
        let mut history = LogHistory {
            storage: self,
            epochs: BTreeMap::new(),
            reads: 0,
            probes: 0,
            base: base
                .as_ref()
                .map(|base| (base.name.owner_epoch, base.name.version)),
        };
        let (resolved, downloaded) = tokio::join!(
            resolve(&mut history, snapshot, base.clone()),
            download_base(base.as_ref(), download)
        );
        let mut resolved = resolved?;
        let mut base_download_ms = None;
        if let (Some((dir, elapsed)), Some(base), Some(path)) =
            (downloaded?, resolved.base.as_mut(), download)
        {
            tokio::fs::rename(dir.path().join("base.sqlite"), path).await?;
            base.downloaded = Some(path.to_owned());
            base_download_ms = Some(elapsed);
        }
        tracing::info!(
            event = "sqlite_history_restored",
            object,
            base_version = resolved.base.as_ref().map(|base| base.name.version),
            base_bytes = resolved.base.as_ref().map(|base| base.name.bytes),
            base_download_ms,
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
}

/// Downloads `base` to `path`, if both are given, and returns how long it took.
async fn download_base(
    base: Option<&super::base::SqliteBase>,
    path: Option<&std::path::Path>,
) -> Result<Option<(tempfile::TempDir, f64)>> {
    let (Some(base), Some(path)) = (base, path) else {
        return Ok(None);
    };
    let started = std::time::Instant::now();
    let dir = tempfile::tempdir_in(path.parent().context("base download has no directory")?)?;
    base.download_to(&dir.path().join("base.sqlite")).await?;
    Ok(Some((dir, started.elapsed().as_secs_f64() * 1000.0)))
}

struct LogHistory<'a> {
    storage: &'a LogStorage,
    epochs: BTreeMap<String, EpochHistory>,
    reads: usize,
    probes: usize,
    /// The owner epoch and version of the base the walk stops at, if any.
    base: Option<(u64, u64)>,
}

struct EpochHistory {
    records: Option<BTreeMap<String, Payload>>,
    /// Every key the archive holds under the epoch, which names its log segments.
    listed: Arc<std::collections::BTreeSet<String>>,
    indexes: Vec<String>,
    indexed: BTreeMap<String, Payload>,
    checkpoints: std::collections::BTreeSet<String>,
    /// Versions stored as their own objects; any other version is only in the log.
    heads: std::collections::BTreeSet<String>,
}

impl EpochHistory {
    /// The newest checkpointed version at or below `version`, where the walk will stop.
    fn floor(&self, version: u64) -> u64 {
        self.checkpoint_at_or_below(version).unwrap_or(0)
    }

    fn checkpoint_at_or_below(&self, version: u64) -> Option<u64> {
        self.checkpoints
            .iter()
            .filter_map(|key| key.strip_suffix(".checkpoint"))
            .filter_map(super::super::snapshots::version)
            .filter(|v| *v <= version)
            .max()
    }
}

impl LogHistory<'_> {
    /// Lists owner epochs from `prefix`'s downward, a wave at a time, until one holds a checkpoint
    /// at or below `version`, then prefetches every batch they hold back to that checkpoint. A
    /// history that spans many activations is walked in a few round trips, not a few per epoch.
    async fn plan(&mut self, prefix: &str, version: u64) -> Result<()> {
        let (root, epoch) = prefix.rsplit_once('/').context("invalid snapshot prefix")?;
        let mut next = u64::from_str_radix(epoch, 16).context("invalid owner epoch")?;
        let starting_epoch = next;
        let mut planned = Vec::new();
        let mut floor = None;
        while floor.is_none() && next > 0 && planned.len() < MAX_PLANNED_EPOCHS {
            let base_epoch = self
                .base
                .filter(|&(epoch, base)| epoch <= next && base <= version)
                .map_or(1, |(epoch, _)| epoch);
            let lowest = next
                .saturating_sub(EPOCHS_PER_WAVE - 1)
                .max(base_epoch)
                .max(1);
            let storage = self.storage;
            let mut wave: Vec<(String, EpochHistory)> = stream::iter((lowest..=next).rev())
                .map(|epoch| async move {
                    let prefix = format!("{root}/{epoch:032x}");
                    let history = list_epoch(storage, &prefix).await?;
                    anyhow::Ok((prefix, history))
                })
                .buffered(EPOCHS_PER_WAVE as usize)
                .try_collect()
                .await?;
            let checkpoint = wave
                .iter()
                .find_map(|(_, epoch)| epoch.checkpoint_at_or_below(version));
            let based = self
                .base
                .filter(|&(epoch, base)| epoch >= lowest && base <= version)
                .map(|(_, base)| base);
            floor = checkpoint.max(based);
            // A base normally seals its epoch. If the chain still needs a record there,
            // the reader can fetch it on demand without speculatively downloading that whole epoch.
            planned.extend(
                wave.iter()
                    .zip((lowest..=next).rev())
                    .filter(|(_, epoch)| {
                        self.base.is_none_or(|(base_epoch, _)| {
                            *epoch > base_epoch || *epoch == starting_epoch
                        })
                    })
                    .map(|((prefix, epoch), _)| {
                        (prefix.clone(), epoch.indexes.clone(), epoch.listed.clone())
                    }),
            );
            for (prefix, epoch) in wave.drain(..) {
                self.epochs.entry(prefix).or_insert(epoch);
            }
            next = lowest - 1;
        }
        let indexed: Vec<_> = planned
            .iter()
            .map(|(prefix, indexes, _)| (prefix.clone(), indexes.clone()))
            .collect();
        let segments: Vec<_> = planned
            .into_iter()
            .map(|(prefix, _, listed)| (prefix, listed))
            .collect();
        let first = floor.unwrap_or(0);
        let (archived, logged) = tokio::join!(
            self.storage.prefetch_indexed(&indexed, first, version),
            self.storage.prefetch_segments(&segments, first, version)
        );
        self.reads += archived.batches + logged.batches;
        for (object, bytes) in archived.records.into_iter().chain(logged.records) {
            let (prefix, _) = object.rsplit_once('/').context("invalid snapshot name")?;
            if let Some(epoch) = self.epochs.get_mut(prefix) {
                epoch.indexed.insert(object, bytes);
            }
        }
        Ok(())
    }
}

/// Owner epochs listed together while planning a restore.
const EPOCHS_PER_WAVE: u64 = 64;
/// Bounds the listing for a history that has gone without checkpoints for a long time.
const MAX_PLANNED_EPOCHS: usize = 1024;

async fn list_epoch(storage: &LogStorage, prefix: &str) -> Result<EpochHistory> {
    let prefix_key = format!("{prefix}/");
    let (keys, listed) = tokio::join!(
        storage.snapshots.list(&prefix_key),
        storage.epoch_keys(&prefix_key)
    );
    let keys = keys?;
    let index = format!("{}index~", object_name(&prefix_key)?);
    let indexes = listed
        .iter()
        .filter(|key| key.starts_with(&index))
        .cloned()
        .collect();
    let (checkpoints, heads) = keys
        .into_iter()
        .filter(|key| key.ends_with(".checkpoint") || key.ends_with(".json"))
        .partition(|key| key.ends_with(".checkpoint"));
    Ok(EpochHistory {
        records: None,
        listed,
        indexes,
        indexed: BTreeMap::new(),
        checkpoints,
        heads,
    })
}

#[async_trait]
impl SnapshotHistory for LogHistory<'_> {
    async fn read(&mut self, object: &str) -> Result<Payload> {
        let (prefix, _) = object.rsplit_once('/').context("invalid snapshot name")?;
        let version =
            super::super::snapshots::version(object).context("invalid snapshot version")?;
        if !self.epochs.contains_key(prefix) {
            self.plan(prefix, version).await?;
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
        let first = epoch.floor(version);
        if let Some(records) = self.storage.live_records(object, first).await? {
            self.reads += 1;
            epoch.indexed.extend(records);
            return epoch
                .indexed
                .remove(object)
                .context("live SQLite dependency missing");
        }
        if epoch.heads.contains(object) {
            self.reads += 1;
            if let Some(bytes) = self.storage.snapshots.get(object).await? {
                return Ok(bytes);
            }
        }
        if let Some(records) = self
            .storage
            .indexed_records(object, first, &epoch.indexes)
            .await
        {
            self.reads += 1;
            epoch.indexed.extend(records);
            return epoch
                .indexed
                .remove(object)
                .context("indexed SQLite dependency missing");
        }
        if epoch.records.is_none() {
            epoch.records = Some(self.storage.records(&format!("{prefix}/"), false).await?);
        }
        if let Some(bytes) = epoch
            .records
            .as_ref()
            .and_then(|records| records.get(object))
        {
            return Ok(bytes.clone());
        }
        self.storage
            .snapshots
            .get(object)
            .await?
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

/// Tracks what was logged since the newest base, to tell at sleep whether a new one is due.
#[derive(Default)]
pub(super) struct Progress {
    based: bool,
    resumed_at: u64,
    previous_bytes: u64,
    completed: u64,
    base_bytes: u64,
    log_bytes: u64,
}

impl Progress {
    /// Resumes from the progress the last owner sealed. Sealing lists the actor's bases, since a
    /// node may have stored a newer one after that owner sealed.
    pub fn resume(progress: CheckpointProgress, version: u64) -> Self {
        Self {
            based: false,
            resumed_at: version,
            log_bytes: 0,
            completed: progress.version,
            base_bytes: progress.base_bytes,
            previous_bytes: if version > progress.version {
                progress.log_bytes
            } else {
                0
            },
        }
    }

    /// Corrects the progress by the actor's newest base: a node may have stored it after the
    /// progress was sealed.
    pub fn found(&mut self, base: Option<BaseName>) {
        if let Some(base) = base.filter(|base| base.version >= self.completed) {
            self.based = true;
            if base.version >= self.resumed_at {
                self.previous_bytes = 0;
            }
            self.completed = base.version;
            self.base_bytes = base.bytes;
        }
    }

    pub fn based(&self) -> bool {
        self.based
    }

    pub fn restored(&mut self, progress: CheckpointProgress, base: Option<BaseName>) {
        *self = Self {
            based: base.is_some(),
            resumed_at: self.resumed_at,
            previous_bytes: progress.log_bytes,
            completed: progress.version,
            base_bytes: progress.base_bytes,
            log_bytes: 0,
        };
    }

    pub fn logged(&mut self, bytes: u64) {
        self.log_bytes += bytes;
    }

    /// The progress to seal at `version`, and whether a base of it is due: when none exists yet,
    /// when the changes since the newest one are as large as it, or when there are too many.
    pub fn seal(&self, version: u64) -> CheckpointProgress {
        let log_bytes = self.previous_bytes.saturating_add(self.log_bytes);
        let due = version > 0
            && (!self.based
                || log_bytes >= self.base_bytes.max(MIN_LOG_BYTES)
                || version.saturating_sub(self.completed) >= max_log_versions(self.base_bytes));
        CheckpointProgress {
            version: self.completed,
            base_bytes: self.base_bytes,
            log_bytes,
            base_due: due,
        }
    }
}
