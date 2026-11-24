use super::{Bucket, SnapshotStore, recovery::CheckpointProgress, replace};
use crate::{
    payload::Payload,
    storage::{SnapshotRef, StateStream},
};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

mod archive;
pub(crate) mod base;
mod checkpoint;
mod frame;
mod gcs;
mod index;
mod prepared;
mod reader;
mod segment;
mod standard;
pub(crate) mod sweep;
mod writer;

use frame::{Frame, Record};
use prepared::Prepared;
use segment::Segment;
use tokio_util::task::AbortOnDropHandle;
use writer::Session;

const ROTATION_INTERVAL: Duration = Duration::from_secs(60);
/// Archived batches or log segments a restore downloads at once; their bodies spool to
/// temporary files.
const PREFETCH_BATCHES: usize = 64;
pub(crate) const RAPID_OPERATION_TIMEOUT: Duration = Duration::from_secs(10);
/// The slowest transfer rate a bounded operation is allowed before it counts as stuck.
const MIN_TRANSFER_BYTES_PER_SECOND: f64 = 32.0 * 1024.0 * 1024.0;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Replica {
    bucket: String,
    object: String,
    generation: i64,
}

#[async_trait]
trait LogWriter: Send {
    async fn append_and_flush(&mut self, header: Bytes, state: Payload) -> Result<u64>;
}

#[async_trait]
trait LogZone: Send + Sync {
    fn bucket(&self) -> &str;
    async fn open(&self, object: &str) -> Result<(Replica, Box<dyn LogWriter>)>;
    async fn read(&self, replica: &Replica, fence: bool) -> Result<Payload>;
    async fn read_range(&self, replica: &Replica, start: u64, length: u64) -> Result<Payload>;
    async fn delete(&self, replica: &Replica) -> Result<()>;
}

pub(crate) struct RapidSnapshots {
    storage: Arc<LogStorage>,
    session: Arc<Mutex<Option<Session>>>,
}

struct LogStorage {
    archive: Arc<dyn Bucket>,
    snapshots: Arc<dyn SnapshotStore>,
    zones: Vec<Arc<dyn LogZone>>,
    placement: super::RapidPlacement,
    prepared: std::sync::Mutex<Option<Prepared>>,
    batch: super::ArchiveBatchConfig,
    live: std::sync::Mutex<BTreeMap<u64, index::LiveRecord>>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    format: u32,
    id: String,
    stream: StateStream,
    first_version: u64,
    replicas: Vec<Replica>,
}

impl RapidSnapshots {
    fn prepare(&self, actor: &crate::actor::ActorKey) -> Result<()> {
        let prefix = object_name(&crate::storage_paths::snapshots(actor)?)?.replacen(
            "snapshots-",
            "logs-",
            1,
        );
        *self.storage.prepared.lock().unwrap() = Some(Prepared::new(
            prefix,
            self.storage.zones.clone(),
            self.storage.placement,
        ));
        Ok(())
    }

    fn new(
        archive: Arc<dyn Bucket>,
        snapshots: Arc<dyn SnapshotStore>,
        zones: Vec<Arc<dyn LogZone>>,
        placement: super::RapidPlacement,
        batch: super::ArchiveBatchConfig,
        stop: CancellationToken,
    ) -> Result<Self> {
        ensure!(
            (1..=2).contains(&zones.len())
                && zones
                    .windows(2)
                    .all(|pair| pair[0].bucket() != pair[1].bucket()),
            "one or two independent log replicas required"
        );
        batch.validate()?;
        let storage = Arc::new(LogStorage {
            archive,
            snapshots,
            zones,
            placement,
            prepared: std::sync::Mutex::new(None),
            batch,
            live: std::sync::Mutex::new(BTreeMap::new()),
        });
        let session = Arc::new(Mutex::new(None));
        writer::start_rotation(storage.clone(), Arc::downgrade(&session), stop);
        Ok(Self { storage, session })
    }
}

#[async_trait]
impl SnapshotStore for RapidSnapshots {
    async fn get(&self, object: &str) -> Result<Option<Payload>> {
        if let Some(bytes) = self
            .session
            .lock()
            .await
            .as_ref()
            .and_then(|session| session.cached(object))
        {
            return Ok(Some(bytes));
        }
        let version = super::snapshots::version(object);
        if let Some(version) = version
            && let Some(mut records) = self.storage.live_records(object, version).await?
        {
            return Ok(records.remove(object));
        }
        if let Some(bytes) = self.storage.snapshots.get(object).await? {
            return Ok(Some(bytes));
        }
        let (prefix, _) = object.rsplit_once('/').context("invalid snapshot name")?;
        let keys = self.storage.index_keys(&format!("{prefix}/")).await?;
        if let Some(mut records) = self
            .storage
            .indexed_records(object, version.context("invalid snapshot version")?, &keys)
            .await
        {
            return Ok(records.remove(object));
        }
        let records = self.storage.records(&format!("{prefix}/"), false).await?;
        Ok(records.get(object).cloned())
    }
    async fn restore_sqlite(
        &self,
        snapshot: crate::storage::VerifiedSnapshot,
        bases: super::Bases,
        download: Option<&std::path::Path>,
    ) -> Result<super::recovery::RestoredSqlite> {
        let restored = self
            .storage
            .restore(snapshot.clone(), bases, download)
            .await?;
        if let Some(session) = self.session.lock().await.as_mut() {
            let base = restored.base.as_ref().map(|base| base.name);
            session.restored(&snapshot, restored.progress, base).await?;
        }
        Ok(super::recovery::RestoredSqlite {
            sqlite: restored.sqlite,
            progress: restored.progress,
            base: restored.base,
        })
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys: std::collections::BTreeSet<_> = self
            .storage
            .snapshots
            .list(prefix)
            .await?
            .into_iter()
            .filter(|key| super::snapshots::version(key).is_some())
            .collect();
        keys.extend(self.storage.records(prefix, false).await?.into_keys());
        Ok(keys.into_iter().collect())
    }
    async fn latest(&self, prefix: &str) -> Result<Option<(String, Payload)>> {
        self.storage.latest(prefix, false).await
    }
    async fn restored_elsewhere(
        &self,
        snapshot: &crate::storage::VerifiedSnapshot,
        progress: CheckpointProgress,
        base: Option<base::BaseName>,
    ) -> Result<()> {
        if let Some(session) = self.session.lock().await.as_mut() {
            session.restored(snapshot, progress, base).await?;
        }
        Ok(())
    }
    async fn put(&self, object: &str, bytes: Payload) -> Result<()> {
        let mut session = self.session.lock().await;
        session
            .as_mut()
            .context("log stream is not activated")?
            .put(self.storage.clone(), object, bytes)
            .await
    }
    async fn start(
        &self,
        stream: &StateStream,
        resume: Option<(&SnapshotRef, CheckpointProgress)>,
    ) -> Result<()> {
        let mut session = self.session.lock().await;
        ensure!(session.is_none(), "log stream is already activated");
        *session = Some(Session::open(self.storage.clone(), stream.clone(), resume)?);
        Ok(())
    }
    async fn recover(&self, prefix: &str) -> Result<Option<(String, Payload)>> {
        for key in self.storage.archive.list(&object_name(prefix)?).await? {
            if !key.ends_with(".manifest") {
                continue;
            }
            let stored = self
                .storage
                .archive
                .get(&key)
                .await?
                .context("manifest disappeared")?;
            let manifest: Manifest = serde_json::from_reader(stored.bytes.reader())?;
            manifest.validate(&key, prefix, &self.storage.zones)?;
            self.storage.recover_segment(&manifest).await?;
        }
        let latest = self.storage.latest(prefix, false).await?;
        if let Some((object, bytes)) = &latest {
            self.storage.snapshots.put(object, bytes.clone()).await?;
        }
        Ok(latest)
    }
    async fn finish(
        &self,
        stream: &StateStream,
        deadline: tokio::time::Instant,
    ) -> Result<Option<CheckpointProgress>> {
        let mut session = self.session.lock().await;
        let active = session.as_mut().context("log stream is not activated")?;
        ensure!(active.stream == *stream, "cannot finish another log stream");
        let progress = active.finish(self.storage.clone(), deadline).await?;
        *session = None;
        Ok(Some(progress))
    }
}

impl Manifest {
    fn key(&self) -> Result<String> {
        Ok(format!(
            "{}log~{}.manifest",
            super::rapid::object_name(&self.stream.prefix)?,
            self.id
        ))
    }
    fn marker(&self, kind: &str) -> Result<String> {
        Ok(self.key()?.replace(".manifest", &format!(".{kind}")))
    }
    fn object(&self) -> Result<String> {
        let actor =
            crate::storage_paths::actor_from_snapshot(&self.stream.object(self.first_version))?;
        Ok(format!(
            "{}{}.segment",
            object_name(&crate::storage_paths::snapshots(&actor)?)?.replacen(
                "snapshots-",
                "logs-",
                1
            ),
            self.id
        ))
    }
    fn validate(&self, key: &str, prefix: &str, zones: &[Arc<dyn LogZone>]) -> Result<()> {
        ensure!(
            self.format == 1 && self.first_version > self.stream.base_version,
            "invalid log manifest"
        );
        uuid::Uuid::parse_str(&self.id)?;
        ensure!(
            self.key()? == key && self.stream.prefix.starts_with(prefix),
            "manifest escaped its stream"
        );
        let object = self.stream.object(self.first_version);
        let actor = crate::storage_paths::actor_from_snapshot(&object)?;
        crate::storage::validate_snapshot_object_name(&actor, self.first_version, &object)?;
        ensure!(
            object.rsplit('/').nth(1) == Some(format!("{:032x}", self.stream.owner_epoch).as_str()),
            "manifest epoch mismatch"
        );
        ensure!(!self.replicas.is_empty(), "missing log replicas");
        let mut buckets = std::collections::HashSet::new();
        for replica in &self.replicas {
            ensure!(
                buckets.insert(&replica.bucket)
                    && zones.iter().any(|zone| zone.bucket() == replica.bucket)
                    && replica.object == self.object()?
                    && replica.generation > 0,
                "invalid log replica descriptor"
            );
        }
        Ok(())
    }
}

impl LogStorage {
    fn zone(&self, replica: &Replica) -> Result<&Arc<dyn LogZone>> {
        self.zones
            .iter()
            .find(|zone| zone.bucket() == replica.bucket)
            .context("log replica names an unconfigured zone")
    }

    fn replica_zones(&self, manifest: &Manifest) -> Result<Vec<Arc<dyn LogZone>>> {
        manifest
            .replicas
            .iter()
            .map(|replica| self.zone(replica).cloned())
            .collect()
    }
}

async fn bounded<T>(operation: impl std::future::Future<Output = Result<T>>) -> Result<T> {
    bounded_transfer(0, operation).await
}

async fn bounded_transfer<T>(
    bytes: u64,
    operation: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    tokio::time::timeout(transfer_timeout(RAPID_OPERATION_TIMEOUT, bytes), operation)
        .await
        .context("log operation timed out")?
}

pub(crate) fn transfer_timeout(base: Duration, bytes: u64) -> Duration {
    base + Duration::from_secs_f64(bytes as f64 / MIN_TRANSFER_BYTES_PER_SECOND)
}

#[cfg(test)]
#[path = "../../tests/unit/bucket/rapid.rs"]
mod tests;

pub(super) fn object_name(logical: &str) -> Result<String> {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    let root = format!("{}snapshots/", crate::storage_paths::ROOT);
    let relative = logical
        .strip_prefix(&root)
        .context("invalid snapshot root")?;
    let parts: Vec<_> = relative.splitn(5, '/').collect();
    ensure!(
        parts.len() == 5 && parts[..4].iter().all(|p| !p.is_empty()),
        "invalid snapshot actor prefix"
    );
    let actor = parts[..4].join("/");
    let hash = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, actor.as_bytes());
    Ok(format!(
        "durable-actors-v3-snapshots-{}~{}",
        URL_SAFE_NO_PAD.encode(hash.as_ref()),
        parts[4].replace('/', "~")
    ))
}
