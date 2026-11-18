use super::SnapshotStore;
use crate::{
    payload::Payload,
    state_log::SqliteSnapshot,
    storage::{SnapshotRef, VerifiedSnapshot},
};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

pub(crate) struct PrefetchedCheckpoint {
    pub source: SnapshotRef,
    pub bytes: Option<Payload>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Checkpoint {
    pub source: SnapshotRef,
    pub sqlite: SqliteSnapshot,
}

#[async_trait]
pub(crate) trait SnapshotHistory: Send {
    async fn read(&mut self, object: &str) -> Result<Payload>;
    async fn checkpoint(&mut self, object: &str) -> Result<Option<Payload>>;
}

pub(crate) struct ResolvedSqlite {
    pub sqlite: SqliteSnapshot,
    pub checkpoint_version: u64,
    pub checkpoint: bool,
    pub parents: usize,
    /// LTX bytes of the full copy the history starts from, and of the deltas after it.
    pub base_bytes: u64,
    pub log_bytes: u64,
}

pub(crate) async fn resolve(
    history: &mut dyn SnapshotHistory,
    snapshot: VerifiedSnapshot,
    prefetched: Option<PrefetchedCheckpoint>,
) -> Result<ResolvedSqlite> {
    let VerifiedSnapshot {
        mut reference,
        snapshot: mut current,
        ..
    } = snapshot;
    let txid = current.sqlite.txid;
    let mut prefetched = prefetched.filter(|checkpoint| checkpoint.source == reference);
    let mut segments = Vec::new();
    let mut checkpoint = false;
    let mut parents = 0;
    loop {
        let checkpoint_bytes = if current.sqlite.parent.is_none() {
            None
        } else if let Some(checkpoint) = prefetched.take() {
            checkpoint.bytes
        } else {
            history.checkpoint(&reference.object).await?
        };
        if let Some(bytes) = checkpoint_bytes {
            let sqlite = Checkpoint::decode(&bytes, &reference, current.sqlite.txid)
                .with_context(|| format!("validate checkpoint for {}", reference.object))?;
            checkpoint = true;
            segments.push(sqlite.files);
            break;
        }
        let first = current.sqlite.files[0].first;
        segments.push(current.sqlite.files);
        let Some(parent) = current.sqlite.parent else {
            break;
        };
        let bytes = history
            .read(&parent.object)
            .await
            .with_context(|| format!("read SQLite dependency {}", parent.object))?;
        VerifiedSnapshot {
            reference,
            snapshot: current,
            ..
        } = VerifiedSnapshot::matching(parent.clone(), bytes)
            .with_context(|| format!("verify SQLite dependency {}", parent.object))?;
        ensure!(
            current.sqlite.txid.checked_add(1) == Some(first),
            "SQLite dependency transaction gap"
        );
        parents += 1;
    }
    let size = |files: &Vec<crate::litestream::storage::LtxFile>| {
        files.iter().map(|file| file.data.len() as u64).sum::<u64>()
    };
    let base_bytes = segments.last().map_or(0, size);
    let log_bytes = segments.iter().map(size).sum::<u64>() - base_bytes;
    Ok(ResolvedSqlite {
        checkpoint_version: reference.state_version,
        checkpoint,
        parents,
        base_bytes,
        log_bytes,
        sqlite: SqliteSnapshot {
            txid,
            parent: None,
            files: segments.into_iter().rev().flatten().collect(),
        },
    })
}

impl Checkpoint {
    pub fn encode(&self) -> Result<Payload> {
        let mut header = self.clone();
        let files = std::mem::take(&mut header.sqlite.files);
        crate::state_log::binary::encode(&header, &files)
    }

    /// Reads binary checkpoints and the JSON ones written before them.
    pub fn decode(bytes: &Payload, source: &SnapshotRef, txid: u64) -> Result<SqliteSnapshot> {
        let checkpoint: Self = if crate::state_log::binary::is_binary(bytes)? {
            let (mut checkpoint, files): (Self, _) = crate::state_log::binary::decode(bytes)?;
            ensure!(checkpoint.sqlite.files.is_empty(), "duplicate LTX files");
            checkpoint.sqlite.files = files;
            checkpoint
        } else {
            serde_json::from_reader(bytes.reader())?
        };
        ensure!(&checkpoint.source == source, "checkpoint source mismatch");
        checkpoint.sqlite.validate(source.state_version)?;
        ensure!(
            checkpoint.sqlite.parent.is_none() && checkpoint.sqlite.txid == txid,
            "checkpoint transaction mismatch"
        );
        Ok(checkpoint.sqlite)
    }
}

pub(super) struct StoredHistory<'a, T: ?Sized>(pub &'a T);

#[async_trait]
impl<T: SnapshotStore + ?Sized> SnapshotHistory for StoredHistory<'_, T> {
    async fn read(&mut self, object: &str) -> Result<Payload> {
        self.0
            .get(object)
            .await?
            .context("SQLite dependency missing")
    }
    async fn checkpoint(&mut self, object: &str) -> Result<Option<Payload>> {
        self.0.get(&format!("{object}.checkpoint")).await
    }
}
