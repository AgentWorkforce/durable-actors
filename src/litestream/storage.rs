use super::{DatabaseRestore, Replicator};
use anyhow::{Context, Result, ensure};
use base64::engine::general_purpose::STANDARD;
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LtxFile {
    pub level: u8,
    pub first: u64,
    pub last: u64,
    pub data: crate::payload::Text,
}

impl LtxFile {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(matches!(self.level, 0 | 9), "invalid LTX level");
        ensure!(
            self.first > 0 && self.first <= self.last,
            "invalid LTX transaction range"
        );
        ensure!(self.level != 9 || self.first == 1, "invalid LTX snapshot");
        // Only the shape is checked: snapshots are validated on every encode and decode, and a
        // full decode here made each of those another pass over the database.
        let encoded = self.data.as_ref();
        ensure!(
            !encoded.is_empty() && encoded.len() % 4 == 0,
            "malformed LTX file"
        );
        Ok(())
    }

    pub(crate) fn decode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut bytes = Vec::new();
        std::io::Read::read_to_end(&mut self.reader(), &mut bytes)?;
        ensure!(!bytes.is_empty(), "empty LTX file");
        Ok(bytes)
    }

    pub(crate) fn reader(&self) -> impl std::io::Read {
        base64::read::DecoderReader::new(self.data.as_ref(), &STANDARD)
    }

    #[cfg(test)]
    fn path(&self, replica: &Path) -> PathBuf {
        replica
            .join("ltx")
            .join(self.level.to_string())
            .join(format!("{:016x}-{:016x}.ltx", self.first, self.last))
    }
}

pub(crate) struct SqliteCapture {
    replication: Arc<dyn Replicator>,
    directory: Option<tempfile::TempDir>,
    txid: u64,
    published: u64,
}

impl SqliteCapture {
    /// `full_snapshots` is how often Litestream adds a full copy to the captured history, for
    /// stores without checkpoints of their own.
    pub(crate) async fn new(
        replication: Arc<dyn Replicator>,
        full_snapshots: Option<Duration>,
    ) -> Result<Self> {
        Self::open(replication, tempfile::tempdir()?, full_snapshots).await
    }

    pub(crate) async fn restore(
        replication: Arc<dyn Replicator>,
        files: &[LtxFile],
        txid: u64,
        full_snapshots: Option<Duration>,
    ) -> Result<Self> {
        let directory = tempfile::tempdir()?;
        restore_database(replication.as_ref(), directory.path(), files, txid).await?;
        Self::open(replication, directory, full_snapshots).await
    }

    pub(crate) fn path(&self) -> PathBuf {
        self.directory
            .as_ref()
            .expect("live SQLite directory")
            .path()
            .join("actor.sqlite")
    }
    fn replica(&self) -> PathBuf {
        self.path().with_file_name("replica")
    }
    pub(crate) fn txid(&self) -> u64 {
        self.txid
    }

    pub(crate) async fn capture(&mut self, txid: u64) -> Result<Vec<LtxFile>> {
        ensure!(txid >= self.txid, "SQLite transaction went backwards");
        let files = collect_ltx_files(&self.path(), self.published, txid).await?;
        // The captured files travel in this commit's snapshot, so the local copies are spent.
        if let Err(error) = self.prune(txid).await {
            tracing::warn!(error = %format!("{error:#}"), "local LTX cleanup deferred");
        }
        self.txid = txid;
        self.published = txid;
        Ok(files)
    }

    async fn prune(&self, through: u64) -> Result<()> {
        let replica = self.replica();
        tokio::task::spawn_blocking(move || {
            use terse_litestream::ReplicaStore;
            let store = terse_litestream::FileStore::scratch(replica);
            for level in [0, 9] {
                for segment in store.list(level)? {
                    if segment.max_txid <= through {
                        store.remove(&segment)?;
                    }
                }
            }
            anyhow::Ok(())
        })
        .await?
    }

    // Litestream captures every page at the first write rather than here, so an activation that
    // only reads never pays for it.
    async fn open(
        replication: Arc<dyn Replicator>,
        directory: tempfile::TempDir,
        full_snapshots: Option<Duration>,
    ) -> Result<Self> {
        let capture = Self {
            replication,
            directory: Some(directory),
            txid: 0,
            published: 0,
        };
        let path = capture.path();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let db = rusqlite::Connection::open(path)?;
            db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;
                CREATE TABLE IF NOT EXISTS __terse_fields (name TEXT PRIMARY KEY, value TEXT NOT NULL CHECK(json_valid(value)))")?;
            Ok(())
        }).await??;
        capture
            .replication
            .register(&capture.path(), &capture.replica(), full_snapshots)
            .await?;
        Ok(capture)
    }
}

impl Drop for SqliteCapture {
    fn drop(&mut self) {
        let replication = self.replication.clone();
        let path = self.path();
        let directory = self.directory.take();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = replication.unregister(&path).await;
                drop(directory);
            });
        }
    }
}

pub(crate) async fn collect_ltx_files(
    path: &Path,
    after: u64,
    through: u64,
) -> Result<Vec<LtxFile>> {
    let checkpoint = read_ltx_files(path, after, 9, through)
        .await?
        .into_iter()
        .filter(|file| file.last > after)
        .max_by_key(|file| file.last);
    let mut next = checkpoint.as_ref().map_or(after + 1, |file| file.last + 1);
    let mut files = checkpoint.into_iter().collect::<Vec<_>>();
    for file in read_ltx_files(path, after, 0, through).await? {
        if file.last < next {
            continue;
        }
        ensure!(
            file.first == next,
            "Litestream capture has a transaction gap"
        );
        next = file
            .last
            .checked_add(1)
            .context("Litestream transaction overflow")?;
        files.push(file);
    }
    ensure!(
        next == through
            .checked_add(1)
            .context("Litestream transaction overflow")?,
        "Litestream has not replicated the actor commit"
    );
    Ok(files)
}

async fn read_ltx_files(path: &Path, after: u64, level: u8, through: u64) -> Result<Vec<LtxFile>> {
    let directory = path
        .with_file_name("replica")
        .join("ltx")
        .join(level.to_string());
    let mut entries = match tokio::fs::read_dir(directory).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
        Err(error) => return Err(error.into()),
    };
    let mut files = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let Some((first, last)) = range(&name.to_string_lossy()) else {
            continue;
        };
        if last > through || last <= after {
            continue;
        }
        files.push((first, last, entry.path()));
    }
    files.sort_by_key(|(first, last, _)| (*first, *last));
    if level == 9 && files.len() > 1 {
        files.drain(..files.len() - 1);
    }
    let mut captured = Vec::with_capacity(files.len());
    for (first, last, path) in files {
        captured.push(read_ltx(level, first, last, path).await?);
    }
    Ok(captured)
}

async fn read_ltx(level: u8, first: u64, last: u64, path: PathBuf) -> Result<LtxFile> {
    tokio::task::spawn_blocking(move || {
        let mut input = std::fs::File::open(path)?;
        let mut spool = crate::payload::Spool::new();
        let mut encoder = base64::write::EncoderWriter::new(&mut spool, &STANDARD);
        std::io::copy(&mut input, &mut encoder)?;
        encoder.finish()?;
        drop(encoder);
        Ok(LtxFile {
            level,
            first,
            last,
            data: crate::payload::Text::from_bytes(spool.finish()?)?,
        })
    })
    .await?
}

fn range(name: &str) -> Option<(u64, u64)> {
    let (first, last) = name.strip_suffix(".ltx")?.split_once('-')?;
    if first.len() != 16 || last.len() != 16 {
        return None;
    }
    let first = u64::from_str_radix(first, 16).ok()?;
    let last = u64::from_str_radix(last, 16).ok()?;
    (first > 0 && first <= last).then_some((first, last))
}

pub(crate) async fn restored_fields(
    restore: &dyn DatabaseRestore,
    files: &[LtxFile],
    txid: u64,
) -> Result<serde_json::Value> {
    let directory = tempfile::tempdir()?;
    restore_database(restore, directory.path(), files, txid).await?;
    tokio::task::spawn_blocking(move || {
        let db = rusqlite::Connection::open(directory.path().join("actor.sqlite"))?;
        read_fields(&db)
    })
    .await?
}

async fn restore_database(
    restore: &dyn DatabaseRestore,
    directory: &Path,
    files: &[LtxFile],
    txid: u64,
) -> Result<()> {
    restore
        .restore(files, &directory.join("actor.sqlite"), txid)
        .await
}

pub(crate) fn read_fields(db: &rusqlite::Connection) -> Result<serde_json::Value> {
    let mut statement = db.prepare("SELECT name, value FROM __terse_fields ORDER BY name")?;
    let rows = statement.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut fields = serde_json::Map::new();
    for row in rows {
        let (name, value) = row?;
        fields.insert(name, serde_json::from_str(&value)?);
    }
    Ok(serde_json::Value::Object(fields))
}

#[cfg(test)]
#[path = "../../tests/unit/sqlite_storage.rs"]
mod tests;
