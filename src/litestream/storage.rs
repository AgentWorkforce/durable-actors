use super::{DatabaseRestore, Replicator};
use crate::payload::Payload;
use anyhow::{Context, Result, ensure};
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
    /// Raw LTX bytes; JSON state, written before binary snapshots, carries them as base64.
    #[serde(with = "base64_data")]
    pub data: Payload,
}

mod base64_data {
    use crate::payload::Payload;
    use base64::{Engine, engine::general_purpose::STANDARD};
    use serde::{Deserializer, Serializer, de, ser::Error};
    use std::fmt;

    pub fn serialize<S: Serializer>(data: &Payload, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&STANDARD.encode(data.to_bytes().map_err(S::Error::custom)?))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Payload, D::Error> {
        deserializer.deserialize_str(Base64)
    }

    struct Base64;

    impl de::Visitor<'_> for Base64 {
        type Value = Payload;
        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("base64 text")
        }
        fn visit_str<E: de::Error>(self, text: &str) -> Result<Payload, E> {
            crate::payload::copy(base64::read::DecoderReader::new(text.as_bytes(), &STANDARD))
                .map_err(E::custom)
        }
    }
}

impl LtxFile {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(matches!(self.level, 0 | 9), "invalid LTX level");
        ensure!(
            self.first > 0 && self.first <= self.last,
            "invalid LTX transaction range"
        );
        ensure!(self.level != 9 || self.first == 1, "invalid LTX snapshot");
        // Only the header magic is checked: snapshots are validated on every encode and decode,
        // and a full LTX decode here made each of those another pass over the database.
        ensure!(self.data.starts_with(b"LTX1")?, "malformed LTX file");
        Ok(())
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
        Self::open(replication, tempfile::tempdir()?, full_snapshots, None).await
    }

    /// With `resume`, capture continues the restored history, so the next commit is a delta at
    /// `txid + 1`; otherwise it starts a new history whose first commit holds every page.
    pub(crate) async fn restore(
        replication: Arc<dyn Replicator>,
        files: &[LtxFile],
        txid: u64,
        full_snapshots: Option<Duration>,
        resume: bool,
    ) -> Result<Self> {
        let directory = tempfile::tempdir()?;
        restore_database(replication.as_ref(), directory.path(), files, txid).await?;
        Self::open(
            replication,
            directory,
            full_snapshots,
            resume.then_some(txid),
        )
        .await
    }

    pub(crate) async fn adopt(
        replication: Arc<dyn Replicator>,
        database: tempfile::NamedTempFile,
        txid: u64,
        full_snapshots: Option<Duration>,
        resume: bool,
    ) -> Result<Self> {
        let directory = tempfile::tempdir()?;
        database.persist(directory.path().join("actor.sqlite"))?;
        Self::open(
            replication,
            directory,
            full_snapshots,
            resume.then_some(txid),
        )
        .await
    }

    /// A page-for-page copy of the committed database, or `None` when it is larger than
    /// `max_bytes`. A resumed capture appends page deltas to the history the copy was saved at,
    /// so unlike `VACUUM INTO` the copy keeps every page where that history put it.
    pub(crate) async fn export(&self, max_bytes: u64) -> Result<Option<tempfile::NamedTempFile>> {
        let source = self.path();
        tokio::task::spawn_blocking(move || {
            let source_wal = wal_path(&source);
            if file_size(&source)? + file_size(&source_wal)? > max_bytes {
                return Ok(None);
            }
            let output = tempfile::NamedTempFile::new()?;
            std::fs::copy(&source, output.path())?;
            if file_size(&source_wal)? > 0 {
                std::fs::copy(&source_wal, wal_path(output.path()))?;
            }
            fold_wal(output.path())?;
            Ok(Some(output))
        })
        .await?
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

    // A new history captures every page at the first write rather than here, so an activation
    // that only reads never pays for it.
    async fn open(
        replication: Arc<dyn Replicator>,
        directory: tempfile::TempDir,
        full_snapshots: Option<Duration>,
        resume_at: Option<u64>,
    ) -> Result<Self> {
        let mut capture = Self {
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
        let position = capture
            .replication
            .register(
                &capture.path(),
                &capture.replica(),
                full_snapshots,
                resume_at,
            )
            .await?;
        capture.txid = position;
        capture.published = position;
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

fn fold_wal(path: &Path) -> Result<()> {
    let db = rusqlite::Connection::open(path)?;
    let busy: i64 = db.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| row.get(0))?;
    ensure!(busy == 0, "exported database checkpoint was blocked");
    db.close().map_err(|(_, error)| error)?;
    ensure!(
        file_size(&wal_path(path))? == 0,
        "exported database kept its WAL"
    );
    Ok(())
}

fn wal_path(path: &Path) -> PathBuf {
    let mut wal = path.as_os_str().to_owned();
    wal.push("-wal");
    wal.into()
}

fn file_size(path: &Path) -> Result<u64> {
    match std::fs::metadata(path) {
        Ok(metadata) => Ok(metadata.len()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error.into()),
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
        Ok(LtxFile {
            level,
            first,
            last,
            data: crate::payload::copy(std::fs::File::open(path)?)?,
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
