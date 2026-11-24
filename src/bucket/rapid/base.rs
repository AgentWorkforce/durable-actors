//! Bases: a whole SQLite file at one published version, stored in the archive bucket beside the
//! actor's history as `durable-actors-v3-snapshots-<actor>~bases~<owner epoch>~<version>~<txid>~
//! <bytes>.sqlite`. Inside the actor's snapshot names, so host tokens need no further rule, and
//! the only `.sqlite` objects there. A restore starts from the newest base at or below its head.

use super::*;
use crate::storage::SnapshotRef;

const SNAPSHOTS: &str = "durable-actors-v3-snapshots-";
const BASES: &str = "~bases~";

/// What a base's name records about it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct BaseName {
    pub owner_epoch: u64,
    pub version: u64,
    pub txid: u64,
    pub bytes: u64,
}

/// A base a restore can start from.
#[derive(Clone)]
pub(crate) struct SqliteBase {
    pub key: String,
    pub name: BaseName,
    pub bucket: Arc<dyn Bucket>,
    /// Where the restore downloaded it while it read the changes since, if it did.
    pub downloaded: Option<std::path::PathBuf>,
}

impl SqliteBase {
    pub fn matches(&self, head: &SnapshotRef, txid: u64) -> bool {
        self.name.txid == txid
            && self.name.version == head.state_version
            && crate::storage::snapshot_position(&head.object)
                == Some((self.name.owner_epoch, self.name.version))
    }

    /// Downloads the base into a new file at `path`.
    pub async fn download_to(&self, path: &std::path::Path) -> Result<()> {
        let bytes = self
            .bucket
            .download_to(&self.key, path)
            .await?
            .with_context(|| format!("SQLite base {} disappeared", self.key))?;
        ensure!(
            bytes == self.name.bytes,
            "SQLite base {} is truncated",
            self.key
        );
        Ok(())
    }
}

/// The base object for the database at `snapshot`: SQLite transaction `txid`, `bytes` long.
pub(crate) fn key(snapshot: &SnapshotRef, txid: u64, bytes: u64) -> Result<String> {
    let name = object_name(&snapshot.object)?;
    let (actor, version) = name
        .strip_suffix(".json")
        .and_then(|name| name.split_once('~'))
        .context("snapshot object is not a version")?;
    Ok(format!("{actor}{BASES}{version}~{txid}~{bytes}.sqlite"))
}

/// The prefix every base of the actor whose snapshots live under `snapshots` starts with.
pub(crate) fn prefix(snapshots: &str) -> Result<String> {
    let name = object_name(snapshots)?;
    let actor = name
        .strip_suffix('~')
        .filter(|actor| !actor.contains('~'))
        .context("invalid snapshot prefix")?;
    Ok(format!("{actor}{BASES}"))
}

impl LogStorage {
    /// The newest base of the actor that owns `head` at or below `head`'s version.
    pub(super) async fn newest_base(&self, head: &SnapshotRef) -> Result<Option<SqliteBase>> {
        self.base_at_or_below(head, head.state_version).await
    }

    pub(super) async fn base_at_or_below(
        &self,
        head: &SnapshotRef,
        version: u64,
    ) -> Result<Option<SqliteBase>> {
        newest_listed(&self.archive, &actor_prefix(head)?, version).await
    }
}

/// The newest base of `actor` in `archive` whatever its head: bases exist only for published
/// versions, so it is at or below any head the actor's activation can reach.
pub(crate) async fn newest_base_of(
    archive: &Arc<dyn Bucket>,
    actor: &crate::actor::ActorKey,
) -> Result<Option<SqliteBase>> {
    let prefix = prefix(&crate::storage_paths::snapshots(actor)?)?;
    newest_listed(archive, &prefix, u64::MAX).await
}

async fn newest_listed(
    archive: &Arc<dyn Bucket>,
    prefix: &str,
    version: u64,
) -> Result<Option<SqliteBase>> {
    let keys = archive.list(prefix).await?;
    Ok(keys
        .into_iter()
        .filter_map(|key| Some((parse(&key)?, key)))
        .filter(|(name, _)| name.version <= version)
        .max_by_key(|(name, _)| name.version)
        .map(|(name, key)| SqliteBase {
            key,
            name,
            bucket: archive.clone(),
            downloaded: None,
        }))
}

pub(crate) async fn upload_file(
    bucket: &dyn Bucket,
    snapshot: &SnapshotRef,
    txid: u64,
    path: &std::path::Path,
) -> Result<u64> {
    let file = tokio::fs::File::open(path).await?.into_std().await;
    let bytes = file.metadata()?.len();
    let payload = Payload::from_file(Arc::new(file), usize::try_from(bytes)?);
    // The name holds the version, transaction and size, so an object already there is this base.
    let key = key(snapshot, txid, bytes)?;
    bounded_transfer(bytes, bucket.compare_and_swap(&key, None, payload)).await?;
    Ok(bytes)
}

fn actor_prefix(head: &SnapshotRef) -> Result<String> {
    let (epoch, _) = head
        .object
        .rsplit_once('/')
        .context("invalid snapshot name")?;
    let (actor, _) = epoch.rsplit_once('/').context("invalid snapshot prefix")?;
    prefix(&format!("{actor}/"))
}

pub(crate) fn parse(key: &str) -> Option<BaseName> {
    let (_, name) = key
        .strip_prefix(SNAPSHOTS)?
        .strip_suffix(".sqlite")?
        .split_once(BASES)?;
    let mut parts = name.split('~');
    let name = BaseName {
        owner_epoch: u64::from_str_radix(parts.next()?, 16).ok()?,
        version: parts.next()?.parse().ok()?,
        txid: parts.next()?.parse().ok()?,
        bytes: parts.next()?.parse().ok()?,
    };
    parts.next().is_none().then_some(name)
}

#[cfg(test)]
#[path = "../../../tests/unit/bucket/rapid/base.rs"]
mod tests;
