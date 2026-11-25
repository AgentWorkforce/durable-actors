pub(crate) mod access;
pub mod checkpoint;
mod file;
mod gcs;
mod rapid;
mod s3;
mod s3_access;
pub(crate) use rapid::{RAPID_OPERATION_TIMEOUT, RapidSnapshots};
pub(crate) mod recovery;
mod snapshots;
#[cfg(test)]
pub(crate) use rapid::base::key as base_key;
pub(crate) use rapid::base::{
    BaseName, SqliteBase, newest_base_of, upload_file as upload_sqlite_base,
};
pub use rapid::sweep::{SweepConfig, serve as serve_history_sweeper};
pub(crate) use recovery::RestoredSqlite;
pub use snapshots::{ArchiveBatchConfig, RapidBucket, RapidPlacement, SnapshotStoreConfig};
pub(crate) use snapshots::{Bases, BucketSnapshots, SnapshotStore};
mod runtime;

use anyhow::Result;
use async_trait::async_trait;

pub use crate::payload::Payload;
pub use file::FileBucket;
pub use gcs::GcsBucket;
pub(crate) use gcs::{WarmStorage, bearer as bearer_headers, forwarded_token};
pub(crate) use runtime::{ActorStateReader, CachedBase};
pub use runtime::{LoadedActor, OwnershipHint, RuntimeStorage, RuntimeStorageReader};
pub use s3::{S3Bucket, S3Endpoint, is_directory_bucket};
pub use s3_access::S3ActorCredentials;

#[derive(Clone, Debug)]
pub struct BucketObject {
    pub generation: i64,
    pub bytes: Payload,
}

#[async_trait]
pub trait Bucket: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<BucketObject>>;
    async fn range(&self, key: &str, start: u64, length: u64) -> Result<Option<BucketObject>>;
    async fn compare_and_swap(
        &self,
        key: &str,
        generation: Option<i64>,
        bytes: Payload,
    ) -> Result<bool>;
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;
    /// Writes the object to a new file at `path` and returns its size, or `None` when missing.
    async fn download_to(&self, key: &str, path: &std::path::Path) -> Result<Option<u64>> {
        let Some(object) = self.get(key).await? else {
            return Ok(None);
        };
        let path = path.to_owned();
        tokio::task::spawn_blocking(move || {
            let mut file = std::fs::File::create_new(path)?;
            Ok(Some(std::io::copy(&mut object.bytes.reader(), &mut file)?))
        })
        .await?
    }
}

pub(crate) async fn replace(
    bucket: &dyn Bucket,
    key: &str,
    generation: Option<i64>,
    bytes: Payload,
) -> Result<bool> {
    match bucket
        .compare_and_swap(key, generation, bytes.clone())
        .await
    {
        Ok(true) => Ok(true),
        result => {
            // A retry can report a failed precondition after the first request succeeded.
            if bucket
                .get(key)
                .await?
                .is_some_and(|object| object.bytes == bytes)
            {
                return Ok(true);
            }
            result
        }
    }
}

#[cfg(test)]
#[path = "../tests/support/bucket.rs"]
pub(crate) mod testing;

#[cfg(test)]
#[path = "../tests/conformance/bucket.rs"]
mod conformance;
