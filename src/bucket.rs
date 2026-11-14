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
pub use snapshots::{ArchiveBatchConfig, RapidBucket, RapidPlacement, SnapshotStoreConfig};
pub(crate) use snapshots::{BucketSnapshots, SnapshotStore};
mod runtime;

use anyhow::Result;
use async_trait::async_trait;

pub use file::FileBucket;
pub use gcs::GcsBucket;
pub(crate) use gcs::WarmStorage;
pub(crate) use runtime::ActorStateReader;
pub use runtime::{LoadedActor, OwnershipHint, RuntimeStorage, RuntimeStorageReader};
pub use s3::{S3Bucket, S3Endpoint, is_directory_bucket};
pub use s3_access::S3ActorCredentials;

#[derive(Clone, Debug)]
pub struct BucketObject {
    pub generation: i64,
    pub bytes: bytes::Bytes,
}

#[async_trait]
pub trait Bucket: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<BucketObject>>;
    async fn range(&self, key: &str, start: u64, length: u64) -> Result<Option<BucketObject>>;
    async fn compare_and_swap(
        &self,
        key: &str,
        generation: Option<i64>,
        bytes: bytes::Bytes,
    ) -> Result<bool>;
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;
}

pub(crate) async fn replace(
    bucket: &dyn Bucket,
    key: &str,
    generation: Option<i64>,
    bytes: bytes::Bytes,
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
