use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use anyhow::{Result, ensure};
use futures_util::{StreamExt, stream};

use super::Mirror;
use crate::bucket::Bucket;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RestoreReport {
    pub listed: u64,
    pub copied: u64,
    pub missing: u64,
}

/// Copies every object under `prefix` from `source` into `target`, keeping generations. Objects
/// the target already holds at the same or a later generation are left alone, so a restore can
/// be re-run after an interruption.
pub async fn restore(
    source: &dyn Bucket,
    target: &dyn Mirror,
    prefix: &str,
    concurrency: usize,
) -> Result<RestoreReport> {
    ensure!(
        prefix.starts_with(crate::storage_paths::ROOT),
        "restore prefix must be under {}",
        crate::storage_paths::ROOT
    );
    let keys = source.list(prefix).await?;
    let listed = keys.len() as u64;
    let copied = AtomicU64::new(0);
    let missing = AtomicU64::new(0);
    stream::iter(keys)
        .map(|key| {
            let (copied, missing) = (&copied, &missing);
            async move {
                match source.get(&key).await? {
                    Some(object) => {
                        target.mirror(&key, object.generation, object.bytes).await?;
                        copied.fetch_add(1, Ordering::Relaxed);
                    }
                    None => {
                        missing.fetch_add(1, Ordering::Relaxed);
                    }
                }
                anyhow::Ok(())
            }
        })
        .buffer_unordered(concurrency.max(1))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    Ok(RestoreReport {
        listed,
        copied: copied.into_inner(),
        missing: missing.into_inner(),
    })
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VerifyReport {
    pub owners: u64,
    pub restorable: u64,
    /// Up to 20 owner records that cannot be activated from the bucket, with the reason.
    pub unrestorable: Vec<(String, String)>,
    pub unrestorable_total: u64,
}

const EXAMPLES: usize = 20;

/// Checks that every actor with an owner record in `bucket` can be activated from it alone: the
/// snapshot activation would load exists, and its whole SQLite dependency chain restores.
pub async fn verify(bucket: Arc<dyn Bucket>, concurrency: usize) -> Result<VerifyReport> {
    let runtime =
        crate::bucket::RuntimeStorage::new(bucket.clone(), Arc::new(crate::clock::SystemClock))?;
    let owners = bucket
        .list(&format!("{}owners/", crate::storage_paths::ROOT))
        .await?;
    let failures: Vec<(String, String)> = stream::iter(owners.iter())
        .map(|owner| {
            let runtime = &runtime;
            async move {
                runtime
                    .verify_restorable(owner)
                    .await
                    .err()
                    .map(|error| (owner.clone(), format!("{error:#}")))
            }
        })
        .buffer_unordered(concurrency.max(1))
        .filter_map(std::future::ready)
        .collect()
        .await;
    Ok(VerifyReport {
        owners: owners.len() as u64,
        restorable: (owners.len() - failures.len()) as u64,
        unrestorable_total: failures.len() as u64,
        unrestorable: failures.into_iter().take(EXAMPLES).collect(),
    })
}
