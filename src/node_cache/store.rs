use std::{path::PathBuf, sync::Arc, time::SystemTime};

use anyhow::{Context, Result};
use moka::{future::Cache, notification::RemovalCause, policy::EvictionPolicy};

const STAGING: &str = "staging";
pub(super) const PARKED: &str = "actors";

pub(super) struct DiskStore {
    root: PathBuf,
    entries: Cache<String, u64>,
}

impl DiskStore {
    pub async fn open(root: PathBuf, capacity_bytes: u64, max_entries: u64) -> Result<Self> {
        let staging = root.join(STAGING);
        if tokio::fs::try_exists(&staging).await? {
            tokio::fs::remove_dir_all(&staging).await?;
        }
        tokio::fs::create_dir_all(&staging).await?;
        let store = Self {
            entries: entries(root.clone(), capacity_bytes, max_entries),
            root,
        };
        for (key, size) in store.existing().await? {
            store.entries.insert(key, size).await;
        }
        Ok(store)
    }

    pub async fn get(&self, key: &str) -> Option<(tokio::fs::File, u64)> {
        let size = self.entries.get(key).await?;
        match tokio::fs::File::open(self.root.join(key)).await {
            Ok(file) => Some((file, size)),
            Err(_) => {
                self.entries.invalidate(key).await;
                None
            }
        }
    }

    pub fn stage(&self) -> Result<tempfile::NamedTempFile> {
        Ok(tempfile::NamedTempFile::new_in(self.root.join(STAGING))?)
    }

    pub async fn insert(
        &self,
        key: &str,
        staged: tempfile::NamedTempFile,
        size: u64,
    ) -> Result<()> {
        let path = self.root.join(key);
        tokio::fs::create_dir_all(path.parent().context("cache entry has no parent")?).await?;
        staged.persist(&path)?;
        self.entries.insert(key.to_owned(), size).await;
        Ok(())
    }

    /// Counts a directory `SqliteSlots` manages against the capacity; eviction deletes it whole.
    pub async fn track(&self, key: &str, size: u64) {
        self.entries.insert(key.to_owned(), size).await;
    }

    pub async fn forget(&self, key: &str) {
        self.entries.invalidate(key).await;
    }

    /// Files a previous process left, oldest first so the newest survive any eviction on reopen.
    async fn existing(&self) -> Result<Vec<(String, u64)>> {
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || {
            let mut found = Vec::new();
            let others = |entry: &walkdir::DirEntry| {
                entry.depth() != 1
                    || ![STAGING, PARKED].contains(&entry.file_name().to_str().unwrap_or_default())
            };
            for entry in walkdir::WalkDir::new(&root)
                .min_depth(1)
                .into_iter()
                .filter_entry(others)
            {
                let entry = entry?;
                let key = entry.path().strip_prefix(&root)?;
                if entry.file_type().is_file() {
                    let key = key.to_str().context("cache key is not UTF-8")?.to_owned();
                    let metadata = entry.metadata()?;
                    let modified = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
                    found.push((modified, key, metadata.len()));
                }
            }
            found.sort_unstable();
            anyhow::Ok(
                found
                    .into_iter()
                    .map(|(_, key, size)| (key, size))
                    .collect(),
            )
        })
        .await?
    }
}

fn entries(root: PathBuf, capacity_bytes: u64, max_entries: u64) -> Cache<String, u64> {
    // Every entry costs index memory, so small ones are weighed as a share of the capacity.
    let min_weight = capacity_bytes.div_ceil(max_entries.max(1));
    Cache::builder()
        .max_capacity(capacity_bytes)
        .weigher(move |_, size: &u64| u32::try_from((*size).max(min_weight)).unwrap_or(u32::MAX))
        .eviction_policy(EvictionPolicy::lru())
        .eviction_listener(move |key: Arc<String>, _, cause| {
            // A replacement has already renamed its own file over this one.
            if cause != RemovalCause::Replaced {
                let path = root.join(key.as_str());
                let _ = if key.starts_with(PARKED) {
                    std::fs::remove_dir_all(path)
                } else {
                    std::fs::remove_file(path)
                };
            }
        })
        .build()
}

#[cfg(test)]
#[path = "../../tests/unit/node_cache/store.rs"]
mod tests;
