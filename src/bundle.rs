#[cfg(test)]
#[path = "../tests/unit/bundle.rs"]
mod tests;
use std::path::{Component, Path};

use anyhow::{Context, Result, ensure};
use aws_lc_rs::digest::{Context as Digest, SHA256};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use futures_util::{Stream, TryStreamExt};
use google_cloud_storage::client::Storage;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

const MAX_CONCURRENT_DOWNLOADS: usize = 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ActorRuntime {
    Typescript,
    Python,
}

impl ActorRuntime {
    pub(crate) fn from_entrypoint(entrypoint: &str) -> Result<Self> {
        if entrypoint.ends_with(".mjs") {
            Ok(Self::Typescript)
        } else if entrypoint.ends_with(".pyz") {
            Ok(Self::Python)
        } else {
            anyhow::bail!("actor entrypoint must be a compiled .mjs or .pyz artifact")
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ActorBundle {
    pub bucket: String,
    pub files: Vec<BundleFile>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BundleFile {
    pub path: String,
    pub object: String,
    pub generation: i64,
    pub sha256: String,
}

impl ActorBundle {
    pub fn encode(&self) -> Result<String> {
        self.validate()?;
        Ok(format!(
            "gcs:{}",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(self)?)
        ))
    }

    pub fn decode(encoded: &str) -> Result<Self> {
        let value: Self = serde_json::from_slice(
            &URL_SAFE_NO_PAD.decode(
                encoded
                    .strip_prefix("gcs:")
                    .context("GCS artifact reference required")?,
            )?,
        )?;
        value.validate()?;
        Ok(value)
    }

    pub async fn install(&self, root: &Path, storage: &Storage) -> Result<()> {
        self.validate()?;
        let bucket = format!("projects/_/buckets/{}", self.bucket);
        futures_util::stream::iter(self.files.iter().map(Ok))
            .try_for_each_concurrent(MAX_CONCURRENT_DOWNLOADS, |file| async {
                let response = storage
                    .read_object(&bucket, &file.object)
                    .set_generation(file.generation)
                    .send()
                    .await?;
                let chunks = futures_util::stream::try_unfold(response, |mut response| async {
                    match response.next().await {
                        Some(chunk) => Ok(Some((chunk?, response))),
                        None => anyhow::Ok(None),
                    }
                });
                install_file(root, file, chunks).await
            })
            .await
    }

    /// Installs from a [`crate::bucket::Bucket`], pinning each file to its recorded generation.
    pub async fn install_from_bucket(
        &self,
        root: &Path,
        bucket: &dyn crate::bucket::Bucket,
    ) -> Result<()> {
        self.validate()?;
        futures_util::stream::iter(self.files.iter().map(Ok))
            .try_for_each_concurrent(MAX_CONCURRENT_DOWNLOADS, |file| async {
                let object = bucket
                    .get(&file.object)
                    .await?
                    .with_context(|| format!("artifact {} is missing", file.path))?;
                ensure!(
                    object.generation == file.generation,
                    "artifact {} changed after deployment",
                    file.path
                );
                install_file(
                    root,
                    file,
                    futures_util::stream::once(async { anyhow::Ok(object.bytes) }),
                )
                .await
            })
            .await
    }

    pub fn entrypoint(&self) -> Result<&str> {
        let names: Vec<_> = self
            .files
            .iter()
            .filter(|file| matches!(file.path.as_str(), "actors.mjs" | "actors.pyz"))
            .collect();
        ensure!(
            names.len() == 1,
            "actor artifact requires exactly one compiled entrypoint"
        );
        Ok(&names[0].path)
    }

    fn validate(&self) -> Result<()> {
        crate::storage::validate_bucket(&self.bucket)?;
        let mut paths = std::collections::HashSet::new();
        for file in &self.files {
            validate_path(&file.path)?;
            ensure!(paths.insert(&file.path), "duplicate artifact path");
            ensure!(
                file.generation > 0
                    && !file.object.is_empty()
                    && URL_SAFE_NO_PAD.decode(&file.sha256)?.len() == 32,
                "invalid artifact identity"
            );
        }
        self.entrypoint()?;
        Ok(())
    }
}

async fn install_file(
    root: &Path,
    artifact: &BundleFile,
    chunks: impl Stream<Item = Result<Bytes>>,
) -> Result<()> {
    validate_path(&artifact.path)?;
    let destination = root.join(&artifact.path);
    let parent = destination.parent().context("artifact parent missing")?;
    tokio::fs::create_dir_all(parent).await?;
    let temporary = tempfile::NamedTempFile::new_in(parent)?;
    let mut file = tokio::fs::File::from_std(temporary.reopen()?);
    let mut hash = Digest::new(&SHA256);
    futures_util::pin_mut!(chunks);
    while let Some(chunk) = chunks.try_next().await? {
        hash.update(&chunk);
        file.write_all(&chunk).await?;
    }
    file.flush().await?;
    ensure!(
        URL_SAFE_NO_PAD.encode(hash.finish().as_ref()) == artifact.sha256,
        "artifact digest mismatch"
    );
    drop(file);
    temporary.persist(destination)?;
    Ok(())
}

pub(crate) fn validate_path(path: &str) -> Result<()> {
    ensure!(
        !path.is_empty()
            && Path::new(path)
                .components()
                .all(|part| matches!(part, Component::Normal(_))),
        "artifact path must stay inside the customer directory"
    );
    Ok(())
}
