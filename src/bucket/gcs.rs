use std::{
    sync::{Arc, OnceLock},
    time::Duration,
};

use anyhow::Result;
use async_trait::async_trait;
use futures_util::FutureExt;
use google_cloud_auth::credentials::{
    CacheableResource, Credentials, CredentialsProvider, EntityTag,
};
use google_cloud_storage::{
    client::{Storage, StorageControl},
    model_ext::ReadRange,
    read_object::ReadObjectResponse,
};

use super::{Bucket, BucketObject};

pub struct GcsBucket {
    bucket: String,
    clients: GcsClients,
}

pub(crate) struct WarmGcs {
    clients: GcsClients,
    credentials: PendingCredentials,
}

/// A spare's clients for the two halves of [`super::access::StorageTokens`].
pub(crate) struct WarmStorage {
    pub ownership: WarmGcs,
    pub state: WarmGcs,
}

#[derive(Clone)]
pub(crate) struct GcsClients {
    pub storage: Storage,
    pub control: StorageControl,
    pub transfers: Arc<tokio::sync::Semaphore>,
}

impl GcsBucket {
    pub(crate) fn with_clients(bucket: &str, clients: GcsClients) -> Result<Self> {
        Ok(Self {
            bucket: bucket_name(bucket)?,
            clients,
        })
    }

    pub(crate) fn clients(&self) -> GcsClients {
        self.clients.clone()
    }

    pub(crate) async fn require_standard(&self) -> Result<()> {
        let bucket = self
            .clients
            .control
            .get_bucket()
            .set_name(&self.bucket)
            .send()
            .await?;
        anyhow::ensure!(
            bucket.storage_class == "STANDARD",
            "archive bucket must use Standard GCS"
        );
        Ok(())
    }

    pub async fn new(bucket: &str) -> Result<Self> {
        Ok(Self {
            bucket: bucket_name(bucket)?,
            clients: GcsClients::new(
                google_cloud_auth::credentials::Builder::default().build()?,
                None,
            )
            .await?,
        })
    }

    /// An actor host's bucket; see [`HOST_GRPC_SUBCHANNELS`].
    pub async fn with_credentials(bucket: &str, credentials: Credentials) -> Result<Self> {
        Ok(Self {
            bucket: bucket_name(bucket)?,
            clients: GcsClients::new(credentials, Some(HOST_GRPC_SUBCHANNELS)).await?,
        })
    }
}

impl WarmStorage {
    pub async fn new() -> Result<Self> {
        let (ownership, state) = tokio::try_join!(WarmGcs::new(), WarmGcs::new())?;
        Ok(Self { ownership, state })
    }

    pub async fn preconnect(&self) {
        tokio::join!(self.ownership.preconnect(), self.state.preconnect());
    }

    pub async fn keep_warm(&self) {
        tokio::join!(self.ownership.keep_warm(), self.state.keep_warm());
    }
}

impl WarmGcs {
    pub async fn new() -> Result<Self> {
        let credentials = PendingCredentials::default();
        Ok(Self {
            clients: GcsClients::new(credentials.clone().into(), Some(HOST_GRPC_SUBCHANNELS))
                .await?,
            credentials,
        })
    }

    pub async fn preconnect(&self) {
        // An anonymous read warms the HTTP pool without granting an idle spare access.
        let probe = self
            .clients
            .storage
            .read_object("projects/_/buckets/durable-actors-warmup", "connection")
            .send();
        let _ = tokio::time::timeout(Duration::from_millis(250), probe).await;
    }

    pub async fn keep_warm(&self) {
        let period = Duration::from_secs(20);
        let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            self.preconnect().await;
        }
    }

    pub fn bind(self, bucket: &str, credentials: Credentials) -> Result<GcsBucket> {
        let bucket = bucket_name(bucket)?;
        anyhow::ensure!(
            self.credentials.0.set(credentials).is_ok(),
            "storage already assigned"
        );
        Ok(GcsBucket {
            bucket,
            clients: self.clients,
        })
    }
}

/// gRPC connections an actor host's storage client opens. The client defaults to one
/// per visible CPU, which in a pod can be far more than the actor uses, all opened at
/// activation through the egress proxy.
/// One actor's storage traffic fits in one HTTP/2 connection (~100 concurrent RPCs).
pub(crate) const HOST_GRPC_SUBCHANNELS: usize = 1;

impl GcsClients {
    async fn transfer(&self, length: u64) -> Result<Option<tokio::sync::SemaphorePermit<'_>>> {
        if length <= crate::payload::BUFFER_BYTES as u64 {
            return Ok(None);
        }
        Ok(Some(self.transfers.acquire().await?))
    }

    async fn new(credentials: Credentials, subchannels: Option<usize>) -> Result<Self> {
        let mut storage = Storage::builder().with_credentials(credentials.clone());
        if let Some(count) = subchannels {
            storage = storage.with_grpc_subchannel_count(count);
        }
        Ok(Self {
            transfers: Arc::new(tokio::sync::Semaphore::new(2)),
            storage: storage.build().await?,
            control: StorageControl::builder()
                .with_credentials(credentials)
                .build()
                .await?,
        })
    }
}

#[async_trait]
impl Bucket for GcsBucket {
    async fn get(&self, key: &str) -> Result<Option<BucketObject>> {
        self.read(key, None).await
    }
    async fn range(&self, key: &str, start: u64, length: u64) -> Result<Option<BucketObject>> {
        let result = self.read(key, Some((start, length))).await?;
        anyhow::ensure!(
            result
                .as_ref()
                .is_none_or(|object| object.bytes.len() as u64 == length),
            "incomplete object range"
        );
        Ok(result)
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        generation: Option<i64>,
        bytes: crate::payload::Payload,
    ) -> Result<bool> {
        let _transfer = self.clients.transfer(bytes.len() as u64).await?;
        let source = crate::payload::Upload::new(bytes);
        match self
            .clients
            .storage
            .write_object(&self.bucket, key, source)
            .set_if_generation_match(generation.unwrap_or(0))
            .send_unbuffered()
            .await
        {
            Ok(_) => Ok(true),
            Err(error)
                if error.http_status_code() == Some(412)
                    || error
                        .status()
                        .is_some_and(|status| status.code.name() == "FAILED_PRECONDITION") =>
            {
                Ok(false)
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut token = String::new();
        loop {
            let response = self
                .clients
                .control
                .list_objects()
                .set_parent(&self.bucket)
                .set_prefix(prefix)
                .set_page_token(&token)
                .send()
                .await?;
            keys.extend(response.objects.into_iter().map(|object| object.name));
            token = response.next_page_token;
            if token.is_empty() {
                return Ok(keys);
            }
        }
    }
}

fn bucket_name(bucket: &str) -> Result<String> {
    anyhow::ensure!(
        !bucket.is_empty() && !bucket.contains('/'),
        "invalid storage bucket"
    );
    Ok(format!("projects/_/buckets/{bucket}"))
}

#[derive(Clone, Default)]
struct PendingCredentials(Arc<OnceLock<Credentials>>);

impl std::fmt::Debug for PendingCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PendingCredentials")
    }
}

impl CredentialsProvider for PendingCredentials {
    async fn headers(
        &self,
        extensions: axum::http::Extensions,
    ) -> std::result::Result<
        CacheableResource<axum::http::HeaderMap>,
        google_cloud_auth::errors::CredentialsError,
    > {
        match self.0.get() {
            Some(credentials) => credentials.headers(extensions).await,
            None => Ok(CacheableResource::New {
                entity_tag: EntityTag::new(),
                data: axum::http::HeaderMap::new(),
            }),
        }
    }

    async fn universe_domain(&self) -> Option<String> {
        Some("googleapis.com".into())
    }
}

#[cfg(test)]
#[path = "../../tests/unit/bucket/gcs.rs"]
mod tests;

impl GcsBucket {
    async fn read(&self, key: &str, range: Option<(u64, u64)>) -> Result<Option<BucketObject>> {
        let mut request = self.clients.storage.read_object(&self.bucket, key);
        if let Some((start, length)) = range {
            request = request.set_read_range(ReadRange::segment(start, length));
        }
        let mut response = match request.send().await {
            Ok(response) => response,
            Err(error)
                if error.http_status_code() == Some(404)
                    || error
                        .status()
                        .is_some_and(|status| status.code.name() == "NOT_FOUND") =>
            {
                return Ok(None);
            }
            Err(error) => return Err(error.into()),
        };
        let (start, length) = range.unwrap_or((0, response.object().size as u64));
        let _transfer = self.clients.transfer(length).await?;
        let generation = response.object().generation;
        let bytes = if length >= PARALLEL_DOWNLOAD_BYTES {
            // Ranged reads skip the client's whole-object CRC32C check, so it runs on the result.
            let crc32c = range
                .is_none()
                .then(|| response.object().checksums.and_then(|c| c.crc32c))
                .flatten();
            self.download_parts(key, generation, (start, length), crc32c, response)
                .await?
        } else {
            let mut download = crate::payload::Download::new();
            while let Some(chunk) = response.next().await {
                download = download.append(chunk?).await?;
            }
            download.finish().await?
        };
        Ok(Some(BucketObject { generation, bytes }))
    }

    /// Streams the first part from the response already open and the rest as concurrent ranges
    /// of the same generation, each written at its offset in one temporary file.
    async fn download_parts(
        &self,
        key: &str,
        generation: i64,
        (start, length): (u64, u64),
        crc32c: Option<u32>,
        first: ReadObjectResponse,
    ) -> Result<crate::payload::Payload> {
        let file = tokio::task::spawn_blocking(move || -> Result<std::fs::File> {
            let file = tempfile::tempfile()?;
            file.set_len(length)?;
            Ok(file)
        })
        .await??;
        let file = Arc::new(file);
        let parts = download_parts(length);
        let (offset, size) = parts[0];
        let mut downloads = vec![write_part(file.clone(), offset, size, first).boxed()];
        for &(offset, size) in &parts[1..] {
            let file = file.clone();
            downloads.push(
                async move {
                    let response = self
                        .clients
                        .storage
                        .read_object(&self.bucket, key)
                        .set_generation(generation)
                        .set_read_range(ReadRange::segment(start + offset, size))
                        .send()
                        .await?;
                    write_part(file, offset, size, response).await
                }
                .boxed(),
            );
        }
        futures_util::future::try_join_all(downloads).await?;
        if let Some(expected) = crc32c {
            verify_crc32c(file.clone(), length, expected).await?;
        }
        Ok(crate::payload::Payload::from_file(
            file,
            usize::try_from(length)?,
        ))
    }
}

/// Objects at least this large download as parallel byte ranges; one stream from Standard GCS
/// reaches about 100 MiB/s.
const PARALLEL_DOWNLOAD_BYTES: u64 = 64 * 1024 * 1024;
const DOWNLOAD_PARTS: u64 = 8;

/// Contiguous `(offset, size)` ranges that cover `length` bytes.
fn download_parts(length: u64) -> Vec<(u64, u64)> {
    let size = length.div_ceil(DOWNLOAD_PARTS).max(1);
    (0..length)
        .step_by(size as usize)
        .map(|offset| (offset, size.min(length - offset)))
        .collect()
}

async fn write_part(
    file: Arc<std::fs::File>,
    offset: u64,
    size: u64,
    mut response: ReadObjectResponse,
) -> Result<()> {
    let mut written = 0u64;
    let mut buffer = Vec::with_capacity(crate::payload::IO_BUFFER_BYTES);
    while written + (buffer.len() as u64) < size {
        let Some(chunk) = response.next().await else {
            break;
        };
        let chunk = chunk?;
        let wanted = (size - written - buffer.len() as u64).min(chunk.len() as u64) as usize;
        buffer.extend_from_slice(&chunk[..wanted]);
        if buffer.len() >= crate::payload::IO_BUFFER_BYTES {
            written += flush_part(&file, offset + written, &mut buffer).await?;
        }
    }
    written += flush_part(&file, offset + written, &mut buffer).await?;
    anyhow::ensure!(written == size, "incomplete object part");
    Ok(())
}

async fn verify_crc32c(file: Arc<std::fs::File>, length: u64, expected: u32) -> Result<()> {
    tokio::task::spawn_blocking(move || {
        use std::os::unix::fs::FileExt;
        let mut buffer = vec![0; crate::payload::IO_BUFFER_BYTES];
        let (mut crc, mut offset) = (0u32, 0u64);
        while offset < length {
            let size = (length - offset).min(buffer.len() as u64) as usize;
            let chunk = &mut buffer[..size];
            file.read_exact_at(chunk, offset)?;
            crc = crc32c::crc32c_append(crc, chunk);
            offset += chunk.len() as u64;
        }
        anyhow::ensure!(crc == expected, "parallel download failed its CRC32C check");
        Ok(())
    })
    .await?
}

async fn flush_part(file: &Arc<std::fs::File>, at: u64, buffer: &mut Vec<u8>) -> Result<u64> {
    let (file, bytes) = (file.clone(), std::mem::take(buffer));
    let length = bytes.len() as u64;
    tokio::task::spawn_blocking(move || {
        use std::os::unix::fs::FileExt;
        file.write_all_at(&bytes, at)
    })
    .await??;
    Ok(length)
}
