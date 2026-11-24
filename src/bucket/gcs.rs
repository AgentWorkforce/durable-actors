use std::{
    sync::{Arc, OnceLock},
    time::Duration,
};

use anyhow::Result;
use async_trait::async_trait;
use google_cloud_auth::credentials::{
    CacheableResource, Credentials, CredentialsProvider, EntityTag,
};
use google_cloud_storage::{
    client::{Storage, StorageControl},
    model_ext::ReadRange,
    read_object::ReadObjectResponse,
};
use tokio_util::task::AbortOnDropHandle;

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

    /// A node's bucket: its own, or one restore's read with a host's forwarded token. Outside the
    /// sandbox it can afford the connections and transfers its parallel reads need.
    pub(crate) async fn for_restore(bucket: &str, credentials: Credentials) -> Result<Self> {
        let mut clients = GcsClients::new(credentials, Some(RESTORE_GRPC_SUBCHANNELS)).await?;
        clients.transfers = Arc::new(tokio::sync::Semaphore::new(RESTORE_TRANSFERS));
        Ok(Self {
            bucket: bucket_name(bucket)?,
            clients,
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
/// A restore reads up to 64 Rapid segments at once; one HTTP/2 connection carried ~140 MB/s.
const RESTORE_GRPC_SUBCHANNELS: usize = 8;
/// Large transfers at once for a restore's client; a host's stays at two to bound its memory.
const RESTORE_TRANSFERS: usize = 8;

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
    async fn download_to(&self, key: &str, path: &std::path::Path) -> Result<Option<u64>> {
        let Some(response) = self.open(key, None).await? else {
            return Ok(None);
        };
        let length = response.object().size as u64;
        let _transfer = self.clients.transfer(length).await?;
        let path = path.to_owned();
        let file = tokio::task::spawn_blocking(move || -> Result<std::fs::File> {
            let file = std::fs::File::create_new(path)?;
            file.set_len(length)?;
            Ok(file)
        })
        .await??;
        let crc32c = response.object().checksums.and_then(|c| c.crc32c);
        let generation = response.object().generation;
        self.download_parts(
            key,
            generation,
            (0, length),
            crc32c,
            response,
            Arc::new(file),
        )
        .await?;
        Ok(Some(length))
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

/// Credentials that send one access token a host forwarded, for the node to act with its scope.
#[derive(Debug)]
struct ForwardedToken(String);

pub(crate) fn forwarded_token(access_token: String) -> Credentials {
    ForwardedToken(access_token).into()
}

impl CredentialsProvider for ForwardedToken {
    async fn headers(
        &self,
        _: axum::http::Extensions,
    ) -> std::result::Result<
        CacheableResource<axum::http::HeaderMap>,
        google_cloud_auth::errors::CredentialsError,
    > {
        bearer(&self.0)
    }

    async fn universe_domain(&self) -> Option<String> {
        Some("googleapis.com".into())
    }
}

/// The headers that authorize requests with `access_token`.
pub(crate) fn bearer(
    access_token: &str,
) -> std::result::Result<
    CacheableResource<axum::http::HeaderMap>,
    google_cloud_auth::errors::CredentialsError,
> {
    let mut value: axum::http::HeaderValue =
        format!("Bearer {access_token}").parse().map_err(|_| {
            google_cloud_auth::errors::CredentialsError::from_msg(
                false,
                "invalid storage credential",
            )
        })?;
    value.set_sensitive(true);
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(axum::http::header::AUTHORIZATION, value);
    Ok(CacheableResource::New {
        entity_tag: EntityTag::new(),
        data: headers,
    })
}

#[cfg(test)]
#[path = "../../tests/unit/bucket/gcs.rs"]
mod tests;

impl GcsBucket {
    async fn read(&self, key: &str, range: Option<(u64, u64)>) -> Result<Option<BucketObject>> {
        let Some(mut response) = self.open(key, range).await? else {
            return Ok(None);
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
            let file = tokio::task::spawn_blocking(move || -> Result<std::fs::File> {
                let file = tempfile::tempfile()?;
                file.set_len(length)?;
                Ok(file)
            })
            .await??;
            let file = Arc::new(file);
            self.download_parts(
                key,
                generation,
                (start, length),
                crc32c,
                response,
                file.clone(),
            )
            .await?;
            crate::payload::Payload::from_file(file, usize::try_from(length)?)
        } else {
            let mut download = crate::payload::Download::new();
            while let Some(chunk) = response.next().await {
                download = download.append(chunk?).await?;
            }
            download.finish().await?
        };
        Ok(Some(BucketObject { generation, bytes }))
    }

    async fn open(
        &self,
        key: &str,
        range: Option<(u64, u64)>,
    ) -> Result<Option<ReadObjectResponse>> {
        let mut request = self.clients.storage.read_object(&self.bucket, key);
        if let Some((start, length)) = range {
            request = request.set_read_range(ReadRange::segment(start, length));
        }
        match request.send().await {
            Ok(response) => Ok(Some(response)),
            Err(error)
                if error.http_status_code() == Some(404)
                    || error
                        .status()
                        .is_some_and(|status| status.code.name() == "NOT_FOUND") =>
            {
                Ok(None)
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Streams the first part from the response already open and, for a large object, the rest
    /// as concurrent ranges of the same generation, each written at its offset in `file`.
    async fn download_parts(
        &self,
        key: &str,
        generation: i64,
        (start, length): (u64, u64),
        crc32c: Option<u32>,
        first: ReadObjectResponse,
        file: Arc<std::fs::File>,
    ) -> Result<()> {
        let parts = if length >= PARALLEL_DOWNLOAD_BYTES {
            download_parts(length)
        } else {
            vec![(0, length)]
        };
        // A task per range: on one task the ranges took turns receiving and writing, at half the
        // speed of the node's disks.
        let check = crc32c.is_some();
        let (offset, size) = parts[0];
        let mut downloads = vec![AbortOnDropHandle::new(tokio::spawn(write_part(
            file.clone(),
            offset,
            size,
            first,
            check,
        )))];
        for &(offset, size) in &parts[1..] {
            let (storage, bucket, key, file) = (
                self.clients.storage.clone(),
                self.bucket.clone(),
                key.to_owned(),
                file.clone(),
            );
            downloads.push(AbortOnDropHandle::new(tokio::spawn(async move {
                let response = storage
                    .read_object(&bucket, &key)
                    .set_generation(generation)
                    .set_read_range(ReadRange::segment(start + offset, size))
                    .send()
                    .await?;
                write_part(file, offset, size, response, check).await
            })));
        }
        let mut checksums = Vec::with_capacity(downloads.len());
        for download in downloads {
            checksums.push(download.await??);
        }
        if let Some(expected) = crc32c {
            let mut crc = 0;
            for ((_, size), part) in parts.into_iter().zip(checksums) {
                crc = crc32c::crc32c_combine(crc, part, usize::try_from(size)?);
            }
            anyhow::ensure!(crc == expected, "parallel download failed its CRC32C check");
        }
        Ok(())
    }
}

/// Objects at least this large download as parallel byte ranges; one stream from Standard GCS
/// reaches about 100 MiB/s.
const PARALLEL_DOWNLOAD_BYTES: u64 = 64 * 1024 * 1024;
const DOWNLOAD_PARTS: u64 = 8;
/// Each range writes 4 MiB at a time, ~10% faster than 256 KiB on the node's local SSDs.
const PART_WRITE_BYTES: usize = 4 << 20;

/// Contiguous `(offset, size)` ranges that cover `length` bytes.
fn download_parts(length: u64) -> Vec<(u64, u64)> {
    let size = length.div_ceil(DOWNLOAD_PARTS).max(1);
    (0..length)
        .step_by(size as usize)
        .map(|offset| (offset, size.min(length - offset)))
        .collect()
}

pub(super) async fn write_part(
    file: Arc<std::fs::File>,
    offset: u64,
    size: u64,
    mut response: ReadObjectResponse,
    checksum: bool,
) -> Result<u32> {
    let mut crc = 0;
    let mut written = 0u64;
    let mut buffer = Vec::with_capacity(PART_WRITE_BYTES);
    while written + (buffer.len() as u64) < size {
        let Some(chunk) = response.next().await else {
            break;
        };
        let chunk = chunk?;
        let wanted = (size - written - buffer.len() as u64).min(chunk.len() as u64) as usize;
        if checksum {
            crc = crc32c::crc32c_append(crc, &chunk[..wanted]);
        }
        buffer.extend_from_slice(&chunk[..wanted]);
        if buffer.len() >= PART_WRITE_BYTES {
            written += flush_part(&file, offset + written, &mut buffer)?;
        }
    }
    written += flush_part(&file, offset + written, &mut buffer)?;
    anyhow::ensure!(written == size, "incomplete object part");
    Ok(crc)
}

/// Writes on the range's own task: in gVisor, handing each buffer to the blocking pool cost ~10x
/// the CPU of the write itself.
fn flush_part(file: &std::fs::File, at: u64, buffer: &mut Vec<u8>) -> Result<u64> {
    use std::os::unix::fs::FileExt;
    file.write_all_at(buffer, at)?;
    let length = buffer.len() as u64;
    buffer.clear();
    Ok(length)
}
