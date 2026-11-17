use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use aws_sdk_s3::presigning::{PresignedRequest, PresigningConfig};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};

use crate::bucket::{
    Bucket, RapidBucket, SnapshotStoreConfig,
    append_log::{AppendLogSnapshots, LogWriter, LogZone, Replica, ReplicaIdentity},
};

const UPLOADS_PER_REQUEST: u64 = 128;
const PREFETCH_BELOW: usize = 32;
const URL_LIFETIME: Duration = Duration::from_secs(300);

pub(crate) fn snapshots(
    config: &SnapshotStoreConfig,
    archive: Arc<dyn Bucket>,
    logs: Arc<dyn ExpressLogs>,
    stop: CancellationToken,
    actor: Option<&crate::actor::ActorKey>,
) -> Result<AppendLogSnapshots> {
    validate_zones(config)?;
    let http = reqwest::Client::new();
    AppendLogSnapshots::with_zones(
        config,
        archive,
        |placement| {
            Arc::new(ExpressZone {
                bucket: placement.bucket.clone(),
                logs: logs.clone(),
                http: http.clone(),
            })
        },
        stop,
        actor,
    )
}

/// Opens one S3 Express session per log bucket at startup, so a controller role without
/// `s3express:CreateSession` fails here instead of on its first signed upload.
pub(crate) async fn validate(
    config: &SnapshotStoreConfig,
    client: &aws_sdk_s3::Client,
) -> Result<()> {
    let Some((buckets, archive)) = config.rapid_settings() else {
        return Ok(());
    };
    for placement in buckets {
        client
            .create_session()
            .bucket(&placement.bucket)
            .send()
            .await
            .with_context(|| {
                format!(
                    "could not open an S3 Express session on {}: the control-plane role needs s3express:CreateSession on it",
                    placement.bucket
                )
            })?;
    }
    if single_zone(config) {
        tracing::warn!(
            event = "rapid_single_zone",
            archive_bucket = archive,
            "Express logs use one zone: losing it loses writes not yet archived to Standard (see rapid_archive_lag)"
        );
    }
    Ok(())
}

/// Whether acknowledged writes live in only one Express zone until they are archived.
pub(crate) fn single_zone(config: &SnapshotStoreConfig) -> bool {
    match config {
        SnapshotStoreConfig::Rapid {
            buckets, placement, ..
        } => buckets.len() < 2 || *placement == crate::bucket::RapidPlacement::Nearest,
        SnapshotStoreConfig::Local => false,
    }
}

pub(crate) fn validate_zones(config: &SnapshotStoreConfig) -> Result<()> {
    config
        .rapid_settings()
        .map_or(&[][..], |(buckets, _)| buckets)
        .iter()
        .try_for_each(validate_zone)
}

// Directory bucket names end with their zone ID, as in `logs--use2-az1--x-s3`.
fn validate_zone(placement: &RapidBucket) -> Result<()> {
    let name = placement
        .bucket
        .strip_suffix("--x-s3")
        .context("Rapid persistence on S3 requires S3 Express directory buckets")?;
    ensure!(
        placement.zone.contains("-az") && name.ends_with(&format!("--{}", placement.zone)),
        "Express bucket does not occupy its configured zone"
    );
    Ok(())
}

/// Admits a host's request only for its own actor's log segments in a configured Express bucket.
pub(crate) fn authorize(
    request: &ExpressLogRequest,
    actor: &crate::actor::ActorKey,
    config: &SnapshotStoreConfig,
) -> Result<()> {
    let (buckets, _) = config
        .rapid_settings()
        .context("Express logs are not configured")?;
    ensure!(
        buckets
            .iter()
            .any(|placement| placement.bucket == request.bucket()),
        "unknown Express log bucket"
    );
    let segment = request
        .segment()
        .strip_prefix(&crate::bucket::append_log::log_prefix(actor)?)
        .context("Express log segment belongs to another actor")?;
    ensure!(
        segment.ends_with(".segment") && !segment.contains('/'),
        "invalid Express log segment"
    );
    Ok(())
}

/// Signs and fences Express log chunks. Only the control plane holds Express credentials;
/// actor hosts reach it through the control-plane connection.
#[async_trait]
pub(crate) trait ExpressLogs: Send + Sync {
    async fn handle(&self, request: ExpressLogRequest) -> Result<ExpressLogReply>;
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub(crate) enum ExpressLogRequest {
    Issue {
        bucket: String,
        segment: String,
        first: u64,
        count: u64,
    },
    Chunks {
        bucket: String,
        segment: String,
        fence: bool,
    },
    Delete {
        bucket: String,
        segment: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "reply", rename_all = "snake_case")]
pub(crate) enum ExpressLogReply {
    Issued { uploads: Vec<Signed> },
    Chunks { chunks: Vec<Chunk> },
    Deleted,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Signed {
    url: String,
    headers: Vec<(String, String)>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Chunk {
    size: u64,
    download: Signed,
}

impl ExpressLogRequest {
    pub(crate) fn bucket(&self) -> &str {
        match self {
            Self::Issue { bucket, .. }
            | Self::Chunks { bucket, .. }
            | Self::Delete { bucket, .. } => bucket,
        }
    }

    pub(crate) fn segment(&self) -> &str {
        match self {
            Self::Issue { segment, .. }
            | Self::Chunks { segment, .. }
            | Self::Delete { segment, .. } => segment,
        }
    }
}

impl std::fmt::Debug for Signed {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Signed { .. }")
    }
}

#[derive(Clone)]
struct ExpressZone {
    bucket: String,
    logs: Arc<dyn ExpressLogs>,
    http: reqwest::Client,
}

#[async_trait]
impl LogZone for ExpressZone {
    fn bucket(&self) -> &str {
        &self.bucket
    }

    fn validate_identity(&self, replica: &Replica) -> Result<()> {
        ensure!(
            matches!(replica.identity, ReplicaIdentity::Express {}),
            "invalid Express log replica"
        );
        Ok(())
    }

    async fn open(&self, object: &str) -> Result<(Replica, Box<dyn LogWriter>)> {
        let writer = ExpressWriter {
            zone: self.clone(),
            segment: object.into(),
            uploads: issue(self.clone(), object.into(), 1).await?.into(),
            pending: None,
            next: 1,
            size: 0,
        };
        let replica = Replica {
            bucket: self.bucket.clone(),
            object: object.into(),
            identity: ReplicaIdentity::Express {},
        };
        Ok((replica, Box::new(writer)))
    }

    async fn read(&self, replica: &Replica, fence: bool) -> Result<Bytes> {
        let mut download = crate::payload::Download::new();
        for chunk in self.chunks(&replica.object, fence).await? {
            download = self.fetch(&chunk.download, None, download).await?;
        }
        download.finish().await
    }

    async fn read_range(&self, replica: &Replica, start: u64, length: u64) -> Result<Bytes> {
        let end = start
            .checked_add(length)
            .context("Express range overflow")?;
        let mut download = crate::payload::Download::new();
        let mut offset = 0;
        for chunk in self.chunks(&replica.object, false).await? {
            let (first, last) = (start.max(offset), end.min(offset + chunk.size));
            if first < last {
                let range = (first - offset, last - offset - 1);
                download = self.fetch(&chunk.download, Some(range), download).await?;
            }
            offset += chunk.size;
        }
        ensure!(offset >= end, "incomplete Express range");
        download.finish().await
    }

    async fn delete(&self, replica: &Replica) -> Result<()> {
        self.logs
            .handle(ExpressLogRequest::Delete {
                bucket: self.bucket.clone(),
                segment: replica.object.clone(),
            })
            .await?;
        Ok(())
    }
}

impl ExpressZone {
    async fn chunks(&self, segment: &str, fence: bool) -> Result<Vec<Chunk>> {
        let request = ExpressLogRequest::Chunks {
            bucket: self.bucket.clone(),
            segment: segment.into(),
            fence,
        };
        match self.logs.handle(request).await? {
            ExpressLogReply::Chunks { chunks } => Ok(chunks),
            _ => anyhow::bail!("unexpected Express log reply"),
        }
    }

    async fn fetch(
        &self,
        signed: &Signed,
        range: Option<(u64, u64)>,
        mut download: crate::payload::Download,
    ) -> Result<crate::payload::Download> {
        let mut request = self.http.get(&signed.url);
        for (name, value) in &signed.headers {
            request = request.header(name, value);
        }
        if let Some((first, last)) = range {
            request = request.header("range", format!("bytes={first}-{last}"));
        }
        let mut response = request.send().await?.error_for_status()?;
        while let Some(chunk) = response.chunk().await? {
            download = download.append(chunk).await?;
        }
        Ok(download)
    }
}

/// Writes one chunk object per flush through create-only URLs issued ahead of time, fetching
/// the next batch in the background once the current one runs low.
struct ExpressWriter {
    zone: ExpressZone,
    segment: String,
    uploads: VecDeque<Signed>,
    pending: Option<AbortOnDropHandle<Result<Vec<Signed>>>>,
    next: u64,
    size: u64,
}

#[async_trait]
impl LogWriter for ExpressWriter {
    async fn append_and_flush(&mut self, bytes: Bytes) -> Result<u64> {
        ensure!(!bytes.is_empty(), "empty Express log append");
        for _ in 0..2 {
            let upload = self.take().await?;
            let mut request = self.zone.http.put(&upload.url).body(bytes.clone());
            for (name, value) in &upload.headers {
                request = request.header(name, value);
            }
            let status = match request.send().await {
                Ok(response) => response.status(),
                Err(error) => {
                    self.reset();
                    return Err(error.into());
                }
            };
            match status {
                status if status.is_success() => {
                    self.next += 1;
                    self.size += bytes.len() as u64;
                    self.prefetch();
                    return Ok(self.size);
                }
                reqwest::StatusCode::PRECONDITION_FAILED => {
                    anyhow::bail!("Express log segment was fenced")
                }
                // An upload URL dies with the control plane's Express session; nothing was written.
                reqwest::StatusCode::FORBIDDEN | reqwest::StatusCode::BAD_REQUEST => self.reset(),
                status => {
                    self.reset();
                    anyhow::bail!("Express log append failed with {status}")
                }
            }
        }
        anyhow::bail!("Express upload URLs expired before use")
    }
}

impl ExpressWriter {
    // The first queued upload signs chunk `next`; a pending batch starts after the last one.
    async fn take(&mut self) -> Result<Signed> {
        if self.uploads.is_empty() {
            let batch = match self.pending.take() {
                Some(pending) => pending.await??,
                None => issue(self.zone.clone(), self.segment.clone(), self.next).await?,
            };
            self.uploads = batch.into();
        }
        self.uploads.pop_front().context("no Express upload URL")
    }

    fn prefetch(&mut self) {
        if self.pending.is_none() && self.uploads.len() <= PREFETCH_BELOW {
            let first = self.next + self.uploads.len() as u64;
            let batch = issue(self.zone.clone(), self.segment.clone(), first);
            self.pending = Some(AbortOnDropHandle::new(tokio::spawn(batch)));
        }
    }

    fn reset(&mut self) {
        self.uploads.clear();
        self.pending = None;
    }
}

async fn issue(zone: ExpressZone, segment: String, first: u64) -> Result<Vec<Signed>> {
    let request = ExpressLogRequest::Issue {
        bucket: zone.bucket,
        segment,
        first,
        count: UPLOADS_PER_REQUEST,
    };
    match zone.logs.handle(request).await? {
        ExpressLogReply::Issued { uploads } => Ok(uploads),
        _ => anyhow::bail!("unexpected Express log reply"),
    }
}

/// The control plane's Express access: signs chunk uploads and fences abandoned segments.
pub(crate) struct S3ExpressLogs(aws_sdk_s3::Client);

impl S3ExpressLogs {
    pub(crate) fn new(client: aws_sdk_s3::Client) -> Self {
        Self(client)
    }
}

#[async_trait]
impl ExpressLogs for S3ExpressLogs {
    async fn handle(&self, request: ExpressLogRequest) -> Result<ExpressLogReply> {
        Ok(match request {
            ExpressLogRequest::Issue {
                bucket,
                segment,
                first,
                count,
            } => ExpressLogReply::Issued {
                uploads: self.issue(&bucket, &segment, first, count).await?,
            },
            ExpressLogRequest::Chunks {
                bucket,
                segment,
                fence,
            } => ExpressLogReply::Chunks {
                chunks: self.chunks(&bucket, &segment, fence).await?,
            },
            ExpressLogRequest::Delete { bucket, segment } => {
                let keys = self.listing(&bucket, &segment).await?.into_keys();
                futures_util::future::try_join_all(
                    keys.map(|key| self.0.delete_object().bucket(&bucket).key(key).send()),
                )
                .await?;
                ExpressLogReply::Deleted
            }
        })
    }
}

impl S3ExpressLogs {
    async fn issue(
        &self,
        bucket: &str,
        segment: &str,
        first: u64,
        count: u64,
    ) -> Result<Vec<Signed>> {
        ensure!(
            first > 0 && (1..=UPLOADS_PER_REQUEST).contains(&count),
            "invalid Express upload request"
        );
        let last = first + count - 1;
        // Fencing tombstones every chunk up to the highest marker and seals before listing, so
        // a marker written before the sealed check is always seen by a concurrent fence.
        self.create(bucket, &format!("{segment}/issued-{last:010}"))
            .await?;
        ensure!(
            !self.exists(bucket, &format!("{segment}/sealed")).await?,
            "Express log segment is sealed"
        );
        let mut uploads = Vec::new();
        for seq in first..=last {
            let request = self
                .0
                .put_object()
                .bucket(bucket)
                .key(chunk(segment, seq))
                .if_none_match("*")
                .presigned(PresigningConfig::expires_in(URL_LIFETIME)?)
                .await?;
            uploads.push(signed(&request));
        }
        Ok(uploads)
    }

    async fn chunks(&self, bucket: &str, segment: &str, fence: bool) -> Result<Vec<Chunk>> {
        if fence {
            self.create(bucket, &format!("{segment}/sealed")).await?;
        }
        let listing = self.listing(bucket, segment).await?;
        let mut sizes = BTreeMap::new();
        let mut issued = 0;
        for (key, size) in listing {
            let name = key.strip_prefix(&format!("{segment}/")).unwrap_or_default();
            if let Some(last) = name.strip_prefix("issued-") {
                issued = issued.max(last.parse()?);
            } else if let Ok(seq) = name.parse::<u64>() {
                sizes.insert(seq, size);
            }
        }
        if fence {
            let missing = (1..=issued).filter(|seq| !sizes.contains_key(seq));
            let landed = futures_util::future::try_join_all(missing.map(|seq| async move {
                Ok::<_, anyhow::Error>(match self.create(bucket, &chunk(segment, seq)).await? {
                    true => None,
                    false => Some((seq, self.size(bucket, &chunk(segment, seq)).await?)),
                })
            }))
            .await?;
            sizes.extend(landed.into_iter().flatten());
        }
        let mut chunks = Vec::new();
        for seq in 1.. {
            // Empty chunks are tombstones: the log ends at the first one.
            let Some(&size) = sizes.get(&seq).filter(|size| **size > 0) else {
                break;
            };
            let request = self
                .0
                .get_object()
                .bucket(bucket)
                .key(chunk(segment, seq))
                .presigned(PresigningConfig::expires_in(URL_LIFETIME)?)
                .await?;
            chunks.push(Chunk {
                size,
                download: signed(&request),
            });
        }
        Ok(chunks)
    }

    async fn listing(&self, bucket: &str, segment: &str) -> Result<BTreeMap<String, u64>> {
        let mut objects = BTreeMap::new();
        let mut token = None;
        loop {
            let page = self
                .0
                .list_objects_v2()
                .bucket(bucket)
                .prefix(format!("{segment}/"))
                .set_continuation_token(token)
                .send()
                .await?;
            for object in page.contents() {
                let size = u64::try_from(object.size().unwrap_or_default())?;
                objects.insert(
                    object.key().context("listed object missing key")?.into(),
                    size,
                );
            }
            token = page.next_continuation_token().map(str::to_owned);
            if token.is_none() {
                return Ok(objects);
            }
        }
    }

    async fn create(&self, bucket: &str, key: &str) -> Result<bool> {
        match self
            .0
            .put_object()
            .bucket(bucket)
            .key(key)
            .if_none_match("*")
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(error)
                if error
                    .raw_response()
                    .is_some_and(|response| response.status().as_u16() == 412) =>
            {
                Ok(false)
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn exists(&self, bucket: &str, key: &str) -> Result<bool> {
        match self.0.head_object().bucket(bucket).key(key).send().await {
            Ok(_) => Ok(true),
            Err(error) if error.as_service_error().is_some_and(|e| e.is_not_found()) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    async fn size(&self, bucket: &str, key: &str) -> Result<u64> {
        let object = self.0.head_object().bucket(bucket).key(key).send().await?;
        Ok(u64::try_from(object.content_length().unwrap_or_default())?)
    }
}

fn chunk(segment: &str, seq: u64) -> String {
    format!("{segment}/{seq:010}")
}

fn signed(request: &PresignedRequest) -> Signed {
    Signed {
        url: request.uri().to_owned(),
        headers: request
            .headers()
            .map(|(name, value)| (name.to_owned(), value.to_owned()))
            .collect(),
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/bucket/s3/express.rs"]
mod tests;
