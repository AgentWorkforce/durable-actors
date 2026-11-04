use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_s3::{
    Client,
    config::{BehaviorVersion, Region},
    error::SdkError,
    primitives::ByteStream,
};
use serde::{Deserialize, Serialize};

use super::{Bucket, BucketObject};

/// S3 has no object generations, so each object written through this bucket records its own in
/// user metadata. Compare-and-swap writes with `If-Match` on the ETag that was current at that
/// generation. Every write's bytes are unique (see [`super::replace`]), so an ETag that still
/// matches means no write happened since, and the precondition is exactly "still at generation".
const GENERATION: &str = "da-generation";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct S3Endpoint {
    pub region: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub force_path_style: bool,
}

pub struct S3Bucket {
    bucket: String,
    client: Client,
    versions: moka::future::Cache<String, (i64, String)>,
    transfers: Arc<tokio::sync::Semaphore>,
}

impl S3Bucket {
    /// The control plane's bucket, signed by the default AWS chain (EKS Pod Identity, IRSA, env).
    pub async fn from_environment(bucket: &str, endpoint: &S3Endpoint) -> Result<Self> {
        let shared = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(endpoint.region.clone()))
            .load()
            .await;
        let config = aws_sdk_s3::config::Builder::from(&shared);
        Self::with_config(bucket, endpoint, config)
    }

    /// An actor host's bucket, signed only by the scoped credentials it was issued.
    pub fn with_credentials(
        bucket: &str,
        endpoint: &S3Endpoint,
        credentials: SharedCredentialsProvider,
    ) -> Result<Self> {
        let config = aws_sdk_s3::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(endpoint.region.clone()))
            .credentials_provider(credentials);
        Self::with_config(bucket, endpoint, config)
    }

    fn with_config(
        bucket: &str,
        endpoint: &S3Endpoint,
        mut config: aws_sdk_s3::config::Builder,
    ) -> Result<Self> {
        validate_bucket(bucket)?;
        ensure!(!endpoint.region.is_empty(), "S3 region is required");
        if let Some(url) = &endpoint.endpoint {
            config = config.endpoint_url(url);
        }
        Ok(Self {
            bucket: bucket.into(),
            client: Client::from_conf(config.force_path_style(endpoint.force_path_style).build()),
            versions: moka::future::Cache::new(16_384),
            transfers: Arc::new(tokio::sync::Semaphore::new(2)),
        })
    }

    async fn transfer(&self, length: u64) -> Result<Option<tokio::sync::SemaphorePermit<'_>>> {
        if length <= crate::payload::BUFFER_BYTES as u64 {
            return Ok(None);
        }
        Ok(Some(self.transfers.acquire().await?))
    }

    async fn read(&self, key: &str, range: Option<(u64, u64)>) -> Result<Option<BucketObject>> {
        let mut request = self.client.get_object().bucket(&self.bucket).key(key);
        if let Some((start, length)) = range {
            let end = start.checked_add(length - 1).context("range overflow")?;
            request = request.range(format!("bytes={start}-{end}"));
        }
        let response = match request.send().await {
            Ok(response) => response,
            Err(error) if status(&error) == Some(404) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let generation = generation(response.metadata())?;
        if range.is_none()
            && let Some(etag) = response.e_tag()
        {
            self.versions
                .insert(key.into(), (generation, etag.into()))
                .await;
        }
        let length = response.content_length().unwrap_or_default().max(0) as u64;
        let _transfer = self.transfer(length).await?;
        let bytes = response.body.collect().await?.into_bytes();
        Ok(Some(BucketObject { generation, bytes }))
    }

    async fn head(&self, key: &str) -> Result<Option<(i64, String)>> {
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(response) => Ok(Some((
                generation(response.metadata())?,
                response.e_tag().context("S3 object has no ETag")?.into(),
            ))),
            Err(error) if status(&error) == Some(404) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// The ETag of `key` at `expected`, or `None` when the object is elsewhere.
    async fn etag_at(&self, key: &str, expected: i64) -> Result<Option<String>> {
        if let Some((generation, etag)) = self.versions.get(key).await
            && generation == expected
        {
            return Ok(Some(etag));
        }
        Ok(self
            .head(key)
            .await?
            .filter(|(generation, _)| *generation == expected)
            .map(|(_, etag)| etag))
    }
}

#[async_trait]
impl Bucket for S3Bucket {
    async fn get(&self, key: &str) -> Result<Option<BucketObject>> {
        self.read(key, None).await
    }

    async fn range(&self, key: &str, start: u64, length: u64) -> Result<Option<BucketObject>> {
        if length == 0 {
            return Ok(self.head(key).await?.map(|(generation, _)| BucketObject {
                generation,
                bytes: bytes::Bytes::new(),
            }));
        }
        let result = self.read(key, Some((start, length))).await?;
        ensure!(
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
        bytes: bytes::Bytes,
    ) -> Result<bool> {
        let mut request = self.client.put_object().bucket(&self.bucket).key(key);
        let next = match generation {
            None => {
                request = request.if_none_match("*");
                1
            }
            Some(expected) => {
                let Some(etag) = self.etag_at(key, expected).await? else {
                    return Ok(false);
                };
                request = request.if_match(etag);
                expected
                    .checked_add(1)
                    .context("bucket generation overflow")?
            }
        };
        let _transfer = self.transfer(bytes.len() as u64).await?;
        match request
            .metadata(GENERATION, next.to_string())
            .content_length(bytes.len() as i64)
            .body(ByteStream::from(bytes))
            .send()
            .await
        {
            Ok(response) => {
                match response.e_tag() {
                    Some(etag) => self.versions.insert(key.into(), (next, etag.into())).await,
                    None => self.versions.invalidate(key).await,
                }
                Ok(true)
            }
            Err(error) if lost_precondition(status(&error)) => {
                self.versions.invalidate(key).await;
                Ok(false)
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut pages = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(prefix)
            .into_paginator()
            .send();
        while let Some(page) = pages.next().await {
            keys.extend(
                page?
                    .contents()
                    .iter()
                    .filter_map(|object| object.key().map(str::to_owned)),
            );
        }
        Ok(keys)
    }
}

/// 412 is a failed `If-Match`/`If-None-Match`; 409 is a conditional write that raced another;
/// 404 is an `If-Match` on an object that no longer exists.
fn lost_precondition(status: Option<u16>) -> bool {
    matches!(status, Some(404 | 409 | 412))
}

fn status<E>(error: &SdkError<E, aws_sdk_s3::config::http::HttpResponse>) -> Option<u16> {
    error
        .raw_response()
        .map(|response| response.status().as_u16())
}

fn generation(metadata: Option<&std::collections::HashMap<String, String>>) -> Result<i64> {
    let generation: i64 = metadata
        .and_then(|metadata| metadata.get(GENERATION))
        .context("S3 object has no durable-actors generation")?
        .parse()
        .context("S3 object generation is invalid")?;
    ensure!(generation > 0, "S3 object generation is invalid");
    Ok(generation)
}

/// S3 bucket names: 3-63 lowercase letters, digits, dots and hyphens.
fn validate_bucket(bucket: &str) -> Result<()> {
    ensure!(
        (3..=63).contains(&bucket.len())
            && bucket.bytes().all(|byte| byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || b".-".contains(&byte))
            && bucket.starts_with(|c: char| c.is_ascii_alphanumeric())
            && bucket.ends_with(|c: char| c.is_ascii_alphanumeric()),
        "invalid S3 bucket"
    );
    Ok(())
}

#[cfg(test)]
#[path = "../../tests/unit/bucket/s3.rs"]
mod tests;
