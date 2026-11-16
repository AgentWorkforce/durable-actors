use std::{path::Path, time::Duration};

use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use futures_util::{TryStreamExt, stream::BoxStream};
use reqwest::header::AUTHORIZATION;
use tokio::io::AsyncWriteExt;

use crate::control_plane::HostAuthorization;

// With no cache pod on the node the Service drops packets instead of refusing them, so only a
// short connect timeout keeps installs from stalling.
const CONNECT_TIMEOUT: Duration = Duration::from_millis(50);
const READ_TIMEOUT: Duration = Duration::from_secs(10);
/// Activation reads code, the database and the snapshot at once, and a read that finds no idle
/// connection pays for a new one inside gVisor.
const WARM_CONNECTIONS: usize = 4;

/// Built and connected while a spare waits: building the client alone costs ~15 ms in gVisor, so
/// assignment only attaches the host token.
#[derive(Clone)]
pub(crate) struct NodeCacheConnection {
    http: reqwest::Client,
    url: reqwest::Url,
}

impl NodeCacheConnection {
    pub(crate) async fn warm_from_env() -> Result<Option<Self>> {
        let Some(connection) = Self::from_env()? else {
            return Ok(None);
        };
        connection.preconnect().await;
        Ok(Some(connection))
    }

    pub(crate) fn from_env() -> Result<Option<Self>> {
        match std::env::var("DURABLE_ACTORS_NODE_CACHE_URL") {
            Ok(url) if !url.is_empty() => Ok(Some(Self::new(&url)?)),
            _ => Ok(None),
        }
    }

    pub(crate) fn new(url: &str) -> Result<Self> {
        Ok(Self {
            http: reqwest::Client::builder()
                .connect_timeout(CONNECT_TIMEOUT)
                .read_timeout(READ_TIMEOUT)
                .pool_idle_timeout(None)
                .build()?,
            url: reqwest::Url::parse(url).context("node cache URL is invalid")?,
        })
    }

    pub(crate) fn authorize(self, authorization: HostAuthorization) -> NodeCacheClient {
        NodeCacheClient {
            connection: self,
            authorization,
        }
    }

    pub(crate) async fn keep_warm(&self) {
        let period = Duration::from_secs(20);
        let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            self.preconnect().await;
        }
    }

    pub(super) async fn preconnect(&self) {
        let Ok(url) = self.url.join("healthz") else {
            return;
        };
        futures_util::future::join_all(
            (0..WARM_CONNECTIONS).map(|_| self.http.get(url.clone()).send()),
        )
        .await;
    }
}

#[derive(Clone)]
pub(crate) struct NodeCacheClient {
    connection: NodeCacheConnection,
    authorization: HostAuthorization,
}

impl NodeCacheClient {
    pub(crate) async fn code(
        &self,
        digest: &str,
    ) -> Result<Option<BoxStream<'static, Result<Bytes>>>> {
        self.read(&format!("code/{digest}")).await
    }

    pub(crate) async fn store_code(&self, digest: &str, path: &Path) -> Result<()> {
        self.write(&format!("code/{digest}"), file_body(path).await?)
            .await
    }

    /// The actor database saved at the snapshot with this digest, once the cache has answered
    /// but before its body has arrived.
    pub(crate) async fn database(&self, digest: &str) -> Result<Option<DatabaseDownload>> {
        Ok(self
            .read(&format!("sqlite/{digest}"))
            .await?
            .map(DatabaseDownload))
    }

    pub(crate) async fn store_database(&self, digest: &str, path: &Path) -> Result<()> {
        self.write(&format!("sqlite/{digest}"), file_body(path).await?)
            .await
    }

    pub(crate) async fn snapshot(&self, digest: &str) -> Result<Option<Bytes>> {
        let Some(chunks) = self.read(&format!("snapshot/{digest}")).await? else {
            return Ok(None);
        };
        let bytes = chunks
            .try_fold(BytesMut::new(), |mut bytes, chunk| async move {
                bytes.extend_from_slice(&chunk);
                Ok(bytes)
            })
            .await?;
        Ok(Some(bytes.freeze()))
    }

    pub(crate) async fn store_snapshot(&self, digest: &str, bytes: Bytes) -> Result<()> {
        self.write(&format!("snapshot/{digest}"), bytes.into())
            .await
    }

    async fn read(&self, entry: &str) -> Result<Option<BoxStream<'static, Result<Bytes>>>> {
        let response = self
            .connection
            .http
            .get(self.connection.url.join(entry)?)
            .header(AUTHORIZATION, self.authorization.header()?)
            .send()
            .await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let chunks = response.error_for_status()?.bytes_stream();
        Ok(Some(Box::pin(chunks.map_err(anyhow::Error::from))))
    }

    async fn write(&self, entry: &str, body: reqwest::Body) -> Result<()> {
        self.connection
            .http
            .put(self.connection.url.join(entry)?)
            .header(AUTHORIZATION, self.authorization.header()?)
            .body(body)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }
}

async fn file_body(path: &Path) -> Result<reqwest::Body> {
    let file = tokio::fs::File::open(path).await?;
    Ok(reqwest::Body::wrap_stream(
        tokio_util::io::ReaderStream::with_capacity(file, super::FILE_CHUNK_BYTES),
    ))
}

pub(crate) struct DatabaseDownload(BoxStream<'static, Result<Bytes>>);

impl DatabaseDownload {
    pub(crate) async fn receive(mut self) -> Result<tempfile::NamedTempFile> {
        let output = tempfile::NamedTempFile::new()?;
        let mut file = tokio::fs::File::from_std(output.reopen()?);
        while let Some(chunk) = self.0.try_next().await? {
            file.write_all(&chunk).await?;
        }
        file.flush().await?;
        Ok(output)
    }
}
