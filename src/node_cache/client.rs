use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result};
use bytes::Bytes;
use futures_util::{TryStreamExt, stream::BoxStream};
use reqwest::header::AUTHORIZATION;

use super::slots::{SlotCheckout, SlotFile, SlotPark};
use crate::{control_plane::HostAuthorization, storage::SnapshotRef};

// With no cache pod on the node the Service drops packets instead of refusing them, so only a
// short connect timeout keeps installs from stalling.
const CONNECT_TIMEOUT: Duration = Duration::from_millis(50);
const READ_TIMEOUT: Duration = Duration::from_secs(10);
/// Activation reads code, the database and the snapshot at once, and a read that finds no idle
/// connection pays for a new one inside gVisor.
const WARM_CONNECTIONS: usize = 4;
/// The spare's private directory on the node SSD; the node cache moves `db` in and out of it.
const SLOT_MOUNT: &str = "/slot";

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

    /// Where this host keeps its database when it has a slot: the directory the local runtime
    /// names, or a GKE spare's `/slot` volume.
    pub(crate) fn slot_database(&self) -> Option<PathBuf> {
        let slot = std::env::var_os("DURABLE_ACTORS_SQLITE_SLOT_DIR")
            .map_or_else(|| PathBuf::from(SLOT_MOUNT), PathBuf::from);
        slot.is_dir().then(|| slot.join("db"))
    }

    /// Whether this node holds the actor's database closed at the snapshot with this digest.
    pub(crate) async fn is_parked(&self, digest: &str) -> Result<bool> {
        let response = self
            .connection
            .http
            .get(
                self.connection
                    .url
                    .join(&format!("sqlite/parked/{digest}"))?,
            )
            .header(AUTHORIZATION, self.authorization.header()?)
            .send()
            .await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(false);
        }
        response.error_for_status()?;
        Ok(true)
    }

    /// Moves the actor's database closed at `snapshot` into this host's slot, or leaves the slot
    /// an empty directory when the node has no such database.
    pub(crate) async fn check_out_slot(
        &self,
        snapshot: Option<&SnapshotRef>,
    ) -> Result<Option<SlotFile>> {
        let request = SlotCheckout {
            snapshot: snapshot.cloned(),
        };
        self.slot_call("checkout", &request).await
    }

    /// Hands the closed database in this host's slot to the node, recorded at `snapshot`, whose
    /// SQLite transaction is `txid`.
    pub(crate) async fn park_slot(&self, snapshot: &SnapshotRef, txid: u64) -> Result<SlotFile> {
        let request = SlotPark {
            snapshot: snapshot.clone(),
            txid,
        };
        self.slot_call("park", &request).await
    }

    async fn slot_call<T: serde::de::DeserializeOwned>(
        &self,
        action: &str,
        body: &impl serde::Serialize,
    ) -> Result<T> {
        Ok(self
            .connection
            .http
            .post(self.connection.url.join(&format!("slot/{action}"))?)
            .header(AUTHORIZATION, self.authorization.header()?)
            .json(body)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
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
