use std::{future::Future, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use tokio::net::TcpListener;

mod bases;
mod client;
mod keys;
mod restores;
mod server;
mod slots;
mod store;

pub(crate) use client::{NodeCacheClient, NodeCacheConnection};
pub(crate) use restores::{SlotRestored, SlotStorage};
pub(crate) use slots::SlotFile;

const MAX_ENTRY_BYTES: u64 = 512 * 1024 * 1024;
/// tokio reads a file through one thread-pool hop per chunk, which at `ReaderStream`'s 4 KiB
/// default took most of a transfer. Chunks larger than this are no faster and hold more memory.
const FILE_CHUNK_BYTES: usize = 256 * 1024;
/// Each indexed entry holds about 350 bytes of memory, so this default fits a 256 MiB cache pod.
const DEFAULT_MAX_ENTRIES: u64 = 500_000;
const LOCAL_CAPACITY_BYTES: u64 = 10 * 1024 * 1024 * 1024;

pub async fn serve_node_cache(shutdown: impl Future<Output = ()> + Send + 'static) -> Result<()> {
    let config = NodeCacheConfig::from_lookup(|name| std::env::var(name).ok())?;
    let store = Arc::new(
        store::DiskStore::open(
            config.directory.clone(),
            config.capacity_bytes,
            config.max_entries,
        )
        .await?,
    );
    // One client for the node's own base uploads and prepared downloads, with a transfer for each.
    let bucket = match &config.base_bucket {
        Some(name) => Some(Arc::new(
            crate::bucket::GcsBucket::for_restore(
                name,
                google_cloud_auth::credentials::Builder::default().build()?,
            )
            .await?,
        ) as Arc<dyn crate::bucket::Bucket>),
        None => None,
    };
    let slots = Arc::new(
        slots::SqliteSlots::open(
            config.directory,
            config.pods_directory.map(slots::SlotLayout::Kubelet),
            store.clone(),
            bases::BaseUploads::new(bucket.clone(), store.staging().join("bases")),
        )
        .await?,
    );
    let tokens = keys::HostTokens::load(config.verifier);
    let listener = TcpListener::bind(config.bind).await?;
    tracing::info!(bind = %config.bind, "node cache is ready");
    axum::serve(listener, server::router(store, slots, tokens, bucket))
        .with_graceful_shutdown(shutdown)
        .await
        .context("serve node cache")
}

/// The node cache inside the local runtime, which plays kubelet: each host gets a directory in
/// `slots`, deleted when the host exits, and parks its database in `directory`.
pub(crate) async fn local_router(
    directory: PathBuf,
    slots: PathBuf,
    verifier: crate::control_plane::ActorJwtVerifier,
) -> Result<axum::Router> {
    let store = Arc::new(
        store::DiskStore::open(directory.clone(), LOCAL_CAPACITY_BYTES, DEFAULT_MAX_ENTRIES)
            .await?,
    );
    let layout = Some(slots::SlotLayout::Hosts(slots));
    let uploads = bases::BaseUploads::new(None, store.staging().join("bases"));
    let slots =
        Arc::new(slots::SqliteSlots::open(directory, layout, store.clone(), uploads).await?);
    Ok(server::router(
        store,
        slots,
        keys::HostTokens::fixed(verifier),
        None,
    ))
}

struct NodeCacheConfig {
    bind: SocketAddr,
    directory: PathBuf,
    /// Kubelet's per-pod directories, on the same filesystem as `directory`; slots are off without.
    pods_directory: Option<PathBuf>,
    capacity_bytes: u64,
    max_entries: u64,
    verifier: keys::VerifierConfig,
    /// The archive bucket parked databases are uploaded to as bases; none means no uploads.
    base_bucket: Option<String>,
}

impl NodeCacheConfig {
    fn from_lookup(mut get: impl FnMut(&str) -> Option<String>) -> Result<Self> {
        let capacity_bytes = get("DURABLE_ACTORS_NODE_CACHE_CAPACITY_BYTES")
            .context("DURABLE_ACTORS_NODE_CACHE_CAPACITY_BYTES is required")?
            .parse()
            .context("DURABLE_ACTORS_NODE_CACHE_CAPACITY_BYTES must be an integer")?;
        ensure!(
            capacity_bytes > 0,
            "DURABLE_ACTORS_NODE_CACHE_CAPACITY_BYTES must be positive"
        );
        let max_entries = get("DURABLE_ACTORS_NODE_CACHE_MAX_ENTRIES")
            .map(|value| value.parse())
            .transpose()
            .context("DURABLE_ACTORS_NODE_CACHE_MAX_ENTRIES must be an integer")?
            .unwrap_or(DEFAULT_MAX_ENTRIES);
        ensure!(
            max_entries > 0,
            "DURABLE_ACTORS_NODE_CACHE_MAX_ENTRIES must be positive"
        );
        Ok(Self {
            bind: get("DURABLE_ACTORS_NODE_CACHE_BIND")
                .unwrap_or_else(|| "0.0.0.0:7103".into())
                .parse()
                .context("DURABLE_ACTORS_NODE_CACHE_BIND must be a socket address")?,
            directory: get("DURABLE_ACTORS_NODE_CACHE_DIR")
                .unwrap_or_else(|| "/cache".into())
                .into(),
            pods_directory: get("DURABLE_ACTORS_NODE_CACHE_PODS_DIR").map(PathBuf::from),
            base_bucket: get("DURABLE_ACTORS_NODE_CACHE_BASE_BUCKET")
                .filter(|name| !name.is_empty()),
            capacity_bytes,
            max_entries,
            verifier: keys::VerifierConfig {
                jwks_url: format!(
                    "{}{}",
                    get("DURABLE_ACTORS_CONTROL_PLANE_URL")
                        .context("DURABLE_ACTORS_CONTROL_PLANE_URL is required")?
                        .trim_end_matches('/'),
                    crate::control_plane::JWKS_PATH
                ),
                issuer: get("DURABLE_ACTORS_JWT_ISSUER")
                    .unwrap_or_else(|| crate::control_plane::DEFAULT_JWT_ISSUER.into()),
                audience: get("DURABLE_ACTORS_AUTHORITY_JWT_AUDIENCE")
                    .unwrap_or_else(|| crate::control_plane::DEFAULT_AUTHORITY_AUDIENCE.into()),
                max_lifetime: Duration::from_secs(
                    get("DURABLE_ACTORS_JWT_MAX_TTL_SECONDS")
                        .map(|value| value.parse())
                        .transpose()
                        .context("DURABLE_ACTORS_JWT_MAX_TTL_SECONDS must be an integer")?
                        .unwrap_or(crate::control_plane::DEFAULT_JWT_TTL_SECONDS),
                ),
            },
        })
    }
}

/// Actor state is keyed by a digest of the actor, so paths never carry customer names.
fn actor_digest(actor: &crate::actor::ActorKey) -> String {
    use aws_lc_rs::digest::{SHA256, digest};
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    URL_SAFE_NO_PAD.encode(digest(&SHA256, actor.storage_key().as_str().as_bytes()).as_ref())
}

fn valid_digest(digest: &str) -> bool {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    URL_SAFE_NO_PAD
        .decode(digest)
        .is_ok_and(|bytes| bytes.len() == 32)
}

#[cfg(test)]
#[path = "../tests/unit/node_cache.rs"]
mod tests;
