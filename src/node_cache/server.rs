use std::{sync::Arc, time::Instant};

use anyhow::{Result, ensure};
use aws_lc_rs::digest::{Context as Digest, SHA256};
use axum::{
    Router,
    body::Body,
    extract::{Json, Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::TryStreamExt;
use tokio::io::AsyncWriteExt;
use tokio_util::io::ReaderStream;

use super::{
    FILE_CHUNK_BYTES, MAX_ENTRY_BYTES,
    keys::HostTokens,
    slots::{SlotCheckout, SlotPark, SqliteSlots},
    store::DiskStore,
    valid_digest,
};
use crate::control_plane::ActorPrincipal;

#[derive(Clone)]
struct NodeCache {
    store: Arc<DiskStore>,
    slots: Arc<SqliteSlots>,
    tokens: Arc<HostTokens>,
}

pub(super) fn router(
    store: Arc<DiskStore>,
    slots: Arc<SqliteSlots>,
    tokens: Arc<HostTokens>,
) -> Router {
    Router::new()
        .route("/healthz", get(|| async { StatusCode::NO_CONTENT }))
        .route("/code/{digest}", get(read_code).put(write_code))
        .route("/sqlite/parked/{digest}", get(is_parked))
        .route("/slot/checkout", post(check_out))
        .route("/slot/park", post(park))
        .with_state(NodeCache {
            store,
            slots,
            tokens,
        })
}

async fn read_code(
    State(cache): State<NodeCache>,
    Path(digest): Path<String>,
    headers: HeaderMap,
) -> Response {
    match cache.authorize(&headers, &digest) {
        Ok(_) => cache.serve(&code_key(&digest)).await,
        Err(status) => status.into_response(),
    }
}

async fn write_code(
    State(cache): State<NodeCache>,
    Path(digest): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> StatusCode {
    match cache.authorize(&headers, &digest) {
        Ok(_) => cache.receive(&code_key(&digest), body, &digest).await,
        Err(status) => status,
    }
}

async fn is_parked(
    State(cache): State<NodeCache>,
    Path(digest): Path<String>,
    headers: HeaderMap,
) -> StatusCode {
    match cache.authorize(&headers, &digest) {
        Ok(host) if cache.slots.is_parked(&host.actor, &digest).await => StatusCode::NO_CONTENT,
        Ok(_) => StatusCode::NOT_FOUND,
        Err(status) => status,
    }
}

async fn check_out(
    State(cache): State<NodeCache>,
    headers: HeaderMap,
    Json(request): Json<SlotCheckout>,
) -> Response {
    let (host, slot) = match cache.slot_holder(&headers) {
        Ok(holder) => holder,
        Err(status) => return status.into_response(),
    };
    let started = Instant::now();
    match cache
        .slots
        .check_out(&host.actor, &slot, request.snapshot)
        .await
    {
        Ok(reused) => {
            tracing::info!(
                event = "sqlite_slot_checkout",
                host_id = %host.host_id,
                reused = reused.is_some(),
                bytes = reused.as_ref().map(|file| file.bytes),
                device = reused.as_ref().map(|file| file.device),
                inode = reused.as_ref().map(|file| file.inode),
                duration_ms = started.elapsed().as_secs_f64() * 1000.0
            );
            Json(reused).into_response()
        }
        Err(error) => {
            tracing::warn!(error = %format!("{error:#}"), host_id = %host.host_id, "slot checkout failed");
            StatusCode::CONFLICT.into_response()
        }
    }
}

async fn park(
    State(cache): State<NodeCache>,
    headers: HeaderMap,
    Json(request): Json<SlotPark>,
) -> Response {
    let (host, slot) = match cache.slot_holder(&headers) {
        Ok(holder) => holder,
        Err(status) => return status.into_response(),
    };
    let started = Instant::now();
    match cache
        .slots
        .park(&host.actor, &slot, request.snapshot, request.txid)
        .await
    {
        Ok(file) => {
            tracing::info!(
                event = "sqlite_slot_park",
                host_id = %host.host_id,
                bytes = file.bytes,
                device = file.device,
                inode = file.inode,
                duration_ms = started.elapsed().as_secs_f64() * 1000.0
            );
            Json(file).into_response()
        }
        Err(error) => {
            tracing::warn!(error = %format!("{error:#}"), host_id = %host.host_id, "slot park failed");
            StatusCode::CONFLICT.into_response()
        }
    }
}

impl NodeCache {
    fn authorize(&self, headers: &HeaderMap, digest: &str) -> Result<ActorPrincipal, StatusCode> {
        let host = self.authenticate(headers)?;
        if !valid_digest(digest) {
            return Err(StatusCode::BAD_REQUEST);
        }
        Ok(host)
    }

    /// The host and the slot its token names: the slot comes from the control plane's signature,
    /// never from the request, so a host can only move databases through its own sandbox.
    fn slot_holder(&self, headers: &HeaderMap) -> Result<(ActorPrincipal, String), StatusCode> {
        let host = self.authenticate(headers)?;
        let slot = self
            .slots
            .slot_of(&host)
            .ok_or(StatusCode::FORBIDDEN)?
            .to_owned();
        Ok((host, slot))
    }

    fn authenticate(&self, headers: &HeaderMap) -> Result<ActorPrincipal, StatusCode> {
        let authorization = headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .ok_or(StatusCode::UNAUTHORIZED)?;
        self.tokens
            .verify(authorization)
            .map_err(|_| StatusCode::UNAUTHORIZED)
    }

    async fn serve(&self, key: &str) -> Response {
        match self.store.get(key).await {
            Some((file, size)) => (
                [(header::CONTENT_LENGTH, size)],
                Body::from_stream(ReaderStream::with_capacity(file, FILE_CHUNK_BYTES)),
            )
                .into_response(),
            None => StatusCode::NOT_FOUND.into_response(),
        }
    }

    async fn receive(&self, key: &str, body: Body, digest: &str) -> StatusCode {
        match self.store_body(key, body, digest).await {
            Ok(()) => StatusCode::CREATED,
            Err(error) => {
                tracing::warn!(error = %format!("{error:#}"), "rejected node cache upload");
                StatusCode::BAD_REQUEST
            }
        }
    }

    async fn store_body(&self, key: &str, body: Body, digest: &str) -> Result<()> {
        let staged = self.store.stage()?;
        let mut file = tokio::fs::File::from_std(staged.reopen()?);
        let mut hash = Digest::new(&SHA256);
        let mut size = 0u64;
        let mut chunks = body.into_data_stream();
        while let Some(chunk) = chunks.try_next().await? {
            size += chunk.len() as u64;
            ensure!(size <= MAX_ENTRY_BYTES, "cache entry is too large");
            hash.update(&chunk);
            file.write_all(&chunk).await?;
        }
        file.flush().await?;
        ensure!(
            URL_SAFE_NO_PAD.encode(hash.finish().as_ref()) == digest,
            "content digest mismatch"
        );
        // Entries outlive this process, so one must be on disk before its name is.
        file.sync_all().await?;
        drop(file);
        self.store.insert(key, staged, size).await
    }
}

fn code_key(digest: &str) -> String {
    format!("code/{digest}")
}
