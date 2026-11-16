use std::sync::Arc;

use anyhow::{Result, ensure};
use aws_lc_rs::digest::{Context as Digest, SHA256, digest};
use axum::{
    Router,
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::TryStreamExt;
use tokio::io::AsyncWriteExt;
use tokio_util::io::ReaderStream;

use super::{FILE_CHUNK_BYTES, MAX_ENTRY_BYTES, keys::HostTokens, store::DiskStore, valid_digest};
use crate::{actor::ActorKey, control_plane::ActorPrincipal};

#[derive(Clone)]
struct NodeCache {
    store: Arc<DiskStore>,
    tokens: Arc<HostTokens>,
}

pub(super) fn router(store: Arc<DiskStore>, tokens: Arc<HostTokens>) -> Router {
    Router::new()
        .route("/healthz", get(|| async { StatusCode::NO_CONTENT }))
        .route("/code/{digest}", get(read_code).put(write_code))
        .route("/sqlite/{digest}", get(read_database).put(write_database))
        .route("/snapshot/{digest}", get(read_snapshot).put(write_snapshot))
        .with_state(NodeCache { store, tokens })
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
        Ok(_) => {
            cache
                .receive(&code_key(&digest), body, Some(&digest), Keep::Every)
                .await
        }
        Err(status) => status,
    }
}

async fn read_database(
    State(cache): State<NodeCache>,
    Path(digest): Path<String>,
    headers: HeaderMap,
) -> Response {
    match cache.authorize(&headers, &digest) {
        Ok(host) => {
            cache
                .serve(&actor_key("sqlite", &host.actor, &digest))
                .await
        }
        Err(status) => status.into_response(),
    }
}

async fn write_database(
    State(cache): State<NodeCache>,
    Path(digest): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> StatusCode {
    match cache.authorize(&headers, &digest) {
        Ok(host) => {
            cache
                .receive(
                    &actor_key("sqlite", &host.actor, &digest),
                    body,
                    None,
                    Keep::Latest,
                )
                .await
        }
        Err(status) => status,
    }
}

async fn read_snapshot(
    State(cache): State<NodeCache>,
    Path(digest): Path<String>,
    headers: HeaderMap,
) -> Response {
    match cache.authorize(&headers, &digest) {
        Ok(host) => {
            cache
                .serve(&actor_key("snapshot", &host.actor, &digest))
                .await
        }
        Err(status) => status.into_response(),
    }
}

async fn write_snapshot(
    State(cache): State<NodeCache>,
    Path(digest): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> StatusCode {
    match cache.authorize(&headers, &digest) {
        Ok(host) => {
            let key = actor_key("snapshot", &host.actor, &digest);
            cache.receive(&key, body, Some(&digest), Keep::Latest).await
        }
        Err(status) => status,
    }
}

impl NodeCache {
    fn authorize(&self, headers: &HeaderMap, digest: &str) -> Result<ActorPrincipal, StatusCode> {
        let authorization = headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .ok_or(StatusCode::UNAUTHORIZED)?;
        let host = self
            .tokens
            .verify(authorization)
            .map_err(|_| StatusCode::UNAUTHORIZED)?;
        if !valid_digest(digest) {
            return Err(StatusCode::BAD_REQUEST);
        }
        Ok(host)
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

    async fn receive(&self, key: &str, body: Body, digest: Option<&str>, keep: Keep) -> StatusCode {
        match self.store_body(key, body, digest, keep).await {
            Ok(()) => StatusCode::CREATED,
            Err(error) => {
                tracing::warn!(error = %format!("{error:#}"), "rejected node cache upload");
                StatusCode::BAD_REQUEST
            }
        }
    }

    async fn store_body(
        &self,
        key: &str,
        body: Body,
        digest: Option<&str>,
        keep: Keep,
    ) -> Result<()> {
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
            digest.is_none_or(|digest| URL_SAFE_NO_PAD.encode(hash.finish().as_ref()) == digest),
            "content digest mismatch"
        );
        // Entries outlive this process, so one must be on disk before its name is.
        file.sync_all().await?;
        drop(file);
        match keep {
            Keep::Every => self.store.insert(key, staged, size).await,
            Keep::Latest => self.store.insert_latest(key, staged, size).await,
        }
    }
}

/// Code is shared by content digest; an actor's database and snapshot are only worth keeping at
/// the version its next activation will ask for.
#[derive(Clone, Copy)]
enum Keep {
    Every,
    Latest,
}

fn code_key(digest: &str) -> String {
    format!("code/{digest}")
}

/// Actor state is keyed by the actor in the host token, so a host can neither read nor replace
/// another actor's database or snapshot. Each actor's versions share a directory, so a new
/// version can drop the ones it supersedes.
fn actor_key(kind: &str, actor: &ActorKey, snapshot: &str) -> String {
    let actor = digest(&SHA256, actor.storage_key().as_str().as_bytes());
    format!(
        "{kind}/{}/{snapshot}",
        URL_SAFE_NO_PAD.encode(actor.as_ref())
    )
}
