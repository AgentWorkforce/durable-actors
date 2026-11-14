//! One suite for every [`Bucket`] backend: the ownership, fencing and immutable-state guarantees
//! the runtime relies on. FileBucket always runs. S3 runs when `DURABLE_ACTORS_TEST_S3_BUCKET` is
//! set (with `DURABLE_ACTORS_TEST_S3_ENDPOINT` for MinIO/LocalStack and the usual `AWS_*`
//! credentials), and GCS when `DURABLE_ACTORS_TEST_GCS_BUCKET` is set (Application Default
//! Credentials). Each run writes under a fresh prefix.

use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;

use super::{
    Bucket, BucketSnapshots, FileBucket, GcsBucket, RuntimeStorage, S3Bucket, S3Endpoint,
    SnapshotStore,
};
use crate::{actor::ActorKey, clock::SystemClock, host::HostId, host_leases::HostLeaseRequest};

struct Backend {
    name: &'static str,
    /// Two independent clients of one bucket, like two hosts or a host and the control plane.
    clients: [Arc<dyn Bucket>; 2],
    prefix: String,
    _directory: Option<tempfile::TempDir>,
}

impl Backend {
    fn key(&self, name: &str) -> String {
        format!("{}{name}", self.prefix)
    }
}

async fn backends() -> Result<Vec<Backend>> {
    let prefix = format!("conformance/{}/", uuid::Uuid::new_v4().simple());
    let directory = tempfile::tempdir()?;
    let file: Arc<dyn Bucket> = Arc::new(FileBucket::new(directory.path().into())?);
    let mut backends = vec![Backend {
        name: "file",
        clients: [file.clone(), file],
        prefix: prefix.clone(),
        _directory: Some(directory),
    }];
    if let Ok(bucket) = std::env::var("DURABLE_ACTORS_TEST_S3_BUCKET") {
        let endpoint = S3Endpoint {
            region: std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into()),
            endpoint: std::env::var("DURABLE_ACTORS_TEST_S3_ENDPOINT").ok(),
            force_path_style: std::env::var("DURABLE_ACTORS_TEST_S3_ENDPOINT").is_ok(),
        };
        backends.push(Backend {
            name: "s3",
            clients: [
                Arc::new(S3Bucket::from_environment(&bucket, &endpoint).await?),
                Arc::new(S3Bucket::from_environment(&bucket, &endpoint).await?),
            ],
            prefix: prefix.clone(),
            _directory: None,
        });
    }
    if let Ok(bucket) = std::env::var("DURABLE_ACTORS_TEST_GCS_BUCKET") {
        backends.push(Backend {
            name: "gcs",
            clients: [gcs(&bucket).await?, gcs(&bucket).await?],
            prefix,
            _directory: None,
        });
    }
    Ok(backends)
}

/// Application Default Credentials, or a short-lived token from `DURABLE_ACTORS_TEST_GCS_TOKEN_FILE`
/// (for example `gcloud auth print-access-token`), so runs need no long-lived key.
async fn gcs(bucket: &str) -> Result<Arc<dyn Bucket>> {
    Ok(match std::env::var("DURABLE_ACTORS_TEST_GCS_TOKEN_FILE") {
        Ok(path) => {
            let token = std::fs::read_to_string(path)?.trim().to_owned();
            Arc::new(GcsBucket::with_credentials(bucket, BearerToken(token).into()).await?)
        }
        Err(_) => Arc::new(GcsBucket::new(bucket).await?),
    })
}

#[derive(Debug)]
struct BearerToken(String);

impl google_cloud_auth::credentials::CredentialsProvider for BearerToken {
    async fn headers(
        &self,
        _: axum::http::Extensions,
    ) -> std::result::Result<
        google_cloud_auth::credentials::CacheableResource<axum::http::HeaderMap>,
        google_cloud_auth::errors::CredentialsError,
    > {
        let mut value: axum::http::HeaderValue =
            format!("Bearer {}", self.0).parse().map_err(|_| {
                google_cloud_auth::errors::CredentialsError::from_msg(false, "invalid test token")
            })?;
        value.set_sensitive(true);
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(axum::http::header::AUTHORIZATION, value);
        Ok(google_cloud_auth::credentials::CacheableResource::New {
            entity_tag: google_cloud_auth::credentials::EntityTag::new(),
            data: headers,
        })
    }

    async fn universe_domain(&self) -> Option<String> {
        Some("googleapis.com".into())
    }
}

fn unique(label: &str) -> Bytes {
    Bytes::from(format!("{label}-{}", uuid::Uuid::new_v4()))
}

#[tokio::test]
async fn reads_report_absence_and_the_written_generation() -> Result<()> {
    for backend in backends().await? {
        let [bucket, other] = &backend.clients;
        let key = backend.key("owner.json");
        assert!(bucket.get(&key).await?.is_none(), "{}", backend.name);
        let bytes = unique("first");
        assert!(
            bucket.compare_and_swap(&key, None, bytes.clone()).await?,
            "{}",
            backend.name
        );
        let read = other.get(&key).await?.expect("written object");
        assert_eq!(read.bytes, bytes, "{}", backend.name);
        assert!(read.generation > 0, "{}", backend.name);
        assert_eq!(
            bucket.get(&key).await?.unwrap().generation,
            read.generation,
            "{}",
            backend.name
        );
    }
    Ok(())
}

#[tokio::test]
async fn concurrent_creates_have_exactly_one_winner() -> Result<()> {
    for backend in backends().await? {
        let key = backend.key("create.json");
        let attempts: Vec<_> = (0..12)
            .map(|index| {
                let bucket = backend.clients[index % 2].clone();
                let key = key.clone();
                let bytes = unique(&format!("create-{index}"));
                tokio::spawn(async move {
                    let won = bucket.compare_and_swap(&key, None, bytes.clone()).await?;
                    anyhow::Ok((won, bytes))
                })
            })
            .collect();
        let mut winners = Vec::new();
        for attempt in attempts {
            let (won, bytes) = attempt.await??;
            if won {
                winners.push(bytes);
            }
        }
        assert_eq!(winners.len(), 1, "{}", backend.name);
        assert_eq!(
            backend.clients[0].get(&key).await?.unwrap().bytes,
            winners[0],
            "{}",
            backend.name
        );
    }
    Ok(())
}

#[tokio::test]
async fn concurrent_swaps_from_one_generation_have_exactly_one_winner() -> Result<()> {
    for backend in backends().await? {
        let key = backend.key("swap.json");
        assert!(
            backend.clients[0]
                .compare_and_swap(&key, None, unique("base"))
                .await?
        );
        let base = backend.clients[1].get(&key).await?.unwrap().generation;
        // Prime both clients' view of the base, as a host does before claiming.
        backend.clients[0].get(&key).await?;
        let attempts: Vec<_> = (0..12)
            .map(|index| {
                let bucket = backend.clients[index % 2].clone();
                let key = key.clone();
                let bytes = unique(&format!("swap-{index}"));
                tokio::spawn(async move {
                    let won = bucket
                        .compare_and_swap(&key, Some(base), bytes.clone())
                        .await?;
                    anyhow::Ok((won, bytes))
                })
            })
            .collect();
        let mut winners = Vec::new();
        for attempt in attempts {
            let (won, bytes) = attempt.await??;
            if won {
                winners.push(bytes);
            }
        }
        assert_eq!(winners.len(), 1, "{}", backend.name);
        let current = backend.clients[0].get(&key).await?.unwrap();
        assert_eq!(current.bytes, winners[0], "{}", backend.name);
        assert_ne!(current.generation, base, "{}", backend.name);
    }
    Ok(())
}

#[tokio::test]
async fn stale_and_mismatched_preconditions_lose() -> Result<()> {
    for backend in backends().await? {
        let [writer, stale] = &backend.clients;
        let key = backend.key("stale.json");
        assert!(
            !writer
                .compare_and_swap(&key, Some(1), unique("absent"))
                .await?,
            "{}: swap of a missing object",
            backend.name
        );
        assert!(writer.compare_and_swap(&key, None, unique("one")).await?);
        assert!(
            !writer.compare_and_swap(&key, None, unique("again")).await?,
            "{}: create over an object",
            backend.name
        );
        let first = stale.get(&key).await?.unwrap().generation;
        assert!(
            writer
                .compare_and_swap(&key, Some(first), unique("two"))
                .await?
        );
        let second = writer.get(&key).await?.unwrap().generation;
        assert_ne!(first, second, "{}", backend.name);
        // `stale` still remembers `first`; its swap must fail without reading again.
        assert!(
            !stale
                .compare_and_swap(&key, Some(first), unique("late"))
                .await?,
            "{}: stale swap",
            backend.name
        );
        assert!(
            stale
                .compare_and_swap(&key, Some(second), unique("three"))
                .await?,
            "{}",
            backend.name
        );
    }
    Ok(())
}

#[tokio::test]
async fn ambiguous_replies_reconcile_by_content() -> Result<()> {
    for backend in backends().await? {
        let key = backend.key("ambiguous.json");
        let bytes = unique("once");
        assert!(super::replace(backend.clients[0].as_ref(), &key, None, bytes.clone()).await?);
        // A retried create of the same bytes reports success; different bytes do not.
        assert!(
            super::replace(backend.clients[1].as_ref(), &key, None, bytes).await?,
            "{}",
            backend.name
        );
        assert!(
            !super::replace(backend.clients[1].as_ref(), &key, None, unique("other")).await?,
            "{}",
            backend.name
        );
    }
    Ok(())
}

#[tokio::test]
async fn ranges_and_prefix_listing() -> Result<()> {
    for backend in backends().await? {
        let bucket = &backend.clients[0];
        let key = backend.key("range/object.json");
        assert!(
            bucket
                .compare_and_swap(&key, None, Bytes::from_static(b"0123456789"))
                .await?
        );
        assert_eq!(
            bucket.range(&key, 2, 5).await?.unwrap().bytes.as_ref(),
            b"23456",
            "{}",
            backend.name
        );
        assert!(
            bucket.range(&key, 8, 5).await.is_err(),
            "{}: short range",
            backend.name
        );
        assert!(
            bucket
                .range(&backend.key("range/missing"), 0, 1)
                .await?
                .is_none()
        );

        let count = if backend.name == "file" { 50 } else { 1005 };
        let writes: Vec<_> = (0..count)
            .map(|index| {
                let bucket = bucket.clone();
                let key = backend.key(&format!("list/{index:05}.json"));
                tokio::spawn(async move {
                    bucket
                        .compare_and_swap(&key, None, Bytes::from_static(b"x"))
                        .await
                })
            })
            .collect();
        for chunk in writes {
            assert!(chunk.await??);
        }
        let mut listed = backend.clients[1].list(&backend.key("list/")).await?;
        listed.sort();
        assert_eq!(listed.len(), count, "{}: listing pages", backend.name);
        assert_eq!(listed[0], backend.key("list/00000.json"));
        assert!(
            backend.clients[1]
                .list(&backend.key("nothing/"))
                .await?
                .is_empty()
        );
    }
    Ok(())
}

#[tokio::test]
async fn immutable_snapshots_cannot_be_replaced() -> Result<()> {
    for backend in backends().await? {
        let snapshots = BucketSnapshots(backend.clients[0].clone());
        let object = backend.key("snapshots/1.json");
        let bytes = unique("state");
        snapshots.put(&object, bytes.clone()).await?;
        snapshots.put(&object, bytes.clone()).await?;
        assert!(
            snapshots.put(&object, unique("forged")).await.is_err(),
            "{}",
            backend.name
        );
        assert_eq!(
            BucketSnapshots(backend.clients[1].clone())
                .get(&object)
                .await?,
            Some(bytes),
            "{}",
            backend.name
        );
    }
    Ok(())
}

fn lease(session: &str) -> HostLeaseRequest {
    HostLeaseRequest {
        id: HostId::new(format!("host-{session}")),
        session_id: session.into(),
        route: format!("http://{session}"),
        duration_ms: 30_000,
    }
}

/// Two hosts, each with its own client, race to activate one actor; the loser is fenced out.
#[tokio::test]
async fn ownership_fences_a_second_host() -> Result<()> {
    for backend in backends().await? {
        let actor = ActorKey {
            project_id: format!(
                "conformance{}",
                &uuid::Uuid::new_v4().simple().to_string()[..12]
            ),
            actor_name: "Counter".into(),
            actor_id: "one".into(),
        };
        let [left, right] = &backend.clients;
        let first = RuntimeStorage::new(left.clone(), Arc::new(SystemClock))?;
        let second = RuntimeStorage::new(right.clone(), Arc::new(SystemClock))?;
        let (left_lease, right_lease) = (lease("left"), lease("right"));
        let (a, b) = tokio::join!(
            first.register_activation(&actor, &left_lease, "north-america-west", true, None),
            second.register_activation(&actor, &right_lease, "north-america-west", true, None),
        );
        assert_ne!(
            a.is_ok(),
            b.is_ok(),
            "{}: exactly one host activates",
            backend.name
        );
        let (winner, winner_lease, loser, loser_lease) = if a.is_ok() {
            (&first, lease("left"), &second, lease("right"))
        } else {
            (&second, lease("right"), &first, lease("left"))
        };
        assert!(
            loser
                .register_activation(&actor, &loser_lease, "north-america-west", false, None)
                .await
                .is_err(),
            "{}: a live lease fences takeover",
            backend.name
        );
        assert!(
            loser.renew_activation(&actor, &loser_lease).await.is_err(),
            "{}",
            backend.name
        );
        winner.renew_activation(&actor, &winner_lease).await?;
        winner
            .release_activation(&actor, &winner_lease.id, &winner_lease.session_id)
            .await?;
        let takeover = loser
            .register_activation(&actor, &loser_lease, "north-america-west", false, None)
            .await?;
        assert_eq!(takeover.placement.owner_epoch, 2, "{}", backend.name);
        assert!(
            winner
                .renew_activation(&actor, &winner_lease)
                .await
                .is_err(),
            "{}: the old owner is fenced",
            backend.name
        );
    }
    Ok(())
}

/// The checkpoint copy's write: an exact generation, never moving an object backwards, and leaving
/// the object ready for the runtime's next compare-and-swap.
#[tokio::test]
async fn s3_mirror_writes_exact_generations_and_never_regresses() -> Result<()> {
    use super::checkpoint::Mirror;
    let Ok(bucket) = std::env::var("DURABLE_ACTORS_TEST_S3_BUCKET") else {
        return Ok(());
    };
    let endpoint = S3Endpoint {
        region: std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into()),
        endpoint: std::env::var("DURABLE_ACTORS_TEST_S3_ENDPOINT").ok(),
        force_path_style: std::env::var("DURABLE_ACTORS_TEST_S3_ENDPOINT").is_ok(),
    };
    let s3 = S3Bucket::from_environment(&bucket, &endpoint).await?;
    let key = format!("conformance/{}/mirrored", uuid::Uuid::new_v4().simple());
    s3.mirror(&key, 3, Bytes::from_static(b"three")).await?;
    assert_eq!(s3.get(&key).await?.unwrap().generation, 3);
    s3.mirror(&key, 2, Bytes::from_static(b"two")).await?;
    let object = s3.get(&key).await?.unwrap();
    assert_eq!((object.generation, &object.bytes[..]), (3, &b"three"[..]));
    s3.mirror(&key, 5, Bytes::from_static(b"five")).await?;
    assert!(s3.compare_and_swap(&key, Some(5), unique("six")).await?);
    assert_eq!(s3.get(&key).await?.unwrap().generation, 6);
    Ok(())
}
