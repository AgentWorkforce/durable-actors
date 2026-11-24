use super::*;
use crate::{
    actor::ActorKey,
    control_plane::{ActorJwtIssuer, ActorJwtVerifier, ActorTokenPurpose, HostAuthorization},
    host::HostId,
};
use aws_lc_rs::{rand::SystemRandom, signature::Ed25519KeyPair};
use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use futures_util::TryStreamExt;
use tokio_util::task::AbortOnDropHandle;

#[tokio::test]
async fn hosts_share_code_through_the_node_cache() -> Result<()> {
    let issuer = issuer()?;
    let cache = NodeCache::start(&issuer).await?;
    let code = tempfile::NamedTempFile::new()?;
    std::fs::write(code.path(), b"export default 1")?;
    let digest = sha256(b"export default 1");
    let host = cache.client(&host_token(&issuer, "one")?)?;

    assert!(host.code(&digest).await?.is_none());
    host.store_code(&digest, code.path()).await?;
    let chunks: Vec<bytes::Bytes> = host.code(&digest).await?.unwrap().try_collect().await?;

    assert_eq!(chunks.concat(), b"export default 1");
    Ok(())
}

#[tokio::test]
async fn a_host_moves_databases_only_through_the_slot_its_token_names() -> Result<()> {
    let issuer = issuer()?;
    let cache = NodeCache::start(&issuer).await?;
    let (mine, other) = (
        uuid::Uuid::new_v4().to_string(),
        uuid::Uuid::new_v4().to_string(),
    );
    for slot in [&mine, &other] {
        let database = cache.slot(slot).join("db");
        std::fs::create_dir_all(&database)?;
        std::fs::write(database.join("actor.sqlite"), b"pages")?;
    }
    let actor = ActorKey {
        project_id: "default".into(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
    };
    let snapshot = crate::storage::SnapshotRef {
        object: crate::storage::snapshot_object_name(&actor, 1, &format!("{:032x}", 1))?,
        state_version: 1,
        request_id: "request-1".into(),
        digest: sha256(b"state"),
    };
    let unslotted = cache.client(&host_token(&issuer, "one")?)?;
    assert!(unslotted.park_slot(&snapshot, 1, false).await.is_err());

    let host = cache.client(&slotted_token(&issuer, "one", &mine)?)?;
    host.park_slot(&snapshot, 1, false).await?;

    assert!(!cache.slot(&mine).join("db").exists());
    assert!(cache.slot(&other).join("db/actor.sqlite").exists());
    Ok(())
}

struct NodeCache {
    url: String,
    directory: tempfile::TempDir,
    _server: AbortOnDropHandle<std::io::Result<()>>,
}

impl NodeCache {
    async fn start(issuer: &ActorJwtIssuer) -> Result<Self> {
        let directory = tempfile::tempdir()?;
        let store =
            Arc::new(store::DiskStore::open(directory.path().into(), 1 << 20, 1 << 10).await?);
        let tokens = keys::HostTokens::fixed(ActorJwtVerifier::for_scope(
            issuer.verifier_keys_json()?,
            "issuer",
            "authority",
            ActorTokenPurpose::ControlPlane,
            Duration::from_secs(1800),
        )?);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let slots = Arc::new(
            slots::SqliteSlots::open(
                directory.path().into(),
                Some(slots::SlotLayout::Kubelet(directory.path().join("pods"))),
                store.clone(),
                bases::BaseUploads::new(None, store.staging().join("bases")),
            )
            .await?,
        );
        let routes = server::router(store, slots, tokens, None);
        Ok(Self {
            url,
            directory,
            _server: AbortOnDropHandle::new(tokio::spawn(async move {
                axum::serve(listener, routes).await
            })),
        })
    }

    fn slot(&self, uid: &str) -> std::path::PathBuf {
        self.directory
            .path()
            .join("pods")
            .join(uid)
            .join("volumes/kubernetes.io~empty-dir/slot")
    }

    fn client(&self, token: &str) -> Result<NodeCacheClient> {
        Ok(NodeCacheConnection::new(&self.url)?.authorize(HostAuthorization::fixed(token)?))
    }
}

fn issuer() -> Result<ActorJwtIssuer> {
    let key = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())?;
    ActorJwtIssuer::from_base64_pkcs8(
        &STANDARD.encode(key.as_ref()),
        "test",
        "issuer",
        "authority",
        "invocation",
        Duration::from_secs(1800),
    )
}

fn host_token(issuer: &ActorJwtIssuer, actor_id: &str) -> Result<String> {
    Ok(issuer
        .issue_host(
            "org-test",
            &HostId::new("host.v3.r1.one"),
            &uuid::Uuid::new_v4().to_string(),
            "r1",
            "us-west",
            &ActorKey {
                project_id: "default".into(),
                actor_name: "Counter".into(),
                actor_id: actor_id.into(),
            },
        )?
        .token)
}

fn slotted_token(issuer: &ActorJwtIssuer, actor_id: &str, slot: &str) -> Result<String> {
    Ok(issuer
        .issue_host_in_slot(
            "org-test",
            &HostId::new("host.v3.r1.one"),
            &uuid::Uuid::new_v4().to_string(),
            "r1",
            "us-west",
            &ActorKey {
                project_id: "default".into(),
                actor_name: "Counter".into(),
                actor_id: actor_id.into(),
            },
            Some(slot),
        )?
        .token)
}

fn sha256(bytes: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, bytes).as_ref())
}
