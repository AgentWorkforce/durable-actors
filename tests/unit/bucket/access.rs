use super::*;
use crate::clock::{Clock, SystemClock};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

fn actor() -> crate::actor::ActorKey {
    crate::actor::ActorKey {
        project_id: "tenant".into(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
    }
}

#[test]
fn credentials_are_scoped_to_one_actor_and_its_deployed_code() -> Result<()> {
    let actor = actor();
    let local = super::super::SnapshotStoreConfig::Local;
    let mut code = crate::bundle::ActorBundle {
        bucket: "code".into(),
        files: vec![crate::bundle::BundleFile {
            path: "actors.mjs".into(),
            object: "durable-actors/v3/artifacts/12345678-1234-4234-8234-123456789012/actors.mjs"
                .into(),
            generation: 1,
            sha256: "unused".into(),
        }],
    };
    let scoped = scopes("authority", &local, Some("code"), &actor, Some(&code))?;
    assert_eq!(
        scoped,
        Scopes {
            ownership: vec![
                rule(
                    "authority",
                    crate::storage_paths::owner(&actor.storage_key())?,
                    Permissions::OBJECT_USER
                ),
                rule(
                    "code",
                    "durable-actors/v3/artifacts/12345678-1234-4234-8234-123456789012/".into(),
                    Permissions::OBJECT_VIEWER
                ),
            ],
            state: vec![rule(
                "authority",
                crate::storage_paths::snapshots(&actor)?,
                Permissions::OBJECT_VIEWER | Permissions::OBJECT_CREATOR
            )],
        }
    );
    for index in 0..1000 {
        let path = format!("modules/{index}.mjs");
        code.files.push(crate::bundle::BundleFile {
            object: format!(
                "durable-actors/v3/artifacts/12345678-1234-4234-8234-123456789012/{path}"
            ),
            path,
            generation: 1,
            sha256: "unused".into(),
        });
    }
    assert_eq!(
        scopes("authority", &local, Some("code"), &actor, Some(&code))?,
        scoped,
        "the boundary does not grow with the bundle"
    );
    code.files[1].object =
        "durable-actors/v3/artifacts/87654321-1234-4234-8234-123456789012/modules/0.mjs".into();
    assert!(scopes("authority", &local, Some("code"), &actor, Some(&code)).is_err());
    code.files.truncate(1);
    let mut other = actor.clone();
    other.project_id = "other".into();
    assert_ne!(
        scoped,
        scopes("authority", &local, Some("code"), &other, Some(&code))?
    );
    assert!(
        scopes(
            "authority",
            &local,
            Some("different-bucket"),
            &actor,
            Some(&code)
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn log_credentials_cover_only_this_actors_logs_and_archive() -> Result<()> {
    let actor = actor();
    let config: super::super::SnapshotStoreConfig = serde_json::from_value(
        serde_json::json!({"type":"rapid","archive_bucket":"archive-test","buckets":[
            {"bucket":"rapid-test-a","zone":"us-west4-a"},{"bucket":"rapid-test-b","zone":"us-west4-b"}
        ]}),
    )?;
    let archive = super::super::rapid::object_name(&crate::storage_paths::snapshots(&actor)?)?;
    let logs = archive.replacen("snapshots-", "logs-", 1);
    assert_eq!(
        scopes("authority", &config, None, &actor, None)?.state,
        [
            rule(
                "archive-test",
                archive,
                Permissions::OBJECT_VIEWER | Permissions::OBJECT_CREATOR
            ),
            rule("rapid-test-a", logs.clone(), Permissions::OBJECT_USER),
            rule("rapid-test-b", logs, Permissions::OBJECT_USER),
        ]
    );
    Ok(())
}

#[test]
fn throttled_grants_back_off_longer_than_transient_failures() {
    let delay = Some(Duration::from_millis(250));
    assert_eq!(
        throttled_delay(ErrorKind::RateLimited, delay),
        Some(Duration::from_millis(1250))
    );
    assert_eq!(throttled_delay(ErrorKind::Unexpected, delay), delay);
    assert_eq!(throttled_delay(ErrorKind::RateLimited, None), None);
}

#[tokio::test]
async fn every_request_mints_its_own_token() -> Result<()> {
    struct Numbered(AtomicUsize);
    #[async_trait]
    impl StorageTokenSource for Numbered {
        async fn issue(&self, _: &[Rule]) -> Result<StorageToken> {
            let n = self.0.fetch_add(1, Ordering::SeqCst);
            Ok(StorageToken {
                access_token: format!("token-{n}"),
                expires_at_ms: 0,
                aws: None,
            })
        }
    }
    let access = RuntimeAccess {
        location: BucketLocation::Gcs {
            bucket: "authority".into(),
            artifact_bucket: "code".into(),
        },
        token_source: Some(Box::new(Numbered(Default::default()))),
        persistence: super::super::SnapshotStoreConfig::Local,
    };
    let first = access.issue(&actor(), None).await?.context("token")?;
    let second = access.issue(&actor(), None).await?.context("token")?;
    assert_ne!(first.ownership.access_token, first.state.access_token);
    assert_ne!(first.ownership.access_token, second.ownership.access_token);
    Ok(())
}

/// A Tink keyset holding one AES-256-GCM key, as STS returns in `access_boundary_session_key`.
const SESSION_KEY: &str = "CCoSYApYCjB0eXBlLmdvb2dsZWFwaXMuY29tL2dvb2dsZS5jcnlwdG8udGluay5BZXNHY21LZXkSIhogBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcYARABGCogAQ==";

#[derive(Debug)]
struct Sts(Arc<AtomicUsize>);

impl reqsign_core::HttpSend for Sts {
    async fn http_send(
        &self,
        request: http::Request<bytes::Bytes>,
    ) -> reqsign_core::Result<http::Response<bytes::Bytes>> {
        assert_eq!(request.uri(), "https://sts.googleapis.com/v1/token");
        let call = self.0.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(20)).await;
        let body = serde_json::json!({
            "access_token": format!("intermediary-{call}"),
            "issued_token_type": "urn:ietf:params:oauth:token-type:access_boundary_intermediary_token",
            "token_type": "Bearer",
            "expires_in": 3600,
            "access_boundary_session_key": SESSION_KEY,
        });
        Ok(http::Response::new(body.to_string().into()))
    }
}

#[tokio::test]
async fn concurrent_launches_share_one_intermediary_exchange() -> Result<()> {
    let exchanges = Arc::new(AtomicUsize::new(0));
    let tokens = Arc::new(ClientSideTokens::new(
        reqsign_core::Context::new().with_http_send(Sts(exchanges.clone())),
        reqsign_google::TokenCredentialProvider::new("source")
            .with_expires_at(reqsign_core::time::Timestamp::now() + Duration::from_secs(3600)),
    ));
    let rules = scopes(
        "authority",
        &super::super::SnapshotStoreConfig::Local,
        None,
        &actor(),
        None,
    )?
    .ownership;
    let issued = futures_util::future::try_join_all((0..50).map(|_| {
        let tokens = tokens.clone();
        let rules = rules.clone();
        async move { tokens.issue(&rules).await }
    }))
    .await?;
    assert_eq!(exchanges.load(Ordering::SeqCst), 1);
    assert!(
        issued
            .iter()
            .all(|token| token.access_token.starts_with("intermediary-0."))
    );
    assert_ne!(
        issued[0].access_token, issued[1].access_token,
        "each token gets its own nonce"
    );
    assert!(issued[0].expires_at_ms > SystemClock.now_ms()? + 59 * 60 * 1000);
    Ok(())
}

#[tokio::test]
async fn each_token_fits_the_boundary_limit_at_the_longest_identifiers() -> Result<()> {
    let tokens = ClientSideTokens::new(
        reqsign_core::Context::new().with_http_send(Sts(Default::default())),
        reqsign_google::TokenCredentialProvider::new("source")
            .with_expires_at(reqsign_core::time::Timestamp::now() + Duration::from_secs(3600)),
    );
    let actor = crate::actor::ActorKey {
        project_id: "p".repeat(64),
        actor_name: "n".repeat(48),
        actor_id: "i".repeat(128),
    };
    let bucket = |name: &str| format!("{name}{}", "x".repeat(63 - name.len()));
    let code = crate::bundle::ActorBundle {
        bucket: bucket("code"),
        files: vec![crate::bundle::BundleFile {
            path: "actors.mjs".into(),
            object: "durable-actors/v3/artifacts/12345678-1234-4234-8234-123456789012/actors.mjs"
                .into(),
            generation: 1,
            sha256: "unused".into(),
        }],
    };
    let rapid = serde_json::from_value(serde_json::json!({"type":"rapid",
        "archive_bucket": bucket("archive"), "buckets":[
        {"bucket": bucket("rapid-a"), "zone":"us-west4-a"},
        {"bucket": bucket("rapid-b"), "zone":"us-west4-b"}]}))?;
    for persistence in [super::super::SnapshotStoreConfig::Local, rapid] {
        let scopes = scopes(
            &bucket("authority"),
            &persistence,
            Some(&code.bucket),
            &actor,
            Some(&code),
        )?;
        tokens.issue(&scopes.ownership).await?;
        tokens.issue(&scopes.state).await?;
        let combined = [scopes.ownership, scopes.state].concat();
        let error = tokens.issue(&combined).await.unwrap_err();
        assert!(format!("{error:#}").contains("exceeds the size limit"));
    }
    Ok(())
}

#[test]
fn checkpoint_rules_mirror_the_primary_prefixes_on_the_regional_bucket() -> Result<()> {
    let actor = actor();
    let local = super::super::SnapshotStoreConfig::Local;
    let express = "actors--usw2-az1--x-s3";
    let base = scopes(express, &local, Some("code"), &actor, None)?;
    let scoped = with_checkpoint(base.clone(), "checkpoint", &local, &actor)?;
    let owner = crate::storage_paths::owner(&actor.storage_key())?;
    let snapshots = crate::storage_paths::snapshots(&actor)?;
    assert_eq!(
        scoped.ownership,
        vec![
            rule(express, owner.clone(), Permissions::OBJECT_USER),
            rule("checkpoint", owner, Permissions::OBJECT_USER),
        ]
    );
    assert_eq!(
        scoped.state,
        vec![
            rule(
                express,
                snapshots.clone(),
                Permissions::OBJECT_VIEWER | Permissions::OBJECT_CREATOR
            ),
            rule(
                "checkpoint",
                snapshots,
                Permissions::OBJECT_VIEWER | Permissions::OBJECT_CREATOR
            ),
        ]
    );
    Ok(())
}

#[test]
fn s3_locations_without_a_checkpoint_keep_their_wire_format() -> Result<()> {
    let location = BucketLocation::S3 {
        bucket: "actors".into(),
        artifact_bucket: "code".into(),
        s3: super::super::S3Endpoint {
            region: "us-west-2".into(),
            endpoint: None,
            force_path_style: false,
        },
        checkpoint: None,
    };
    let encoded = serde_json::to_value(&location)?;
    assert!(encoded.get("checkpoint").is_none());
    let BucketLocation::S3 { checkpoint, .. } = serde_json::from_value(encoded)? else {
        panic!("expected S3");
    };
    assert_eq!(checkpoint, None);
    Ok(())
}
