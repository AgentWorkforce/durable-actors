use super::*;
use crate::control_plane::auth::unix_seconds;
use serde_json::{Value, json};

#[tokio::test]
async fn project_sessions_discover_all_published_actors_but_never_grant_admin_access() -> Result<()>
{
    let (service, admin, expiry, _verifier) = fixture(Some("admin-secret")).await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let router = super::super::super::public_api::router(service, admin);
    let server = tokio::spawn(async { axum::serve(listener, router).await });
    let client = reqwest::Client::new();
    let session_request = json!({"subject":"terse-credential", "expiresAtMs":expiry * 1000});
    let response = client
        .post(format!("{origin}/v1/projects/default/sessions"))
        .bearer_auth("admin-secret")
        .json(&session_request)
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.headers()["cache-control"], "no-store");
    let session: Value = response.json().await?;
    assert!(session["expiresAtMs"].as_i64().unwrap() <= expiry * 1000);
    let token = session["token"].as_str().unwrap();
    for credential in ["invalid", token] {
        let response = client
            .post(format!("{origin}/v1/projects/default/sessions"))
            .bearer_auth(credential)
            .json(&session_request)
            .send()
            .await?;
        assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
    }
    for (actor_name, actor_id) in [
        ("ChatRoom", "room"),
        ("ChatRoom", "another-room"),
        ("ArchivedRoom", "room"),
    ] {
        let reply: Value = client
            .post(format!(
                "{origin}/v1/projects/default/actors/{actor_name}/{actor_id}/find-actor"
            ))
            .bearer_auth(token)
            .json(&json!({}))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert_eq!(reply, json!({"homeRegion":"north-america-central"}));
    }
    assert_eq!(
        client
            .post(format!(
                "{origin}/v1/projects/default/actors/PrivateActor/room/find-actor"
            ))
            .bearer_auth(token)
            .json(&json!({}))
            .send()
            .await?
            .status(),
        reqwest::StatusCode::NOT_FOUND
    );

    for (method, path) in [
        (
            reqwest::Method::POST,
            "/v1/projects/other/actors/ChatRoom/room/find-actor",
        ),
        (reqwest::Method::GET, "/v1/projects/default/deployment"),
        (
            reqwest::Method::GET,
            "/v1/projects/default/deployment/contract",
        ),
        (reqwest::Method::PUT, "/v1/projects/default/deployment"),
        (reqwest::Method::DELETE, "/v1/projects/default/deployment"),
        (
            reqwest::Method::POST,
            "/v1/projects/default/actors/ChatRoom/room/find-websocket",
        ),
    ] {
        assert_eq!(
            client
                .request(method, format!("{origin}{path}"))
                .bearer_auth(&token)
                .json(&json!({}))
                .send()
                .await?
                .status(),
            reqwest::StatusCode::UNAUTHORIZED,
            "{path}"
        );
    }
    for invalid in [
        json!({"subject":"","expiresAtMs":expiry * 1000}),
        json!({"subject":"terse-credential","expiresAtMs":0}),
    ] {
        let response = client
            .post(format!("{origin}/v1/projects/default/sessions"))
            .bearer_auth("admin-secret")
            .json(&invalid)
            .send()
            .await?;
        assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
    }
    server.abort();
    Ok(())
}

#[tokio::test]
async fn session_issuance_requires_configured_administrative_authentication() -> Result<()> {
    let (_, admin, _, _) = fixture(None).await?;
    assert!(admin.authorize_session_issuance("").is_err());
    assert!(
        admin
            .authorize_session_issuance("Bearer arbitrary")
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn invoke_api_key_is_distinct_from_the_administrative_secret() -> Result<()> {
    let (_, admin, _, _) = fixture(Some("admin-secret")).await?;
    for invalid in ["", " ", " key", "key ", "admin-secret"] {
        assert!(
            admin
                .clone()
                .with_invoke_api_key(Some(invalid.into()))
                .is_err(),
            "{invalid:?}"
        );
    }
    let admin = admin.with_invoke_api_key(Some("invoke-secret".into()))?;
    for accepted in ["Bearer admin-secret", "Bearer invoke-secret"] {
        assert!(admin.authorize_socket_discovery(accepted).is_ok());
    }
    for rejected in [
        "",
        "Bearer",
        "Bearer ",
        "Bearer other",
        "Basic invoke-secret",
        "Bearer invoke-secret ",
    ] {
        assert!(admin.authenticate(rejected).is_err(), "{rejected:?}");
        assert!(
            admin.authorize_socket_discovery(rejected).is_err(),
            "{rejected:?}"
        );
        assert!(
            admin.authorize_discovery(rejected, "default").is_err(),
            "{rejected:?}"
        );
    }
    assert!(admin.authenticate("Bearer invoke-secret").is_err());
    assert!(
        admin
            .authorize_session_issuance("Bearer invoke-secret")
            .is_err()
    );
    assert!(
        admin
            .authorize_discovery("Bearer admin-secret", "default")?
            .is_none()
    );
    let session = admin
        .authorize_discovery("Bearer invoke-secret", "default")?
        .expect("invoke key grants a session");
    assert_eq!(session.subject, "invoke-api-key");
    assert_eq!(session.project_id, "default");
    assert!(
        session.expires_at - session.iat
            <= crate::control_plane::session::MAX_SESSION_LIFETIME_SECONDS
    );

    let (_, anyone, _, _) = fixture(None).await?;
    let anyone = anyone.with_invoke_api_key(Some("invoke-secret".into()))?;
    assert!(
        anyone
            .authorize_discovery("Bearer anything", "default")?
            .is_none()
    );
    assert!(anyone.authorize_socket_discovery("").is_ok());
    Ok(())
}

async fn fixture(
    api_key: Option<&str>,
) -> Result<(ControlPlaneService, AdminService, i64, ActorJwtVerifier)> {
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())?;
    let issuer = ActorJwtIssuer::from_base64_pkcs8(
        &STANDARD.encode(pkcs8.as_ref()),
        "test-key",
        "issuer",
        "authority",
        "invocation",
        Duration::from_secs(60),
    )?;
    let verifier = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        "issuer",
        "invocation",
        ActorTokenPurpose::Invocation,
        Duration::from_secs(60),
    )?;
    let auth = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        "issuer",
        "authority",
        ActorTokenPurpose::ControlPlane,
        Duration::from_secs(60),
    )?;
    let registry = Arc::new(LocalAdminRegistry::default());
    let admin = AdminService::new(api_key.map(str::to_owned), registry.clone(), issuer.clone())?;
    let spec = HostLaunchSpec {
        image_policy: Default::default(),
        organization_id: "org-test".into(),
        sandboxes: Default::default(),
        project_id: "default".into(),
        source: None,
        image_ref: "im-runtime".into(),
        code_snapshot: None,
        working_directory: "/customer".into(),
        actor_entrypoint: None,
        secret_refs: vec![],
    };
    let mut document: Value = serde_json::from_str(include_str!(
        "../../../sdk/tests/fixtures/public-contract.json"
    ))?;
    let mut archived_room = document["actors"][0].clone();
    archived_room["actorName"] = json!("ArchivedRoom");
    archived_room["socket"]["actorName"] = json!("ArchivedRoom");
    document["actors"]
        .as_array_mut()
        .unwrap()
        .push(archived_room);
    document["typescript"]["declarations"] = json!(format!(
        "{}\nexport interface ActorTypes {{ ArchivedRoom: ActorTypes[\"ChatRoom\"] }}\n",
        document["typescript"]["declarations"].as_str().unwrap()
    ));
    let contract = crate::control_plane::contracts::PublicActorContract::new(document)?;
    admin.register_deployment(&spec, Some(&contract)).await?;
    let service = ControlPlaneService::new(
        Arc::new(LocalObjectPlacementStore::default()),
        auth,
        registry,
        issuer,
        Arc::new(FakeRoutingProvisioner {
            failed_regions: vec![],
            calls: Mutex::new(vec![]),
        }),
    );
    let now = unix_seconds()?;
    let expiry = now + 25;
    Ok((service, admin, expiry, verifier))
}
