use super::*;
use crate::{
    control_plane::{ActorTokenPurpose, admin::LocalAdminRegistry},
    placement::testing::LocalObjectPlacementStore,
    sandbox::ActorHostHandle,
};
use std::sync::Mutex;

#[path = "access.rs"]
mod access_tests;

#[tokio::test]
async fn persisted_deployments_cold_start_on_the_current_runtime_without_redeployment() -> Result<()>
{
    use crate::control_plane::admin::{AdminRegistry, ImagePolicy, PostgresAdminRegistry};
    crate::postgres::testing::with_postgres(async |fixture| {
        let registry = PostgresAdminRegistry::from_database(
            crate::postgres::PostgresDatabase::connect(&fixture.url).await?,
        );
        let mut deployed = source();
        deployed.image_ref = format!("runtime/platform@sha256:{}", "a".repeat(64));
        registry.register_test_deployment(&deployed).await?;
        let client = fixture.pool.get().await?;
        client
            .batch_execute("ALTER TABLE durable_actors_deployment DROP COLUMN image_policy")
            .await?;
        client
            .batch_execute(include_str!(
                "../../../migrations/V22__deployment_image_policy.sql"
            ))
            .await?;
        let identity = deployed.host_config_key();
        let current = format!("runtime/platform@sha256:{}", "b".repeat(64));
        let provisioner = SandboxHostProvisioner::new(
            Arc::new(Provider::default()),
            HostSandboxRuntimeConfig {
                otlp_endpoint: None,
                control_plane_url: "http://control".into(),
                jwt_issuer: "issuer".into(),
                invocation_jwt_audience: "invocation".into(),
                host_idle_timeout_ms: 10_000,
            },
            super::tests::test_issuer()?,
            Some(super::super::process::RuntimeImages {
                typescript: current.clone(),
                python: "unused".into(),
            }),
        );
        let actor = ActorKey {
            project_id: "default".into(),
            actor_name: "Counter".into(),
            actor_id: "one".into(),
        };
        let saved = registry.launch_spec("default").await?.unwrap();
        let request = provisioner.request(&saved, "north-america-west", &actor)?;
        assert_eq!(request.image_ref, current);
        assert_eq!(request.code_snapshot, deployed.code_snapshot);
        assert_eq!(request.host_config_key, identity);
        assert_eq!(
            registry.launch_spec("default").await?,
            Some(deployed.clone())
        );
        deployed.image_policy = ImagePolicy::Custom;
        registry.register_test_deployment(&deployed).await?;
        let custom = registry.launch_spec("default").await?.unwrap();
        assert_eq!(
            provisioner
                .request(&custom, "north-america-west", &actor)?
                .image_ref,
            deployed.image_ref
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn custom_image_registration_round_trips_without_adopting_the_platform_image() -> Result<()> {
    let (service, admin, _) = fixture()?;
    let routes = super::super::public_api::router(service, admin.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!(
        "http://{}/v1/projects/default/deployment",
        listener.local_addr()?
    );
    let server = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async {
        axum::serve(listener, routes).await
    }));
    let client = reqwest::Client::new();
    let image = format!("customer/runtime@sha256:{}", "a".repeat(64));
    let bundle = crate::bundle::ActorBundle::decode(source().code_snapshot.as_ref().unwrap())?;
    let body = serde_json::json!({"organizationId":"org-test", "imageRef":image, "bundle":bundle, "contract":contract()});
    client
        .put(&url)
        .bearer_auth("api-key")
        .json(&body)
        .send()
        .await?
        .error_for_status()?;
    let saved = admin.current_deployment("default").await?.unwrap();
    assert_eq!(saved.image_policy, super::super::admin::ImagePolicy::Custom);
    assert_eq!(saved.image_ref, image);
    let mut current: serde_json::Value = client
        .get(&url)
        .bearer_auth("api-key")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(current["imageRef"], image);
    current["secretRefs"] = serde_json::json!(["rotated"]);
    client
        .put(&url)
        .bearer_auth("api-key")
        .json(&current)
        .send()
        .await?
        .error_for_status()?;
    assert_eq!(
        admin
            .current_deployment("default")
            .await?
            .unwrap()
            .image_ref,
        image
    );
    drop(server);
    Ok(())
}

#[tokio::test]
async fn openapi_is_available_without_credentials_or_a_deployment() -> Result<()> {
    let (service, admin, _) = fixture()?;
    let routes = super::super::public_api::router(service, admin);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}/openapi.yaml", listener.local_addr()?);
    let server = tokio::spawn(async { axum::serve(listener, routes).await });
    let response = reqwest::get(url).await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "application/yaml");
    let expected = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/docs/reference/openapi.yaml"
    ))?;
    assert_eq!(response.text().await?, expected);
    server.abort();
    Ok(())
}

#[tokio::test]
async fn bundle_registration_preserves_code_and_contract_during_secret_rotation() -> Result<()> {
    let (service, admin, provider) = fixture()?;
    let routes = super::super::public_api::router(service, admin.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!(
        "http://{}/v1/projects/default/deployment",
        listener.local_addr()?
    );
    let server = tokio::spawn(async { axum::serve(listener, routes).await });
    let client = reqwest::Client::new();
    let bundle = crate::bundle::ActorBundle::decode(&crate::sandbox::testing::code_artifact(1))?;
    let mut document: serde_json::Value = serde_json::from_str(include_str!(
        "../../../sdk/tests/fixtures/public-contract.json"
    ))?;
    document["actors"][0]["sandbox"] = serde_json::json!({"cpu":2,"memoryMiB":4096});
    client
        .put(&url)
        .bearer_auth("api-key")
        .json(&serde_json::json!({"organizationId":"org-test","bundle":bundle,"contract":document}))
        .send()
        .await?
        .error_for_status()?;
    let active = admin.current_deployment("default").await?.unwrap();
    assert_eq!(active.image_ref, "im-runtime");
    assert_eq!(active.organization_id, "org-test");
    assert_eq!(active.actor_entrypoint.as_deref(), Some("actors.mjs"));
    assert_eq!(active.code_snapshot, Some(bundle.encode()?));
    assert!(active.sandboxes.contains_key("ChatRoom"));
    let mut current: serde_json::Value = client
        .get(&url)
        .bearer_auth("api-key")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(current["bundle"], serde_json::to_value(bundle)?);
    current["secretRefs"] = serde_json::json!(["rotated"]);
    client
        .put(&url)
        .bearer_auth("api-key")
        .json(&current)
        .send()
        .await?
        .error_for_status()?;
    let rotated = admin.current_deployment("default").await?.unwrap();
    assert_eq!(rotated.code_snapshot, active.code_snapshot);
    assert_eq!(rotated.organization_id, active.organization_id);
    assert_eq!(rotated.sandboxes, active.sandboxes);
    assert_ne!(rotated.host_config_key(), active.host_config_key());
    assert_eq!(
        admin
            .deployment_contract("default")
            .await?
            .unwrap()
            .contract,
        document
    );
    assert_eq!(
        provider.retired.lock().unwrap().as_slice(),
        &[active.host_config_key()]
    );
    // A new bundle must carry a contract before replacing the running deployment.
    current["bundle"]["files"][0]["generation"] = 2.into();
    assert_eq!(
        client
            .put(&url)
            .bearer_auth("api-key")
            .json(&current)
            .send()
            .await?
            .status(),
        reqwest::StatusCode::BAD_REQUEST
    );
    assert_eq!(admin.current_deployment("default").await?, Some(rotated));
    current["contract"] = contract();
    client
        .put(&url)
        .bearer_auth("api-key")
        .json(&current)
        .send()
        .await?
        .error_for_status()?;
    assert!(
        admin
            .current_deployment("default")
            .await?
            .unwrap()
            .sandboxes
            .is_empty()
    );
    server.abort();
    Ok(())
}

#[tokio::test]
async fn compiled_artifact_selects_the_language_image() -> Result<()> {
    let (service, admin, _) = fixture()?;
    let contract = super::super::contracts::PublicActorContract::new(contract())?;
    for (entrypoint, image) in [("actors.mjs", "im-runtime"), ("actors.pyz", "im-python")] {
        let mut source = source();
        let mut bundle =
            crate::bundle::ActorBundle::decode(source.code_snapshot.as_ref().unwrap())?;
        bundle.files[0].path = entrypoint.into();
        bundle.files[0].object = format!(
            "durable-actors/v3/artifacts/00000000-0000-4000-8000-000000000001/{entrypoint}"
        );
        source.actor_entrypoint = Some(entrypoint.into());
        source.code_snapshot = Some(bundle.encode()?);
        service
            .deploy_source(&admin, &source, Some(&contract))
            .await?;
        let active = admin.current_deployment("default").await?.unwrap();
        assert_eq!(active.image_ref, image);
        assert_eq!(active.actor_entrypoint.as_deref(), Some(entrypoint));
    }
    Ok(())
}

#[tokio::test]
async fn bundle_registration_validates_immutable_artifacts_before_retiring_hosts() -> Result<()> {
    let (service, admin, provider) = fixture()?;
    let source = source();
    let contract = super::super::contracts::PublicActorContract::new(contract())?;
    service
        .deploy_source(&admin, &source, Some(&contract))
        .await?;
    let active = admin.current_deployment("default").await?;
    for mutation in ["bucket", "object", "generation", "sha256", "path"] {
        let mut bundle =
            crate::bundle::ActorBundle::decode(source.code_snapshot.as_ref().unwrap())?;
        match mutation {
            "bucket" => bundle.bucket = "another-bucket".into(),
            "object" => bundle.files[0].object = "outside/actors.mjs".into(),
            "generation" => bundle.files[0].generation = 0,
            "sha256" => bundle.files[0].sha256 = "bad".into(),
            _ => bundle.files[0].path = "../actors.mjs".into(),
        }
        if let Ok(snapshot) = bundle.encode() {
            let mut invalid = source.clone();
            invalid.code_snapshot = Some(snapshot);
            assert!(
                service
                    .deploy_source(&admin, &invalid, Some(&contract))
                    .await
                    .is_err()
            );
        }
        assert_eq!(admin.current_deployment("default").await?, active);
    }
    assert!(provider.retired.lock().unwrap().is_empty());
    Ok(())
}

#[tokio::test]
async fn registration_requires_an_organization() -> Result<()> {
    let (service, admin, _) = fixture()?;
    let routes = super::super::public_api::router(service, admin.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!(
        "http://{}/v1/projects/default/deployment",
        listener.local_addr()?
    );
    let server = tokio::spawn(async { axum::serve(listener, routes).await });
    let bundle = crate::bundle::ActorBundle::decode(&crate::sandbox::testing::code_artifact(1))?;
    for organization in [None, Some("")] {
        let mut body = serde_json::json!({"bundle": bundle, "contract": contract()});
        if let Some(organization) = organization {
            body["organizationId"] = organization.into();
        }
        let response = reqwest::Client::new()
            .put(&url)
            .bearer_auth("api-key")
            .json(&body)
            .send()
            .await?;
        assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
    }
    assert!(admin.current_deployment("default").await?.is_none());
    server.abort();
    Ok(())
}

#[tokio::test]
async fn bundle_contracts_are_scoped_to_the_project() -> Result<()> {
    let (service, admin, _) = fixture()?;
    let first = source();
    let mut second = first.clone();
    second.project_id = "team-b".into();
    let contract = super::super::contracts::PublicActorContract::new(contract())?;
    service
        .deploy_source(&admin, &first, Some(&contract))
        .await?;
    assert!(service.deploy_source(&admin, &second, None).await.is_err());
    service
        .deploy_source(&admin, &second, Some(&contract))
        .await?;
    service.delete_deployment(&admin, "default").await?;
    assert!(admin.current_deployment("team-b").await?.is_some());
    Ok(())
}

fn fixture() -> Result<(ControlPlaneService, AdminService, Arc<Provider>)> {
    fixture_with_idle_timeout(60_000)
}

fn fixture_with_idle_timeout(
    host_idle_timeout_ms: u64,
) -> Result<(ControlPlaneService, AdminService, Arc<Provider>)> {
    let issuer = super::tests::test_issuer()?;
    let auth = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        "issuer",
        "authority",
        ActorTokenPurpose::ControlPlane,
        std::time::Duration::from_secs(60),
    )?;
    let registry = Arc::new(LocalAdminRegistry::default());
    let admin = AdminService::new(Some("api-key".into()), registry.clone(), issuer.clone())?;
    let provider = Arc::new(Provider::default());
    let provisioner = Arc::new(
        SandboxHostProvisioner::new(
            provider.clone(),
            HostSandboxRuntimeConfig {
                otlp_endpoint: None,
                control_plane_url: "http://control".into(),
                jwt_issuer: "issuer".into(),
                invocation_jwt_audience: "invocation".into(),
                host_idle_timeout_ms,
            },
            issuer.clone(),
            Some(super::super::process::RuntimeImages {
                typescript: "im-runtime".into(),
                python: "im-python".into(),
            }),
        )
        .with_runtime_access(Arc::new(crate::bucket::access::RuntimeAccess::new(
            crate::bucket::access::BucketLocation::Gcs {
                bucket: "owner-bucket".into(),
                artifact_bucket: "test-artifacts".into(),
            },
            crate::bucket::SnapshotStoreConfig::Local,
        )?)),
    );
    let service = ControlPlaneService::new(
        Arc::new(LocalObjectPlacementStore::default()),
        auth,
        registry,
        issuer,
        provisioner,
    );
    Ok((service, admin, provider))
}

fn source() -> HostLaunchSpec {
    HostLaunchSpec {
        image_policy: super::super::admin::ImagePolicy::Platform,
        organization_id: "org-test".into(),
        sandboxes: Default::default(),
        project_id: "default".into(),
        source: None,
        image_ref: "bundle".into(),
        code_snapshot: Some(crate::sandbox::testing::code_artifact(1)),
        working_directory: "/customer".into(),
        actor_entrypoint: Some("actors.mjs".into()),
        secret_refs: vec![],
    }
}

fn contract() -> serde_json::Value {
    serde_json::json!({"version":1,"actors":[],"typescript":{"declarations":"export interface ActorTypes {}","dependencies":{}}})
}

#[derive(Default)]
struct Provider {
    retired: Mutex<Vec<String>>,
    fail_termination: std::sync::atomic::AtomicBool,
}

#[async_trait]
impl SandboxProvider for Provider {
    async fn stopped_spares(&self, _: &[crate::sandbox::SpareHandle]) -> Result<Vec<String>> {
        anyhow::bail!("unexpected spare inspection")
    }
    async fn ensure_host(&self, _: &EnsureHostRequest) -> Result<ActorHostHandle> {
        anyhow::bail!("no actor invocation in deployment test")
    }
    async fn terminate_hosts(&self, request: &TerminateHostsRequest) -> Result<HostTermination> {
        self.retired
            .lock()
            .unwrap()
            .push(request.host_config_key.clone());
        if self
            .fail_termination
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            anyhow::bail!("temporary shutdown failure");
        }
        Ok(HostTermination {
            provider: "test".into(),
            resource_ids: vec![],
        })
    }
}
