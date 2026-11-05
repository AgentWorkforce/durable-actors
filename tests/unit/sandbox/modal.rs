use super::*;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};

#[tokio::test]
async fn pinned_images_missing_at_startup_are_imported_once_per_digest() -> Result<()> {
    let importer = Arc::new(RecordingImporter::default());
    let images = ImportedImages::new(importer.clone());
    let current = image_ref('b');
    let custom = image_ref('a');
    images.get(&current).await?;
    let loaded = futures_util::future::try_join_all((0..16).map(|_| images.get(&custom))).await?;
    assert!(
        loaded
            .iter()
            .all(|image| image.image_id() == Some(custom.as_str()))
    );
    images.get(&custom).await?;
    assert_eq!(*importer.calls.lock().unwrap(), [current, custom]);
    Ok(())
}

#[tokio::test]
async fn failed_image_imports_can_be_retried() -> Result<()> {
    let importer = Arc::new(RecordingImporter::default());
    importer.fail.store(true, Ordering::SeqCst);
    let images = ImportedImages::new(importer.clone());
    let reference = image_ref('a');
    let error = images.get(&reference).await.unwrap_err().to_string();
    assert!(error.contains(&reference));
    assert!(error.contains("registry unavailable"));
    assert_eq!(
        images.get(&reference).await?.image_id(),
        Some(reference.as_str())
    );
    assert_eq!(importer.calls.lock().unwrap().len(), 2);
    Ok(())
}

#[derive(Default)]
struct RecordingImporter {
    calls: Mutex<Vec<String>>,
    fail: AtomicBool,
}

#[async_trait]
impl ImageImporter for RecordingImporter {
    async fn import(&self, reference: &str) -> Result<Image> {
        self.calls.lock().unwrap().push(reference.into());
        tokio::task::yield_now().await;
        anyhow::ensure!(
            !self.fail.swap(false, Ordering::SeqCst),
            "registry unavailable"
        );
        Ok(Image::from_id(reference))
    }
}

fn image_ref(digit: char) -> String {
    format!("registry/runtime@sha256:{}", digit.to_string().repeat(64))
}

fn config() -> ModalConfig {
    ModalConfig {
        app_name: "durable-actors-test".into(),
        regions: BTreeMap::from([("north-america-west".into(), "us-west".into())]),
        timeout: Duration::from_secs(600),
    }
}

fn request() -> CreateSpareRequest {
    CreateSpareRequest {
        otlp_endpoint: None,
        control_plane_url: Some("https://control.example.com".into()),
        kind: SpareKind::Actor,
        name: "spare-one".into(),
        image_ref: "registry/runtime@sha256:abc".into(),
        canonical_region: "north-america-west".into(),
        resources: ResourceLimits {
            cpu_millis: 250,
            cpu_limit_millis: Some(250),
            memory_mib: 256,
            memory_limit_mib: Some(256),
        },
    }
}

#[test]
fn modal_spare_preserves_resource_requests_and_limits() -> Result<()> {
    let request = request();
    let params = spare_params(&request, &config(), "assignment-token")?;
    assert_eq!((params.cpu, params.cpu_limit), (Some(0.25), Some(0.25)));
    assert_eq!(
        (params.memory_mib, params.memory_limit_mib),
        (Some(256), Some(256))
    );
    assert_eq!(params.command, ["/usr/local/bin/durable-actors"]);
    assert_eq!(params.env["DURABLE_ACTORS_SPARE_TOKEN"], "assignment-token");
    assert_eq!(
        params.env["DURABLE_ACTORS_CONTROL_PLANE_URL"],
        "https://control.example.com"
    );
    assert_eq!(params.h2_ports, [7101, 7102]);
    assert!(params.unencrypted_ports.is_empty());
    assert_eq!(params.cloud.as_deref(), Some("gcp"));
    assert_eq!(params.regions, ["us-west"]);
    assert_eq!(params.timeout, Some(Duration::from_secs(600)));
    Ok(())
}

#[test]
fn modal_spare_requires_reachable_control_plane_and_configured_region() {
    let mut request = request();
    request.control_plane_url = None;
    assert!(spare_params(&request, &config(), "token").is_err());
    request.control_plane_url = Some("https://control.example.com".into());
    request.canonical_region = "north-america-east".into();
    assert!(spare_params(&request, &config(), "token").is_err());
}
