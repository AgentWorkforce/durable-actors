use super::*;
use futures_util::stream;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

#[tokio::test]
async fn bundle_downloads_are_bounded_until_their_bodies_finish() -> Result<()> {
    use axum::{Router, body::Body};
    use google_cloud_auth::credentials::anonymous;
    use tokio_util::task::AbortOnDropHandle;

    let requests = Arc::new(AtomicUsize::new(0));
    let bodies = Arc::new(tokio::sync::Semaphore::new(0));
    let routes = Router::new().fallback({
        let requests = requests.clone();
        let bodies = bodies.clone();
        move || {
            requests.fetch_add(1, Ordering::SeqCst);
            let bodies = bodies.clone();
            async move {
                let body = Body::from_stream(stream::once(async move {
                    let _ = bodies.acquire().await;
                    Ok::<_, std::io::Error>(Bytes::from_static(b"code"))
                }));
                (
                    [("x-goog-generation", "123"), ("content-length", "4")],
                    body,
                )
            }
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let storage = Storage::builder()
        .with_endpoint(format!("http://{}", listener.local_addr()?))
        .with_credentials(anonymous::Builder::new().build())
        .build()
        .await?;
    let _server =
        AbortOnDropHandle::new(tokio::spawn(async { axum::serve(listener, routes).await }));
    let bundle = ActorBundle {
        bucket: "code-bucket".into(),
        files: std::iter::once(artifact("actors.mjs", b"code"))
            .chain((1..12).map(|index| artifact(&format!("python/{index}.py"), b"code")))
            .collect(),
    };
    let root = tempfile::tempdir()?;
    let directory = root.path().to_owned();
    let install = AbortOnDropHandle::new(tokio::spawn(async move {
        bundle.install(&directory, &storage).await
    }));
    tokio::time::timeout(Duration::from_secs(5), async {
        while requests.load(Ordering::SeqCst) < MAX_CONCURRENT_DOWNLOADS {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(requests.load(Ordering::SeqCst), MAX_CONCURRENT_DOWNLOADS);
    bodies.close();
    tokio::time::timeout(Duration::from_secs(5), install).await???;
    assert_eq!(requests.load(Ordering::SeqCst), 12);
    assert_eq!(
        tokio::fs::read(root.path().join("actors.mjs")).await?,
        b"code"
    );
    for index in 1..12 {
        assert_eq!(
            tokio::fs::read(root.path().join(format!("python/{index}.py"))).await?,
            b"code"
        );
    }
    Ok(())
}

fn artifact(path: &str, bytes: &[u8]) -> BundleFile {
    BundleFile {
        path: path.into(),
        object: "compiled/actors.mjs".into(),
        generation: 123,
        sha256: checksum(bytes),
    }
}

#[tokio::test]
async fn streamed_code_is_published_only_after_its_digest_matches() -> Result<()> {
    let root = tempfile::tempdir()?;
    tokio::fs::write(root.path().join("actors.mjs"), b"old").await?;
    let file = artifact("actors.mjs", b"new code");
    assert!(
        install_file(
            root.path(),
            &file,
            stream::iter([Ok(Bytes::from_static(b"wrong"))])
        )
        .await
        .is_err()
    );
    assert_eq!(
        tokio::fs::read(root.path().join("actors.mjs")).await?,
        b"old"
    );
    install_file(
        root.path(),
        &file,
        stream::iter([
            Ok(Bytes::from_static(b"new ")),
            Ok(Bytes::from_static(b"code")),
        ]),
    )
    .await?;
    assert_eq!(
        tokio::fs::read(root.path().join("actors.mjs")).await?,
        b"new code"
    );
    Ok(())
}

#[tokio::test]
async fn interrupted_code_downloads_preserve_existing_files_and_remove_staging() -> Result<()> {
    let root = tempfile::tempdir()?;
    tokio::fs::write(root.path().join("actors.mjs"), b"old").await?;
    let result = install_file(
        root.path(),
        &artifact("actors.mjs", b"new code"),
        stream::iter([
            Ok(Bytes::from_static(b"new ")),
            Err(anyhow::anyhow!("download interrupted")),
        ]),
    )
    .await;
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("download interrupted")
    );
    assert_eq!(
        tokio::fs::read(root.path().join("actors.mjs")).await?,
        b"old"
    );
    assert_eq!(std::fs::read_dir(root.path())?.count(), 1);
    Ok(())
}

#[tokio::test]
async fn artifact_paths_cannot_escape_the_sandbox_code_directory() -> Result<()> {
    let root = tempfile::tempdir()?;
    for path in ["../outside.mjs", "/outside.mjs", "nested/../../outside.mjs"] {
        assert!(
            install_file(
                root.path(),
                &artifact(path, b"code"),
                stream::iter([Ok(Bytes::from_static(b"code"))])
            )
            .await
            .is_err()
        );
    }
    Ok(())
}

fn checksum(bytes: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(aws_lc_rs::digest::digest(&SHA256, bytes).as_ref())
}

#[test]
fn python_code_manifests_include_the_entrypoint_and_installed_dependencies() -> Result<()> {
    let manifest = ActorBundle {
        bucket: "code-bucket".into(),
        files: vec![
            artifact("actors.pyz", b"code"),
            artifact("python/dependency/module.so", b"native"),
        ],
    };
    let decoded = ActorBundle::decode(&manifest.encode()?)?;
    assert_eq!(decoded.entrypoint()?, "actors.pyz");
    assert_eq!(decoded.files.len(), 2);
    Ok(())
}

#[tokio::test]
async fn bucket_installs_pin_each_file_to_its_deployed_generation() -> Result<()> {
    use crate::bucket::Bucket;
    let objects = tempfile::tempdir()?;
    let bucket = crate::bucket::FileBucket::new(objects.path().into())?;
    let object = "durable-actors/v3/artifacts/12345678-1234-4234-8234-123456789012/actors.mjs";
    assert!(
        bucket
            .compare_and_swap(object, None, Bytes::from_static(b"export {}"))
            .await?
    );
    let mut file = artifact("actors.mjs", b"export {}");
    file.object = object.into();
    file.generation = bucket.get(object).await?.unwrap().generation;
    let mut bundle = ActorBundle {
        bucket: "code".into(),
        files: vec![file],
    };
    let root = tempfile::tempdir()?;
    bundle.install_from_bucket(root.path(), &bucket).await?;
    assert_eq!(std::fs::read(root.path().join("actors.mjs"))?, b"export {}");

    bundle.files[0].generation += 1;
    let error = bundle
        .install_from_bucket(tempfile::tempdir()?.path(), &bucket)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("changed after deployment"),
        "{error}"
    );

    bundle.files[0].generation -= 1;
    bundle.files[0].sha256 = checksum(b"something else");
    assert!(
        bundle
            .install_from_bucket(tempfile::tempdir()?.path(), &bucket)
            .await
            .is_err()
    );
    bundle.files[0].object = format!("{object}.missing");
    assert!(
        bundle
            .install_from_bucket(tempfile::tempdir()?.path(), &bucket)
            .await
            .is_err()
    );
    Ok(())
}
