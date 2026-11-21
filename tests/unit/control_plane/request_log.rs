use anyhow::Result;
use axum::{Router, body::Body, http::Request, routing::get};
use tower::ServiceExt;

use super::logged;
use crate::{control_plane::public_api::ApiError, test_logs::LogCapture};

#[tokio::test]
async fn logs_api_requests_but_not_health_probes() -> Result<()> {
    let logs = LogCapture::start();
    let router = logged(
        Router::new()
            .route("/healthz", get(|| async {}))
            .route(
                "/v1/projects/{project_id}/actors/{actor_name}/{actor_id}/state",
                get(|| async {}),
            )
            .route(
                "/v1/projects/{project_id}/deployment",
                get(|| async { Err::<(), _>(ApiError::unavailable("registry is down")) }),
            ),
    );
    for path in [
        "/healthz",
        "/v1/projects/project-a/actors/Counter/one/state",
        "/v1/projects/project-a/deployment",
    ] {
        let request = Request::get(path)
            .header("x-request-id", "request-1")
            .header("x-durable-actors-organization-id", "org-a")
            .body(Body::empty())?;
        router.clone().oneshot(request).await?;
    }
    let lines = logs.lines();
    assert_eq!(lines.len(), 2, "{lines:?}");

    let completed = &lines[0];
    assert!(
        completed["message"].as_str().is_some_and(|message| message
            .starts_with("GET /v1/projects/project-a/actors/Counter/one/state 200 ")
            && message.ends_with("ms")),
        "{completed}"
    );
    assert_eq!(completed["event"], "http_request");
    assert_eq!(completed["level"], "INFO");
    assert_eq!(completed["http.method"], "GET");
    assert_eq!(
        completed["http.route"],
        "/v1/projects/{project_id}/actors/{actor_name}/{actor_id}/state"
    );
    assert_eq!(
        completed["http.url_details.path"],
        "/v1/projects/project-a/actors/Counter/one/state"
    );
    assert_eq!(completed["http.status_code"], 200);
    assert!(completed["duration"].as_u64().is_some_and(|ns| ns > 0));
    assert_eq!(completed["outcome"], "completed");
    assert_eq!(completed["fault"], "none");
    assert_eq!(completed["organization_id"], "org-a");
    assert_eq!(completed["project_id"], "project-a");
    assert_eq!(completed["actor_name"], "Counter");
    assert_eq!(completed["actor_id"], "one");
    assert_eq!(completed["request_id"], "request-1");

    let failed = &lines[1];
    assert_eq!(failed["level"], "ERROR");
    assert_eq!(failed["http.status_code"], 503);
    assert_eq!(failed["outcome"], "failed");
    assert_eq!(failed["fault"], "server");
    assert_eq!(failed["error_code"], "unavailable");
    assert_eq!(failed["error.message"], "registry is down");
    Ok(())
}
