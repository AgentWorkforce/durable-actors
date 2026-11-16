use super::*;
use crate::host::HostId;

#[tokio::test]
async fn assignment_is_authenticated_single_use_and_waits_for_readiness() -> anyhow::Result<()> {
    for with_operation in [false, true] {
        let (send, receive) = tokio::sync::oneshot::channel();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/assign", listener.local_addr()?);
        let server = tokio::spawn(async move {
            axum::serve(listener, router("secret".into(), send))
                .await
                .unwrap();
        });
        let client = reqwest::Client::new();
        let health = client
            .get(url.replace("/assign", "/healthz"))
            .send()
            .await?;
        assert_eq!(health.status(), 200);
        let response = client
            .post(&url)
            .json(&serde_json::json!({}))
            .send()
            .await?;
        assert_eq!(response.status(), 401);
        let actor =
            serde_json::json!({"project_id":"project", "actor_name":"Counter", "actor_id":"one"});
        let environment =
            serde_json::json!({"actor":"one", "DURABLE_ACTORS_ACTOR":actor.to_string()});
        let body = if with_operation {
            let mut invalid = serde_json::json!({"environment":environment, "operation":{
                "routingMs":1.0, "request":{"type":"invoke", "invocation":{
                    "actor":actor, "request_id":"first", "method":"read", "args":[]
                }, "grant":null}
            }});
            invalid["operation"]["request"]["invocation"]["actor"]["actor_id"] = "other".into();
            assert_eq!(
                client
                    .post(&url)
                    .bearer_auth("secret")
                    .json(&invalid)
                    .send()
                    .await?
                    .status(),
                400
            );
            invalid["operation"]["request"]["invocation"]["actor"]["actor_id"] = "one".into();
            invalid
        } else {
            environment
        };
        let request = client.post(&url).bearer_auth("secret").json(&body);
        let call = tokio::spawn(async move { request.send().await });
        let assigned = tokio::time::timeout(std::time::Duration::from_secs(1), receive).await??;
        assert_eq!(assigned.environment["actor"], "one");
        assert_eq!(assigned.operation.is_some(), with_operation);
        assert!(
            !call.is_finished(),
            "assignment must wait for hydration and ownership"
        );
        let duplicate = client
            .post(&url)
            .bearer_auth("secret")
            .json(&serde_json::json!({}))
            .send()
            .await?;
        assert_eq!(duplicate.status(), 409);
        assert!(
            assigned
                .ready
                .send(Ok(super::super::process::HostReadiness {
                    initial_reply: with_operation
                        .then(|| serde_json::json!({"type":"completed", "result":7}).into()),
                    host_id: HostId::new("host.v3.test.one"),
                    session_id: "session".into(),
                    route: "https://host.test".into(),
                    canonical_region: "north-america-east".into(),
                    owner_epoch: 42,
                    lease: crate::host_leases::HostLease {
                        id: HostId::new("host.v3.test.one"),
                        session_id: "session".into(),
                        route: "https://host.test".into(),
                        expires_at_ms: 60_000,
                    },
                }))
                .is_ok()
        );
        let reply = call.await??;
        assert_eq!(reply.status(), 200);
        let reply = reply.json::<serde_json::Value>().await?;
        assert_eq!(reply["ownerEpoch"], 42);
        if with_operation {
            assert_eq!(reply["initialReply"]["result"], 7);
        }
        server.abort();
    }
    Ok(())
}

#[tokio::test]
async fn failed_initialization_never_reports_ready() -> anyhow::Result<()> {
    let (send, receive) = tokio::sync::oneshot::channel();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}/assign", listener.local_addr()?);
    let server = tokio::spawn(async move {
        axum::serve(listener, router("secret".into(), send))
            .await
            .unwrap();
    });
    let request = reqwest::Client::new()
        .post(url)
        .bearer_auth("secret")
        .json(&serde_json::json!({}));
    let call = tokio::spawn(async move { request.send().await });
    drop(receive.await?);
    assert_eq!(call.await??.status(), 503);
    server.abort();
    Ok(())
}
