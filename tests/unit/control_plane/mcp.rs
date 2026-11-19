use std::sync::{Arc, Mutex};

use axum::{
    Json,
    http::{HeaderMap, Uri},
    routing::{get, post},
};

use super::*;

#[tokio::test]
async fn lists_developer_tools() {
    let reply = rpc(test_router(Router::new()).await, "tools/list", json!({})).await;
    let tools = reply["result"]["tools"].as_array().expect("tools");
    assert_eq!(tools.len(), 11);
    for tool in tools {
        assert_eq!(tool["outputSchema"]["type"], "object", "{}", tool["name"]);
    }
    let tool = |name: &str| tools.iter().find(|tool| tool["name"] == name).expect(name);
    assert_eq!(tool("invoke_actor")["annotations"]["destructiveHint"], true);
    assert_eq!(tool("invoke_actor")["annotations"]["openWorldHint"], true);
    assert_eq!(tool("list_logs")["annotations"]["openWorldHint"], false);
}

#[tokio::test]
async fn tool_calls_forward_the_credential_and_filters() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let api = Router::new().route(
        "/v1/projects/{project}/observe/requests",
        get({
            let seen = seen.clone();
            move |headers: HeaderMap, uri: Uri| async move {
                seen.lock().unwrap().push((
                    headers[header::AUTHORIZATION].to_str().unwrap().to_owned(),
                    uri.path().to_owned(),
                    uri.query().unwrap_or_default().to_owned(),
                ));
                Json(json!({ "records": [], "nextCursor": null }))
            }
        }),
    );
    let admin = test_admin(&[("demo", "org-test"), ("second", "org-test")]).await;
    let router = local_router(api, admin);
    for project in ["demo", "second"] {
        let arguments =
            json!({ "projectId": project, "actorName": "Counter", "outcome": "failed" });
        let reply = rpc(
            router.clone(),
            "tools/call",
            json!({"name":"list_requests", "arguments":arguments}),
        )
        .await;

        assert_eq!(reply["result"]["isError"], false);
        assert_eq!(text(&reply), json!({ "records": [] }));
        assert_eq!(
            reply["result"]["structuredContent"],
            json!({ "records": [] })
        );
        let (authorization, path, query) = seen.lock().unwrap().pop().expect("API call");
        assert_eq!(authorization, "Bearer secret");
        assert_eq!(path, format!("/v1/projects/{project}/observe/requests"));
        for pair in ["actorName=Counter", "outcome=failed", "limit=25"] {
            assert!(query.split('&').any(|item| item == pair), "{query}");
        }
    }
}

#[tokio::test]
async fn resources_read_one_instance_with_readable_times() {
    let api = Router::new().route(
        "/v1/projects/{project}/observe/resources",
        get(|uri: Uri| async move {
            let query = uri.query().unwrap_or_default().to_owned();
            for pair in ["actorName=Counter", "actorId=one"] {
                assert!(query.split('&').any(|item| item == pair), "{query}");
            }
            Json(json!({
                "sandboxes": [{ "claimedAtMs": 0, "cpuMillis": 500, "memoryMib": 512, "memoryLimitMib": 512 }],
                "usage": [{ "atMs": 0, "cpuCores": 0.5, "memoryBytes": 1024 }],
                "database": [{ "atMs": 0, "bytes": 4096 }],
                "databaseBytes": 4096
            }))
        }),
    );
    let reply = call_tool(
        api,
        "get_actor_resources",
        json!({ "actorName": "Counter", "actorId": "one" }),
    )
    .await;
    assert_eq!(
        text(&reply),
        json!({
            "sandboxes": [{ "since": "1970-01-01T00:00:00.000Z", "cpuMillis": 500, "memoryMib": 512, "memoryLimitMib": 512 }],
            "databaseBytes": 4096,
            "usage": [{ "at": "1970-01-01T00:00:00.000Z", "cpuCores": 0.5, "memoryBytes": 1024 }],
            "database": [{ "at": "1970-01-01T00:00:00.000Z", "bytes": 4096 }]
        })
    );
}

#[tokio::test]
async fn invoke_returns_the_outcome_without_the_host_target() {
    let seen = Arc::new(Mutex::new(None));
    let api = Router::new().route(
        "/v1/projects/demo/actors/Counter/one/invoke",
        post({
            let seen = seen.clone();
            move |Json(body): Json<Value>| async move {
                *seen.lock().unwrap() = Some(body);
                Json(json!({
                    "target": { "token": "host-ticket" },
                    "outcome": { "type": "completed", "result": 7 }
                }))
            }
        }),
    );
    let arguments =
        json!({ "actorName": "Counter", "actorId": "one", "method": "increment", "args": [2] });
    let reply = call_tool(api, "invoke_actor", arguments).await;

    assert_eq!(text(&reply), json!({ "status": "completed", "result": 7 }));
    let body = seen.lock().unwrap().take().expect("invocation");
    assert_eq!(body["method"], "increment");
    assert_eq!(body["args"], json!([2]));
    assert!(body["requestId"].as_str().is_some_and(|id| !id.is_empty()));
}

#[tokio::test]
async fn sessions_page_with_the_status_gateways_reported() {
    let sessions = json!([
        { "connectionId": "open-one", "actorName": "Chat", "actorId": "one", "status": "open",
            "openedAtMs": 1_794_330_000_000_u64, "closedAtMs": null },
        { "connectionId": "lost-one", "actorName": "Chat", "actorId": "one", "status": "lost",
            "openedAtMs": 1_794_330_000_000_u64, "closedAtMs": 1_794_330_060_000_u64 },
    ]);
    let api = Router::new().route(
        "/v1/projects/demo/observe/websockets",
        get(move || async move { Json(sessions) }),
    );
    let first = text(
        &call_tool(
            api.clone(),
            "list_websocket_sessions",
            json!({ "limit": 1 }),
        )
        .await,
    );
    let cursor = first["nextCursor"].as_str().expect("cursor").to_owned();
    let second =
        text(&call_tool(api, "list_websocket_sessions", json!({ "cursor": cursor })).await);

    assert_eq!(first["total"], 2);
    assert_eq!(first["records"][0]["status"], "open");
    assert_eq!(first["records"][0]["openedAt"], "2026-11-10T17:00:00.000Z");
    assert!(first["records"][0].get("closedAt").is_none());
    assert_eq!(second["records"][0]["status"], "lost");
    assert_eq!(second["records"][0]["closedAt"], "2026-11-10T17:01:00.000Z");
    assert!(second.get("nextCursor").is_none());
}

#[tokio::test]
async fn invalid_cursors_are_tool_errors_the_model_can_fix() {
    let reply = call_tool(
        Router::new(),
        "list_actors",
        json!({ "cursor": "not-a-cursor" }),
    )
    .await;

    assert_eq!(reply["result"]["isError"], true);
    assert_eq!(text(&reply)["error"]["code"], "invalid_request");
}

async fn call_tool(api: Router, name: &str, mut arguments: Value) -> Value {
    arguments["projectId"] = json!("demo");
    let params = json!({ "name": name, "arguments": arguments });
    rpc(test_router(api).await, "tools/call", params).await
}

fn text(reply: &Value) -> Value {
    let text = reply["result"]["content"][0]["text"]
        .as_str()
        .expect("text");
    serde_json::from_str(text).expect("JSON text")
}

async fn rpc(router: Router, method: &str, params: Value) -> Value {
    rpc_as(router, method, params, None, "secret").await.1
}

async fn rpc_as(
    router: Router,
    method: &str,
    mut params: Value,
    organization: Option<&str>,
    credential: &str,
) -> (StatusCode, Value) {
    params["_meta"] = json!({
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientInfo": { "name": "test", "version": "1.0" },
        "io.modelcontextprotocol/clientCapabilities": {}
    });
    let mut request = Request::post("/mcp")
        .header(header::HOST, "localhost")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::ACCEPT, "application/json, text/event-stream")
        .header(header::AUTHORIZATION, format!("Bearer {credential}"))
        .header("mcp-protocol-version", "2026-07-28")
        .header("mcp-method", method);
    if let Some(organization) = organization {
        request = request.header(ORGANIZATION_HEADER, organization);
    }
    if let Some(name) = params["name"].as_str() {
        request = request.header("mcp-name", name);
    }
    let body = json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params });
    let request = request.body(Body::from(body.to_string())).unwrap();
    let Ok(response) = router.oneshot(request).await;
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (
        status,
        serde_json::from_slice(&bytes).expect("JSON-RPC reply"),
    )
}

async fn test_router(api: Router) -> Router {
    local_router(api, test_admin(&[("demo", "org-test")]).await)
}

async fn test_admin(projects: &[(&str, &str)]) -> AdminService {
    use crate::control_plane::{
        admin::{AdminRegistry, HostLaunchSpec, LocalAdminRegistry},
        service::tests::test_issuer,
    };
    let registry = Arc::new(LocalAdminRegistry::default());
    for (project, organization) in projects {
        registry
            .register_test_deployment(&HostLaunchSpec {
                project_id: (*project).into(),
                organization_id: (*organization).into(),
                image_policy: Default::default(),
                sandboxes: Default::default(),
                source: None,
                code_snapshot: None,
                image_ref: "image".into(),
                working_directory: "/workspace".into(),
                actor_entrypoint: Some("src/actors.ts".into()),
                secret_refs: vec!["must-not-leak".into()],
            })
            .await
            .unwrap();
    }
    AdminService::new(Some("secret".into()), registry, test_issuer().unwrap()).unwrap()
}

#[tokio::test]
async fn lists_only_projects_in_the_authenticated_organization() {
    let admin = test_admin(&[
        ("demo", "org-test"),
        ("second", "org-test"),
        ("private", "org-other"),
    ])
    .await;
    let router = local_router(Router::new(), admin);
    let (_, reply) = rpc_as(
        router,
        "tools/call",
        json!({"name":"list_projects", "arguments":{}}),
        Some("org-test"),
        "secret",
    )
    .await;
    assert_eq!(
        text(&reply)["records"],
        json!([{"projectId":"demo"}, {"projectId":"second"}])
    );
}

#[tokio::test]
async fn every_actor_tool_hides_projects_outside_the_organization() {
    let admin = test_admin(&[("demo", "org-test"), ("private", "org-other")]).await;
    async fn unexpected_request() -> StatusCode {
        panic!("Must not contact a foreign project's API")
    }
    let api = Router::new().fallback(unexpected_request);
    let router = local_router(api, admin);
    for name in [
        "describe_deployment",
        "list_actors",
        "get_actor_state",
        "list_state_history",
        "list_requests",
        "list_logs",
        "get_health",
        "get_actor_resources",
        "list_websocket_sessions",
        "invoke_actor",
    ] {
        let (_, reply) = rpc_as(router.clone(), "tools/call", json!({"name":name, "arguments":{
            "projectId":"private", "organizationId":"org-other", "actorName":"Counter", "actorId":"one", "method":"increment"
        }}), Some("org-test"), "secret").await;
        assert_eq!(reply["result"]["isError"], true, "{name}: {reply}");
        assert_eq!(
            text(&reply)["error"]["code"],
            "not_found",
            "{name}: {reply}"
        );
    }
}

#[tokio::test]
async fn requires_authentication_and_explicit_context_for_multiple_organizations() {
    let admin = test_admin(&[("demo", "org-test"), ("private", "org-other")]).await;
    let router = local_router(Router::new(), admin);
    let (status, _) = rpc_as(
        router.clone(),
        "tools/list",
        json!({}),
        Some("org-test"),
        "wrong",
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = rpc_as(router.clone(), "tools/list", json!({}), None, "secret").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}
