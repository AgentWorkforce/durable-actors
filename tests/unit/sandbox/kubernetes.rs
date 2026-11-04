use super::*;

#[test]
fn customer_pod_enforces_isolation_and_allows_node_scale_down() -> Result<()> {
    let request = CreateSpareRequest {
        control_plane_url: Some("http://control:7100".into()),
        kind: SpareKind::Actor,
        name: "warm-one".into(),
        image_ref: "runtime@sha256:abc".into(),
        canonical_region: "north-america-west".into(),
        resources: ResourceLimits {
            cpu_millis: 2000,
            cpu_limit_millis: None,
            memory_mib: 4096,
            memory_limit_mib: Some(4096),
        },
    };
    let pod = spare_pod(
        &request,
        &[
            "us-west4-a".into(),
            "us-west4-b".into(),
            "us-west4-c".into(),
        ],
        &PodScheduling::default(),
        "test-token",
    )?;
    assert_eq!(
        pod.metadata
            .annotations
            .as_ref()
            .and_then(
                |annotations| annotations.get("cluster-autoscaler.kubernetes.io/safe-to-evict")
            )
            .map(String::as_str),
        Some("true"),
    );
    let spec = pod.spec.unwrap();
    assert_eq!(spec.runtime_class_name.as_deref(), Some("gvisor"));
    assert_eq!(
        spec.node_selector.as_ref().unwrap()["sandbox.gke.io/runtime"],
        "gvisor"
    );
    assert!(spec.tolerations.is_none());
    assert_eq!(
        spec.security_context
            .as_ref()
            .and_then(|context| context.seccomp_profile.as_ref())
            .map(|profile| profile.type_.as_str()),
        Some("RuntimeDefault")
    );
    assert_eq!(spec.automount_service_account_token, Some(false));
    let affinity = spec
        .affinity
        .as_ref()
        .unwrap()
        .node_affinity
        .as_ref()
        .unwrap()
        .required_during_scheduling_ignored_during_execution
        .as_ref()
        .unwrap();
    assert_eq!(
        affinity.node_selector_terms[0]
            .match_expressions
            .as_ref()
            .unwrap()[0]
            .values
            .as_ref()
            .unwrap(),
        &["us-west4-a", "us-west4-b", "us-west4-c"]
    );
    let spread = &spec.topology_spread_constraints.as_ref().unwrap()[0];
    assert_eq!(spread.topology_key, "topology.kubernetes.io/zone");
    assert_eq!(spread.when_unsatisfiable, "ScheduleAnyway");
    assert_eq!(
        spec.containers[0]
            .resources
            .as_ref()
            .unwrap()
            .limits
            .as_ref()
            .unwrap()["memory"]
            .0,
        "4096Mi"
    );
    assert_eq!(
        spec.containers[0]
            .security_context
            .as_ref()
            .unwrap()
            .allow_privilege_escalation,
        Some(false)
    );
    Ok(())
}

#[tokio::test]
async fn retirement_recovers_a_lost_create_reply_and_uses_a_uid_precondition() -> Result<()> {
    use axum::{Json, Router, routing::get};
    let deleted = Arc::new(std::sync::Mutex::new(None));
    let recorded = deleted.clone();
    let pod = json!({"apiVersion":"v1","kind":"Pod","metadata":{"name":"reserved-pod","namespace":"sandboxes","uid":"created-uid","labels":{"app.kubernetes.io/managed-by":"terse"}}});
    let routes = Router::new().route(
        "/api/v1/namespaces/sandboxes/pods/reserved-pod",
        get(move || async move { Json(pod) }).delete(
            move |Json(body): Json<serde_json::Value>| async move {
                *recorded.lock().unwrap() = Some(body);
                Json(json!({"apiVersion":"v1","kind":"Status","status":"Success"}))
            },
        ),
    );
    let (cluster, _server) = fake_cluster(routes).await?;
    cluster
        .retire_spare(&SpareHandle {
            name: "reserved-pod".into(),
            resource_id: String::new(),
            route: String::new(),
            canonical_region: String::new(),
            control_route: String::new(),
            control_token: String::new(),
        })
        .await?;
    assert_eq!(
        deleted.lock().unwrap().as_ref().unwrap()["preconditions"]["uid"],
        "created-uid"
    );
    Ok(())
}

#[tokio::test]
async fn spares_start_from_one_shared_watch_and_retry_throttled_creates() -> Result<()> {
    use axum::{
        Json, Router, extract::RawQuery, http::StatusCode, response::IntoResponse, routing::get,
    };
    let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (listed, created) = (requests.clone(), requests.clone());
    let events = vec![
        ready_event("warm-a", "10.0.0.1"),
        ready_event("warm-b", "10.0.0.2"),
    ];
    let routes = Router::new().route(
        "/api/v1/namespaces/sandboxes/pods",
        get(move |RawQuery(query): RawQuery| {
            let response = pod_collection(query, Vec::new(), events.clone());
            listed
                .lock()
                .unwrap()
                .push(if response.1 { "watch" } else { "list" });
            async move { response.0 }
        })
        .post(move |Json(mut pod): Json<serde_json::Value>| async move {
            let throttled = {
                let mut requests = created.lock().unwrap();
                requests.push("create");
                requests
                    .iter()
                    .filter(|request| **request == "create")
                    .count()
                    == 1
            };
            if throttled {
                return (
                    StatusCode::TOO_MANY_REQUESTS,
                    "Too many requests, please try again later.\n",
                )
                    .into_response();
            }
            let name = pod["metadata"]["name"].as_str().unwrap().to_owned();
            pod["metadata"]["namespace"] = json!("sandboxes");
            pod["metadata"]["uid"] = json!(format!("{name}-uid"));
            (StatusCode::CREATED, Json(pod)).into_response()
        }),
    );
    let (cluster, _server) = fake_cluster(routes).await?;
    let (first, second) = futures_util::future::try_join(
        cluster.create_spare(&spare_request("warm-a")),
        cluster.create_spare(&spare_request("warm-b")),
    )
    .await?;
    assert_eq!(first.resource_id, "sandboxes/warm-a/warm-a-uid");
    assert_eq!(first.route, "http://10.0.0.1:7101");
    assert_eq!(second.control_route, "http://10.0.0.2:7102");
    let mut requests = requests.lock().unwrap().clone();
    requests.sort();
    assert_eq!(requests, ["create", "create", "create", "list", "watch"]);
    Ok(())
}

#[tokio::test]
async fn stopped_spares_reads_the_watch_and_confirms_pods_it_has_not_seen() -> Result<()> {
    use axum::{
        Json, Router,
        extract::{Path, RawQuery},
        http::StatusCode,
        response::IntoResponse,
        routing::get,
    };
    let items: Vec<_> = [
        ("live", "Running"),
        ("pending", "Pending"),
        ("unknown", "Unknown"),
        ("done", "Succeeded"),
        ("crashed", "Failed"),
        ("replaced", "Running"),
    ]
    .into_iter()
    .map(|(name, phase)| pod(name, phase))
    .collect();
    let fail = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let fail_request = fail.clone();
    let routes = Router::new()
        .route("/api/v1/namespaces/sandboxes/pods", get(move |RawQuery(query): RawQuery| {
            let response = pod_collection(query, items.clone(), Vec::new()).0;
            async move { response }
        }))
        .route("/api/v1/namespaces/sandboxes/pods/{name}", get(move |Path(name): Path<String>| {
            let fail = fail_request.load(std::sync::atomic::Ordering::SeqCst);
            async move {
                match name.as_str() {
                    _ if fail => StatusCode::SERVICE_UNAVAILABLE.into_response(),
                    "fresh" | "replaced" => Json(pod(&name, "Running")).into_response(),
                    _ => (StatusCode::NOT_FOUND, Json(json!({"apiVersion":"v1","kind":"Status","status":"Failure","reason":"NotFound","code":404}))).into_response(),
                }
            }
        }));
    let (cluster, _server) = fake_cluster(routes).await?;
    let spares: Vec<_> = [
        "live", "pending", "unknown", "fresh", "done", "crashed", "missing", "replaced",
    ]
    .into_iter()
    .map(|name| SpareHandle {
        name: name.into(),
        resource_id: format!(
            "sandboxes/{name}/{}-uid",
            if name == "replaced" { "old" } else { name }
        ),
        route: String::new(),
        canonical_region: String::new(),
        control_route: String::new(),
        control_token: String::new(),
    })
    .collect();
    let stopped = cluster.stopped_spares(&spares).await?;
    assert_eq!(
        stopped,
        spares[4..]
            .iter()
            .map(|s| s.resource_id.clone())
            .collect::<Vec<_>>()
    );
    fail.store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(cluster.stopped_spares(&spares).await.is_err());
    Ok(())
}

#[tokio::test]
async fn finished_pods_are_reaped_once_from_the_watch() -> Result<()> {
    use axum::{
        Json, Router,
        extract::{Path, RawQuery},
        routing::{delete, get},
    };
    let mut leaving = pod("leaving", "Succeeded");
    leaving["metadata"]["deletionTimestamp"] = json!("2026-01-01T00:00:00Z");
    let items = vec![
        pod("done", "Succeeded"),
        pod("crashed", "Failed"),
        pod("live", "Running"),
        leaving,
    ];
    let deleted = Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorded = deleted.clone();
    let routes = Router::new()
        .route(
            "/api/v1/namespaces/sandboxes/pods",
            get(move |RawQuery(query): RawQuery| {
                let response = pod_collection(query, items.clone(), Vec::new()).0;
                async move { response }
            }),
        )
        .route(
            "/api/v1/namespaces/sandboxes/pods/{name}",
            delete(move |Path(name): Path<String>| async move {
                recorded.lock().unwrap().push(name);
                Json(json!({"apiVersion":"v1","kind":"Status","status":"Success"}))
            }),
        );
    let (cluster, _server) = fake_cluster(routes).await?;
    cluster.observed.wait_until_ready().await?;
    cluster.reap_finished().await?;
    let deleted = deleted.lock().unwrap().clone();
    assert!(
        deleted.contains(&"done".to_owned()) && deleted.contains(&"crashed".to_owned()),
        "{deleted:?}"
    );
    assert!(
        !deleted
            .iter()
            .any(|name| name == "live" || name == "leaving"),
        "{deleted:?}"
    );
    Ok(())
}

async fn fake_cluster(
    routes: axum::Router,
) -> Result<(Kubernetes, AbortOnDropHandle<std::io::Result<()>>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let config = kube::Config::new(format!("http://{}", listener.local_addr()?).parse()?);
    let server =
        AbortOnDropHandle::new(tokio::spawn(
            async move { axum::serve(listener, routes).await },
        ));
    let cluster = Kubernetes::new(
        Client::try_from(config)?,
        GkeConfig {
            namespace: "sandboxes".into(),
            zones: BTreeMap::from([("north-america-west".into(), vec!["us-west4-a".into()])]),
            reap_concurrency: 4,
            scheduling: PodScheduling::default(),
        },
    );
    Ok((cluster, server))
}

/// Serves a pod list, or a watch that sends `events` and then stays open.
fn pod_collection(
    query: Option<String>,
    items: Vec<serde_json::Value>,
    events: Vec<serde_json::Value>,
) -> (axum::response::Response, bool) {
    use axum::response::IntoResponse;
    if !query.unwrap_or_default().contains("watch=true") {
        let list = json!({"apiVersion":"v1","kind":"PodList","metadata":{"resourceVersion":"10"},"items":items});
        return (axum::Json(list).into_response(), false);
    }
    let lines = events
        .into_iter()
        .map(|event| Ok::<_, std::io::Error>(bytes::Bytes::from(format!("{event}\n"))));
    let stream = futures_util::stream::iter(lines).chain(futures_util::stream::pending());
    (axum::body::Body::from_stream(stream).into_response(), true)
}

fn pod(name: &str, phase: &str) -> serde_json::Value {
    json!({"apiVersion":"v1","kind":"Pod","metadata":{"name":name,"namespace":"sandboxes","uid":format!("{name}-uid"),"resourceVersion":"11"},"status":{"phase":phase}})
}

fn ready_event(name: &str, ip: &str) -> serde_json::Value {
    let mut pod = pod(name, "Running");
    pod["status"]["podIP"] = json!(ip);
    pod["status"]["conditions"] = json!([{"type":"Ready","status":"True"}]);
    json!({"type":"ADDED","object":pod})
}

fn spare_request(name: &str) -> CreateSpareRequest {
    CreateSpareRequest {
        control_plane_url: Some("http://control:7100".into()),
        kind: SpareKind::Actor,
        name: name.into(),
        image_ref: "runtime@sha256:abc".into(),
        canonical_region: "north-america-west".into(),
        resources: ResourceLimits::default(),
    }
}

#[test]
fn temporary_storage_is_disk_backed_and_bounded() {
    let pod = base_pod(
        "actor",
        "image",
        &["us-west4-a".into()],
        &ResourceLimits {
            cpu_millis: 500,
            cpu_limit_millis: None,
            memory_mib: 143,
            memory_limit_mib: Some(143),
        },
    );
    let resources = &pod["spec"]["containers"][0]["resources"];
    assert_eq!(resources["requests"]["ephemeral-storage"], "256Mi");
    assert_eq!(resources["limits"]["ephemeral-storage"], "8Gi");
    for volume in pod["spec"]["volumes"].as_array().unwrap() {
        assert!(volume["emptyDir"]["medium"].is_null());
        assert!(volume["emptyDir"]["sizeLimit"].is_string());
    }
}

#[test]
fn pods_run_on_the_configured_runtime_and_reserved_nodes() -> Result<()> {
    let scheduling = PodScheduling {
        runtime_class: None,
        node_selector: BTreeMap::from([("durable-actors/role".into(), "actor-host".into())]),
        tolerations: vec![serde_json::json!({
            "key": "durable-actors/actor-host", "operator": "Exists", "effect": "NoSchedule"
        })],
    };
    let pod = spare_pod(
        &spare_request("runc-one"),
        &["us-west-2a".into()],
        &scheduling,
        "test-token",
    )?;
    let spec = pod.spec.unwrap();
    assert_eq!(spec.runtime_class_name, None);
    assert_eq!(
        spec.node_selector,
        Some(BTreeMap::from([(
            "durable-actors/role".into(),
            "actor-host".into()
        )]))
    );
    let toleration = &spec.tolerations.unwrap()[0];
    assert_eq!(toleration.key.as_deref(), Some("durable-actors/actor-host"));
    assert_eq!(toleration.effect.as_deref(), Some("NoSchedule"));
    assert_eq!(spec.automount_service_account_token, Some(false));
    let container = &spec.containers[0];
    let security = container.security_context.as_ref().unwrap();
    assert_eq!(security.read_only_root_filesystem, Some(true));
    assert_eq!(security.allow_privilege_escalation, Some(false));
    Ok(())
}
