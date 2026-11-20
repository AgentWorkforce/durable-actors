use serde_json::json;

use super::*;
use crate::actor::ActorKey;

#[test]
fn customer_pod_enforces_isolation_and_allows_node_scale_down() -> Result<()> {
    let request = CreateSpareRequest {
        otlp_endpoint: None,
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
        "test-token",
        Some("http://node-cache:7103"),
        None,
        GKE_RUNTIME_CLASS,
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
    assert!(spec.containers[0].env.as_ref().unwrap().iter().any(|var| {
        var.name == "DURABLE_ACTORS_NODE_CACHE_URL"
            && var.value.as_deref() == Some("http://node-cache:7103")
    }));
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
            node: String::new(),
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
    assert_eq!(first.node, "sandbox-node-1");
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
        node: String::new(),
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

#[tokio::test]
async fn the_watch_reports_each_pod_killed_under_its_actor_once() -> Result<()> {
    use crate::platform_events::sandboxes::KillReason;
    use axum::{Router, extract::RawQuery, routing::get};
    let killed = out_of_memory("killed");
    let mut evicted = pod("evicted", "Failed");
    evicted["status"]["reason"] = json!("Evicted");
    evicted["status"]["conditions"] = json!([{
        "type": "Ready",
        "status": "False",
        "lastTransitionTime": "2026-01-01T00:00:05Z",
    }]);
    // The kubelet reports an evicted pod's containers afterwards, which moves when it stopped.
    let mut stopped = evicted.clone();
    stopped["status"]["containerStatuses"] = terminated("Error");
    let items = vec![
        pod("live", "Running"),
        pod("crashed", "Failed"),
        out_of_memory("unclaimed"),
        killed.clone(),
    ];
    let events = vec![
        json!({"type": "MODIFIED", "object": killed}),
        json!({"type": "ADDED", "object": evicted}),
        json!({"type": "MODIFIED", "object": stopped}),
        json!({"type": "ADDED", "object": pod("last", "Running")}),
    ];
    let routes = Router::new().route(
        "/api/v1/namespaces/sandboxes/pods",
        get(move |RawQuery(query): RawQuery| {
            let response = pod_collection(query, items.clone(), events.clone()).0;
            async move { response }
        }),
    );
    let (reported, mut kills) = tokio::sync::mpsc::unbounded_channel();
    let reporting = KillReporting {
        claims: Arc::new(Claimed::sandboxes(["killed", "evicted"])),
        events: Arc::new(ReportedKills(reported)),
    };
    let (cluster, _server) = reporting_cluster(routes, Some(reporting)).await?;
    let mut changes = cluster.changes.clone();
    tokio::time::timeout(Duration::from_secs(5), async {
        while cluster.observed_pod("last", Some("last-uid")).is_none() {
            changes.changed().await?;
        }
        anyhow::Ok(())
    })
    .await??;
    let reported: Vec<_> = std::iter::from_fn(|| kills.try_recv().ok())
        .map(|kill| (kill.actor, kill.sandbox_id, kill.reason, kill.at_ms))
        .collect();
    let new_year = 1_767_225_600_000;
    assert_eq!(
        reported,
        [
            (
                counter(),
                "sandboxes/killed/killed-uid".to_owned(),
                KillReason::OutOfMemory,
                new_year
            ),
            (
                counter(),
                "sandboxes/evicted/evicted-uid".to_owned(),
                KillReason::Evicted,
                new_year + 5000
            ),
        ]
    );
    Ok(())
}

/// The pool forgets a failed assignment's claim as soon as it has deleted the pod, while the
/// kubelet can take longer to say the pod ran out of memory.
#[tokio::test]
async fn a_kill_that_shows_after_its_sandbox_was_retired_still_names_the_actor() -> Result<()> {
    use axum::{
        Json, Router,
        extract::RawQuery,
        routing::{delete, get},
    };
    let (show, shown) = watch::channel(false);
    let routes = Router::new()
        .route(
            "/api/v1/namespaces/sandboxes/pods",
            get(move |RawQuery(query): RawQuery| {
                let response = pod_collection(query, vec![out_of_memory("failed")], Vec::new()).0;
                let mut shown = shown.clone();
                async move {
                    let _ = shown.wait_for(|shown| *shown).await;
                    response
                }
            }),
        )
        .route(
            "/api/v1/namespaces/sandboxes/pods/failed",
            delete(|| async {
                Json(json!({"apiVersion":"v1","kind":"Status","status":"Success"}))
            }),
        );
    let (reported, mut kills) = tokio::sync::mpsc::unbounded_channel();
    let claims = Arc::new(Claimed::sandboxes(["failed"]));
    let reporting = KillReporting {
        claims: claims.clone(),
        events: Arc::new(ReportedKills(reported)),
    };
    let (cluster, _server) = reporting_cluster(routes, Some(reporting)).await?;
    cluster
        .retire_spare(&SpareHandle {
            node: String::new(),
            name: "failed".into(),
            resource_id: "sandboxes/failed/failed-uid".into(),
            route: String::new(),
            canonical_region: String::new(),
            control_route: String::new(),
            control_token: String::new(),
        })
        .await?;
    claims.0.lock().unwrap().clear();
    show.send_replace(true);
    let kill = tokio::time::timeout(Duration::from_secs(5), kills.recv())
        .await?
        .context("the watch stopped")?;
    assert_eq!(kill.actor, counter());
    Ok(())
}

struct ReportedKills(tokio::sync::mpsc::UnboundedSender<SandboxKill>);

impl PlatformEvents<SandboxKill> for ReportedKills {
    fn record(&self, kill: SandboxKill) {
        let _ = self.0.send(kill);
    }
}

/// The sandboxes claimed for `counter()`.
struct Claimed(std::sync::Mutex<Vec<&'static str>>);

impl Claimed {
    fn sandboxes(names: impl IntoIterator<Item = &'static str>) -> Self {
        Self(names.into_iter().collect::<Vec<_>>().into())
    }
}

#[async_trait]
impl ClaimedActors for Claimed {
    async fn actor_of(&self, sandbox: &str) -> Result<Option<ActorKey>> {
        Ok(self.0.lock().unwrap().contains(&sandbox).then(counter))
    }
}

fn counter() -> ActorKey {
    ActorKey {
        project_id: "project".into(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
    }
}

fn out_of_memory(name: &str) -> serde_json::Value {
    let mut pod = pod(name, "Failed");
    pod["status"]["containerStatuses"] = terminated("OOMKilled");
    pod
}

fn terminated(reason: &str) -> serde_json::Value {
    json!([{
        "name": "runtime",
        "image": "runtime",
        "imageID": "",
        "ready": false,
        "restartCount": 0,
        "state": {"terminated": {
            "exitCode": 137,
            "reason": reason,
            "finishedAt": "2026-01-01T00:00:00Z",
        }},
    }])
}

async fn fake_cluster(
    routes: axum::Router,
) -> Result<(Kubernetes, AbortOnDropHandle<std::io::Result<()>>)> {
    reporting_cluster(routes, None).await
}

async fn reporting_cluster(
    routes: axum::Router,
    kills: Option<KillReporting>,
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
            node_cache_url: None,
            sqlite_slot_gib: None,
            runtime_class: GKE_RUNTIME_CLASS.into(),
        },
        kills,
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
    pod["spec"] = json!({"nodeName":"sandbox-node-1","containers":[]});
    json!({"type":"ADDED","object":pod})
}

fn spare_request(name: &str) -> CreateSpareRequest {
    CreateSpareRequest {
        otlp_endpoint: None,
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
    let pod = spare_json();
    let resources = &pod["spec"]["containers"][0]["resources"];
    assert_eq!(resources["requests"]["ephemeral-storage"], "256Mi");
    assert_eq!(resources["limits"]["ephemeral-storage"], "8Gi");
    for volume in pod["spec"]["volumes"].as_array().unwrap() {
        assert!(volume["emptyDir"]["medium"].is_null());
        assert!(volume["emptyDir"]["sizeLimit"].is_string());
    }
}

#[test]
fn runtime_binaries_run_from_a_read_only_copy() {
    let pod = spare_json();
    let runtime = &pod["spec"]["containers"][0];
    assert_eq!(runtime["command"][0], "/runtime/durable-actors");
    assert!(
        runtime["volumeMounts"]
            .as_array()
            .unwrap()
            .iter()
            .any(|mount| mount["mountPath"] == "/runtime" && mount["readOnly"] == true)
    );
    let copy = &pod["spec"]["initContainers"][0];
    assert_eq!(copy["volumeMounts"][0]["mountPath"], "/runtime");
    assert_eq!(copy["resources"], runtime["resources"]);
}

#[test]
fn spare_pod_manifest_only_uses_fields_kubernetes_knows() {
    let written: serde_json::Value = serde_yaml::from_str(SPARE_POD).unwrap();
    let parsed: Pod = serde_yaml::from_str(SPARE_POD).unwrap();
    assert_eq!(serde_json::to_value(parsed).unwrap(), written);
}

fn spare_json() -> serde_json::Value {
    serde_json::to_value(spare_under(GKE_RUNTIME_CLASS)).unwrap()
}

fn spare_under(runtime_class: &str) -> Pod {
    spare_pod(
        &spare_request("actor"),
        &["us-west4-a".into()],
        "token",
        None,
        None,
        runtime_class,
    )
    .unwrap()
}

#[test]
fn a_spare_gets_a_sized_database_slot_the_node_cache_can_reach() -> Result<()> {
    let pod = spare_pod(
        &spare_request("actor"),
        &["us-west4-a".into()],
        "token",
        Some("http://node-cache:7103"),
        Some(8),
        "gvisor-with-overhead",
    )?;
    let annotations = pod.metadata.annotations.unwrap();
    assert_eq!(annotations["dev.gvisor.spec.mount.slot.share"], "shared");
    let spec = pod.spec.unwrap();
    let slot = spec
        .volumes
        .unwrap()
        .into_iter()
        .find(|volume| volume.name == "slot")
        .unwrap();
    assert_eq!(
        slot.empty_dir.unwrap().size_limit,
        Some(Quantity("8Gi".into()))
    );
    assert!(
        spec.containers[0]
            .volume_mounts
            .as_ref()
            .unwrap()
            .iter()
            .any(|mount| mount.name == "slot" && mount.mount_path == "/slot")
    );
    Ok(())
}

/// The expected values are what GKE writes on a spare under its own class.
#[test]
fn a_spare_under_another_runtime_class_carries_the_mount_annotations_gke_would_add() {
    assert_eq!(gvisor_mounts(spare_under(GKE_RUNTIME_CLASS)), []);
    let spare = spare_under("gvisor-with-overhead");
    assert_eq!(
        spare.spec.as_ref().unwrap().runtime_class_name.as_deref(),
        Some("gvisor-with-overhead")
    );
    assert_eq!(
        gvisor_mounts(spare),
        [
            ("customer.options", "rw,rprivate"),
            ("customer.share", "container"),
            ("customer.type", "bind"),
            ("runtime.options", "rw,rprivate"),
            ("runtime.share", "pod"),
            ("runtime.type", "bind"),
            ("tmp.options", "rw,rprivate"),
            ("tmp.share", "container"),
            ("tmp.type", "bind"),
        ]
        .map(|(mount, value)| (mount.to_owned(), value.to_owned()))
    );
}

fn gvisor_mounts(pod: Pod) -> Vec<(String, String)> {
    pod.metadata
        .annotations
        .unwrap()
        .into_iter()
        .filter_map(|(name, value)| {
            let mount = name.strip_prefix("dev.gvisor.spec.mount.")?;
            Some((mount.to_owned(), value))
        })
        .collect()
}
