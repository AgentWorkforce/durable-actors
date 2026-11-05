use crate::request_tracking::HostState;
use std::{sync::Arc, time::Duration};

use anyhow::Result;
use aws_lc_rs::{rand::SystemRandom, signature::Ed25519KeyPair};
use base64::{Engine, engine::general_purpose::STANDARD};
use reqwest::StatusCode;
use serde_json::{Value, json};

use crate::{
    actor::ActorKey,
    actor_lifecycle::{LifecycleEvent, LifecycleKind, LifecycleReport, LifecycleStore},
    bucket::{RuntimeStorage, testing::RuntimeFixture},
    clock::{Clock, SystemClock},
    host::HostId,
    host_leases::HostLeaseRequest,
};

use super::{ActorJwtIssuer, admin::AdminService, inspection::ActorInspector};
use crate::request_traces::{
    RequestKind, RequestOutcome, RequestTrace, TraceEvent,
    live::InProcessFeed,
    logs::ActorLog,
    otlp::{self, Signal},
    persistence::sqlite::SqliteTracePersistence,
};
use opentelemetry_proto::tonic::{
    collector::{logs::v1::ExportLogsServiceRequest, trace::v1::ExportTraceServiceRequest},
    common::v1::{AnyValue, KeyValue, any_value},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    trace::v1::{ResourceSpans, ScopeSpans, Span},
};
use prost::Message;

#[tokio::test]
async fn inventory_reads_and_streams_filter_projects_before_grouping() -> Result<()> {
    let fixture = Fixture::start().await?;
    for (project, id) in [("alpha", "same"), ("beta", "same"), ("beta", "extra")] {
        let actor = ActorKey {
            project_id: project.into(),
            ..fixture.actor(id)
        };
        fixture
            .report_lifecycle(actor, LifecycleKind::Started, 1, vec![])
            .await?;
    }
    let page: Value = fixture
        .get("/v1/projects/alpha/observe/actors")
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(page["actors"][0]["live"], 1);
    assert_eq!(page["actors"][0]["instances"].as_array().unwrap().len(), 1);
    let mut stream = fixture
        .get("/v1/projects/alpha/observe/events")
        .await?
        .error_for_status()?;
    assert_eq!(stream_inventory(&mut stream).await?, page);
    let empty: Value = fixture
        .get("/v1/projects/empty/observe/actors")
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(empty["actors"], json!([]));
    Ok(())
}

#[tokio::test]
async fn project_observability_isolates_history_metrics_and_streams() -> Result<()> {
    let fixture = Fixture::start().await?;
    for (project, duration) in [("alpha", 10), ("beta", 90)] {
        for index in 0..2 {
            let trace = serde_json::from_value(json!({
                "projectId": project, "requestId": format!("{project}-{index}"),
                "actorName": "Room", "actorId": "same", "kind": "websocket",
                "operation": "onConnect", "connectionId": "same",
                "startedAtMs": index + 1, "durationMs": duration, "routingMs": 25.0,
                "queueWaitMs": duration, "outcome": "completed"
            }))?;
            fixture.record(trace).await?;
        }
    }
    let base = "/v1/projects/alpha/observe";
    let page: Value = fixture
        .get(&format!("{base}/requests?limit=1"))
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(page["records"].as_array().unwrap().len(), 1);
    assert_eq!(page["records"][0]["projectId"], "alpha");
    assert_eq!(page["records"][0]["routingMs"], 25.0);
    let metrics: Value = fixture
        .get(&format!("{base}/metrics"))
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(metrics["total"]["count"], 2);
    assert_eq!(metrics["total"]["p95"], 10.0);
    let waits: Value = fixture
        .get(&format!("{base}/queue-waits"))
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(waits[0]["admitted"], 2);
    assert_eq!(waits[0]["averageMs"], 10.0);
    let sockets: Value = fixture
        .get(&format!("{base}/websockets"))
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(sockets.as_array().unwrap().len(), 1);
    let mut stream = fixture
        .get(&format!("{base}/requests/events"))
        .await?
        .error_for_status()?;
    let chunk = tokio::time::timeout(Duration::from_secs(5), stream.chunk())
        .await??
        .unwrap();
    let event = String::from_utf8(chunk.to_vec())?;
    assert!(event.contains("alpha"));
    assert!(!event.contains("beta"));
    for (route, cursor) in [
        ("requests?cursor=", "nextCursor"),
        ("requests/events?after=", "resumeCursor"),
    ] {
        let url = format!(
            "/v1/projects/beta/observe/{route}{}",
            page[cursor].as_str().unwrap()
        );
        assert_eq!(fixture.get(&url).await?.status(), StatusCode::BAD_REQUEST);
    }
    for route in [
        "requests",
        "requests/events",
        "logs",
        "logs/events",
        "metrics",
        "queue-waits",
        "websockets",
    ] {
        let url = format!("/v1/projects/bad!/observe/{route}");
        assert_eq!(fixture.get(&url).await?.status(), StatusCode::BAD_REQUEST);
    }
    Ok(())
}

#[tokio::test]
async fn durability_endpoint_is_not_exposed() -> Result<()> {
    let fixture = Fixture::start().await?;
    assert_eq!(
        fixture.get("/v1/durability").await?.status(),
        StatusCode::NOT_FOUND
    );
    Ok(())
}

#[tokio::test]
async fn observability_requires_admin_credentials() -> Result<()> {
    let fixture = Fixture::start().await?;
    let token = fixture
        .issuer
        .issue_host(
            "org-test",
            &fixture.host,
            &uuid::Uuid::new_v4().to_string(),
            "r1",
            "north-america-east",
            &fixture.actor("one"),
        )?
        .token;
    for path in [
        "/v1/projects/default/observe/actors",
        "/v1/projects/default/observe/events",
        "/v1/projects/default/observe/requests/events",
        "/v1/projects/default/observe/requests",
        "/v1/projects/default/observe/metrics",
        "/v1/projects/default/observe/queue-waits",
        "/v1/projects/default/observe/websockets",
        "/v1/projects/default/observe/state?actorName=Room&actorId=one",
        "/v1/projects/default/observe/state/history?actorName=Room&actorId=one",
    ] {
        for credential in [None, Some(""), Some("wrong"), Some(token.as_str())] {
            let mut request = fixture.client.get(format!("{}{path}", fixture.origin));
            if let Some(credential) = credential {
                request = request.bearer_auth(credential);
            }
            assert_eq!(request.send().await?.status(), StatusCode::UNAUTHORIZED);
        }
    }
    Ok(())
}

struct Fixture {
    lifecycle: LifecycleStore,
    traces: crate::request_traces::TraceStore,
    telemetry: Arc<SqliteTracePersistence>,
    live: InProcessFeed,
    changes: crate::postgres::notifications::ChangeFeed,
    admin: AdminService,
    _runtime: RuntimeFixture,
    store: Arc<RuntimeStorage>,
    host: HostId,
    issuer: ActorJwtIssuer,
    origin: String,
    client: reqwest::Client,
    server: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl Fixture {
    async fn start() -> Result<Self> {
        Self::start_with_telemetry(SqliteTracePersistence::in_memory()).await
    }

    async fn start_with_telemetry(telemetry: SqliteTracePersistence) -> Result<Self> {
        let telemetry = Arc::new(telemetry);
        let live = InProcessFeed::default();
        let traces =
            crate::request_traces::TraceStore::new(telemetry.clone(), Arc::new(live.clone()));
        let runtime = RuntimeFixture::new()?;
        let store = runtime.runtime.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let origin = format!("http://{}", listener.local_addr()?);
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())?;
        let issuer = ActorJwtIssuer::from_base64_pkcs8(
            &STANDARD.encode(pkcs8.as_ref()),
            "key",
            "issuer",
            "authority",
            "invocation",
            Duration::from_secs(60),
        )?;
        let admin = AdminService::new(
            Some("api-key".into()),
            Arc::new(super::admin::LocalAdminRegistry::default()),
            issuer.clone(),
        )?;
        let changes = crate::postgres::notifications::ChangeFeed::default();
        let lifecycle = LifecycleStore::default();
        let inspector =
            ActorInspector::new(Arc::new(lifecycle.clone()), store.clone(), changes.clone())
                .with_traces(traces.clone());
        let routes = super::inspection::router(inspector, admin.clone());
        let server = tokio::spawn(async { axum::serve(listener, routes).await });
        let host = HostId::new("host.v3.test");
        Ok(Self {
            lifecycle,
            traces,
            telemetry,
            live,
            changes,
            admin,
            _runtime: runtime,
            store,
            host,
            issuer,
            origin,
            client: reqwest::Client::new(),
            server,
        })
    }

    fn actor(&self, id: &str) -> ActorKey {
        ActorKey {
            project_id: "default".into(),
            actor_name: "Room.with.dots".into(),
            actor_id: id.into(),
        }
    }

    async fn report_lifecycle(
        &self,
        actor: ActorKey,
        kind: LifecycleKind,
        sequence: u64,
        waiting: Vec<crate::host_leases::WaitingOperation>,
    ) -> Result<()> {
        let now = SystemClock.now_ms()?;
        self.lifecycle
            .record(LifecycleEvent {
                organization_id: Some("org-test".into()),
                event_id: format!("session:{sequence}"),
                actor,
                host_id: self.host.to_string(),
                session_id: "session".into(),
                region: "north-america-east".into(),
                received_at_ms: now,
                report: LifecycleReport {
                    kind,
                    owner_epoch: 1,
                    sequence,
                    started_at_ms: 1,
                    occurred_at_ms: now,
                    resources: Default::default(),
                    resident: Some(kind != LifecycleKind::Stopped),
                    last_active_at_ms: None,
                    waiting,
                },
            })
            .await
    }

    async fn record(&self, trace: RequestTrace) -> Result<()> {
        let event = TraceEvent {
            event_id: uuid::Uuid::new_v4().to_string(),
            host_id: "host".into(),
            session_id: "session".into(),
            trace,
        };
        self.telemetry.append(std::slice::from_ref(&event)).await?;
        self.live.publish(
            otlp::live_channel(&event.trace.project_id, Signal::Requests),
            span_request(&event).encode_to_vec(),
        );
        Ok(())
    }

    async fn get(&self, path: &str) -> Result<reqwest::Response> {
        Ok(self
            .client
            .get(format!("{}{path}", self.origin))
            .bearer_auth("api-key")
            .send()
            .await?)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

#[tokio::test]
async fn inventory_stream_follows_lifecycle_reports_and_clears_stopped_queues() -> Result<()> {
    let fixture = Fixture::start().await?;
    let actor = fixture.actor("unsaved");
    fixture
        .report_lifecycle(actor.clone(), LifecycleKind::Started, 1, vec![])
        .await?;
    let mut stream = fixture
        .get("/v1/projects/default/observe/events")
        .await?
        .error_for_status()?;
    let first = stream_inventory(&mut stream).await?;
    assert!(first.get("namespaceId").is_none());
    assert_eq!(first["actors"][0]["live"], 1);
    let waiting = vec![crate::host_leases::WaitingOperation {
        id: "queued-1".into(),
        operation: "sendMessage".into(),
    }];
    fixture
        .report_lifecycle(actor.clone(), LifecycleKind::Heartbeat, 2, waiting)
        .await?;
    fixture.changes.notify().await;
    let connected = stream_inventory(&mut stream).await?;
    assert_eq!(connected["actors"][0]["live"], 1);
    assert_eq!(
        connected["actors"][0]["instances"][0]["waiting"][0]["operation"],
        "sendMessage"
    );
    fixture
        .report_lifecycle(actor, LifecycleKind::Stopped, 3, vec![])
        .await?;
    fixture.changes.notify().await;
    let expired = stream_inventory(&mut stream).await?;
    assert_eq!(expired["actors"][0]["dormant"], 1);
    assert_eq!(expired["actors"][0]["instances"][0]["waiting"], json!([]));
    assert_eq!(
        expired["actors"][0]["instances"][0]["connections"],
        json!([])
    );
    let inventory: Value = fixture
        .get("/v1/projects/default/observe/actors")
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(inventory["actors"][0]["dormant"], 1);
    assert_eq!(inventory["actors"][0]["instances"][0]["waiting"], json!([]));
    assert_eq!(
        inventory["actors"][0]["instances"][0]["connections"],
        json!([])
    );
    Ok(())
}

async fn stream_inventory(stream: &mut reqwest::Response) -> Result<Value> {
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut bytes = Vec::new();
        loop {
            bytes.extend(
                stream
                    .chunk()
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("stream ended"))?,
            );
            let text = String::from_utf8_lossy(&bytes);
            if text.ends_with("\n\n") {
                if let Some(data) = text.lines().find_map(|line| line.strip_prefix("data: ")) {
                    return serde_json::from_str(data).map_err(Into::into);
                }
            }
        }
    })
    .await?
}

#[tokio::test]
async fn inventory_includes_unused_deployed_types_without_loading_actors() -> Result<()> {
    let fixture = Fixture::start().await?;
    let contract = super::contracts::PublicActorContract::new(serde_json::from_str(
        include_str!("../../../sdk/tests/fixtures/public-contract.json"),
    )?)?;
    fixture
        .admin
        .register_deployment(
            &super::admin::HostLaunchSpec {
                image_policy: Default::default(),
                organization_id: "org-test".into(),
                sandboxes: Default::default(),
                project_id: "default".into(),
                source: None,
                code_snapshot: Some(crate::sandbox::testing::code_artifact(1)),
                image_ref: "test-image".into(),
                working_directory: "/customer".into(),
                actor_entrypoint: Some("actors.mjs".into()),
                secret_refs: vec![],
            },
            Some(&contract),
        )
        .await?;
    let inventory: Value = fixture
        .get("/v1/projects/default/observe/actors")
        .await?
        .error_for_status()?
        .json()
        .await?;
    let row = inventory["actors"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["actorName"] == "ChatRoom")
        .unwrap();
    assert_eq!(row["live"], 0);
    assert_eq!(row["dormant"], 0);
    assert_eq!(row["instances"], json!([]));
    assert_eq!(row["methods"], json!(["clear", "sendMessage"]));
    let mut stream = fixture
        .get("/v1/projects/default/observe/events")
        .await?
        .error_for_status()?;
    assert_eq!(stream_inventory(&mut stream).await?, inventory);
    Ok(())
}

#[tokio::test]
async fn request_history_streams_distinct_records_and_replays_on_reconnect() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("request-traces.sqlite3");
    let fixture = Fixture::start_with_telemetry(SqliteTracePersistence::new(path.clone())).await?;
    let mut stream = fixture
        .get("/v1/projects/default/observe/requests/events")
        .await?
        .error_for_status()?;
    assert_eq!(stream.headers()["cache-control"], "no-store");
    let empty = stream_inventory(&mut stream).await?;
    assert_eq!(empty["records"], json!([]));
    for id in ["first", "second"] {
        fixture
            .record(request_trace(
                id,
                "Room",
                "one",
                1000,
                RequestOutcome::Completed,
            ))
            .await?;
        let page = stream_inventory(&mut stream).await?;
        assert_eq!(page["records"].as_array().unwrap().len(), 1);
        assert_eq!(page["records"][0]["requestId"], id);
        assert_eq!(page["records"][0]["queueWaitMs"], 10.0);
    }
    drop(stream);
    let snapshot = serde_json::to_value(
        fixture
            .traces
            .replay("default", &Default::default())
            .await?,
    )?;
    let mut replay = fixture
        .get("/v1/projects/default/observe/requests/events")
        .await?;
    assert_eq!(stream_inventory(&mut replay).await?, snapshot);
    assert_eq!(snapshot["records"].as_array().unwrap().len(), 2);
    drop(replay);
    drop(fixture);
    let restarted = Fixture::start_with_telemetry(SqliteTracePersistence::new(path)).await?;
    let mut replay = restarted
        .get("/v1/projects/default/observe/requests/events")
        .await?;
    let restored = stream_inventory(&mut replay).await?;
    assert_eq!(restored["records"], snapshot["records"]);
    assert_eq!(restored["epoch"], snapshot["epoch"]);
    Ok(())
}

#[tokio::test]
async fn log_filters_apply_to_history_live_tail_and_histogram() -> Result<()> {
    let fixture = Fixture::start().await?;
    let log = |id: &str, request: Option<&str>, level: &str| ActorLog {
        log_id: id.into(),
        project_id: "default".into(),
        actor_name: "Room".into(),
        actor_id: "one".into(),
        host_id: "host".into(),
        request_id: request.map(str::to_owned),
        timestamp_ms: 1000,
        level: level.into(),
        message: format!("<b>{id}</b>"),
    };
    fixture
        .telemetry
        .append_logs(vec![
            log("a", Some("request-1"), "info"),
            log("b", None, "error"),
            log("e", None, "debug"),
        ])
        .await?;
    let page: Value = fixture
        .get("/v1/projects/default/observe/logs?requestId=request-1")
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(page["records"].as_array().unwrap().len(), 1);
    assert_eq!(page["records"][0]["message"], "<b>a</b>");
    let severe: Value = fixture
        .get("/v1/projects/default/observe/logs?level=warn")
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(severe["records"].as_array().unwrap().len(), 1);
    assert_eq!(severe["records"][0]["logId"], "b");
    let found: Value = fixture
        .get("/v1/projects/default/observe/logs?search=E%3C")
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(found["records"][0]["logId"], "e");
    assert_eq!(found["records"].as_array().unwrap().len(), 1);
    let histogram: Value = fixture
        .get("/v1/projects/default/observe/logs/histogram?fromMs=0&toMs=2000&bucketMs=1000")
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(
        histogram["buckets"],
        serde_json::json!([
            {"debug": 0, "info": 0, "warn": 0, "error": 0},
            {"debug": 1, "info": 1, "warn": 0, "error": 1}
        ])
    );
    let mut stream = fixture
        .get("/v1/projects/default/observe/logs/events?requestId=request-2&level=warn&search=D")
        .await?
        .error_for_status()?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    fixture.live.publish(
        otlp::live_channel("default", Signal::Logs),
        log_request(&[
            log("c", Some("request-1"), "error"),
            log("d", Some("request-2"), "warn"),
            log("f", Some("request-2"), "info"),
        ])
        .encode_to_vec(),
    );
    let tail = stream_inventory(&mut stream).await?;
    assert_eq!(tail["records"].as_array().unwrap().len(), 1);
    assert_eq!(tail["records"][0]["logId"], "d");
    for invalid in [
        "logs?limit=501",
        "logs/histogram?fromMs=0&toMs=2000&bucketMs=1",
    ] {
        assert_eq!(
            fixture
                .get(&format!("/v1/projects/default/observe/{invalid}"))
                .await?
                .status(),
            StatusCode::BAD_REQUEST
        );
    }
    Ok(())
}

#[tokio::test]
async fn request_history_validates_filters_and_returns_empty_pages() -> Result<()> {
    let fixture = Fixture::start().await?;
    let response = fixture.get("/v1/projects/default/observe/requests").await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["cache-control"], "no-store");
    let page: Value = response.json().await?;
    assert_eq!(page["records"], json!([]));
    assert_eq!(page["nextCursor"], Value::Null);
    assert_eq!(page["capacity"], 100);
    for query in [
        "limit=0",
        "limit=501",
        "limit=-1",
        "limit=1.5",
        "outcome=unknown",
        "fromMs=-1",
        "fromMs=9007199254740992",
        "fromMs=2&toMs=1",
        "actorName=",
        "actorId=bad%2Fid",
        "cursor=broken",
        "unknown=true",
    ] {
        assert_eq!(
            fixture
                .get(&format!("/v1/projects/default/observe/requests?{query}"))
                .await?
                .status(),
            StatusCode::BAD_REQUEST,
            "{query}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn request_history_filters_and_pages_without_including_later_appends() -> Result<()> {
    let fixture = Fixture::start().await?;
    for (id, name, actor, time, outcome) in [
        (
            "old",
            "Room",
            "one",
            999,
            crate::request_traces::RequestOutcome::Failed,
        ),
        (
            "first",
            "Room",
            "one",
            1000,
            crate::request_traces::RequestOutcome::Failed,
        ),
        (
            "second",
            "Room",
            "one",
            1000,
            crate::request_traces::RequestOutcome::Failed,
        ),
        (
            "other-class",
            "Counter",
            "one",
            1000,
            crate::request_traces::RequestOutcome::Failed,
        ),
        (
            "other-actor",
            "Room",
            "two",
            1000,
            crate::request_traces::RequestOutcome::Failed,
        ),
        (
            "completed",
            "Room",
            "one",
            1000,
            crate::request_traces::RequestOutcome::Completed,
        ),
        (
            "future",
            "Room",
            "one",
            1001,
            crate::request_traces::RequestOutcome::Failed,
        ),
    ] {
        record_request(&fixture, id, name, actor, time, outcome).await?;
    }
    let filters = "actorName=Room&actorId=one&outcome=failed&fromMs=1000&toMs=1000&limit=1";
    let first: Value = fixture
        .get(&format!("/v1/projects/default/observe/requests?{filters}"))
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(first["records"].as_array().unwrap().len(), 1);
    assert_eq!(first["records"][0]["requestId"], "second");
    let cursor = first["nextCursor"].as_str().unwrap();
    record_request(
        &fixture,
        "late",
        "Room",
        "one",
        1000,
        crate::request_traces::RequestOutcome::Failed,
    )
    .await?;
    let next: Value = fixture
        .get(&format!(
            "/v1/projects/default/observe/requests?{filters}&cursor={cursor}"
        ))
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(next["records"].as_array().unwrap().len(), 1);
    assert_eq!(next["records"][0]["requestId"], "first");
    assert_eq!(next["nextCursor"], Value::Null);
    assert_eq!(next["reset"], false);
    assert_eq!(
        fixture
            .get(&format!(
                "/v1/projects/default/observe/requests?actorId=two&cursor={cursor}"
            ))
            .await?
            .status(),
        StatusCode::BAD_REQUEST
    );
    Ok(())
}

async fn record_request(
    fixture: &Fixture,
    id: &str,
    actor_name: &str,
    actor_id: &str,
    time: u64,
    outcome: RequestOutcome,
) -> Result<()> {
    fixture
        .record(request_trace(id, actor_name, actor_id, time, outcome))
        .await
}

fn request_trace(
    id: &str,
    actor_name: &str,
    actor_id: &str,
    time: u64,
    outcome: RequestOutcome,
) -> RequestTrace {
    RequestTrace {
        state_version: None,
        project_id: "default".into(),
        request_id: id.into(),
        actor_name: actor_name.into(),
        actor_id: actor_id.into(),
        kind: RequestKind::Method,
        operation: "post".into(),
        connection_id: None,
        started_at_ms: time,
        duration_ms: 25.0,
        queue_wait_ms: Some(10.0),
        host_state: HostState::Warm,
        routing_ms: 0.0,
        outcome,
        metadata: None,
    }
}

fn span_request(event: &TraceEvent) -> ExportTraceServiceRequest {
    let trace = &event.trace;
    let mut resource = None;
    identity(&trace.project_id, &trace.actor_name, &trace.actor_id).stamp(&mut resource);
    let started = trace.started_at_ms * 1_000_000;
    let mut attributes = vec![
        text(otlp::REQUEST_ID, &trace.request_id),
        text(otlp::KIND, trace.kind.as_str()),
        text(otlp::OUTCOME, trace.outcome.as_str()),
        text(otlp::HOST_STATE, trace.host_state.as_str()),
    ];
    attributes.extend(
        trace
            .queue_wait_ms
            .map(|wait| text(otlp::QUEUE_WAIT_MS, &wait.to_string())),
    );
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource,
            scope_spans: vec![ScopeSpans {
                spans: vec![Span {
                    span_id: event.event_id.as_bytes().to_vec(),
                    name: trace.operation.clone(),
                    start_time_unix_nano: started,
                    end_time_unix_nano: started + (trace.duration_ms * 1e6) as u64,
                    attributes,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn log_request(logs: &[ActorLog]) -> ExportLogsServiceRequest {
    let mut resource = None;
    identity("default", "Room", "one").stamp(&mut resource);
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource,
            scope_logs: vec![ScopeLogs {
                log_records: logs
                    .iter()
                    .map(|log| LogRecord {
                        time_unix_nano: log.timestamp_ms * 1_000_000,
                        severity_text: log.level.clone(),
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(log.message.clone())),
                        }),
                        attributes: std::iter::once(text(otlp::LOG_ID, &log.log_id))
                            .chain(log.request_id.iter().map(|id| text(otlp::REQUEST_ID, id)))
                            .collect(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn identity(project_id: &str, actor_name: &str, actor_id: &str) -> otlp::Identity {
    otlp::Identity {
        organization_id: None,
        project_id: project_id.into(),
        actor_name: actor_name.into(),
        actor_id: actor_id.into(),
        host_id: "host".into(),
        session_id: "session".into(),
    }
}

fn text(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.into())),
        }),
        ..Default::default()
    }
}

#[tokio::test]
async fn request_stream_resumes_saved_cursors() -> Result<()> {
    let fixture = Fixture::start().await?;
    let snapshot = serde_json::to_value(
        fixture
            .traces
            .replay("default", &Default::default())
            .await?,
    )?;
    let cursor = snapshot["resumeCursor"].as_str().unwrap();
    let mut replay = fixture
        .get(&format!(
            "/v1/projects/default/observe/requests/events?after={cursor}"
        ))
        .await?;
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(stream_inventory(&mut replay).await?["records"], json!([]));
    assert_eq!(
        fixture
            .get("/v1/projects/default/observe/requests/events?after=broken")
            .await?
            .status(),
        StatusCode::BAD_REQUEST
    );
    Ok(())
}

#[tokio::test]
async fn typed_observer_metrics_validate_ranges_and_return_uncached_results() -> Result<()> {
    let fixture = Fixture::start().await?;
    for path in ["metrics", "queue-waits", "websockets"] {
        let response = fixture
            .get(&format!("/v1/projects/default/observe/{path}"))
            .await?;
        assert_eq!(response.status(), StatusCode::OK, "{path}");
        assert_eq!(response.headers()["cache-control"], "no-store");
        let body: Value = response.json().await?;
        if path == "metrics" {
            assert_eq!(body["total"]["count"], 0);
            assert_eq!(body["total"]["p95"], Value::Null);
            assert_eq!(body["classes"], json!([]));
        } else {
            assert_eq!(body, json!([]));
        }
        for query in [
            "fromMs=-1",
            "fromMs=9007199254740992",
            "fromMs=2&toMs=1",
            "unknown=true",
        ] {
            assert_eq!(
                fixture
                    .get(&format!("/v1/projects/default/observe/{path}?{query}"))
                    .await?
                    .status(),
                StatusCode::BAD_REQUEST,
                "{path}?{query}"
            );
        }
    }
    assert_eq!(
        fixture
            .get("/v1/projects/default/observe/queue-waits?actorName=")
            .await?
            .status(),
        StatusCode::BAD_REQUEST
    );
    Ok(())
}

#[tokio::test]
async fn state_inspection_reads_committed_values_and_retained_attribution() -> Result<()> {
    use crate::{state_log::StateSnapshot, state_transport::SnapshotWriter};
    let fixture = Fixture::start().await?;
    let actor = fixture.actor("state-demo");
    let loaded = fixture
        .store
        .register_activation(
            &actor,
            &HostLeaseRequest {
                id: fixture.host.clone(),
                session_id: "state-session".into(),
                route: "http://localhost:7101".into(),
                duration_ms: 60_000,
            },
            "north-america-east",
            true,
            None,
        )
        .await?;
    for version in 1..=3 {
        let plan = fixture
            .store
            .prepare_actor_write(&actor, &loaded.placement.lease, 1, version)
            .await?;
        let snapshot: StateSnapshot = serde_json::from_value(json!({
            "stateVersion": version, "ownerEpoch": 1, "requestId": format!("req-{version}"),
            "sqlite": crate::test_sqlite::snapshot(json!({"count": version, "settings": {"enabled": true}}))?, "completion": {"result": null},
            "attribution": {"operation": "onMessage", "connectionId": "socket-a", "committedAtMs": 1234, "interleaved": false}
        }))?;
        fixture
            .store
            .write_snapshot(&plan, snapshot.encode()?)
            .await?;
    }
    let path = "/v1/projects/default/observe/state?actorName=Room.with.dots&actorId=state-demo";
    let response = fixture.get(path).await?;
    assert_eq!(response.status(), StatusCode::OK);
    let current: Value = response.json().await?;
    assert_eq!(current["snapshot"]["state"]["count"], 3);
    assert_eq!(current["snapshot"]["requestId"], "req-3");
    assert!(current["snapshot"].get("result").is_none());
    let page: Value = fixture
        .get(&path.replace("/state?", "/state/history?"))
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(page["records"].as_array().unwrap().len(), 3);
    assert_eq!(
        page["records"][0]["attribution"]["connectionId"],
        "socket-a"
    );
    assert!(page["records"][0].get("state").is_none());
    let page_path = path.replace("/state?", "/state/history?");
    let first: Value = fixture
        .get(&format!("{page_path}&limit=2"))
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(first["records"].as_array().unwrap().len(), 2);
    assert_eq!(first["nextBefore"], 2);
    let next: Value = fixture
        .get(&format!("{page_path}&before=2&limit=2"))
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(next["records"][0]["stateVersion"], 1);
    assert!(next["nextBefore"].is_null());
    let old: Value = fixture
        .get(&format!("{path}&version=1"))
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(old["snapshot"]["state"]["count"], 1);
    let isolated: Value = fixture
        .get(&path.replace("projects/default", "projects/other"))
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert!(isolated["snapshot"].is_null());
    assert_eq!(
        fixture.get(&format!("{path}&version=0")).await?.status(),
        StatusCode::BAD_REQUEST
    );
    Ok(())
}

#[tokio::test]
async fn residency_api_filters_projects_and_actors_and_validates_ranges() -> Result<()> {
    let fixture = Fixture::start().await?;
    for (project, id) in [("alpha", "one"), ("beta", "two")] {
        fixture
            .report_lifecycle(
                ActorKey {
                    project_id: project.into(),
                    ..fixture.actor(id)
                },
                LifecycleKind::Stopped,
                1,
                vec![],
            )
            .await?;
    }
    let base = "/v1/projects/alpha/observe/residency";
    let history: Value = fixture
        .get(&format!(
            "{base}?actorName=Room.with.dots&toMs=9007199254740991"
        ))
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(history["periods"][0]["actorId"], "one");
    assert_eq!(history["periods"][0]["status"], "dormant");
    assert_eq!(history["periods"].as_array().unwrap().len(), 1);
    let empty: Value = fixture
        .get(&format!("{base}?actorName=Other"))
        .await?
        .json()
        .await?;
    assert_eq!(empty["periods"], json!([]));
    for query in [
        "",
        "?actorName=Room&fromMs=20&toMs=10",
        "?actorName=Room&fromMs=-1",
        "?actorName=Room&extra=1",
    ] {
        assert_eq!(
            fixture.get(&format!("{base}{query}")).await?.status(),
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(
        fixture
            .client
            .get(format!("{}{base}?actorName=Room", fixture.origin))
            .send()
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );
    Ok(())
}
