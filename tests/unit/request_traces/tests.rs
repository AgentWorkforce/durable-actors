use super::live::InProcessFeed;
use super::persistence::sqlite::SqliteTracePersistence;
use super::*;
use crate::request_tracking::{HostState, RequestTracker};
use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
use std::time::Instant;

#[tokio::test]
async fn completed_requests_become_one_span_with_request_attributes() -> Result<()> {
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let invocation = crate::actor::ActorInvocation {
        request_id: "request-1".into(),
        actor: crate::actor::ActorKey {
            project_id: "default".into(),
            actor_name: "Room".into(),
            actor_id: "lobby".into(),
        },
        method: "onConnect".into(),
        args: vec![],
    };
    let recorder = TraceRecorder::new(
        provider.tracer("test"),
        &invocation,
        RequestKind::Websocket,
        Some("socket-1".into()),
        Some(serde_json::json!({"userId": "alice"})),
    );
    let (mut tracker, timing) = RequestTracker::new(
        Instant::now(),
        HostState::Cold,
        2.5,
        Some(Box::new(recorder)),
    );
    tracker.admitted();
    tracker.state_version(Some(7));
    tracker.finish(RequestOutcome::Completed);
    let timing = timing.await?;

    let spans = exporter.get_finished_spans()?;
    assert_eq!(spans.len(), 1);
    let span = &spans[0];
    assert_eq!(span.name, "onConnect");
    let attribute = |key: &str| {
        span.attributes
            .iter()
            .find(|attribute| attribute.key.as_str() == key)
            .map(|attribute| attribute.value.to_string())
    };
    for (key, value) in [
        (otlp::REQUEST_ID, "request-1"),
        (otlp::KIND, "websocket"),
        (otlp::CONNECTION_ID, "socket-1"),
        (otlp::OUTCOME, "completed"),
        (otlp::HOST_STATE, "cold"),
        (otlp::ROUTING_MS, "2.5"),
        (otlp::STATE_VERSION, "7"),
        (otlp::METADATA, r#"{"userId":"alice"}"#),
    ] {
        assert_eq!(attribute(key).as_deref(), Some(value), "{key}");
    }
    assert!(attribute(otlp::QUEUE_WAIT_MS).is_some());
    let duration = span.end_time.duration_since(span.start_time)?;
    assert!((duration.as_secs_f64() * 1000.0 - timing.duration_ms).abs() < 1.0);
    Ok(())
}

#[tokio::test]
async fn sqlite_replay_resumes_after_restart_with_a_durable_cursor() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("requests.sqlite3");
    let open = || {
        let persistence = Arc::new(SqliteTracePersistence::new(path.clone()));
        (
            persistence.clone(),
            TraceStore::new(persistence, Arc::new(InProcessFeed::default())),
        )
    };
    let (persistence, store) = open();
    persistence
        .append(&(0..3).map(event).collect::<Vec<_>>())
        .await?;
    let cursor = store
        .replay("default", &ReplayQuery::default())
        .await?
        .resume_cursor;
    drop((persistence, store));

    let (persistence, store) = open();
    persistence.append(&[event(9)]).await?;
    let delta = store
        .replay(
            "default",
            &ReplayQuery {
                cursor: Some(cursor),
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(delta.records.len(), 1);
    assert_eq!(delta.records[0].event.trace.request_id, "9");
    Ok(())
}

fn event(id: usize) -> TraceEvent {
    TraceEvent {
        event_id: id.to_string(),
        host_id: "host".into(),
        session_id: "session".into(),
        trace: RequestTrace {
            state_version: None,
            project_id: "default".into(),
            request_id: id.to_string(),
            actor_name: "Counter".into(),
            actor_id: "one".into(),
            kind: RequestKind::Method,
            operation: "increment".into(),
            connection_id: None,
            started_at_ms: 1000,
            duration_ms: 12.0,
            queue_wait_ms: Some(5.0),
            host_state: HostState::Warm,
            routing_ms: 0.0,
            outcome: RequestOutcome::Completed,
            metadata: None,
        },
    }
}
