use super::queries::SpanRow;
use super::*;
use crate::request_traces::{RequestKind, RequestOutcome};

#[tokio::test]
async fn lifecycle_inventory_is_shared_and_tracks_activation_handoffs() -> Result<()> {
    use crate::{
        actor_lifecycle::*,
        clock::{Clock, SystemClock},
        placement::ActorInventoryReader,
    };
    let Ok(url) = std::env::var("DURABLE_ACTORS_TEST_CLICKHOUSE_URL") else {
        return Ok(());
    };
    let stop = CancellationToken::new();
    let _guard = stop.clone().drop_guard();
    let backend = || {
        ClickHouseTracePersistence::start(
            config(url.clone()),
            Duration::from_secs(86400),
            stop.clone(),
        )
        .map(Arc::new)
    };
    let persistence = backend()?;
    // Surfaces schema errors directly; the writer would otherwise retry them silently.
    ingestion::initialize(&persistence.client, 86400).await?;
    let writer = LifecycleStore::new(backend()?);
    let reader = LifecycleStore::new(persistence.clone());
    let project = format!("lifecycle-{}", uuid::Uuid::new_v4());
    let mut event = LifecycleEvent {
        organization_id: Some("org-test".into()),
        event_id: "session:1".into(),
        actor: crate::actor::ActorKey {
            project_id: project.clone(),
            actor_name: "Counter".into(),
            actor_id: "one".into(),
        },
        host_id: "host".into(),
        session_id: "session".into(),
        region: "us-east".into(),
        received_at_ms: SystemClock.now_ms()?,
        report: LifecycleReport {
            kind: LifecycleKind::Started,
            owner_epoch: 1,
            sequence: 1,
            started_at_ms: 1000,
            occurred_at_ms: 1000,
            resources: Default::default(),
            resident: Some(true),
            last_active_at_ms: None,
            waiting: Vec::new(),
        },
    };
    for (index, kind) in [
        LifecycleKind::Started,
        LifecycleKind::Heartbeat,
        LifecycleKind::Stopped,
    ]
    .into_iter()
    .enumerate()
    {
        event.report.kind = kind;
        event.report.sequence = index as u64 + 1;
        event.report.occurred_at_ms = event.report.sequence * 1000;
        if kind == LifecycleKind::Heartbeat {
            event
                .report
                .waiting
                .push(crate::host_leases::WaitingOperation {
                    id: "request".into(),
                    operation: "increment".into(),
                });
        }
        event.event_id = format!("session:{}", event.report.sequence);
        writer.record(event.clone()).await?;
        wait_lifecycle(&persistence, &project, &event.event_id).await?;
        let snapshot = reader.actor_inventory(&project).await?;
        assert_eq!(snapshot.actors.len(), 1);
        let actor = &snapshot.actors[0];
        assert_eq!(actor.actor_name, "Counter");
        assert_eq!(actor.instances[0].actor_id, "one");
        assert_eq!(actor.unknown, 0);
        if kind == LifecycleKind::Stopped {
            assert_eq!((actor.live, actor.dormant), (0, 1));
            assert!(actor.instances[0].waiting.as_ref().unwrap().is_empty());
        } else {
            assert_eq!((actor.live, actor.dormant), (1, 0));
            assert_eq!(
                actor.instances[0].waiting.as_ref().unwrap().len(),
                event.report.waiting.len()
            );
        }
    }
    let stopped = event.clone();
    event.event_id = "next-session:1".into();
    event.host_id = "next-host".into();
    event.session_id = "next-session".into();
    event.report.kind = LifecycleKind::Started;
    event.report.owner_epoch = 2;
    event.report.sequence = 1;
    event.report.started_at_ms = 4000;
    event.report.occurred_at_ms = 4000;
    event.report.waiting.clear();
    writer.record(event.clone()).await?;
    writer.record(stopped).await?;
    writer.record(event.clone()).await?;
    wait_lifecycle(&persistence, &project, &event.event_id).await?;
    assert_eq!(reader.actor_inventory(&project).await?.actors[0].live, 1);
    let history = reader
        .residency_history(&project, "Counter", 1500, 4500)
        .await?;
    let periods: Vec<_> = history
        .periods
        .iter()
        .map(|period| (period.status, period.from_ms, period.to_ms))
        .collect();
    assert_eq!(
        periods,
        vec![
            (crate::placement::ActorResidency::Live, 1500, 3000),
            (crate::placement::ActorResidency::Dormant, 3000, 4000),
            (crate::placement::ActorResidency::Live, 4000, 4500),
        ]
    );
    assert!(!history.truncated);
    assert!(
        reader
            .residency_history("different-project", "Counter", 0, 4500)
            .await?
            .periods
            .is_empty()
    );
    assert!(
        reader
            .actor_inventory("different-project")
            .await?
            .actors
            .is_empty()
    );
    read_collector_rows(&persistence, &project).await
}

// Inserts with the exporter's column lists, so the schema stays compatible with the collector.
async fn read_collector_rows(store: &ClickHouseTracePersistence, project: &str) -> Result<()> {
    use crate::actor::LogLevel;
    use crate::request_traces::{
        history::HistoryQuery,
        logs::{LogHistogramQuery, LogQuery},
        metrics::{QueueWaitQuery, TimeRange},
        persistence::TraceQueries,
    };
    let resource = format!(
        "map('durable_actors.project_id', '{project}', 'durable_actors.actor_name', 'Room', \
         'durable_actors.actor_id', 'lobby', 'durable_actors.host_id', 'host', \
         'durable_actors.session_id', 'session')"
    );
    store
        .client
        .query(&format!(
            "INSERT INTO otel_traces (Timestamp, TraceId, SpanId, ParentSpanId, TraceState, SpanName, \
             SpanKind, ServiceName, ResourceAttributes, ScopeName, ScopeVersion, SpanAttributes, Duration, \
             StatusCode, StatusMessage, `Events.Timestamp`, `Events.Name`, `Events.Attributes`, \
             `Links.TraceId`, `Links.SpanId`, `Links.TraceState`, `Links.Attributes`) VALUES \
             (now64(9), 't', 'span-1', '', '', 'onConnect', 'Server', 'durable-actors-host', {resource}, \
             'durable-actors-host', '', map('durable_actors.request_id', 'request-1', \
             'durable_actors.kind', 'websocket', 'durable_actors.connection_id', 'socket-1', \
             'durable_actors.outcome', 'completed', 'durable_actors.queue_wait_ms', '2.5', \
             'durable_actors.metadata', '{{\"userId\":\"alice\"}}'), 12500000, 'Unset', '', \
             [], [], [], [], [], [], [])"
        ))
        .execute()
        .await?;
    store
        .client
        .query(&format!(
            "INSERT INTO otel_logs (Timestamp, TraceId, SpanId, TraceFlags, SeverityText, SeverityNumber, \
             ServiceName, Body, ResourceSchemaUrl, ResourceAttributes, ScopeSchemaUrl, ScopeName, ScopeVersion, \
             ScopeAttributes, LogAttributes, EventName) VALUES (now64(9), '', '', 0, 'info', 9, \
             'durable-actors-host', 'hello', '', {resource}, '', 'durable-actors-host', '', map(), \
             map('durable_actors.log_id', 'log-1', 'durable_actors.request_id', 'request-1'), '')"
        ))
        .execute()
        .await?;
    let history = store.history(project, &HistoryQuery::default()).await?;
    assert_eq!(ids(&history), ["span-1"]);
    let trace = &history.records[0].event.trace;
    assert_eq!(
        (trace.actor_name.as_str(), trace.actor_id.as_str()),
        ("Room", "lobby")
    );
    assert_eq!(trace.duration_ms, 12.5);
    assert_eq!(trace.queue_wait_ms, Some(2.5));
    let filtered = HistoryQuery {
        request_id: Some("request-1".into()),
        connection_id: Some("socket-1".into()),
        outcome: Some(RequestOutcome::Completed),
        ..Default::default()
    };
    assert_eq!(ids(&store.history(project, &filtered).await?), ["span-1"]);
    let logs = store
        .logs(
            project,
            &LogQuery {
                request_id: Some("request-1".into()),
                level: Some(LogLevel::Info),
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(logs.records.len(), 1);
    assert_eq!(logs.records[0].message, "hello");
    let severe = LogQuery {
        level: Some(LogLevel::Warn),
        ..Default::default()
    };
    assert!(store.logs(project, &severe).await?.records.is_empty());
    let search = LogQuery {
        search: Some("HELL".into()),
        ..Default::default()
    };
    assert_eq!(store.logs(project, &search).await?.records.len(), 1);
    let now = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?;
    let histogram = store
        .log_histogram(
            project,
            &LogHistogramQuery {
                actor_name: Some("Room".into()),
                actor_id: None,
                request_id: None,
                search: Some("hello".into()),
                from_ms: now - 3_600_000,
                to_ms: now + 60_000,
                bucket_ms: 60_000,
            },
        )
        .await?;
    assert_eq!(
        histogram
            .buckets
            .iter()
            .map(|bucket| bucket.info)
            .sum::<u64>(),
        1
    );
    assert_eq!(
        store
            .metrics(project, &TimeRange::default())
            .await?
            .total
            .count,
        1
    );
    assert_eq!(
        store
            .queue_waits(project, &QueueWaitQuery::default())
            .await?[0]
            .admitted,
        1
    );
    let sockets = store.websockets(project, &TimeRange::default()).await?;
    assert_eq!(
        sockets[0].metadata,
        Some(serde_json::json!({"userId": "alice"}))
    );
    assert!(
        store
            .history("different-project", &HistoryQuery::default())
            .await?
            .records
            .is_empty()
    );
    Ok(())
}

fn ids(page: &TracePage) -> Vec<&str> {
    page.records
        .iter()
        .map(|record| record.event.event_id.as_str())
        .collect()
}

async fn wait_lifecycle(
    store: &ClickHouseTracePersistence,
    project: &str,
    event_id: &str,
) -> Result<()> {
    use crate::actor_lifecycle::LifecyclePersistence;
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Ok(events) = store.latest(project).await
                && events
                    .first()
                    .is_some_and(|event| event.event_id == event_id)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn unavailable_clickhouse_cannot_block_lifecycle_reports_and_the_queue_is_bounded()
-> Result<()> {
    use crate::actor_lifecycle::{LifecycleEvent, LifecyclePersistence};
    let stop = CancellationToken::new();
    let _guard = stop.clone().drop_guard();
    let backend = ClickHouseTracePersistence::start(
        config("http://127.0.0.1:9".into()),
        Duration::from_secs(86400),
        stop,
    )?;
    let event = LifecycleEvent {
        organization_id: None,
        event_id: "session:1".into(),
        actor: crate::actor::ActorKey {
            project_id: "default".into(),
            actor_name: "Counter".into(),
            actor_id: "one".into(),
        },
        host_id: "host".into(),
        session_id: "session".into(),
        region: "us-east".into(),
        received_at_ms: 1,
        report: crate::actor_lifecycle::LifecycleReport {
            kind: crate::actor_lifecycle::LifecycleKind::Started,
            owner_epoch: 1,
            sequence: 1,
            started_at_ms: 1,
            occurred_at_ms: 1,
            resources: Default::default(),
            resident: None,
            last_active_at_ms: None,
            waiting: Vec::new(),
        },
    };
    tokio::time::timeout(Duration::from_secs(2), async {
        for _ in 0..MAX_QUEUED_ROWS {
            backend.record(&event).await?;
        }
        assert!(backend.record(&event).await.is_err());
        assert_eq!(backend.queued_rows.available_permits(), 0);
        anyhow::Ok(())
    })
    .await??;
    Ok(())
}

#[test]
fn span_rows_map_to_request_traces() {
    let row = SpanRow {
        event_id: "00f067aa0ba902b7".into(),
        host_id: "host".into(),
        session_id: "session".into(),
        project_id: "default".into(),
        request_id: "request".into(),
        actor_name: "Room".into(),
        actor_id: "lobby".into(),
        kind: "websocket".into(),
        operation: "onConnect".into(),
        connection_id: "socket-1".into(),
        started_at_ms: 1_000,
        routing_ms: 1.5,
        duration_ms: 12.25,
        queue_wait_ms: Some(3.0),
        host_state: "cold".into(),
        outcome: "completed".into(),
        state_version: Some(7),
        metadata: r#"{"userId":"alice"}"#.into(),
    };
    let event = row.into_event().unwrap();
    assert_eq!(event.event_id, "00f067aa0ba902b7");
    assert_eq!(event.trace.kind, RequestKind::Websocket);
    assert_eq!(event.trace.outcome, RequestOutcome::Completed);
    assert_eq!(
        event.trace.host_state,
        crate::request_tracking::HostState::Cold
    );
    assert_eq!(event.trace.connection_id.as_deref(), Some("socket-1"));
    assert_eq!(
        event.trace.metadata,
        Some(serde_json::json!({"userId": "alice"}))
    );
    assert_eq!(event.trace.state_version, Some(7));
}

fn config(url: String) -> ClickHouseConfig {
    ClickHouseConfig {
        url,
        database: "trace_test".into(),
        user: "trace_test".into(),
        password: "local-test-only".into(),
    }
}
