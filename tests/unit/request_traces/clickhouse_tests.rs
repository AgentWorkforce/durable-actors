use super::*;
use crate::request_traces::live::{LiveTraceFeed, LiveTraceUpdate};
use crate::request_traces::{InvalidTraceQuery, RequestKind, RequestOutcome, TraceStore};

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
            Arc::new(LiveTraceFeed::default()),
            stop.clone(),
        )
        .map(Arc::new)
    };
    let writer = LifecycleStore::new(backend()?);
    let persistence = backend()?;
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
    Ok(())
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
async fn unavailable_clickhouse_cannot_block_reports_and_the_queue_is_bounded() -> Result<()> {
    let stop = CancellationToken::new();
    let _guard = stop.clone().drop_guard();
    let feed = LiveTraceFeed::default();
    let mut live = feed.subscribe();
    let backend = Arc::new(ClickHouseTracePersistence::start(
        config("http://127.0.0.1:9".into()),
        Duration::from_secs(86400),
        Arc::new(feed),
        stop,
    )?);
    let store = TraceStore::open(backend.clone()).await?;
    let trace = super::super::contract_tests::event("bounded").trace;
    tokio::time::timeout(Duration::from_secs(2), async {
        for _ in 0..MAX_QUEUED_ROWS / TRACE_BATCH_SIZE {
            store
                .record(
                    "default",
                    "host",
                    "session",
                    vec![trace.clone(); TRACE_BATCH_SIZE],
                    0,
                )
                .await?;
        }
        assert!(
            store
                .record("default", "host", "session", vec![trace], 0)
                .await
                .is_err()
        );
        assert_eq!(backend.queued_rows.available_permits(), 0);
        assert!(matches!(
            live.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));
        anyhow::Ok(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn clickhouse_ingestion_deduplication_history_and_rollups() -> Result<()> {
    let Ok(url) = std::env::var("DURABLE_ACTORS_TEST_CLICKHOUSE_URL") else {
        return Ok(());
    };
    let stop = CancellationToken::new();
    let _guard = stop.clone().drop_guard();
    let feed = LiveTraceFeed::default();
    let mut live = feed.subscribe();
    let store = ClickHouseTracePersistence::start(
        config(url.clone()),
        Duration::from_secs(86400),
        Arc::new(feed),
        stop.clone(),
    )?;
    let project = format!("test-{}", uuid::Uuid::new_v4());
    let mut events: Vec<_> = ["a", "b", "c", "d"]
        .map(|id| {
            let mut event = super::super::contract_tests::event(id);
            event.trace.project_id = project.clone();
            event.trace.started_at_ms = 1000;
            event.trace.duration_ms = 10.0;
            event.trace.queue_wait_ms = Some(1.0);
            event
        })
        .into();
    events[0].trace.request_id = "request\0id".into();
    for event in &mut events[1..3] {
        event.trace.kind = RequestKind::Websocket;
        event.trace.connection_id = Some("socket".into());
    }
    events[1].trace.operation = "onConnect".into();
    events[1].trace.metadata = Some(serde_json::json!({"client": "test"}));
    events[2].trace.operation = "onMessage".into();
    events[2].trace.outcome = RequestOutcome::Failed;
    events[3].trace.outcome = RequestOutcome::Rerouted;
    store.record(&project, events.clone(), 3).await?;
    let committed = tokio::time::timeout(Duration::from_secs(20), live.recv()).await??;
    let LiveTraceUpdate::Batch(committed) = committed.as_ref() else {
        panic!("expected committed traces");
    };
    assert_eq!(committed.len(), 4);
    assert_eq!(
        store
            .metrics(&project, &TimeRange::default())
            .await?
            .total
            .count,
        4
    );

    // Retry after an uncertain response with the same event IDs.
    store.record(&project, events.clone(), 0).await?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let other = ClickHouseTracePersistence::start(
        config(url),
        Duration::from_secs(86400),
        Arc::new(LiveTraceFeed::default()),
        stop,
    )?;
    let metrics = other.metrics(&project, &TimeRange::default()).await?;
    assert_eq!(metrics.total.count, 4);
    assert!((metrics.total.success.unwrap() - 200.0 / 3.0).abs() < 0.001);
    assert_eq!(other.status(&project).await?.dropped, 3);
    let first = other
        .history(
            &project,
            &HistoryQuery {
                limit: 2,
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(ids(&first), ["d", "c"]);
    let next = HistoryQuery {
        cursor: first.next_cursor,
        limit: 2,
        ..Default::default()
    };
    assert_eq!(ids(&other.history(&project, &next).await?), ["b", "a"]);
    assert!(
        other
            .history("foreign", &next)
            .await
            .err()
            .unwrap()
            .is::<InvalidTraceQuery>()
    );
    let request = other
        .history(
            &project,
            &HistoryQuery {
                request_id: Some("request\0id".into()),
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(ids(&request), ["a"]);
    assert_eq!(
        other
            .queue_waits(&project, &QueueWaitQuery::default())
            .await?[0]
            .admitted,
        3
    );
    let sockets = other.websockets(&project, &TimeRange::default()).await?;
    assert_eq!(sockets.len(), 1);
    assert_eq!((sockets[0].messages, sockets[0].failures), (1, 1));
    assert_eq!(sockets[0].metadata, events[1].trace.metadata);
    assert_eq!(sockets[0].opened_at_ms, Some(1000));
    assert_eq!(sockets[0].closed_at_ms, None);
    assert!(
        other
            .history("foreign", &HistoryQuery::default())
            .await?
            .records
            .is_empty()
    );

    let mut late = events[0].clone();
    late.event_id = "late".into();
    late.trace.started_at_ms = 500;
    store.record(&project, vec![late], 0).await?;
    wait_count(&store, &project, 5).await?;
    let snapshot = other
        .replay(
            &project,
            &ReplayQuery {
                cursor: Some("snapshot".into()),
                ..Default::default()
            },
        )
        .await?;
    assert!(snapshot.snapshot);
    assert!(!snapshot.reset);
    assert!(snapshot.next_cursor.is_none());
    assert_eq!(ids(&snapshot), ["d", "c", "b", "a", "late"]);
    let burst_project = format!("burst-{}", uuid::Uuid::new_v4());
    for index in 0..1000 {
        let mut event = events[0].clone();
        event.event_id = format!("burst-{index}");
        event.trace.project_id = burst_project.clone();
        store.record(&burst_project, vec![event], 0).await?;
    }
    wait_count(&other, &burst_project, 1000).await?;
    Ok(())
}

fn config(url: String) -> ClickHouseConfig {
    ClickHouseConfig {
        url,
        database: "trace_test".into(),
        user: "trace_test".into(),
        password: "local-test-only".into(),
    }
}

async fn wait_count(store: &ClickHouseTracePersistence, project: &str, count: u64) -> Result<()> {
    let mut observed = String::new();
    let result = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            match store.metrics(project, &TimeRange::default()).await {
                Ok(result) if result.total.count == count => return,
                Ok(result) => observed = format!("count was {}", result.total.count),
                Err(error) => observed = format!("{error:#}"),
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    ensure!(result.is_ok(), "waiting for {count} traces: {observed}");
    Ok(())
}

fn ids(page: &TracePage) -> Vec<&str> {
    page.records
        .iter()
        .map(|record| record.event.event_id.as_str())
        .collect()
}
