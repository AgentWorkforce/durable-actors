use super::fixtures::event;
use super::*;
use crate::request_traces::{
    RequestKind, RequestOutcome,
    metrics::{QueueWaitQuery, TimeRange},
};

#[tokio::test]
async fn replay_metadata_counts_only_the_requested_project() -> Result<()> {
    let store = SqliteTracePersistence::in_memory();
    let mut records: Vec<_> = (0..501).map(|id| event(&id.to_string())).collect();
    let mut isolated = event("isolated");
    isolated.trace.project_id = "other".into();
    records.push(isolated);
    store.append(&records).await?;
    let page = store.replay("other", &ReplayQuery::default()).await?;
    assert_eq!(page.records.len(), 1);
    assert_eq!(
        store.replay("empty", &ReplayQuery::default()).await?.cursor,
        0
    );
    Ok(())
}

#[tokio::test]
async fn overview_counts_reroutes_but_percentiles_and_success_use_attempts() -> Result<()> {
    let store = SqliteTracePersistence::in_memory();
    let mut events = Vec::new();
    for i in 1..=21 {
        let mut record = event(&i.to_string());
        record.trace.actor_name = "Room".into();
        record.trace.started_at_ms = 5000;
        record.trace.duration_ms = i as f64;
        record.trace.queue_wait_ms = Some(i as f64 * 2.0);
        events.push(record);
    }
    for (id, name, outcome, duration, queue, time) in [
        (
            "failed",
            "Room",
            RequestOutcome::Failed,
            100.0,
            Some(5.0),
            5000,
        ),
        (
            "reroute",
            "Room",
            RequestOutcome::Rerouted,
            900.0,
            Some(900.0),
            5000,
        ),
        (
            "rejected",
            "Room",
            RequestOutcome::Rejected,
            3.0,
            None,
            5000,
        ),
        (
            "counter",
            "Counter",
            RequestOutcome::Completed,
            7.0,
            Some(1.0),
            5000,
        ),
        (
            "counter-reroute",
            "Counter",
            RequestOutcome::Rerouted,
            8.0,
            Some(1.0),
            5000,
        ),
        (
            "idle",
            "Idle",
            RequestOutcome::Rerouted,
            1.0,
            Some(1.0),
            5000,
        ),
        (
            "old",
            "Room",
            RequestOutcome::Completed,
            5000.0,
            Some(5000.0),
            100,
        ),
    ] {
        let mut record = event(id);
        record.trace.actor_name = name.into();
        record.trace.outcome = outcome;
        record.trace.duration_ms = duration;
        record.trace.queue_wait_ms = queue;
        record.trace.started_at_ms = time;
        events.push(record);
    }
    store.append(&events).await?;
    let metrics = store
        .metrics(
            "default",
            &TimeRange {
                from_ms: Some(1000),
                to_ms: Some(5000),
            },
        )
        .await?;
    assert_eq!(metrics.total.count, 27);
    assert_eq!(metrics.total.success, Some(100.0 * 22.0 / 24.0));
    let room = metrics
        .classes
        .iter()
        .find(|row| row.actor_name == "Room")
        .unwrap();
    assert_eq!(room.count, 24);
    assert_eq!(room.p95, Some(21.0));
    assert_eq!(room.queue_p95, Some(40.0));
    let idle = metrics
        .classes
        .iter()
        .find(|row| row.actor_name == "Idle")
        .unwrap();
    assert_eq!(idle.count, 1);
    assert_eq!((idle.success, idle.p95, idle.queue_p95), (None, None, None));
    let old = store
        .metrics(
            "default",
            &TimeRange {
                from_ms: Some(0),
                to_ms: Some(999),
            },
        )
        .await?;
    assert_eq!(old.total.count, 1);
    assert_eq!(
        store
            .metrics("default", &TimeRange::default())
            .await?
            .total
            .count,
        28
    );
    Ok(())
}

#[tokio::test]
async fn queue_waits_group_admitted_attempts_and_filter_actor_and_time() -> Result<()> {
    let store = SqliteTracePersistence::in_memory();
    let mut events = Vec::new();
    for (id, actor, wait, outcome, time) in [
        ("one", "a", Some(10.0), RequestOutcome::Completed, 1000),
        ("two", "a", Some(30.0), RequestOutcome::Failed, 2000),
        ("three", "b", Some(90.0), RequestOutcome::Completed, 1500),
        ("reroute", "a", Some(999.0), RequestOutcome::Rerouted, 1500),
        ("rejected", "a", None, RequestOutcome::Rejected, 1500),
        ("old", "a", Some(999.0), RequestOutcome::Completed, 999),
    ] {
        let mut record = event(id);
        record.trace.actor_name = "Room".into();
        record.trace.actor_id = actor.into();
        record.trace.queue_wait_ms = wait;
        record.trace.outcome = outcome;
        record.trace.started_at_ms = time;
        events.push(record);
    }
    store.append(&events).await?;
    let query = QueueWaitQuery {
        from_ms: Some(1000),
        to_ms: Some(2000),
        actor_name: Some("Room".into()),
    };
    let rows = store.queue_waits("default", &query).await?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].actor_id, "a");
    assert_eq!(rows[0].admitted, 2);
    assert_eq!(rows[0].average_ms, 20.0);
    assert_eq!(rows[0].max_ms, 30.0);
    assert!(
        store
            .queue_waits(
                "default",
                &QueueWaitQuery {
                    actor_name: Some("Other".into()),
                    ..query
                }
            )
            .await?
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn websocket_history_keeps_full_sessions_overlapping_the_range_and_connect_metadata()
-> Result<()> {
    let store = SqliteTracePersistence::in_memory();
    let mut events = Vec::new();
    for (id, operation, time, outcome) in [
        ("connect", "onConnect", 1000, RequestOutcome::Completed),
        ("message", "onMessage", 3000, RequestOutcome::Failed),
        ("close", "onDisconnect", 5000, RequestOutcome::Completed),
    ] {
        let mut record = event(id);
        record.trace.kind = RequestKind::Websocket;
        record.trace.connection_id = Some("socket".into());
        record.trace.operation = operation.into();
        record.trace.started_at_ms = time;
        record.trace.outcome = outcome;
        record.trace.metadata =
            (operation == "onConnect").then(|| serde_json::json!({"userId":"ada"}));
        events.push(record);
    }
    store.append(&events).await?;
    let sessions = store
        .websockets(
            "default",
            &TimeRange {
                from_ms: Some(2000),
                to_ms: Some(4000),
            },
        )
        .await?;
    assert_eq!(sessions.len(), 1);
    let session = &sessions[0];
    assert_eq!(session.connection_id, "socket");
    assert_eq!(session.opened_at_ms, Some(1000));
    assert_eq!(session.closed_at_ms, Some(5000));
    assert_eq!(session.last_seen_ms, Some(5000));
    assert_eq!((session.messages, session.failures), (1, 1));
    assert_eq!(session.metadata, Some(serde_json::json!({"userId":"ada"})));
    assert!(
        store
            .websockets(
                "default",
                &TimeRange {
                    from_ms: Some(5001),
                    to_ms: None
                }
            )
            .await?
            .is_empty()
    );
    assert!(
        store
            .websockets(
                "default",
                &TimeRange {
                    from_ms: None,
                    to_ms: Some(999)
                }
            )
            .await?
            .is_empty()
    );
    Ok(())
}
