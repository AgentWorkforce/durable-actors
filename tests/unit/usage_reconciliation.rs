use super::*;
use reconciliation::{MemoryUsageSessions, RedisUsageSessions, UsageSessions};

#[tokio::test]
async fn stale_usage_is_corrected_by_a_heartbeat_and_then_a_real_stop() -> Result<()> {
    let sessions = Arc::new(MemoryUsageSessions::default());
    reconcile_and_correct(sessions.clone(), sessions).await
}

#[tokio::test]
async fn redis_coordinates_estimates_and_corrections_across_replicas() -> Result<()> {
    let Ok(url) = std::env::var("DURABLE_ACTORS_TEST_REDIS_URL") else {
        return Ok(());
    };
    let config = crate::redis_client::RedisConfig { url, ca_cert: None };
    let namespace = format!("test:usage:{{{}}}", uuid::Uuid::new_v4());
    let replica = || -> Result<Arc<dyn UsageSessions>> {
        Ok(Arc::new(RedisUsageSessions::new(
            crate::redis_client::Redis::connect(&config)?,
            &namespace,
        )))
    };
    reconcile_and_correct(replica()?, replica()?).await
}

async fn reconcile_and_correct(
    first_sessions: Arc<dyn UsageSessions>,
    second_sessions: Arc<dyn UsageSessions>,
) -> Result<()> {
    let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
    let publisher = Arc::new(Publisher(sent));
    let persistence = Arc::new(MemoryLifecyclePersistence::default());
    let first = LifecycleStore::new(persistence.clone())
        .with_publisher(publisher.clone())
        .with_usage_sessions(first_sessions);
    let second = LifecycleStore::new(persistence.clone())
        .with_publisher(publisher)
        .with_usage_sessions(second_sessions);
    first.record(event(LifecycleKind::Started, 1, 1000)).await?;
    let started = next(&mut received).await?;
    second
        .record(event(LifecycleKind::Heartbeat, 2, 11000))
        .await?;
    first.reconcile(45999).await?;
    assert!(received.try_recv().is_err());
    tokio::try_join!(first.reconcile(46000), second.reconcile(46000))?;
    let estimate = next(&mut received).await?;
    assert_eq!(estimate.usage_id, started.usage_id);
    assert_eq!(estimate.event.organization_id.as_deref(), Some("org-test"));
    assert_eq!(estimate.revision, 5);
    assert!(estimate.estimated);
    assert_eq!(estimate.ended_at_ms, Some(11000));
    assert_eq!(estimate.event.event_id, "session:2:estimated");
    assert_eq!(estimate.event.received_at_ms, 46000);
    assert_eq!(estimate.event.report.kind, LifecycleKind::Stopped);
    assert_eq!(first.actor_inventory("project").await?.actors[0].unknown, 1);
    assert_eq!(
        persistence.latest("project").await?[0].report.kind,
        LifecycleKind::Heartbeat
    );

    first
        .record(event(LifecycleKind::Heartbeat, 3, 47000))
        .await?;
    let resumed = next(&mut received).await?;
    assert_eq!(resumed.usage_id, started.usage_id);
    assert_eq!(resumed.event.organization_id, started.event.organization_id);
    assert_eq!(resumed.revision, 6);
    assert!(!resumed.estimated);
    assert_eq!(resumed.ended_at_ms, None);
    assert_eq!(resumed.event.report.kind, LifecycleKind::Heartbeat);
    tokio::try_join!(first.reconcile(82000), second.reconcile(82000))?;
    let estimate = next(&mut received).await?;
    assert_eq!(estimate.revision, 7);
    assert!(estimate.estimated);
    assert_eq!(estimate.ended_at_ms, Some(47000));

    second
        .record(event(LifecycleKind::Stopped, 4, 83000))
        .await?;
    let stopped = next(&mut received).await?;
    assert_eq!(stopped.usage_id, started.usage_id);
    assert_eq!(stopped.revision, 8);
    assert!(!stopped.estimated);
    assert_eq!(stopped.ended_at_ms, Some(83000));
    tokio::try_join!(first.reconcile(118000), second.reconcile(118000))?;
    assert!(received.try_recv().is_err());
    Ok(())
}

async fn next(
    receiver: &mut tokio::sync::mpsc::UnboundedReceiver<UsageUpdate>,
) -> Result<UsageUpdate> {
    Ok(
        tokio::time::timeout(Duration::from_secs(1), receiver.recv())
            .await?
            .unwrap(),
    )
}

fn event(kind: LifecycleKind, sequence: u64, time: u64) -> LifecycleEvent {
    LifecycleEvent {
        organization_id: Some("org-test".into()),
        event_id: format!("session:{sequence}"),
        actor: ActorKey {
            project_id: "project".into(),
            actor_name: "Counter".into(),
            actor_id: "one".into(),
        },
        host_id: "host".into(),
        session_id: "session".into(),
        region: "us-east".into(),
        received_at_ms: time,
        report: LifecycleReport {
            kind,
            owner_epoch: 1,
            sequence,
            started_at_ms: 1000,
            occurred_at_ms: time,
            resources: Default::default(),
            resident: Some(kind != LifecycleKind::Stopped),
            last_active_at_ms: None,
            database_bytes: None,
            waiting: Vec::new(),
        },
    }
}
