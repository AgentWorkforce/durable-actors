use super::*;

#[path = "usage_reconciliation.rs"]
mod usage_reconciliation;

#[tokio::test]
async fn usage_publishes_activation_boundaries_for_each_project() -> Result<()> {
    let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
    let store = LifecycleStore::default().with_publisher(Arc::new(Publisher(sent)));
    for project in ["first", "second"] {
        for (sequence, kind) in [
            (1, LifecycleKind::Started),
            (2, LifecycleKind::Heartbeat),
            (3, LifecycleKind::Draining),
            (4, LifecycleKind::Stopped),
        ] {
            store
                .record(LifecycleEvent {
                    organization_id: Some("org-test".into()),
                    event_id: format!("{project}-session:{sequence}"),
                    actor: ActorKey {
                        project_id: project.into(),
                        actor_name: "Counter".into(),
                        actor_id: "one".into(),
                    },
                    host_id: "host".into(),
                    session_id: format!("{project}-session"),
                    region: "us-east".into(),
                    received_at_ms: sequence * 1000,
                    report: LifecycleReport {
                        kind,
                        owner_epoch: 1,
                        sequence,
                        started_at_ms: 1000,
                        occurred_at_ms: sequence * 1000,
                        resources: ResourceLimits {
                            cpu_millis: 500,
                            memory_mib: 768,
                            memory_limit_mib: Some(768),
                            ..Default::default()
                        },
                        resident: Some(kind != LifecycleKind::Stopped),
                        last_active_at_ms: None,
                        database_bytes: None,
                        waiting: Vec::new(),
                    },
                })
                .await?;
        }
    }
    let mut updates = Vec::new();
    for _ in 0..4 {
        updates.push(
            tokio::time::timeout(Duration::from_secs(1), received.recv())
                .await?
                .unwrap(),
        );
    }
    updates.sort_by_key(|update| (update.event.actor.project_id.clone(), update.revision));
    for (project, pair) in ["first", "second"].into_iter().zip(updates.chunks_exact(2)) {
        let [started, stopped] = pair else {
            unreachable!()
        };
        assert_eq!(started.event.report.kind, LifecycleKind::Started);
        assert_eq!(stopped.event.report.kind, LifecycleKind::Stopped);
        assert_eq!(started.ended_at_ms, None);
        assert_eq!(started.usage_id, stopped.usage_id);
        assert_eq!((started.revision, stopped.revision), (2, 8));
        let payload = serde_json::to_value(stopped)?;
        assert_eq!(payload["organizationId"], "org-test");
        assert_eq!(payload["usageId"], format!("host:{project}-session"));
        assert_eq!(payload["eventId"], format!("{project}-session:4"));
        assert_eq!(
            payload["actor"],
            serde_json::json!({
                "project_id": project, "actor_name": "Counter", "actor_id": "one"
            })
        );
        assert_eq!(payload["resources"]["cpuMillis"], 500);
        assert_eq!(payload["resources"]["memoryMib"], 768);
        assert_eq!(payload["startedAtMs"], 1000);
        assert_eq!(payload["endedAtMs"], 4000);
        assert_eq!(payload["ownerEpoch"], 1);
        assert_eq!(payload["revision"], 8);
        assert_eq!(payload["estimated"], false);
    }
    assert!(received.try_recv().is_err());
    Ok(())
}

struct Publisher(tokio::sync::mpsc::UnboundedSender<UsageUpdate>);

#[async_trait]
impl UsagePublisher for Publisher {
    async fn publish(&self, event: &UsageUpdate) -> Result<()> {
        self.0.send(event.clone())?;
        Ok(())
    }
}

#[tokio::test]
async fn inventory_distinguishes_resident_idle_stopped_and_stale_actors() -> Result<()> {
    let store = LifecycleStore::default();
    let now = SystemClock.now_ms()?;
    for (id, kind, resident, received_at_ms) in [
        ("resident", LifecycleKind::Heartbeat, Some(true), now),
        ("idle", LifecycleKind::Heartbeat, Some(false), now),
        (
            "stopped",
            LifecycleKind::Stopped,
            Some(false),
            now - STALE_AFTER_MS - 1,
        ),
        (
            "stale",
            LifecycleKind::Heartbeat,
            Some(true),
            now - STALE_AFTER_MS - 1,
        ),
    ] {
        store
            .record(LifecycleEvent {
                organization_id: Some("org-test".into()),
                event_id: id.into(),
                actor: ActorKey {
                    project_id: "project".into(),
                    actor_name: "Counter".into(),
                    actor_id: id.into(),
                },
                host_id: id.into(),
                session_id: id.into(),
                region: "us-east".into(),
                received_at_ms,
                report: LifecycleReport {
                    kind,
                    owner_epoch: 1,
                    sequence: 1,
                    started_at_ms: 1000,
                    occurred_at_ms: 1000,
                    resources: Default::default(),
                    resident,
                    last_active_at_ms: None,
                    database_bytes: None,
                    waiting: vec![WaitingOperation {
                        id: "request".into(),
                        operation: "increment".into(),
                    }],
                },
            })
            .await?;
    }
    let snapshot = store.actor_inventory("project").await?;
    let actor = &snapshot.actors[0];
    assert_eq!((actor.live, actor.dormant, actor.unknown), (1, 2, 1));
    for instance in &actor.instances {
        match instance.actor_id.as_str() {
            "resident" | "idle" => {
                assert_eq!(instance.waiting.as_ref().unwrap()[0].operation, "increment")
            }
            "stopped" => assert!(instance.waiting.as_ref().unwrap().is_empty()),
            "stale" => assert!(instance.waiting.is_none()),
            _ => unreachable!(),
        }
    }
    assert!(
        store
            .actor_inventory("another-project")
            .await?
            .actors
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn latest_report_follows_activation_shutdown_and_reactivation_per_project() -> Result<()> {
    let persistence = Arc::new(MemoryLifecyclePersistence::default());
    let store = LifecycleStore::new(persistence.clone());
    let actor = ActorKey {
        project_id: "project".into(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
    };
    let mut event = LifecycleEvent {
        organization_id: Some("org-test".into()),
        event_id: "session:1".into(),
        actor: actor.clone(),
        host_id: "host".into(),
        session_id: "session".into(),
        region: "us-east".into(),
        received_at_ms: 1000,
        report: LifecycleReport {
            kind: LifecycleKind::Started,
            owner_epoch: 1,
            sequence: 1,
            started_at_ms: 1000,
            occurred_at_ms: 1000,
            resources: Default::default(),
            resident: Some(true),
            last_active_at_ms: None,
            database_bytes: None,
            waiting: Vec::new(),
        },
    };
    store.record(event.clone()).await?;
    event.actor.project_id = "other-project".into();
    store.record(event.clone()).await?;
    let other_actor = event.actor.clone();
    event.actor = actor.clone();
    event.event_id = "session:2".into();
    event.report.kind = LifecycleKind::Stopped;
    event.report.sequence = 2;
    event.report.occurred_at_ms = 2000;
    event.received_at_ms = 2000;
    store.record(event.clone()).await?;
    assert_eq!(
        persistence.0.get(&actor).await.unwrap().report.kind,
        LifecycleKind::Stopped
    );
    assert_eq!(
        persistence.0.get(&other_actor).await.unwrap().report.kind,
        LifecycleKind::Started
    );
    let stopped = event.clone();
    event.event_id = "next-session:1".into();
    event.host_id = "next-host".into();
    event.session_id = "next-session".into();
    event.report.kind = LifecycleKind::Started;
    event.report.owner_epoch = 2;
    event.report.sequence = 1;
    event.report.started_at_ms = 3000;
    event.report.occurred_at_ms = 3000;
    event.received_at_ms = 3000;
    store.record(event.clone()).await?;
    store.record(stopped).await?;
    assert_eq!(
        persistence.0.get(&actor).await.unwrap().event_id,
        event.event_id
    );
    Ok(())
}

#[tokio::test]
async fn residency_tracks_eviction_reactivation_gaps_and_range_boundaries() -> Result<()> {
    let store = LifecycleStore::default();
    for (project, actor_name) in [
        ("project", "Room"),
        ("other", "Room"),
        ("project", "Counter"),
    ] {
        for (sequence, time, resident, kind, epoch) in [
            (1, 1_000, true, LifecycleKind::Started, 1),
            (2, 11_000, true, LifecycleKind::Heartbeat, 1),
            (3, 21_000, false, LifecycleKind::Heartbeat, 1),
            (4, 31_000, true, LifecycleKind::Heartbeat, 1),
            (5, 41_000, false, LifecycleKind::Stopped, 1),
            (1, 61_000, true, LifecycleKind::Started, 2),
        ] {
            let event = LifecycleEvent {
                organization_id: None,
                event_id: format!("{epoch}:{sequence}"),
                actor: ActorKey {
                    project_id: project.into(),
                    actor_name: actor_name.into(),
                    actor_id: "one".into(),
                },
                host_id: "host".into(),
                session_id: format!("session-{epoch}"),
                region: "us-east".into(),
                received_at_ms: time,
                report: LifecycleReport {
                    kind,
                    owner_epoch: epoch,
                    sequence,
                    started_at_ms: 1000,
                    occurred_at_ms: time,
                    resources: Default::default(),
                    resident: Some(resident),
                    last_active_at_ms: Some(500),
                    database_bytes: None,
                    waiting: vec![],
                },
            };
            store.record(event.clone()).await?;
            store.record(event).await?;
        }
    }
    let history = store
        .residency_history("project", &rooms(15_000, 105_000))
        .await?;
    assert!(!history.truncated);
    let periods: Vec<_> = history
        .periods
        .iter()
        .map(|period| (period.status, period.from_ms, period.to_ms))
        .collect();
    assert_eq!(
        periods,
        vec![
            (ActorResidency::Live, 15_000, 21_000),
            (ActorResidency::Dormant, 21_000, 31_000),
            (ActorResidency::Live, 31_000, 41_000),
            (ActorResidency::Dormant, 41_000, 61_000),
            (ActorResidency::Live, 61_000, 96_000),
            (ActorResidency::Unknown, 96_000, 105_000),
        ]
    );
    assert!(
        store
            .residency_history("absent", &rooms(0, 105_000))
            .await?
            .periods
            .is_empty()
    );
    let historical = store
        .residency_history("project", &rooms(45_000, 50_000))
        .await?;
    assert_eq!(historical.periods.len(), 1);
    assert_eq!(historical.periods[0].status, ActorResidency::Dormant);
    assert_eq!(
        store.actor_inventory("project").await?.actors[0].instances[0].last_active_at_ms,
        Some(500)
    );
    Ok(())
}

fn rooms(from_ms: u64, to_ms: u64) -> ResidencyQuery {
    ResidencyQuery {
        actor_name: "Room".into(),
        actor_id: None,
        from_ms,
        to_ms,
    }
}

#[tokio::test]
async fn residency_reads_only_reports_that_can_reach_the_window() -> Result<()> {
    use testing::report;
    let store = LifecycleStore::default();
    for event in [
        report(
            "project",
            0,
            1,
            1,
            10_000,
            LifecycleKind::Stopped,
            Some(false),
        ),
        report(
            "project",
            1,
            1,
            1,
            70_000,
            LifecycleKind::Heartbeat,
            Some(true),
        ),
        report(
            "project",
            2,
            1,
            1,
            10_000,
            LifecycleKind::Heartbeat,
            Some(true),
        ),
        report(
            "project",
            2,
            1,
            2,
            120_000,
            LifecycleKind::Heartbeat,
            Some(true),
        ),
    ] {
        store.record(event).await?;
    }
    let periods = |history: ResidencyHistory| -> Vec<_> {
        history
            .periods
            .into_iter()
            .map(|period| (period.actor_id, period.status, period.from_ms, period.to_ms))
            .collect()
    };
    let class = store
        .residency_history("project", &rooms(100_000, 130_000))
        .await?;
    assert_eq!(
        periods(class),
        [
            ("room-1".into(), ActorResidency::Live, 100_000, 105_000),
            ("room-1".into(), ActorResidency::Unknown, 105_000, 130_000),
            ("room-2".into(), ActorResidency::Live, 120_000, 130_000),
        ]
    );
    let one = ResidencyQuery {
        actor_id: Some("room-2".into()),
        ..rooms(100_000, 130_000)
    };
    assert_eq!(
        periods(store.residency_history("project", &one).await?),
        [("room-2".into(), ActorResidency::Live, 120_000, 130_000)]
    );
    Ok(())
}
