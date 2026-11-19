use super::*;

struct Transport(mpsc::UnboundedSender<LifecycleReport>);

#[async_trait]
impl LifecycleTransport for Transport {
    async fn report(&self, report: LifecycleReport) -> Result<()> {
        self.0.send(report)?;
        Ok(())
    }
}

struct Executor;

#[async_trait]
impl ActorExecutor for Executor {
    fn supports(&self, _: &str) -> bool {
        true
    }
    fn resident(&self) -> Option<bool> {
        Some(true)
    }
    async fn invoke(
        &self,
        _: crate::actor::ActorMethodInvocation,
        _: Arc<dyn crate::actor::ActorDatabase>,
    ) -> Result<crate::actor::ActorMethodOutcome> {
        unreachable!()
    }
}

struct Database;

#[async_trait]
impl DatabaseSize for Database {
    async fn database_bytes(&self) -> Option<u64> {
        Some(4096)
    }
}

#[tokio::test(start_paused = true)]
async fn reports_activation_heartbeat_and_graceful_shutdown() -> Result<()> {
    let (sent, mut received) = mpsc::unbounded_channel();
    let (status, observed) = watch::channel(ActorStatus::default());
    let lifecycle = HostLifecycle::start(
        Arc::new(Transport(sent)),
        7,
        ResourceLimits::default(),
        Arc::new(Executor),
        Arc::new(Database),
        observed,
    )?;
    let started = received.recv().await.unwrap();
    assert_eq!(started.kind, LifecycleKind::Started);
    assert_eq!(started.owner_epoch, 7);
    assert_eq!(started.sequence, 1);
    assert!(started.occurred_at_ms >= started.started_at_ms);
    assert_eq!(started.database_bytes, Some(4096));
    let changed_at = tokio::time::Instant::now();
    status.send_modify(|status| {
        status.last_active_at_ms = Some(started.occurred_at_ms);
        status.waiting.push(crate::host_leases::WaitingOperation {
            id: "request".into(),
            operation: "increment".into(),
        })
    });
    let heartbeat = received.recv().await.unwrap();
    assert!(changed_at.elapsed() < HEARTBEAT_INTERVAL);
    assert_eq!(heartbeat.kind, LifecycleKind::Heartbeat);
    assert_eq!(heartbeat.sequence, 2);
    assert_eq!(heartbeat.resident, Some(true));
    assert_eq!(heartbeat.waiting[0].operation, "increment");
    assert_eq!(heartbeat.last_active_at_ms, Some(started.occurred_at_ms));
    drop(status);
    tokio::task::yield_now().await;
    lifecycle.draining();
    lifecycle.stop().await;
    let draining = received.recv().await.unwrap();
    assert_eq!(draining.kind, LifecycleKind::Draining);
    assert_eq!(draining.sequence, 3);
    let stopped = received.recv().await.unwrap();
    assert_eq!(stopped.kind, LifecycleKind::Stopped);
    assert_eq!(stopped.sequence, 4);
    assert_eq!(stopped.started_at_ms, started.started_at_ms);
    assert_eq!(stopped.resources, started.resources);
    assert_eq!(stopped.resident, Some(false));
    assert!(stopped.waiting.is_empty());
    assert_eq!(stopped.database_bytes, Some(4096));
    Ok(())
}
