use std::{sync::Arc, time::Duration};

use anyhow::Result;
use async_trait::async_trait;
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};

use crate::{
    actor::ActorExecutor,
    actor_lifecycle::{HEARTBEAT_INTERVAL, LifecycleKind, LifecycleReport},
    clock::{Clock, SystemClock},
    control_plane::ControlPlaneClient,
    sandbox::ResourceLimits,
};

use super::actor_host::ActorStatus;

const REPORT_TIMEOUT: Duration = Duration::from_secs(5);
const SIZE_TIMEOUT: Duration = Duration::from_secs(1);

#[async_trait]
pub(super) trait LifecycleTransport: Send + Sync {
    async fn report(&self, report: LifecycleReport) -> Result<()>;
}

#[async_trait]
pub(super) trait DatabaseSize: Send + Sync {
    /// `None` when the size can't be read now, such as while a transaction is open.
    async fn database_bytes(&self) -> Option<u64>;
}

#[async_trait]
impl LifecycleTransport for ControlPlaneClient {
    async fn report(&self, report: LifecycleReport) -> Result<()> {
        self.report_lifecycle(report).await
    }
}

pub(super) struct HostLifecycle {
    commands: mpsc::Sender<LifecycleKind>,
    task: JoinHandle<()>,
}

impl HostLifecycle {
    pub(super) fn start(
        transport: Arc<dyn LifecycleTransport>,
        owner_epoch: u64,
        resources: ResourceLimits,
        executor: Arc<dyn ActorExecutor>,
        database: Arc<dyn DatabaseSize>,
        status: watch::Receiver<ActorStatus>,
    ) -> Result<Self> {
        let (commands, receiver) = mpsc::channel(2);
        let reporter = Reporter {
            transport,
            executor,
            database,
            sampled_at: None,
            status,
            report: LifecycleReport {
                kind: LifecycleKind::Started,
                owner_epoch,
                sequence: 0,
                started_at_ms: SystemClock.now_ms()?,
                occurred_at_ms: 0,
                resources,
                resident: None,
                last_active_at_ms: None,
                waiting: Vec::new(),
                database_bytes: None,
            },
        };
        let task = tokio::spawn(reporter.run(receiver));
        Ok(Self { commands, task })
    }

    pub(super) fn draining(&self) {
        let _ = self.commands.try_send(LifecycleKind::Draining);
    }

    pub(super) async fn stop(mut self) {
        let _ = self.commands.try_send(LifecycleKind::Stopped);
        let _ = tokio::time::timeout(REPORT_TIMEOUT + Duration::from_secs(1), &mut self.task).await;
    }
}

impl Drop for HostLifecycle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Reporter {
    transport: Arc<dyn LifecycleTransport>,
    executor: Arc<dyn ActorExecutor>,
    database: Arc<dyn DatabaseSize>,
    sampled_at: Option<tokio::time::Instant>,
    status: watch::Receiver<ActorStatus>,
    report: LifecycleReport,
}

impl Reporter {
    async fn run(mut self, mut commands: mpsc::Receiver<LifecycleKind>) {
        let mut residency = self.executor.residency_changes();
        self.send(LifecycleKind::Started).await;
        let mut interval = tokio::time::interval_at(
            tokio::time::Instant::now() + HEARTBEAT_INTERVAL,
            HEARTBEAT_INTERVAL,
        );
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut draining = false;
        let mut status_open = true;
        loop {
            let kind = tokio::select! {
                biased;
                command = commands.recv() => match command { Some(kind) => kind, None => break },
                _ = interval.tick(), if !draining => LifecycleKind::Heartbeat,
                changed = self.status.changed(), if !draining && status_open => {
                    if changed.is_err() { status_open = false; continue; }
                    LifecycleKind::Heartbeat
                },
                changed = residency_changed(&mut residency), if !draining => {
                    if changed.is_err() { residency = None; continue; }
                    LifecycleKind::Heartbeat
                },
            };
            self.send(kind).await;
            if kind == LifecycleKind::Stopped {
                break;
            }
            draining |= kind == LifecycleKind::Draining;
        }
    }

    async fn send(&mut self, kind: LifecycleKind) {
        let Ok(now) = SystemClock.now_ms() else {
            return;
        };
        self.report.kind = kind;
        self.report.sequence += 1;
        self.report.occurred_at_ms = now;
        self.report.resident = self.executor.resident();
        let status = self.status.borrow_and_update().clone();
        self.report.waiting = status.waiting;
        self.report.last_active_at_ms = status.last_active_at_ms;
        if kind == LifecycleKind::Stopped {
            self.report.resident = Some(false);
            self.report.waiting.clear();
        }
        self.sample_database(kind).await;
        if let Err(error) =
            tokio::time::timeout(REPORT_TIMEOUT, self.transport.report(self.report.clone()))
                .await
                .map_err(anyhow::Error::from)
                .and_then(|result| result)
        {
            tracing::warn!(?kind, %error, "host lifecycle report failed");
        }
    }

    /// Status changes also send reports, so the size is read at most once per heartbeat interval,
    /// and once more on stopping. A size that can't be read now keeps the last one.
    async fn sample_database(&mut self, kind: LifecycleKind) {
        let due = kind == LifecycleKind::Stopped
            || self
                .sampled_at
                .is_none_or(|at| at.elapsed() >= HEARTBEAT_INTERVAL);
        if !due {
            return;
        }
        self.sampled_at = Some(tokio::time::Instant::now());
        if let Ok(Some(bytes)) =
            tokio::time::timeout(SIZE_TIMEOUT, self.database.database_bytes()).await
        {
            self.report.database_bytes = Some(bytes);
        }
    }
}

async fn residency_changed(
    changes: &mut Option<watch::Receiver<()>>,
) -> Result<(), watch::error::RecvError> {
    match changes {
        Some(changes) => changes.changed().await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
#[path = "../../tests/unit/host/lifecycle.rs"]
mod tests;
