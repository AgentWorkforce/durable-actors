use std::{collections::BTreeMap, sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use moka::{future::Cache, ops::compute::Op};
use serde::{Deserialize, Serialize};

use crate::{
    actor::ActorKey,
    clock::{Clock, SystemClock},
    host_leases::WaitingOperation,
    placement::{
        ActorInstanceOverview, ActorInventory, ActorInventoryReader, ActorInventorySnapshot,
        ActorResidency, ResidencyHistory, ResidencyQuery,
    },
    platform_events::{NoPlatformEvents, PlatformEvents},
    sandbox::ResourceLimits,
};

pub(crate) const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
pub(crate) const STALE_AFTER_MS: u64 = 35_000;

mod pubsub;
pub(crate) mod reconciliation;
pub(crate) mod residency;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LifecycleKind {
    Started,
    Heartbeat,
    Draining,
    Stopped,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LifecycleReport {
    pub kind: LifecycleKind,
    pub owner_epoch: u64,
    pub sequence: u64,
    pub started_at_ms: u64,
    pub occurred_at_ms: u64,
    pub resources: ResourceLimits,
    pub resident: Option<bool>,
    #[serde(default)]
    pub last_active_at_ms: Option<u64>,
    pub waiting: Vec<WaitingOperation>,
    /// The SQLite database's committed size: page count × page size.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database_bytes: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LifecycleEvent {
    pub organization_id: Option<String>,
    pub event_id: String,
    pub actor: ActorKey,
    pub host_id: String,
    pub session_id: String,
    pub region: String,
    pub received_at_ms: u64,
    #[serde(flatten)]
    pub report: LifecycleReport,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UsageUpdate {
    pub usage_id: String,
    pub revision: u64,
    pub ended_at_ms: Option<u64>,
    pub estimated: bool,
    #[serde(flatten)]
    pub event: LifecycleEvent,
}

impl From<LifecycleEvent> for UsageUpdate {
    fn from(event: LifecycleEvent) -> Self {
        Self {
            usage_id: format!("{}:{}", event.host_id, event.session_id),
            revision: event.report.sequence * 2,
            ended_at_ms: (event.report.kind == LifecycleKind::Stopped)
                .then_some(event.report.occurred_at_ms),
            estimated: false,
            event,
        }
    }
}

impl UsageUpdate {
    fn estimated_stop(mut event: LifecycleEvent, now: u64) -> Self {
        event.event_id.push_str(":estimated");
        event.report.kind = LifecycleKind::Stopped;
        event.report.resident = None;
        event.report.waiting.clear();
        event.received_at_ms = now;
        let mut update = Self::from(event);
        update.revision += 1;
        update.estimated = true;
        update
    }
}

#[async_trait]
pub(crate) trait LifecyclePersistence: Send + Sync {
    async fn record(&self, event: &LifecycleEvent) -> Result<()>;
    async fn latest(&self, project: &str) -> Result<Vec<LifecycleEvent>>;

    /// The runs formed by the reports the query covers.
    async fn residency_runs(
        &self,
        project: &str,
        query: &ResidencyQuery,
    ) -> Result<Vec<residency::ResidencyRun>> {
        let latest = self.latest(project).await?;
        Ok(residency::runs(reports_within(&latest, query)))
    }
}

pub(crate) fn reports_within<'a>(
    events: impl IntoIterator<Item = &'a LifecycleEvent>,
    query: &ResidencyQuery,
) -> Vec<residency::ResidencyReport> {
    events
        .into_iter()
        .filter(|event| {
            event.actor.actor_name == query.actor_name
                && query
                    .actor_id
                    .as_ref()
                    .is_none_or(|actor_id| *actor_id == event.actor.actor_id)
                && (query.from_ms..=query.to_ms).contains(&event.report.occurred_at_ms)
        })
        .map(residency::ResidencyReport::from)
        .collect()
}

#[async_trait]
pub(crate) trait UsagePublisher: Send + Sync {
    async fn publish(&self, event: &UsageUpdate) -> Result<()>;
}

#[async_trait]
pub(crate) trait LifecycleObserver: Send + Sync {
    async fn observe(&self, event: &LifecycleEvent) -> Result<()>;
}

#[derive(Clone)]
pub(crate) struct LifecycleStore {
    persistence: Arc<dyn LifecyclePersistence>,
    usage: Option<Arc<dyn UsagePublisher>>,
    usage_sessions: Arc<dyn reconciliation::UsageSessions>,
    observer: Option<Arc<dyn LifecycleObserver>>,
    events: Arc<dyn PlatformEvents<LifecycleEvent>>,
}

impl Default for LifecycleStore {
    fn default() -> Self {
        Self::new(Arc::new(MemoryLifecyclePersistence::default()))
    }
}

impl LifecycleStore {
    pub(crate) fn new(persistence: Arc<dyn LifecyclePersistence>) -> Self {
        Self {
            persistence,
            usage: None,
            observer: None,
            events: Arc::new(NoPlatformEvents),
            usage_sessions: Arc::new(reconciliation::MemoryUsageSessions::default()),
        }
    }

    /// Reports each recorded event to observability, which stores and publishes it like any
    /// other platform event.
    pub(crate) fn with_events(mut self, events: Arc<dyn PlatformEvents<LifecycleEvent>>) -> Self {
        self.events = events;
        self
    }

    pub(crate) fn with_observer(mut self, observer: Arc<dyn LifecycleObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    pub(crate) fn with_publisher(mut self, publisher: Arc<dyn UsagePublisher>) -> Self {
        self.usage = Some(publisher);
        self
    }

    pub(crate) fn with_usage_sessions(
        mut self,
        sessions: Arc<dyn reconciliation::UsageSessions>,
    ) -> Self {
        self.usage_sessions = sessions;
        self
    }

    pub(crate) async fn with_usage_topic(mut self, topic: Option<&str>) -> Result<Self> {
        if let Some(topic) = topic {
            let topic = crate::pubsub::Topic::connect(topic).await;
            self = self.with_publisher(Arc::new(topic.context("usage topic")?));
        }
        Ok(self)
    }

    pub(crate) async fn record(&self, event: LifecycleEvent) -> Result<()> {
        ensure!(
            event.report.sequence > 0 && event.report.sequence < (1 << 52),
            "invalid lifecycle sequence"
        );
        ensure!(event.report.owner_epoch > 0, "invalid ownership epoch");
        ensure!(
            event.report.occurred_at_ms >= event.report.started_at_ms,
            "invalid lifecycle time range"
        );
        let (persisted, (), observed) = tokio::join!(
            self.persistence.record(&event),
            self.record_usage(&event),
            async {
                match &self.observer {
                    Some(observer) => observer.observe(&event).await,
                    None => Ok(()),
                }
            },
        );
        self.events.record(event);
        persisted.and(observed)
    }

    pub(crate) fn start_reconciliation(&self, stop: tokio_util::sync::CancellationToken) {
        if self.usage.is_none() {
            return;
        }
        let store = self.clone();
        tokio::spawn(async move {
            let work = async {
                let mut interval = tokio::time::interval(HEARTBEAT_INTERVAL);
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    interval.tick().await;
                    let result = async { store.reconcile(SystemClock.now_ms()?).await }.await;
                    if let Err(error) = result {
                        tracing::warn!(%error, "usage reconciliation failed");
                    }
                }
            };
            tokio::select! { _ = stop.cancelled() => {}, _ = work => {} }
        });
    }

    async fn record_usage(&self, event: &LifecycleEvent) {
        if self.usage.is_none() {
            return;
        }
        let boundary = matches!(
            event.report.kind,
            LifecycleKind::Started | LifecycleKind::Stopped
        );
        if boundary {
            self.publish_usage(UsageUpdate::from(event.clone()));
        }
        match self.usage_sessions.observe(event).await {
            Ok(true) if !boundary => self.publish_usage(UsageUpdate::from(event.clone())),
            Err(error) => tracing::warn!(%error, "usage session could not be recorded"),
            _ => {}
        }
    }

    async fn reconcile(&self, now: u64) -> Result<()> {
        for event in self.usage_sessions.expire(now).await? {
            self.publish_usage(UsageUpdate::estimated_stop(event, now));
        }
        Ok(())
    }

    fn publish_usage(&self, update: UsageUpdate) {
        let Some(publisher) = self.usage.clone() else {
            return;
        };
        tokio::spawn(async move {
            if let Err(error) =
                tokio::time::timeout(Duration::from_secs(5), publisher.publish(&update))
                    .await
                    .map_err(anyhow::Error::from)
                    .and_then(|result| result)
            {
                tracing::warn!(event_id = update.event.event_id, %error, "usage publication failed");
            }
        });
    }
}

#[async_trait]
impl ActorInventoryReader for LifecycleStore {
    async fn actor_inventory(&self, project: &str) -> Result<ActorInventorySnapshot> {
        let now = SystemClock.now_ms()?;
        let mut actors = BTreeMap::<String, ActorInventory>::new();
        for event in self.persistence.latest(project).await? {
            let actor = actors
                .entry(event.actor.actor_name.clone())
                .or_insert_with(|| ActorInventory {
                    actor_name: event.actor.actor_name.clone(),
                    ..Default::default()
                });
            let instance = instance_overview(event, now);
            match instance.status {
                ActorResidency::Live => actor.live += 1,
                ActorResidency::Dormant => actor.dormant += 1,
                ActorResidency::Unknown => actor.unknown += 1,
            }
            actor.instances.push(instance);
        }
        Ok(ActorInventorySnapshot {
            actors: actors.into_values().collect(),
            connections_complete: true,
            gateways: BTreeMap::new(),
        })
    }

    async fn residency_history(
        &self,
        project: &str,
        query: &ResidencyQuery,
    ) -> Result<ResidencyHistory> {
        let reachable = ResidencyQuery {
            from_ms: query.from_ms.saturating_sub(residency::LOOKBACK_MS),
            ..query.clone()
        };
        let runs = self.persistence.residency_runs(project, &reachable).await?;
        Ok(residency::history(runs, query.from_ms, query.to_ms))
    }
}

/// The instance as its latest report leaves it at `now`, without connections.
pub(crate) fn instance_overview(event: LifecycleEvent, now: u64) -> ActorInstanceOverview {
    let status = residency(&event, now);
    let stopped = event.report.kind == LifecycleKind::Stopped;
    ActorInstanceOverview {
        actor_id: event.actor.actor_id,
        status,
        last_active_at_ms: event.report.last_active_at_ms,
        reported_at_ms: Some(event.received_at_ms),
        stale_at_ms: (!stopped).then(|| event.received_at_ms.saturating_add(STALE_AFTER_MS)),
        connections: Vec::new(),
        waiting: match status {
            ActorResidency::Unknown => None,
            ActorResidency::Dormant if stopped => Some(Vec::new()),
            _ => Some(event.report.waiting),
        },
    }
}

fn residency(event: &LifecycleEvent, now: u64) -> ActorResidency {
    if event.report.kind == LifecycleKind::Stopped {
        return ActorResidency::Dormant;
    }
    if now.saturating_sub(event.received_at_ms) > STALE_AFTER_MS {
        return ActorResidency::Unknown;
    }
    match event.report.resident {
        Some(true) => ActorResidency::Live,
        Some(false) => ActorResidency::Dormant,
        None => ActorResidency::Unknown,
    }
}

struct MemoryLifecyclePersistence(
    Cache<ActorKey, LifecycleEvent>,
    Cache<(ActorKey, String), LifecycleEvent>,
);

impl Default for MemoryLifecyclePersistence {
    fn default() -> Self {
        Self(
            Cache::builder().max_capacity(10_000).build(),
            Cache::builder()
                .max_capacity(100_000)
                .time_to_live(Duration::from_secs(86_400))
                .build(),
        )
    }
}

#[async_trait]
impl LifecyclePersistence for MemoryLifecyclePersistence {
    async fn record(&self, event: &LifecycleEvent) -> Result<()> {
        self.1
            .insert((event.actor.clone(), event.event_id.clone()), event.clone())
            .await;
        self.0
            .entry(event.actor.clone())
            .and_compute_with(|previous| async move {
                if previous.is_some_and(|previous| {
                    let old = &previous.value().report;
                    (old.owner_epoch, old.sequence)
                        >= (event.report.owner_epoch, event.report.sequence)
                }) {
                    Op::Nop
                } else {
                    Op::Put(event.clone())
                }
            })
            .await;
        Ok(())
    }

    async fn latest(&self, project: &str) -> Result<Vec<LifecycleEvent>> {
        let mut events: Vec<_> = self
            .0
            .iter()
            .filter(|(actor, _)| actor.project_id == project)
            .map(|(_, event)| event)
            .collect();
        events.sort_by(|a, b| {
            (&a.actor.actor_name, &a.actor.actor_id).cmp(&(&b.actor.actor_name, &b.actor.actor_id))
        });
        Ok(events)
    }

    async fn residency_runs(
        &self,
        project: &str,
        query: &ResidencyQuery,
    ) -> Result<Vec<residency::ResidencyRun>> {
        let events: Vec<_> = self
            .1
            .iter()
            .map(|(_, event)| event)
            .chain(self.0.iter().map(|(_, event)| event))
            .filter(|event| event.actor.project_id == project)
            .collect();
        Ok(residency::runs(reports_within(&events, query)))
    }
}

#[cfg(test)]
#[path = "../tests/support/residency.rs"]
pub(crate) mod testing;

#[cfg(test)]
#[path = "../tests/unit/actor_lifecycle.rs"]
mod tests;
