use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result};
use async_trait::async_trait;
use backon::{ExponentialBuilder, Retryable};
use rusqlite::{Connection, OptionalExtension, params};
use tokio::{sync::Semaphore, task::JoinSet};
use tokio_util::sync::CancellationToken;

use crate::{actor::ActorKey, clock::Clock, postgres::PostgresDatabase};

use super::ControlPlaneService;

pub(super) const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(100);
pub(super) const MAX_IN_FLIGHT: usize = 100;
pub(super) const MAX_ATTEMPTS: usize = 3;
pub(super) const MIN_RETRY_DELAY: Duration = Duration::from_secs(1);
pub(super) const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(60);
// A live delivery must finish before its claim expires, or another poll delivers it concurrently.
pub(super) const QUEUED_TIMEOUT: Duration = Duration::from_secs(300);
pub(super) const ALARM_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
pub(super) const ALARM_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, PartialEq)]
pub(super) struct ClaimedAlarm {
    pub actor: ActorKey,
    pub revision: u64,
}

#[async_trait]
pub(super) trait AlarmStore: Send + Sync {
    async fn get(&self, actor: &ActorKey) -> Result<Option<i64>>;
    async fn update(&self, actor: &ActorKey, timestamp_ms: Option<i64>) -> Result<()>;
    async fn claim_due(
        &self,
        now_ms: i64,
        expired_before_ms: i64,
        limit: usize,
    ) -> Result<Vec<ClaimedAlarm>>;
    async fn finish(&self, alarm: &ClaimedAlarm) -> Result<()>;
}

#[async_trait]
pub(super) trait AlarmDelivery: Send + Sync {
    async fn attempt(&self, alarm: &ClaimedAlarm) -> Result<()>;
}

pub(super) fn start(
    service: ControlPlaneService,
    store: Arc<dyn AlarmStore>,
    poll_interval: Duration,
    stop: CancellationToken,
) {
    let client = reqwest::Client::builder()
        .connect_timeout(ALARM_CONNECT_TIMEOUT)
        .timeout(ALARM_REQUEST_TIMEOUT)
        .build()
        .expect("alarm invocation HTTP client");
    let delivery = Arc::new(InvocationDelivery { service, client });
    tokio::spawn(run(store, delivery, poll_interval, stop));
}

async fn run(
    store: Arc<dyn AlarmStore>,
    delivery: Arc<dyn AlarmDelivery>,
    poll_interval: Duration,
    stop: CancellationToken,
) {
    let slots = Arc::new(Semaphore::new(MAX_IN_FLIGHT));
    let mut tasks = JoinSet::new();
    loop {
        tokio::select! {
            _ = stop.cancelled() => return,
            _ = tokio::time::sleep(poll_interval) => {}
        }
        while tasks.try_join_next().is_some() {}
        for alarm in claim(&*store, slots.available_permits()).await {
            let slot = slots
                .clone()
                .try_acquire_owned()
                .expect("alarm claims stay within free slots");
            let (store, delivery) = (store.clone(), delivery.clone());
            tasks.spawn(async move {
                deliver(&*store, &*delivery, &alarm).await;
                drop(slot);
            });
        }
    }
}

async fn claim(store: &dyn AlarmStore, limit: usize) -> Vec<ClaimedAlarm> {
    if limit == 0 {
        return Vec::new();
    }
    match claim_due(store, limit).await {
        Ok(alarms) => alarms,
        Err(error) => {
            tracing::warn!(error = %format!("{error:#}"), "could not claim due alarms");
            Vec::new()
        }
    }
}

async fn claim_due(store: &dyn AlarmStore, limit: usize) -> Result<Vec<ClaimedAlarm>> {
    let now_ms =
        i64::try_from(crate::clock::SystemClock.now_ms()?).context("alarm clock overflow")?;
    let queued_timeout_ms = i64::try_from(QUEUED_TIMEOUT.as_millis())?;
    store
        .claim_due(now_ms, now_ms.saturating_sub(queued_timeout_ms), limit)
        .await
}

async fn deliver(store: &dyn AlarmStore, delivery: &dyn AlarmDelivery, alarm: &ClaimedAlarm) {
    let actor = alarm.actor.storage_key();
    if let Err(error) = attempt_with_retries(delivery, alarm).await {
        if error.is::<super::access::AccessDisabled>() {
            return;
        }
        tracing::warn!(error = %format!("{error:#}"), %actor, "alarm abandoned after retries");
    }
    if let Err(error) = store.finish(alarm).await {
        tracing::warn!(error = %format!("{error:#}"), %actor, "could not finish alarm");
    }
}

async fn attempt_with_retries(delivery: &dyn AlarmDelivery, alarm: &ClaimedAlarm) -> Result<()> {
    (|| attempt(delivery, alarm))
        .retry(
            ExponentialBuilder::default()
                .with_min_delay(MIN_RETRY_DELAY)
                .with_max_times(MAX_ATTEMPTS - 1)
                .with_jitter(),
        )
        .when(|error| !error.is::<super::access::AccessDisabled>())
        .await
}

async fn attempt(delivery: &dyn AlarmDelivery, alarm: &ClaimedAlarm) -> Result<()> {
    tokio::time::timeout(ATTEMPT_TIMEOUT, delivery.attempt(alarm))
        .await
        .context("alarm attempt timed out")?
}

struct InvocationDelivery {
    service: ControlPlaneService,
    client: reqwest::Client,
}

#[async_trait]
impl AlarmDelivery for InvocationDelivery {
    async fn attempt(&self, alarm: &ClaimedAlarm) -> Result<()> {
        super::invocation::invoke_alarm(&self.service, &self.client, alarm).await
    }
}

pub(super) struct PostgresAlarmStore {
    database: PostgresDatabase,
}

impl PostgresAlarmStore {
    pub(super) fn new(database: PostgresDatabase) -> Self {
        Self { database }
    }
}

#[async_trait]
impl AlarmStore for PostgresAlarmStore {
    async fn get(&self, actor: &ActorKey) -> Result<Option<i64>> {
        Ok(self
            .database
            .query_opt(
                "SELECT timestamp_ms
                 FROM durable_actors_alarm
                 WHERE project_id = $1 AND actor_name = $2 AND actor_id = $3",
                &[&actor.project_id, &actor.actor_name, &actor.actor_id],
            )
            .await?
            .and_then(|row| row.get("timestamp_ms")))
    }

    async fn update(&self, actor: &ActorKey, timestamp_ms: Option<i64>) -> Result<()> {
        self.database
            .execute(
                "INSERT INTO durable_actors_alarm (
                    project_id, actor_name, actor_id, revision, timestamp_ms
                 ) VALUES ($1, $2, $3, 1, $4)
                 ON CONFLICT (project_id, actor_name, actor_id) DO UPDATE SET
                    revision = durable_actors_alarm.revision + 1,
                    timestamp_ms = EXCLUDED.timestamp_ms,
                    queued_at_ms = NULL",
                &[
                    &actor.project_id,
                    &actor.actor_name,
                    &actor.actor_id,
                    &timestamp_ms,
                ],
            )
            .await?;
        Ok(())
    }

    async fn claim_due(
        &self,
        now_ms: i64,
        expired_before_ms: i64,
        limit: usize,
    ) -> Result<Vec<ClaimedAlarm>> {
        let limit = i64::try_from(limit).context("alarm batch size overflow")?;
        let client = self.database.connection().await?;
        let statement = client
            .prepare_cached(
                "WITH due AS (
                    SELECT project_id, actor_name, actor_id
                    FROM durable_actors_alarm a
                    WHERE timestamp_ms <= $1
                      AND (queued_at_ms IS NULL OR queued_at_ms <= $2)
                      AND NOT EXISTS (
                          SELECT 1 FROM durable_actors_organization_suspensions s
                          WHERE s.organization_id = (
                              SELECT organization_id FROM durable_actors_deployment WHERE project_id = a.project_id))
                    ORDER BY timestamp_ms
                    LIMIT $3
                    FOR UPDATE SKIP LOCKED
                 )
                 UPDATE durable_actors_alarm AS alarm
                 SET queued_at_ms = $1
                 FROM due
                 WHERE alarm.project_id = due.project_id
                   AND alarm.actor_name = due.actor_name
                   AND alarm.actor_id = due.actor_id
                 RETURNING alarm.project_id, alarm.actor_name, alarm.actor_id,
                           alarm.revision",
            )
            .await?;
        client
            .query(&statement, &[&now_ms, &expired_before_ms, &limit])
            .await?
            .into_iter()
            .map(|row| {
                let revision: i64 = row.get("revision");
                Ok(ClaimedAlarm {
                    actor: ActorKey {
                        project_id: row.get("project_id"),
                        actor_name: row.get("actor_name"),
                        actor_id: row.get("actor_id"),
                    },
                    revision: u64::try_from(revision).context("negative alarm revision")?,
                })
            })
            .collect()
    }

    async fn finish(&self, alarm: &ClaimedAlarm) -> Result<()> {
        let revision = i64::try_from(alarm.revision).context("alarm revision overflow")?;
        self.database
            .execute(
                "UPDATE durable_actors_alarm
                 SET timestamp_ms = NULL, queued_at_ms = NULL
                 WHERE project_id = $1 AND actor_name = $2 AND actor_id = $3
                   AND revision = $4",
                &[
                    &alarm.actor.project_id,
                    &alarm.actor.actor_name,
                    &alarm.actor.actor_id,
                    &revision,
                ],
            )
            .await?;
        Ok(())
    }
}

pub(super) struct SqliteAlarmStore(Arc<Mutex<Connection>>);

impl SqliteAlarmStore {
    pub(super) async fn open(path: PathBuf) -> Result<Self> {
        let connection = tokio::task::spawn_blocking(move || {
            let connection = Connection::open(path).context("open local alarms")?;
            // The local data-directory lock excludes other runtimes; earlier claims are stale.
            connection.execute_batch(
                "CREATE TABLE IF NOT EXISTS alarms (
                    project_id TEXT NOT NULL, actor_name TEXT NOT NULL, actor_id TEXT NOT NULL,
                    revision INTEGER NOT NULL, timestamp_ms INTEGER, queued_at_ms INTEGER,
                    PRIMARY KEY (project_id, actor_name, actor_id)
                );
                UPDATE alarms SET queued_at_ms = NULL;",
            )?;
            anyhow::Ok(connection)
        })
        .await??;
        Ok(Self(Arc::new(Mutex::new(connection))))
    }

    async fn run<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&Connection) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let connection = self.0.clone();
        tokio::task::spawn_blocking(move || operation(&connection.lock().unwrap())).await?
    }
}

#[async_trait]
impl AlarmStore for SqliteAlarmStore {
    async fn get(&self, actor: &ActorKey) -> Result<Option<i64>> {
        let actor = actor.clone();
        self.run(move |connection| {
            Ok(connection
                .query_row(
                    "SELECT timestamp_ms FROM alarms
                     WHERE project_id = ?1 AND actor_name = ?2 AND actor_id = ?3",
                    params![actor.project_id, actor.actor_name, actor.actor_id],
                    |row| row.get(0),
                )
                .optional()?
                .flatten())
        })
        .await
    }

    async fn update(&self, actor: &ActorKey, timestamp_ms: Option<i64>) -> Result<()> {
        let actor = actor.clone();
        self.run(move |connection| {
            connection.execute(
                "INSERT INTO alarms (project_id, actor_name, actor_id, revision, timestamp_ms)
                 VALUES (?1, ?2, ?3, 1, ?4)
                 ON CONFLICT (project_id, actor_name, actor_id) DO UPDATE SET
                    revision = revision + 1,
                    timestamp_ms = excluded.timestamp_ms,
                    queued_at_ms = NULL",
                params![
                    actor.project_id,
                    actor.actor_name,
                    actor.actor_id,
                    timestamp_ms
                ],
            )?;
            Ok(())
        })
        .await
    }

    async fn claim_due(
        &self,
        now_ms: i64,
        expired_before_ms: i64,
        limit: usize,
    ) -> Result<Vec<ClaimedAlarm>> {
        let limit = i64::try_from(limit).context("alarm batch size overflow")?;
        self.run(move |connection| {
            connection
                .prepare(
                    "UPDATE alarms SET queued_at_ms = ?1
                     WHERE rowid IN (
                        SELECT rowid FROM alarms
                        WHERE timestamp_ms <= ?1
                          AND (queued_at_ms IS NULL OR queued_at_ms <= ?2)
                        ORDER BY timestamp_ms
                        LIMIT ?3
                     )
                     RETURNING project_id, actor_name, actor_id, revision",
                )?
                .query_map(params![now_ms, expired_before_ms, limit], |row| {
                    Ok((
                        ActorKey {
                            project_id: row.get(0)?,
                            actor_name: row.get(1)?,
                            actor_id: row.get(2)?,
                        },
                        row.get::<_, i64>(3)?,
                    ))
                })?
                .map(|row| {
                    let (actor, revision) = row?;
                    Ok(ClaimedAlarm {
                        actor,
                        revision: u64::try_from(revision).context("negative alarm revision")?,
                    })
                })
                .collect()
        })
        .await
    }

    async fn finish(&self, alarm: &ClaimedAlarm) -> Result<()> {
        let revision = i64::try_from(alarm.revision).context("alarm revision overflow")?;
        let actor = alarm.actor.clone();
        self.run(move |connection| {
            connection.execute(
                "UPDATE alarms SET timestamp_ms = NULL, queued_at_ms = NULL
                 WHERE project_id = ?1 AND actor_name = ?2 AND actor_id = ?3
                   AND revision = ?4",
                params![actor.project_id, actor.actor_name, actor.actor_id, revision],
            )?;
            Ok(())
        })
        .await
    }
}

#[derive(Default)]
pub(super) struct MemoryAlarmStore {
    alarms: Mutex<HashMap<ActorKey, MemoryAlarm>>,
}

struct MemoryAlarm {
    revision: u64,
    timestamp_ms: Option<i64>,
    queued_at_ms: Option<i64>,
}

#[async_trait]
impl AlarmStore for MemoryAlarmStore {
    async fn get(&self, actor: &ActorKey) -> Result<Option<i64>> {
        Ok(self
            .alarms
            .lock()
            .unwrap()
            .get(actor)
            .and_then(|alarm| alarm.timestamp_ms))
    }

    async fn update(&self, actor: &ActorKey, timestamp_ms: Option<i64>) -> Result<()> {
        let mut alarms = self.alarms.lock().unwrap();
        let revision = match alarms.get(actor) {
            Some(alarm) => alarm
                .revision
                .checked_add(1)
                .context("alarm revision overflow")?,
            None => 1,
        };
        alarms.insert(
            actor.clone(),
            MemoryAlarm {
                revision,
                timestamp_ms,
                queued_at_ms: None,
            },
        );
        Ok(())
    }

    async fn claim_due(
        &self,
        now_ms: i64,
        expired_before_ms: i64,
        limit: usize,
    ) -> Result<Vec<ClaimedAlarm>> {
        let mut alarms = self.alarms.lock().unwrap();
        let mut due = alarms
            .iter()
            .filter_map(|(actor, alarm)| {
                let timestamp_ms = alarm.timestamp_ms.filter(|&due_ms| due_ms <= now_ms)?;
                alarm
                    .queued_at_ms
                    .is_none_or(|queued_at_ms| queued_at_ms <= expired_before_ms)
                    .then(|| (timestamp_ms, actor.clone()))
            })
            .collect::<Vec<_>>();
        due.sort_by_key(|(timestamp_ms, _)| *timestamp_ms);
        Ok(due
            .into_iter()
            .take(limit)
            .map(|(_, actor)| {
                let alarm = alarms.get_mut(&actor).expect("selected alarm disappeared");
                alarm.queued_at_ms = Some(now_ms);
                ClaimedAlarm {
                    actor,
                    revision: alarm.revision,
                }
            })
            .collect())
    }

    async fn finish(&self, alarm: &ClaimedAlarm) -> Result<()> {
        if let Some(current) = self.alarms.lock().unwrap().get_mut(&alarm.actor)
            && current.revision == alarm.revision
        {
            current.timestamp_ms = None;
            current.queued_at_ms = None;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/alarms.rs"]
mod tests;
