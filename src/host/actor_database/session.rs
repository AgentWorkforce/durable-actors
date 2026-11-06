use super::{
    ActorActivation, ActorStorage, CommitOrigin, DatabaseReply, DatabaseTimings,
    PreparedActivation, PreparedInvocation, sqlite::SqliteDatabase,
};
use crate::host::HostEndpoint;
use crate::{
    actor::{
        ActorExecutionResult, ActorInvocation, ActorInvocationFailure, DatabaseCall, DatabaseResult,
    },
    litestream::{Replicator, storage::SqliteCapture},
    request_tracking::RequestStage,
    state_log::{SqliteSnapshot, StateSnapshot},
    storage::{SnapshotRef, WritePlan},
};
use anyhow::{Context, Result, ensure};
use serde_json::Value;
use std::{sync::Arc, time::Instant};
use tracing::{info, warn};

pub(super) struct DatabaseSession {
    endpoint: HostEndpoint,
    storage: Arc<dyn ActorStorage>,
    state: Arc<dyn crate::state_transport::SnapshotWriter>,
    replication: Arc<dyn Replicator>,
    cached_state: Option<ResidentDatabase>,
    prepared_activation: Option<ActorActivation>,
    activation: Option<ActorActivation>,
}

impl DatabaseSession {
    pub(super) fn new(
        endpoint: HostEndpoint,
        storage: Arc<dyn ActorStorage>,
        state: Arc<dyn crate::state_transport::SnapshotWriter>,
        replication: Arc<dyn Replicator>,
    ) -> Self {
        Self {
            endpoint,
            storage,
            state,
            replication,
            cached_state: None,
            prepared_activation: None,
            activation: None,
        }
    }
    pub(super) fn state_version(&self) -> Option<u64> {
        self.cached_state
            .as_ref()
            .map(|state| state.state_version)
            .filter(|version| *version > 0)
    }

    pub(super) async fn prepare_invocation(
        &mut self,
        invocation: &ActorInvocation,
        owner_epoch: Option<u64>,
        replay: bool,
        timings: &mut DatabaseTimings,
    ) -> Result<PreparedInvocation> {
        let owner_epoch = owner_epoch.context("activation scopes cannot run invocations")?;
        if self.storage.ensure_authority().is_err() {
            return Ok(PreparedInvocation::Completed(
                ActorExecutionResult::HostUnavailable,
            ));
        }
        let mut cached = self.take_or_load_state(owner_epoch, timings).await?;
        if let Err(error) = self.finish_pending_commit(&mut cached).await {
            self.cached_state = Some(cached);
            warn!(actor = %invocation.actor.storage_key(), %error, "pending actor state commit remains unresolved");
            return Ok(PreparedInvocation::Completed(
                ActorExecutionResult::Failed {
                    failure: ActorInvocationFailure::outcome_unknown_after_execution(),
                },
            ));
        }
        timings.mark(RequestStage::PendingCommitResolved);
        let result = if replay {
            cached.replay(&invocation.request_id)
        } else {
            None
        };
        self.cached_state = Some(cached);
        if self.storage.ensure_authority().is_err() {
            return Ok(PreparedInvocation::Completed(
                ActorExecutionResult::HostUnavailable,
            ));
        }
        Ok(match result {
            Some(result) => PreparedInvocation::Completed(ActorExecutionResult::Completed {
                result,
                effects: vec![],
            }),
            None => PreparedInvocation::Execute,
        })
    }

    pub(super) async fn database_call(
        &mut self,
        invocation: &ActorInvocation,
        owner_epoch: Option<u64>,
        origin: CommitOrigin,
        call: DatabaseCall,
    ) -> Result<DatabaseReply> {
        let Some(mut cached) = self.cached_state.take() else {
            return Ok(DatabaseReply::Rejected(anyhow::anyhow!(
                "actor database is not activated"
            )));
        };
        let reply = self
            .execute_database_call(invocation, owner_epoch, origin, &mut cached, call)
            .await;
        self.cached_state = Some(cached);
        reply
    }

    async fn execute_database_call(
        &self,
        invocation: &ActorInvocation,
        owner_epoch: Option<u64>,
        origin: CommitOrigin,
        cached: &mut ResidentDatabase,
        call: DatabaseCall,
    ) -> Result<DatabaseReply> {
        ensure!(
            owner_epoch.is_none_or(|owner_epoch| cached.owner_epoch == owner_epoch),
            "database call ownership changed"
        );
        let owner_epoch = cached.owner_epoch;
        match &call {
            DatabaseCall::Alarm => {
                let timestamp_ms = self.storage.get_alarm().await?;
                return Ok(DatabaseReply::Executed {
                    result: DatabaseResult::Alarm { timestamp_ms },
                    committed: None,
                });
            }
            DatabaseCall::SetAlarm { timestamp_ms, .. } => {
                self.storage.update_alarm(Some(*timestamp_ms)).await?;
                return Ok(DatabaseReply::Executed {
                    result: DatabaseResult::Done,
                    committed: None,
                });
            }
            DatabaseCall::DeleteAlarm { .. } => {
                self.storage.update_alarm(None).await?;
                return Ok(DatabaseReply::Executed {
                    result: DatabaseResult::Done,
                    committed: None,
                });
            }
            _ => {}
        }
        let immediate = call.immediate();
        let commit = matches!(call, DatabaseCall::Commit);
        let executed = match cached.database.call(call).await {
            Ok(executed) => executed,
            Err(error) => return Ok(DatabaseReply::Rejected(error)),
        };
        if executed.committed {
            let previous = cached.synced;
            cached.synced = self.replication.sync(&cached.sqlite.path()).await?;
            // SQLite counts a row rewritten with identical bytes as a change but appends no WAL frames.
            if immediate && cached.synced > previous {
                self.publish_immediate(invocation, owner_epoch, cached, origin)
                    .await?;
            }
        }
        Ok(match executed.result {
            Ok(result) => DatabaseReply::Executed {
                result,
                committed: commit.then_some(cached.synced),
            },
            Err(error) => DatabaseReply::Rejected(error),
        })
    }

    async fn publish_immediate(
        &self,
        invocation: &ActorInvocation,
        owner_epoch: u64,
        cached: &mut ResidentDatabase,
        origin: CommitOrigin,
    ) -> Result<()> {
        self.storage.ensure_authority()?;
        let txid = cached.synced;
        ensure!(
            cached.published.is_none_or(|published| published < txid),
            "immediate write must advance SQLite transaction"
        );
        self.publish_state(invocation, owner_epoch, cached, None, txid, origin)
            .await?;
        self.storage.ensure_authority()
    }

    pub(super) async fn prepare_activation(&mut self) -> Result<PreparedActivation> {
        let prepared = self.prepared_activation.take();
        self.storage.ensure_authority()?;
        if let Some(activation) = &self.activation {
            return Ok(PreparedActivation::Completed(activation.clone()));
        }
        if let Some(activation) = prepared {
            self.prepared_activation = Some(activation.clone());
            return Ok(PreparedActivation::Hydrate { activation });
        }
        let mut activation = self.storage.acquire_actor().await?;
        if self.cached_state.is_none() {
            let cached = if activation.state_version == 0 {
                ResidentDatabase::open(
                    activation.owner_epoch,
                    SqliteCapture::new(self.replication.clone(), self.storage.full_snapshots())
                        .await?,
                )
                .await?
            } else {
                let bytes = activation
                    .state
                    .take()
                    .context("activation has no recovered state")?;
                self.load_cached(activation.owner_epoch, activation.state_version, &bytes)
                    .await?
            };
            self.cached_state = Some(cached);
        }
        self.storage.ensure_authority()?;
        self.prepared_activation = Some(activation.clone());
        Ok(PreparedActivation::Hydrate { activation })
    }

    pub(super) fn finish_activation(
        &mut self,
        mut activation: ActorActivation,
        result: Result<()>,
    ) -> Result<ActorActivation> {
        self.prepared_activation = None;
        result?;
        self.storage.ensure_authority()?;
        let cached = self
            .cached_state
            .as_ref()
            .context("activation has no cached state")?;
        ensure!(
            cached.owner_epoch == activation.owner_epoch,
            "activation ownership changed"
        );
        activation.state_version = cached.state_version;
        self.activation = Some(activation.clone());
        Ok(activation)
    }

    pub(super) async fn complete(
        &mut self,
        invocation: &ActorInvocation,
        owner_epoch: Option<u64>,
        result: Value,
        committed: Option<u64>,
        origin: CommitOrigin,
    ) -> Result<u64> {
        let owner_epoch = owner_epoch.context("activation scopes cannot complete invocations")?;
        let mut cached = self
            .cached_state
            .take()
            .context("actor invocation has no state")?;
        ensure!(
            cached.owner_epoch == owner_epoch,
            "database completion ownership changed"
        );
        let publication = match cached.unpublished(committed) {
            Some(txid) => {
                self.publish_result(invocation, owner_epoch, &mut cached, result, txid, origin)
                    .await
            }
            None => Ok(()),
        };
        let version = cached.state_version;
        if publication.is_err() {
            self.activation = None;
        }
        self.cached_state = if publication.is_err() && cached.pending.is_none() {
            None
        } else {
            Some(cached)
        };
        publication.context(super::PublicationError)?;
        self.storage.ensure_authority()?;
        Ok(version)
    }

    pub(super) fn reset(&mut self) {
        self.cached_state = None;
        self.prepared_activation = None;
        self.activation = None;
    }
    async fn take_or_load_state(
        &mut self,
        owner_epoch: u64,
        timings: &mut DatabaseTimings,
    ) -> Result<ResidentDatabase> {
        let cached = self.cached_state.take();
        timings.mark(RequestStage::StateCacheChecked);
        if let Some(cached) = cached
            && cached.owner_epoch == owner_epoch
        {
            return Ok(cached);
        }
        let (state_version, loaded) = self.storage.load_actor_state(owner_epoch).await?;
        if state_version == 0 {
            ensure!(loaded.is_empty(), "uninitialized actor has state");
            return ResidentDatabase::open(
                owner_epoch,
                SqliteCapture::new(self.replication.clone(), self.storage.full_snapshots()).await?,
            )
            .await;
        }
        timings.mark(RequestStage::StateDownloaded);
        let cached = self
            .load_cached(owner_epoch, state_version, &loaded)
            .await?;
        timings.mark(RequestStage::StateDecoded);
        Ok(cached)
    }

    async fn load_cached(
        &self,
        owner_epoch: u64,
        state_version: u64,
        loaded: &[u8],
    ) -> Result<ResidentDatabase> {
        let snapshot = StateSnapshot::decode(loaded)?;
        ensure!(
            snapshot.state_version == state_version,
            "actor snapshot version does not match its state head"
        );
        ensure!(
            snapshot.owner_epoch <= owner_epoch,
            "actor snapshot belongs to a newer owner epoch"
        );
        let started = std::time::Instant::now();
        let sqlite = self.storage.read_sqlite(loaded).await?;
        let history_ms = started.elapsed().as_secs_f64() * 1000.0;
        let apply_started = std::time::Instant::now();
        let capture = SqliteCapture::restore(
            self.replication.clone(),
            &sqlite.files,
            sqlite.txid,
            self.storage.full_snapshots(),
        )
        .await?;
        tracing::info!(event = "actor_sqlite_restored", host_id = %self.endpoint.id, state_version,
            history_ms, apply_ms = apply_started.elapsed().as_secs_f64() * 1000.0,
            duration_ms = started.elapsed().as_secs_f64() * 1000.0);
        let mut cached = ResidentDatabase::open(owner_epoch, capture).await?;
        cached.state_version = state_version;
        cached.published = Some(cached.synced);
        cached.set_completion(snapshot.request_id, snapshot.completion);
        Ok(cached)
    }

    async fn publish_result(
        &self,
        invocation: &ActorInvocation,
        owner_epoch: u64,
        cached: &mut ResidentDatabase,
        result: Value,
        txid: u64,
        origin: CommitOrigin,
    ) -> Result<()> {
        self.publish_state(
            invocation,
            owner_epoch,
            cached,
            Some(crate::state_log::RequestCompletion { result }),
            txid,
            origin,
        )
        .await?;
        Ok(())
    }

    async fn publish_state(
        &self,
        invocation: &ActorInvocation,
        owner_epoch: u64,
        cached: &mut ResidentDatabase,
        completion: Option<crate::state_log::RequestCompletion>,
        txid: u64,
        origin: CommitOrigin,
    ) -> Result<()> {
        let mut timings = StateWriteTimings::new();
        let next_version = cached.state_version.checked_add(1);
        let outcome = self
            .publish_state_once(
                invocation,
                owner_epoch,
                cached,
                completion,
                txid,
                origin,
                &mut timings,
            )
            .await;
        self.log_state_write(invocation, owner_epoch, next_version, &timings, &outcome);
        outcome
    }

    async fn publish_state_once(
        &self,
        invocation: &ActorInvocation,
        owner_epoch: u64,
        cached: &mut ResidentDatabase,
        completion: Option<crate::state_log::RequestCompletion>,
        txid: u64,
        origin: CommitOrigin,
        timings: &mut StateWriteTimings,
    ) -> Result<()> {
        let next_version = cached
            .state_version
            .checked_add(1)
            .context("actor state version overflow")?;
        let ticket = match cached.next_write.take() {
            Some(ticket)
                if ticket.state_version == next_version
                    && ticket.stream.owner_epoch == owner_epoch =>
            {
                ticket
            }
            _ => {
                self.storage
                    .prepare_state_write(owner_epoch, cached.state_version)
                    .await?
            }
        };
        ensure!(
            ticket.state_version == next_version,
            "state write ticket has the wrong version"
        );
        ensure!(
            ticket.stream.owner_epoch == owner_epoch,
            "write capability belongs to another owner epoch"
        );
        timings.write_ticket_ready_at_ms = Some(timings.elapsed_ms());
        let mut snapshot = StateSnapshot {
            state_version: next_version,
            owner_epoch,
            request_id: invocation.request_id.clone(),
            sqlite: cached.capture_sqlite(txid).await?,
            completion,
            attribution: None,
        };
        snapshot.attribution = Some(crate::state_log::StateAttribution {
            operation: invocation.method.clone(),
            connection_id: origin.connection_id,
            committed_at_ms: unix_millis()? as u64,
            interleaved: origin.interleaved,
        });
        timings.snapshot_created_at_ms = Some(timings.elapsed_ms());
        let bytes = snapshot.encode()?;
        timings.snapshot_encoded_at_ms = Some(timings.elapsed_ms());
        cached.pending = Some(PendingStateCommit {
            snapshot,
            txid,
            ticket,
            durable: false,
        });
        let pending = cached
            .pending
            .as_ref()
            .expect("pending write was installed");
        self.state.write_snapshot(&pending.ticket, bytes).await?;
        cached
            .pending
            .as_mut()
            .expect("pending write was installed")
            .durable = true;
        timings.snapshot_persisted_at_ms = Some(timings.elapsed_ms());
        timings.durability_proof = Some("object_storage");
        timings.snapshot_uploaded_at_ms = timings.snapshot_persisted_at_ms;
        self.finish_pending_commit(cached).await?;
        timings.state_finalized_at_ms = Some(timings.elapsed_ms());
        Ok(())
    }

    fn log_state_write(
        &self,
        invocation: &ActorInvocation,
        owner_epoch: u64,
        state_version: Option<u64>,
        timings: &StateWriteTimings,
        outcome: &Result<()>,
    ) {
        match outcome {
            Ok(_) => info!(
                event = "actor_state_write",
                request_id = %invocation.request_id,

                actor_name = %invocation.actor.actor_name,
                actor_id = %invocation.actor.actor_id,
                host_id = %self.endpoint.id,
                owner_epoch,
                state_version,
                started_at_ms = 0,
                write_ticket_ready_at_ms = timings.write_ticket_ready_at_ms,
                snapshot_created_at_ms = timings.snapshot_created_at_ms,
                snapshot_encoded_at_ms = timings.snapshot_encoded_at_ms,
                snapshot_uploaded_at_ms = timings.snapshot_uploaded_at_ms,
                snapshot_persisted_at_ms = timings.snapshot_persisted_at_ms,
                durability_proof = timings.durability_proof,
                state_finalized_at_ms = timings.state_finalized_at_ms,
                completed_at_ms = timings.elapsed_ms(),
                outcome = "committed",
                "immutable actor state committed"
            ),
            Err(error) => warn!(
                event = "actor_state_write",
                request_id = %invocation.request_id,

                actor_name = %invocation.actor.actor_name,
                actor_id = %invocation.actor.actor_id,
                host_id = %self.endpoint.id,
                owner_epoch,
                state_version,
                started_at_ms = 0,
                write_ticket_ready_at_ms = timings.write_ticket_ready_at_ms,
                snapshot_created_at_ms = timings.snapshot_created_at_ms,
                snapshot_encoded_at_ms = timings.snapshot_encoded_at_ms,
                snapshot_uploaded_at_ms = timings.snapshot_uploaded_at_ms,
                snapshot_persisted_at_ms = timings.snapshot_persisted_at_ms,
                durability_proof = timings.durability_proof,
                state_finalized_at_ms = timings.state_finalized_at_ms,
                completed_at_ms = timings.elapsed_ms(),
                outcome = "failed",
                error = %format!("{error:#}"),
                "immutable actor state commit failed"
            ),
        }
    }

    async fn finish_pending_commit(&self, cached: &mut ResidentDatabase) -> Result<()> {
        let Some(pending) = &mut cached.pending else {
            return Ok(());
        };
        if !pending.durable {
            let renewed = self
                .storage
                .prepare_state_write(cached.owner_epoch, cached.state_version)
                .await?;
            ensure!(
                renewed.stream == pending.ticket.stream
                    && renewed.object_name == pending.ticket.object_name,
                "pending writer has been fenced"
            );
            pending.ticket = renewed;
            self.state
                .write_snapshot(&pending.ticket, pending.snapshot.encode()?)
                .await?;
            pending.durable = true;
        }
        let stream = &pending.ticket.stream;
        let mut next_write = pending.ticket.clone();
        next_write.state_version = pending
            .snapshot
            .state_version
            .checked_add(1)
            .context("state version overflow")?;
        next_write.object_name = stream.object(next_write.state_version);
        let pending = cached.pending.take().expect("pending commit checked above");
        cached.sqlite_head = Some(SnapshotRef::new(
            pending.ticket.object_name.clone(),
            &pending.snapshot,
            &pending.snapshot.encode()?,
        ));
        cached.state_version = pending.snapshot.state_version;
        cached.published = Some(pending.txid);
        cached.set_completion(pending.snapshot.request_id, pending.snapshot.completion);
        cached.next_write = Some(next_write);
        Ok(())
    }
}

struct ResidentDatabase {
    database: SqliteDatabase,
    sqlite: SqliteCapture,
    sqlite_head: Option<SnapshotRef>,
    owner_epoch: u64,
    state_version: u64,
    published: Option<u64>,
    synced: u64,
    last_request_id: Option<String>,
    last_result: Option<Value>,
    next_write: Option<WritePlan>,
    pending: Option<PendingStateCommit>,
}

struct PendingStateCommit {
    snapshot: StateSnapshot,
    txid: u64,
    ticket: WritePlan,
    durable: bool,
}

impl ResidentDatabase {
    async fn open(owner_epoch: u64, sqlite: SqliteCapture) -> Result<Self> {
        Ok(Self {
            database: SqliteDatabase::open(sqlite.path()).await?,
            synced: sqlite.txid(),
            sqlite,
            sqlite_head: None,
            owner_epoch,
            state_version: 0,
            published: None,
            last_request_id: None,
            last_result: None,
            next_write: None,
            pending: None,
        })
    }

    fn unpublished(&self, committed: Option<u64>) -> Option<u64> {
        committed.filter(|txid| self.published.is_none_or(|published| published < *txid))
    }

    async fn capture_sqlite(&mut self, txid: u64) -> Result<SqliteSnapshot> {
        let files = self.sqlite.capture(txid).await?;
        let first = files
            .first()
            .context("actor commit has no replication files")?;
        let parent = if first.first == 1 {
            None
        } else {
            Some(
                self.sqlite_head
                    .clone()
                    .context("SQLite state has no durable head")?,
            )
        };
        Ok(SqliteSnapshot {
            txid: self.sqlite.txid(),
            parent,
            files,
        })
    }

    fn set_completion(
        &mut self,
        request_id: String,
        completion: Option<crate::state_log::RequestCompletion>,
    ) {
        self.last_request_id = completion.as_ref().map(|_| request_id);
        self.last_result = completion.map(|completion| completion.result);
    }

    fn replay(&self, request_id: &str) -> Option<Value> {
        (self.last_request_id.as_deref() == Some(request_id))
            .then(|| self.last_result.clone())
            .flatten()
    }
}

struct StateWriteTimings {
    started_at: Instant,
    write_ticket_ready_at_ms: Option<f64>,
    snapshot_created_at_ms: Option<f64>,
    snapshot_encoded_at_ms: Option<f64>,
    snapshot_uploaded_at_ms: Option<f64>,
    snapshot_persisted_at_ms: Option<f64>,
    durability_proof: Option<&'static str>,
    state_finalized_at_ms: Option<f64>,
}

impl StateWriteTimings {
    fn new() -> Self {
        Self {
            started_at: Instant::now(),
            write_ticket_ready_at_ms: None,
            snapshot_created_at_ms: None,
            snapshot_encoded_at_ms: None,
            snapshot_uploaded_at_ms: None,
            snapshot_persisted_at_ms: None,
            durability_proof: None,
            state_finalized_at_ms: None,
        }
    }

    fn elapsed_ms(&self) -> f64 {
        elapsed_ms(self.started_at)
    }
}

fn elapsed_ms(started_at: Instant) -> f64 {
    started_at.elapsed().as_secs_f64() * 1_000.0
}

fn unix_millis() -> Result<i64> {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .context("system clock is before the Unix epoch")?
            .as_millis(),
    )
    .context("system clock exceeds supported state-write timestamp range")
}
