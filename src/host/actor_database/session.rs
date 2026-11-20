use super::{
    ActorActivation, ActorStorage, CommitOrigin, DatabaseReply, DatabaseTimings,
    PreparedActivation, PreparedInvocation, sqlite::SqliteDatabase,
};
use crate::host::HostEndpoint;
use crate::node_cache::{NodeCacheClient, SlotFile};
use crate::payload::{JsonText, Payload};
use crate::{
    actor::{
        ActorExecutionResult, ActorInvocation, ActorInvocationFailure, DatabaseCall,
        DatabaseResult, RowSink,
    },
    litestream::{
        Replicator,
        storage::{CaptureDirectory, SqliteCapture},
    },
    request_tracking::RequestStage,
    state_log::{SqliteSnapshot, StateSnapshot},
    storage::{SnapshotRef, VerifiedSnapshot, WritePlan},
};
use anyhow::{Context, Result, ensure};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};
use tracing::{info, warn};

pub(super) struct DatabaseSession {
    endpoint: HostEndpoint,
    storage: Arc<dyn ActorStorage>,
    state: Arc<dyn crate::state_transport::SnapshotWriter>,
    replication: Arc<dyn Replicator>,
    node_cache: Option<NodeCacheClient>,
    /// The node slot this host's first database opens in; any later one uses temporary storage.
    slot: Option<PathBuf>,
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
        node_cache: Option<NodeCacheClient>,
    ) -> Self {
        Self {
            endpoint,
            storage,
            state,
            replication,
            slot: node_cache.as_ref().and_then(NodeCacheClient::slot_database),
            node_cache,
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

    pub(super) async fn database_bytes(&self) -> Option<u64> {
        let resident = self.cached_state.as_ref()?;
        resident.database.size().await.ok().flatten()
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
            match self
                .replay(&mut cached, &invocation.request_id, owner_epoch)
                .await
            {
                Ok(result) => result,
                Err(error) => {
                    self.cached_state = Some(cached);
                    return Err(error);
                }
            }
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
                result: result.into(),
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
        rows: Box<dyn RowSink>,
    ) -> Result<DatabaseReply> {
        let Some(mut cached) = self.cached_state.take() else {
            return Ok(DatabaseReply::Rejected(anyhow::anyhow!(
                "actor database is not activated"
            )));
        };
        let reply = self
            .execute_database_call(invocation, owner_epoch, origin, &mut cached, call, rows)
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
        rows: Box<dyn RowSink>,
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
        let executed = match cached.database.call(call, rows).await {
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
                ResidentDatabase::open(activation.owner_epoch, self.new_capture().await?).await?
            } else if let Some(snapshot) = activation.state.take() {
                self.load_cached(activation.owner_epoch, activation.state_version, snapshot)
                    .await?
            } else {
                let head = activation
                    .head
                    .clone()
                    .context("activation has no recovered state")?;
                self.load_parked(activation.owner_epoch, activation.state_version, head)
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
        result: Option<JsonText>,
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
        let publication = match (cached.unpublished(committed), result) {
            (Some(txid), Some(result)) => {
                self.publish_result(invocation, owner_epoch, &mut cached, result, txid, origin)
                    .await
            }
            (Some(_), None) => Err(anyhow::anyhow!(
                "a result passed on as it arrived cannot be kept with its state"
            )),
            (None, _) => Ok(()),
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

    /// Closes the database and hands its file to the node with the snapshot and transaction it was
    /// committed at, so the actor's next host on this node reopens it instead of rebuilding it.
    /// Nothing is parked outside a slot or with a commit still pending.
    pub(super) async fn park(&mut self) -> Result<Option<SlotFile>> {
        let (Some(cache), Some(cached)) = (self.node_cache.clone(), &self.cached_state) else {
            return Ok(None);
        };
        if !cached.sqlite.in_slot() || cached.published_snapshot().is_none() {
            return Ok(None);
        }
        let ResidentDatabase {
            database,
            sqlite,
            snapshot,
            synced,
            ..
        } = self.cached_state.take().context("no database to park")?;
        let snapshot = snapshot.context("parked database has no snapshot")?;
        drop(database);
        // SQLite's commit hook misses some writes, such as VACUUM, so the file can hold frames the
        // history never got. Parking it would put later commits on a layout the history lacks.
        let position = self.replication.sync(&sqlite.path()).await?;
        ensure!(
            position == synced,
            "database has changes its history lacks; leaving it to kubelet"
        );
        sqlite.close().await?;
        cache.park_slot(&snapshot, synced).await.map(Some)
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
            ensure!(loaded.is_none(), "uninitialized actor has state");
            return ResidentDatabase::open(owner_epoch, self.new_capture().await?).await;
        }
        timings.mark(RequestStage::StateDownloaded);
        let snapshot = loaded.context("actor state head has no snapshot")?;
        let cached = self
            .load_cached(owner_epoch, state_version, snapshot)
            .await?;
        timings.mark(RequestStage::StateDecoded);
        Ok(cached)
    }

    async fn load_cached(
        &mut self,
        owner_epoch: u64,
        state_version: u64,
        loaded: VerifiedSnapshot,
    ) -> Result<ResidentDatabase> {
        let slot = self.claim_slot(Some(&loaded.reference)).await;
        self.restore(owner_epoch, state_version, loaded, slot).await
    }

    /// Reopens the database this node parked at `head` without reading the head's snapshot, whose
    /// pages the file already holds. The snapshot is read only if the file is gone, or for a retry
    /// of the head's request.
    async fn load_parked(
        &mut self,
        owner_epoch: u64,
        state_version: u64,
        head: SnapshotRef,
    ) -> Result<ResidentDatabase> {
        let started = Instant::now();
        let (slot, parked) = self.claim_slot(Some(&head)).await;
        let parent = self.storage.resume_parent(&head)?;
        if let (Some(path), Some(file)) = (&slot, &parked)
            && let Some(capture) = self.reopen(path, file, file.txid, parent.is_some()).await
        {
            info!(event = "actor_sqlite_restored", host_id = %self.endpoint.id, state_version,
                source = "slot", duration_ms = elapsed_ms(started));
            let mut cached = Self::resident(owner_epoch, state_version, capture, parent).await?;
            cached.snapshot = Some(head.clone());
            cached.unread_head = Some(head);
            return Ok(cached);
        }
        let (state_version, loaded) = self.storage.load_actor_state(owner_epoch).await?;
        let loaded = loaded.context("actor state head has no snapshot")?;
        self.restore(owner_epoch, state_version, loaded, (slot, None))
            .await
    }

    async fn restore(
        &mut self,
        owner_epoch: u64,
        state_version: u64,
        loaded: VerifiedSnapshot,
        slot: (Option<PathBuf>, Option<SlotFile>),
    ) -> Result<ResidentDatabase> {
        ensure!(
            loaded.snapshot.state_version == state_version,
            "actor snapshot version does not match its state head"
        );
        ensure!(
            loaded.snapshot.owner_epoch <= owner_epoch,
            "actor snapshot belongs to a newer owner epoch"
        );
        let started = Instant::now();
        let parent = self.storage.resume_parent(&loaded.reference)?;
        let (capture, source) = self.open_sqlite(&loaded, parent.is_some(), slot).await?;
        info!(event = "actor_sqlite_restored", host_id = %self.endpoint.id, state_version,
            source, duration_ms = elapsed_ms(started));
        let mut cached = Self::resident(owner_epoch, state_version, capture, parent).await?;
        let VerifiedSnapshot {
            reference,
            snapshot,
            ..
        } = loaded;
        cached.snapshot = Some(reference);
        cached.set_completion(snapshot.request_id, snapshot.completion);
        Ok(cached)
    }

    async fn resident(
        owner_epoch: u64,
        state_version: u64,
        capture: SqliteCapture,
        parent: Option<SnapshotRef>,
    ) -> Result<ResidentDatabase> {
        let mut cached = ResidentDatabase::open(owner_epoch, capture).await?;
        cached.sqlite_head = parent.filter(|_| cached.synced > 0);
        cached.state_version = state_version;
        cached.published = Some(cached.synced);
        Ok(cached)
    }

    async fn open_sqlite(
        &self,
        loaded: &VerifiedSnapshot,
        resume: bool,
        (slot, parked): (Option<PathBuf>, Option<SlotFile>),
    ) -> Result<(SqliteCapture, &'static str)> {
        let full_snapshots = self.storage.full_snapshots();
        let txid = loaded.snapshot.sqlite.txid;
        if let (Some(path), Some(file)) = (&slot, &parked)
            && let Some(capture) = self.reopen(path, file, txid, resume).await
        {
            return Ok((capture, "slot"));
        }
        let sqlite = self.storage.read_sqlite(loaded).await?;
        let capture = SqliteCapture::restore(
            emptied(slot.as_deref()).await?,
            self.replication.clone(),
            &sqlite.files,
            sqlite.txid,
            full_snapshots,
            resume,
        )
        .await?;
        Ok((capture, "history"))
    }

    async fn reopen(
        &self,
        slot: &Path,
        file: &SlotFile,
        txid: u64,
        resume: bool,
    ) -> Option<SqliteCapture> {
        let directory = CaptureDirectory::slot(slot.to_owned()).ok()?;
        let full_snapshots = self.storage.full_snapshots();
        match SqliteCapture::reopen(
            directory,
            self.replication.clone(),
            txid,
            full_snapshots,
            resume,
        )
        .await
        {
            Ok(capture) => {
                info!(event = "actor_sqlite_reopened", host_id = %self.endpoint.id,
                    bytes = file.bytes, device = file.device, inode = file.inode, txid);
                Some(capture)
            }
            Err(error) => {
                warn!(error = %format!("{error:#}"), "parked database could not be opened, restoring from history");
                None
            }
        }
    }

    /// A reopened database skipped its head's snapshot, so the head's stored result is read only
    /// when the head's own request is retried.
    async fn replay(
        &self,
        cached: &mut ResidentDatabase,
        request_id: &str,
        owner_epoch: u64,
    ) -> Result<Option<JsonText>> {
        if cached
            .unread_head
            .as_ref()
            .is_some_and(|head| head.request_id == request_id)
        {
            let (_, loaded) = self.storage.load_actor_state(owner_epoch).await?;
            let loaded = loaded
                .filter(|loaded| Some(&loaded.reference) == cached.unread_head.as_ref())
                .context("the retried request's snapshot is no longer the head")?;
            cached.unread_head = None;
            cached.set_completion(loaded.snapshot.request_id, loaded.snapshot.completion);
        }
        Ok(cached.replay(request_id))
    }

    async fn new_capture(&mut self) -> Result<SqliteCapture> {
        let (slot, _) = self.claim_slot(None).await;
        SqliteCapture::new(
            emptied(slot.as_deref()).await?,
            self.replication.clone(),
            self.storage.full_snapshots(),
        )
        .await
    }

    /// Checks out this host's slot, once: it holds the actor's parked database when the node has
    /// one closed at `snapshot`, and is empty otherwise. Without a slot, databases stay in
    /// temporary storage.
    async fn claim_slot(
        &mut self,
        snapshot: Option<&SnapshotRef>,
    ) -> (Option<PathBuf>, Option<SlotFile>) {
        let (Some(cache), Some(path)) = (&self.node_cache, self.slot.take()) else {
            return (None, None);
        };
        match cache.check_out_slot(snapshot).await {
            Ok(parked) => (Some(path), parked),
            Err(error) => {
                warn!(error = %format!("{error:#}"), "node slot checkout failed, using temporary storage");
                (None, None)
            }
        }
    }

    async fn publish_result(
        &self,
        invocation: &ActorInvocation,
        owner_epoch: u64,
        cached: &mut ResidentDatabase,
        result: JsonText,
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
            bytes,
            txid,
            ticket,
            durable: false,
        });
        let pending = cached
            .pending
            .as_ref()
            .expect("pending write was installed");
        self.state
            .write_snapshot(&pending.ticket, pending.bytes.clone())
            .await?;
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
                .write_snapshot(&pending.ticket, pending.bytes.clone())
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
        let head = SnapshotRef::new(
            pending.ticket.object_name.clone(),
            &pending.snapshot,
            &pending.bytes,
        )?;
        cached.snapshot = Some(head.clone());
        cached.unread_head = None;
        cached.sqlite_head = Some(head);
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
    snapshot: Option<SnapshotRef>,
    /// The head of a reopened database, whose stored result has not been read.
    unread_head: Option<SnapshotRef>,
    owner_epoch: u64,
    state_version: u64,
    published: Option<u64>,
    synced: u64,
    last_request_id: Option<String>,
    last_result: Option<JsonText>,
    next_write: Option<WritePlan>,
    pending: Option<PendingStateCommit>,
}

struct PendingStateCommit {
    snapshot: StateSnapshot,
    bytes: Payload,
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
            snapshot: None,
            unread_head: None,
            owner_epoch,
            state_version: 0,
            published: None,
            last_request_id: None,
            last_result: None,
            next_write: None,
            pending: None,
        })
    }

    fn published_snapshot(&self) -> Option<&SnapshotRef> {
        if self.pending.is_some() || self.published != Some(self.synced) {
            return None;
        }
        self.snapshot.as_ref()
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

    fn replay(&self, request_id: &str) -> Option<JsonText> {
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

/// The slot emptied of a database that failed to open, or new temporary storage.
async fn emptied(slot: Option<&Path>) -> Result<CaptureDirectory> {
    let Some(path) = slot else {
        return CaptureDirectory::temporary();
    };
    tokio::fs::create_dir_all(path).await?;
    let mut entries = tokio::fs::read_dir(path).await?;
    while let Some(entry) = entries.next_entry().await? {
        tokio::fs::remove_file(entry.path()).await?;
    }
    CaptureDirectory::slot(path.to_owned())
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
