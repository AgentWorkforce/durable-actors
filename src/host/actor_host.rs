use std::{
    borrow::Cow,
    collections::VecDeque,
    panic::AssertUnwindSafe,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use futures_util::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{error, info, warn};

use crate::actor::{
    ActorDatabase, ActorExecutionResult, ActorExecutor, ActorInvocation, ActorInvocationFailure,
    ActorKey, ActorMethodInvocation, ActorMethodOutcome, ActorSocketEffect, ActorSocketEvent,
    ActorSocketInvocation, ActorSocketOutcome, ActorSocketPublisher, validate_socket_effects,
};
use crate::host_leases::WaitingOperation;
use crate::request_traces::{RequestKind, TraceRecorder};
use crate::request_tracking::{
    HostState, RequestCompletion, RequestMetadata, RequestObserver, RequestStage, RequestTracker,
};
use opentelemetry_sdk::trace::SdkTracer;

use super::{
    HostEndpoint,
    actor_database::{
        ActorActivation, CommitOrigin, DatabaseInvocation, HostDatabase, PreparedActivation,
        PreparedInvocation, PublicationError,
    },
};

pub(crate) struct ActorHost {
    actor: ActorKey,
    endpoint: HostEndpoint,
    mailbox: mpsc::UnboundedSender<ActorMessage>,
    status: watch::Receiver<ActorStatus>,
    traces: Option<SdkTracer>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ActorStatus {
    pub phase: ActorPhase,
    pub idle_since: Option<tokio::time::Instant>,
    pub waiting: Vec<WaitingOperation>,
    pub last_active_at_ms: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ActorPhase {
    Serving,
    Draining,
    Stopped,
}

impl Default for ActorStatus {
    fn default() -> Self {
        Self {
            phase: ActorPhase::Serving,
            idle_since: Some(tokio::time::Instant::now()),
            waiting: Vec::new(),
            last_active_at_ms: None,
        }
    }
}

impl ActorStatus {
    fn drained(&self) -> bool {
        self.phase == ActorPhase::Stopped
    }
}

impl ActorHost {
    pub(crate) fn new(
        endpoint: HostEndpoint,
        actor: ActorKey,
        executor: Arc<dyn ActorExecutor>,
        database: Arc<HostDatabase>,
        publisher: Arc<dyn ActorSocketPublisher>,
    ) -> Self {
        let (status, observed) = watch::channel(ActorStatus::default());
        let (mailbox, requests) = mpsc::unbounded_channel();
        Actor {
            actor: actor.clone(),
            owner_epoch: 0,
            admission: executor.invocation_admission(),
            endpoint: endpoint.clone(),
            executor,
            database,
            publisher,
            status,
            requests,
            pending: VecDeque::new(),
            running: FuturesUnordered::new(),
            next_invocation: 0,
            blocked_on: None,
            interleaved: false,
            hydrating: false,
            draining: false,
        }
        .spawn();
        Self {
            actor,
            endpoint,
            mailbox,
            status: observed,
            traces: None,
        }
    }

    pub(crate) fn with_traces(mut self, tracer: SdkTracer) -> Self {
        self.traces = Some(tracer);
        self
    }

    pub(crate) fn id(&self) -> &super::HostId {
        &self.endpoint.id
    }

    pub(crate) fn actor(&self) -> &ActorKey {
        &self.actor
    }

    pub(crate) fn status(&self) -> watch::Receiver<ActorStatus> {
        self.status.clone()
    }

    pub(crate) async fn activate(&self) -> Result<ActorActivation> {
        let (reply, result) = oneshot::channel();
        let actor = self.actor.clone();
        self.submit(ActorOperation::Activate { actor, reply }, 0)
            .await?;
        result.await.context("actor activation was not completed")?
    }

    pub(crate) async fn invoke_actor(
        &self,
        invocation: ActorInvocation,
        owner_epoch: u64,
        host_state: HostState,
        routing_ms: f64,
    ) -> (Result<ActorExecutionResult>, Option<RequestMetadata>) {
        self.submit_traced(
            ActorOperation::Method(invocation),
            owner_epoch,
            Instant::now(),
            host_state,
            routing_ms,
        )
        .await
    }

    pub(crate) async fn handle_socket_event_since(
        &self,
        invocation: ActorSocketInvocation,
        owner_epoch: u64,
        started: Instant,
        host_state: HostState,
        routing_ms: f64,
    ) -> Result<ActorExecutionResult> {
        self.submit_traced(
            ActorOperation::Socket(invocation),
            owner_epoch,
            started,
            host_state,
            routing_ms,
        )
        .await
        .0
    }

    pub(crate) async fn drain(&self, timeout: Duration) -> Result<()> {
        let _ = self.mailbox.send(ActorMessage::Drain);
        let mut status = self.status.clone();
        tokio::time::timeout(timeout, status.wait_for(ActorStatus::drained))
            .await
            .context("actor invocations did not drain before shutdown")??;
        Ok(())
    }

    async fn submit(
        &self,
        operation: ActorOperation,
        owner_epoch: u64,
    ) -> Result<ActorExecutionResult> {
        self.submit_traced(operation, owner_epoch, Instant::now(), HostState::Warm, 0.0)
            .await
            .0
    }

    async fn submit_traced(
        &self,
        operation: ActorOperation,
        owner_epoch: u64,
        started: Instant,
        host_state: HostState,
        routing_ms: f64,
    ) -> (Result<ActorExecutionResult>, Option<RequestMetadata>) {
        let (reply, result) = oneshot::channel();
        let (tracker, timing) =
            RequestTracker::new(started, host_state, routing_ms, self.observer(&operation));
        let request = Box::new(ActorRequest {
            tracker,
            operation,
            owner_epoch,
            reply,
        });
        if let Err(mpsc::error::SendError(ActorMessage::Request(request))) =
            self.mailbox.send(ActorMessage::Request(request))
        {
            request.finish(&self.endpoint, Ok(ActorExecutionResult::HostUnavailable));
        }
        let outcome = result.await.unwrap_or_else(|_| {
            Ok(ActorExecutionResult::Failed {
                failure: ActorInvocationFailure::outcome_unknown_after_execution(),
            })
        });
        let timing = timing.await.ok();
        (outcome, timing)
    }

    fn observer(&self, operation: &ActorOperation) -> Option<Box<dyn RequestObserver>> {
        let tracer = self.traces.as_ref()?.clone();
        let (kind, connection, metadata) = match operation {
            ActorOperation::Activate { .. } => return None,
            ActorOperation::Method(_) => (RequestKind::Method, None, None),
            ActorOperation::Socket(invocation) => match &invocation.event {
                ActorSocketEvent::Connect { connection } => (
                    RequestKind::Websocket,
                    Some(connection.id.clone()),
                    Some(connection.metadata.clone()),
                ),
                ActorSocketEvent::Disconnect { connection, .. } => {
                    (RequestKind::Websocket, Some(connection.id.clone()), None)
                }
                ActorSocketEvent::Message { connection_id, .. } => {
                    (RequestKind::Websocket, Some(connection_id.clone()), None)
                }
            },
        };
        Some(Box::new(TraceRecorder::new(
            tracer,
            &operation.invocation(),
            kind,
            connection,
            metadata,
        )))
    }
}

struct Actor {
    actor: ActorKey,
    owner_epoch: u64,
    endpoint: HostEndpoint,
    executor: Arc<dyn ActorExecutor>,
    database: Arc<HostDatabase>,
    publisher: Arc<dyn ActorSocketPublisher>,
    status: watch::Sender<ActorStatus>,
    requests: mpsc::UnboundedReceiver<ActorMessage>,
    admission: Option<watch::Receiver<()>>,
    pending: VecDeque<Queued>,
    running: FuturesUnordered<BoxFuture<'static, Completion>>,
    next_invocation: u64,
    blocked_on: Option<u64>,
    interleaved: bool,
    hydrating: bool,
    draining: bool,
}

impl Actor {
    fn spawn(self) {
        tokio::spawn(async move {
            if AssertUnwindSafe(self.run()).catch_unwind().await.is_err() {
                error!("actor mailbox panicked");
            }
        });
    }

    async fn run(mut self) {
        let stopped = self.database.stopped.clone();
        let mut open = true;
        loop {
            self.start_ready();
            if self.draining && self.running.is_empty() && self.pending.is_empty() {
                self.evict(true).await;
                return;
            }
            self.publish();
            if !open && self.running.is_empty() {
                return;
            }
            tokio::select! {
                biased;
                Some(completion) = self.running.next(), if !self.running.is_empty() => {
                    if !self.executed(completion).await { return; }
                }
                _ = stopped.cancelled() => {
                    self.evict(false).await;
                    return;
                }
                permission = admitted(&mut self.admission), if self.blocked_on.is_some() && !self.hydrating => {
                    if permission.is_err() { return; }
                    self.interleaved = true;
                    self.blocked_on = None;
                }
                message = self.requests.recv(), if open => match message {
                    Some(ActorMessage::Request(request)) => self.receive(*request),
                    Some(ActorMessage::Drain) => self.draining = true,
                    None => open = false,
                },
            }
        }
    }

    fn receive(&mut self, request: ActorRequest) {
        match self.rejection(&request.operation) {
            Some(result) => self.finish(request, result),
            None => self.pending.push_back(Queued::new(request)),
        }
    }

    fn rejection(&self, operation: &ActorOperation) -> Option<Result<ActorExecutionResult>> {
        if self.draining && !operation.is_disconnect() {
            return Some(Ok(ActorExecutionResult::HostUnavailable));
        }
        if let Err(error) = operation.validate() {
            return Some(Err(error));
        }
        None
    }

    fn start_ready(&mut self) {
        while self.blocked_on.is_none()
            && let Some(Queued { request, .. }) = self.pending.pop_front()
        {
            match self.rejection(&request.operation) {
                Some(result) => self.finish(request, result),
                None => self.start(request),
            }
        }
    }

    fn start(&mut self, mut request: ActorRequest) {
        self.record_activity();
        if let Some(admission) = &mut self.admission {
            admission.borrow_and_update();
        }
        request.tracker.admitted();
        self.next_invocation += 1;
        self.blocked_on = Some(self.next_invocation);
        self.hydrating = matches!(request.operation, ActorOperation::Activate { .. });
        self.running.push(
            run_request(
                self.next_invocation,
                request,
                self.executor.clone(),
                self.database.clone(),
                self.publisher.clone(),
            )
            .boxed(),
        );
    }

    async fn executed(&mut self, completion: Completion) -> bool {
        self.hydrating = false;
        let recovery = self.recovery(&completion);
        self.interleaved |= completion.interleaved;
        let Completion {
            id,
            request,
            result,
            ..
        } = completion;
        self.owner_epoch = request.owner_epoch;
        match recovery {
            Recovery::Keep => {}
            Recovery::Evict => self.evict(false).await,
            Recovery::Reset => {
                self.evict(false).await;
                let _ = self.database.reset().await;
            }
            Recovery::Stop => {
                self.evict(false).await;
                self.finish(
                    request,
                    Ok(ActorExecutionResult::Failed {
                        failure: ActorInvocationFailure::outcome_unknown_after_execution(),
                    }),
                );
                return false;
            }
        }
        if self.blocked_on == Some(id) {
            self.blocked_on = None;
        }
        self.finish(request, result);
        true
    }

    fn recovery(&self, completion: &Completion) -> Recovery {
        let unexpected = completion.executed
            && self.interleaved
            && !completion.interleaved
            && completion.failure_code.as_deref().is_none_or(|code| {
                !matches!(
                    code,
                    "actor_method_failed"
                        | "actor_socket_failed"
                        | "method_not_found"
                        | "method_not_callable"
                )
            });
        let interleaving_failed = completion.interleaved
            && !matches!(
                completion.result,
                Ok(ActorExecutionResult::Completed { .. })
            );
        if unexpected || interleaving_failed || self.database.stopped.is_cancelled() {
            Recovery::Stop
        } else {
            completion.recovery
        }
    }

    async fn evict(&mut self, graceful: bool) {
        let scope = self.database.invocation(
            ActorInvocation {
                actor: self.actor.clone(),
                request_id: "evict".into(),
                method: "evict".into(),
                args: vec![],
            },
            self.owner_epoch,
            Default::default(),
        );
        let _guard = ScopeGuard(scope.clone());
        if let Err(error) = self.executor.evict(graceful, scope).await {
            warn!(%error, "failed to evict actor");
        }
    }

    fn finish(&self, request: ActorRequest, result: Result<ActorExecutionResult>) {
        self.record_activity();
        self.publish();
        request.finish(&self.endpoint, result);
    }

    fn record_activity(&self) {
        use crate::clock::{Clock, SystemClock};
        if let Ok(now) = SystemClock.now_ms() {
            self.status
                .send_modify(|status| status.last_active_at_ms = Some(now));
        }
    }

    fn publish(&self) {
        let phase = if self.draining {
            ActorPhase::Draining
        } else {
            ActorPhase::Serving
        };
        let busy = !self.running.is_empty() || !self.pending.is_empty();
        let waiting = self
            .pending
            .iter()
            .map(|queued| queued.waiting.clone())
            .collect();
        self.status.send_if_modified(|status| {
            let idle_since =
                (!busy).then(|| status.idle_since.unwrap_or_else(tokio::time::Instant::now));
            let next = ActorStatus {
                phase,
                idle_since,
                waiting,
                last_active_at_ms: status.last_active_at_ms,
            };
            let changed = *status != next;
            *status = next;
            changed
        });
    }
}

impl Drop for Actor {
    fn drop(&mut self) {
        self.status.send_modify(|status| {
            status.phase = ActorPhase::Stopped;
            status.waiting.clear();
        });
        self.requests.close();
        for queued in self.pending.drain(..) {
            queued
                .request
                .finish(&self.endpoint, Ok(ActorExecutionResult::HostUnavailable));
        }
        while let Ok(message) = self.requests.try_recv() {
            if let ActorMessage::Request(request) = message {
                request.finish(&self.endpoint, Ok(ActorExecutionResult::HostUnavailable));
            }
        }
    }
}

async fn admitted(
    admission: &mut Option<watch::Receiver<()>>,
) -> Result<(), watch::error::RecvError> {
    match admission {
        Some(admission) => admission.changed().await,
        None => std::future::pending().await,
    }
}

struct Queued {
    waiting: WaitingOperation,
    request: ActorRequest,
}

impl Queued {
    fn new(request: ActorRequest) -> Self {
        Self {
            waiting: WaitingOperation {
                id: uuid::Uuid::new_v4().to_string(),
                operation: request.operation.invocation().method.clone(),
            },
            request,
        }
    }
}

struct Completion {
    id: u64,
    request: ActorRequest,
    result: Result<ActorExecutionResult>,
    interleaved: bool,
    recovery: Recovery,
    failure_code: Option<String>,
    executed: bool,
}

#[derive(Clone, Copy)]
enum Recovery {
    Keep,
    Evict,
    Reset,
    Stop,
}

async fn run_request(
    id: u64,
    mut request: ActorRequest,
    executor: Arc<dyn ActorExecutor>,
    database: Arc<HostDatabase>,
    publisher: Arc<dyn ActorSocketPublisher>,
) -> Completion {
    if matches!(request.operation, ActorOperation::Activate { .. }) {
        let ActorOperation::Activate { actor, reply } = request.operation else {
            unreachable!()
        };
        request.operation = ActorOperation::Method(ActorInvocation {
            actor: actor.clone(),
            request_id: "activate".into(),
            method: "activate".into(),
            args: vec![],
        });
        let result = activate(actor, &executor, &database).await;
        let recovery = match &result {
            Ok(activation) => {
                request.owner_epoch = activation.owner_epoch;
                Recovery::Keep
            }
            Err(_) => Recovery::Stop,
        };
        request.tracker.state_version(database.state_version());
        let _ = reply.send(result);
        return Completion {
            id,
            request,
            result: Ok(ActorExecutionResult::Completed {
                result: Value::Null,
                effects: vec![],
            }),
            interleaved: false,
            recovery,
            failure_code: None,
            executed: false,
        };
    }
    let scope = database.invocation(
        request.operation.invocation().into_owned(),
        request.owner_epoch,
        request.operation.commit_origin(),
    );
    let _guard = ScopeGuard(scope.clone());
    let prepared = scope
        .prepare(
            matches!(request.operation, ActorOperation::Method(_)),
            &mut request.tracker,
        )
        .await;
    let outcome = match prepared {
        Ok(PreparedInvocation::Execute) => {
            execute(&*executor, &request.operation, scope.clone()).await
        }
        Ok(PreparedInvocation::Completed(result)) => {
            request.tracker.state_version(database.state_version());
            return Completion {
                id,
                request,
                result: Ok(result),
                interleaved: false,
                recovery: Recovery::Keep,
                failure_code: None,
                executed: false,
            };
        }
        Err(error) => {
            return Completion {
                id,
                request,
                result: Err(error),
                interleaved: false,
                recovery: Recovery::Stop,
                failure_code: None,
                executed: false,
            };
        }
    };
    scope.close();
    request.tracker.mark(RequestStage::ActorExecutionCompleted);
    let interleaved = matches!(outcome, Ok(ActorMethodOutcome::Interleaved(_)));
    let failure_code = match &outcome {
        Ok(ActorMethodOutcome::Failed(failure)) => Some(failure.code.clone()),
        _ => None,
    };
    let (result, recovery) = commit_outcome(outcome, &database, &scope, &publisher).await;
    request
        .tracker
        .mark(RequestStage::StatePublicationCompleted);
    request.tracker.state_version(database.state_version());
    Completion {
        id,
        request,
        result,
        interleaved,
        recovery,
        failure_code,
        executed: true,
    }
}

struct ScopeGuard(Arc<DatabaseInvocation>);
impl Drop for ScopeGuard {
    fn drop(&mut self) {
        self.0.close();
    }
}

async fn activate(
    actor: ActorKey,
    executor: &Arc<dyn ActorExecutor>,
    database: &Arc<HostDatabase>,
) -> Result<ActorActivation> {
    ensure!(
        executor.supports(&actor.actor_name),
        "actor name {} is not loaded by this host",
        actor.actor_name
    );
    let activation = match database.activate().await? {
        PreparedActivation::Completed(activation) => return Ok(activation),
        PreparedActivation::Hydrate { activation } => activation,
    };
    let scope = database.invocation(
        ActorInvocation {
            actor,
            request_id: "activate".into(),
            method: "activate".into(),
            args: vec![],
        },
        activation.owner_epoch,
        Default::default(),
    );
    let _guard = ScopeGuard(scope.clone());
    let result = executor.hydrate("activate".into(), scope.clone()).await;
    scope.close();
    database.hydrated(activation, result).await
}

async fn execute(
    executor: &dyn ActorExecutor,
    operation: &ActorOperation,
    database: Arc<dyn ActorDatabase>,
) -> Result<ActorMethodOutcome> {
    match operation {
        ActorOperation::Method(invocation) => {
            executor
                .invoke(
                    ActorMethodInvocation {
                        request_id: invocation.request_id.clone(),
                        method: invocation.method.clone(),
                        args: invocation.args.clone(),
                    },
                    database,
                )
                .await
        }
        ActorOperation::Socket(invocation) => Ok(
            match executor.handle_socket(invocation.clone(), database).await? {
                ActorSocketOutcome::Interleaved(outcome) => {
                    ActorMethodOutcome::Interleaved(outcome)
                }
                ActorSocketOutcome::Handled { effects } => ActorMethodOutcome::Completed {
                    result: Value::Null,
                    effects,
                },
                ActorSocketOutcome::Failed(failure) => ActorMethodOutcome::Failed(failure),
            },
        ),
        ActorOperation::Activate { .. } => unreachable!(),
    }
}

async fn commit_outcome(
    outcome: Result<ActorMethodOutcome>,
    database: &Arc<HostDatabase>,
    scope: &DatabaseInvocation,
    publisher: &Arc<dyn ActorSocketPublisher>,
) -> (Result<ActorExecutionResult>, Recovery) {
    let (result, effects, sequence) = match outcome {
        Ok(ActorMethodOutcome::Completed { result, effects }) => (result, effects, None),
        Ok(ActorMethodOutcome::Interleaved(outcome)) => {
            (outcome.result, outcome.effects, Some(outcome.sequence))
        }
        Ok(ActorMethodOutcome::Failed(failure)) => {
            let recovery = if matches!(
                failure.code.as_str(),
                "actor_method_failed" | "actor_socket_failed"
            ) {
                Recovery::Keep
            } else {
                Recovery::Reset
            };
            let code = if failure.code == "resource_exhausted" {
                "resource_exhausted"
            } else {
                "actor_error"
            };
            return (Ok(failed(code, failure.message)), recovery);
        }
        Err(error) => {
            return (
                Ok(failed(
                    "actor_error",
                    format!("actor executor failed: {error:#}"),
                )),
                Recovery::Reset,
            );
        }
    };
    if let Err(error) = validate_socket_effects(&effects) {
        return (
            Ok(failed(
                "actor_error",
                format!("actor returned invalid socket effects: {error:#}"),
            )),
            Recovery::Reset,
        );
    }
    let version = match scope.complete(result.clone(), sequence).await {
        Ok(version) => version,
        Err(error) if error.is::<PublicationError>() || sequence.is_some() => {
            warn!(error = %format!("{error:#}"), "actor completed but state publication could not be confirmed");
            return (
                Ok(ActorExecutionResult::Failed {
                    failure: ActorInvocationFailure::outcome_unknown_after_execution(),
                }),
                Recovery::Evict,
            );
        }
        Err(error) => return (Err(error), Recovery::Keep),
    };
    let result = complete_with_state(version, result, effects, database, publisher);
    (result, Recovery::Keep)
}

fn complete_with_state(
    state_version: u64,
    result: Value,
    effects: Vec<ActorSocketEffect>,
    database: &HostDatabase,
    publisher: &Arc<dyn ActorSocketPublisher>,
) -> Result<ActorExecutionResult> {
    database.ensure_authority()?;
    let (mut automatic, effects): (Vec<_>, Vec<_>) = effects.into_iter().partition(|effect| {
        matches!(
            effect,
            ActorSocketEffect::StateSnapshot { .. } | ActorSocketEffect::StateUpdate { .. }
        )
    });
    for effect in &mut automatic {
        match effect {
            ActorSocketEffect::StateSnapshot { version, .. }
            | ActorSocketEffect::StateUpdate { version, .. } => *version = Some(state_version),
            _ => unreachable!(),
        }
    }
    if !automatic.is_empty() {
        let publisher = publisher.clone();
        tokio::spawn(async move {
            if let Err(error) = publisher.publish(automatic).await {
                warn!(error = %format!("{error:#}"), "committed state notification failed");
            }
        });
    }
    Ok(ActorExecutionResult::Completed { result, effects })
}

fn failed(code: &str, message: impl Into<String>) -> ActorExecutionResult {
    ActorExecutionResult::Failed {
        failure: ActorInvocationFailure {
            code: code.into(),
            message: message.into(),
        },
    }
}

enum ActorMessage {
    Request(Box<ActorRequest>),
    Drain,
}

struct ActorRequest {
    tracker: RequestTracker,
    operation: ActorOperation,
    owner_epoch: u64,
    reply: oneshot::Sender<Result<ActorExecutionResult>>,
}

impl ActorRequest {
    fn finish(mut self, endpoint: &HostEndpoint, result: Result<ActorExecutionResult>) {
        self.tracker.complete(&result);
        log_invocation(
            endpoint,
            &self.operation.invocation(),
            self.tracker.completion(),
            &result,
        );
        let _ = self.reply.send(result);
    }
}

enum ActorOperation {
    Activate {
        actor: ActorKey,
        reply: oneshot::Sender<Result<ActorActivation>>,
    },
    Method(ActorInvocation),
    Socket(ActorSocketInvocation),
}

impl ActorOperation {
    fn commit_origin(&self) -> CommitOrigin {
        match self {
            Self::Socket(invocation) => CommitOrigin::socket(&invocation.event),
            _ => CommitOrigin::default(),
        }
    }

    fn is_disconnect(&self) -> bool {
        matches!(
            self,
            Self::Socket(ActorSocketInvocation {
                event: ActorSocketEvent::Disconnect { .. },
                ..
            })
        )
    }

    fn validate(&self) -> Result<()> {
        self.invocation().validate()
    }

    fn invocation(&self) -> Cow<'_, ActorInvocation> {
        match self {
            Self::Activate { actor, .. } => Cow::Owned(ActorInvocation {
                request_id: "activate".into(),
                actor: actor.clone(),
                method: "activate".into(),
                args: vec![],
            }),
            Self::Method(invocation) => Cow::Borrowed(invocation),
            Self::Socket(invocation) => Cow::Owned(ActorInvocation {
                request_id: invocation.request_id.clone(),
                actor: invocation.actor.clone(),
                method: socket_event_name(&invocation.event).into(),
                args: Vec::new(),
            }),
        }
    }
}

fn socket_event_name(event: &ActorSocketEvent) -> &'static str {
    match event {
        ActorSocketEvent::Connect { .. } => "onConnect",
        ActorSocketEvent::Message { .. } => "onMessage",
        ActorSocketEvent::Disconnect { .. } => "onDisconnect",
    }
}

fn log_invocation(
    endpoint: &HostEndpoint,
    invocation: &ActorInvocation,
    completion: &RequestCompletion,
    outcome: &Result<ActorExecutionResult>,
) {
    match outcome {
        Ok(result) => log_invocation_result(
            endpoint,
            invocation,
            completion,
            actor_execution_outcome(result),
            actor_execution_failure_code(result).unwrap_or(""),
            None,
        ),
        Err(error) => log_invocation_result(
            endpoint,
            invocation,
            completion,
            "host_error",
            "",
            Some(format!("{error:#}")),
        ),
    }
}

fn log_invocation_result(
    endpoint: &HostEndpoint,
    invocation: &ActorInvocation,
    completion: &RequestCompletion,
    outcome: &str,
    failure_code: &str,
    error: Option<String>,
) {
    info!(
            event = "actor_host_invocation",
            request_id = %invocation.request_id,

            actor_name = %invocation.actor.actor_name,
            actor_id = %invocation.actor.actor_id,
            method = %invocation.method,
            host_id = %endpoint.id,
            started_at_ms = 0,
            host_state = completion.timings.host_state.as_str(),
            routing_ms = completion.timings.routing_ms,
            queue_admitted_at_ms = completion.timings.queue_wait_ms,
            state_cache_checked_at_ms = completion.checkpoints.state_cache_checked_at_ms,
            state_downloaded_at_ms = completion.checkpoints.state_downloaded_at_ms,
            state_decoded_at_ms = completion.checkpoints.state_decoded_at_ms,
            pending_commit_resolved_at_ms = completion.checkpoints.pending_commit_resolved_at_ms,
            actor_execution_completed_at_ms = completion.checkpoints.actor_execution_completed_at_ms,
            state_publication_completed_at_ms = completion.checkpoints.state_publication_completed_at_ms,
            completed_at_ms = completion.timings.duration_ms,
            outcome,
            failure_code,
            error,
            "actor host invocation completed"
    );
}

fn actor_execution_outcome(result: &ActorExecutionResult) -> &'static str {
    match result {
        ActorExecutionResult::Completed { .. } => "completed",
        ActorExecutionResult::Failed { .. } => "failed",
        ActorExecutionResult::HostUnavailable => "host_unavailable",
    }
}

fn actor_execution_failure_code(result: &ActorExecutionResult) -> Option<&str> {
    match result {
        ActorExecutionResult::Failed { failure } => Some(&failure.code),
        _ => None,
    }
}

#[cfg(test)]
#[path = "../../tests/unit/host/actor_host.rs"]
mod tests;
