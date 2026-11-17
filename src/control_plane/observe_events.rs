//! The project's observability events as the dashboard applies them. Views read the store once
//! when they open; afterwards this stream forwards what the collector publishes on the project's
//! live channels, decoded into the change each record makes to what an observer sees. Each
//! message names one event type and carries the records of that type in the order they arrived.
//! `event_history` reads past events back from the store through the same decoder.

use anyhow::{Result, bail};
use futures_util::{Stream, StreamExt, stream};
use opentelemetry_proto::tonic::collector::{
    logs::v1::ExportLogsServiceRequest, trace::v1::ExportTraceServiceRequest,
};
use prost::Message;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    actor::LogLevel,
    actor_lifecycle::{LifecycleEvent, instance_overview},
    clock::{Clock, SystemClock},
    host_leases::WaitingOperation,
    placement::ActorResidency,
    platform_events::{
        deployments, lifecycle,
        sandboxes::{self, KillReason},
        sockets,
    },
    request_traces::{
        TraceRecord, TraceStore,
        logs::{ActorLog, LogQuery},
        otlp::{self, PlatformLog, Signal},
    },
};

/// Which live channels to forward, and which logs.
#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct EventsQuery {
    /// Comma-separated: `requests`, `logs`, `platform`.
    pub signals: String,
    pub actor_name: Option<String>,
    pub actor_id: Option<String>,
    pub request_id: Option<String>,
    pub level: Option<LogLevel>,
    pub search: Option<String>,
}

impl EventsQuery {
    pub(super) fn signals(&self) -> Result<Vec<Signal>> {
        self.signals
            .split(',')
            .map(|signal| match signal.trim() {
                "requests" => Ok(Signal::Requests),
                "logs" => Ok(Signal::Logs),
                "platform" => Ok(Signal::Platform),
                other => bail!("unknown signal {other:?}; use requests, logs or platform"),
            })
            .collect()
    }

    pub(super) fn log_filter(&self) -> LogQuery {
        LogQuery {
            actor_name: self.actor_name.clone(),
            actor_id: self.actor_id.clone(),
            request_id: self.request_id.clone(),
            level: self.level,
            search: self.search.clone(),
            ..Default::default()
        }
    }
}

#[derive(Debug, PartialEq, Serialize)]
#[serde(untagged)]
pub(super) enum ObserveEvent {
    Request(TraceRecord),
    Log(ActorLog),
    Instance(InstanceChange),
    ConnectionOpened(ConnectionOpened),
    ConnectionClosed(ConnectionClosed),
    GatewayAlive(GatewayAlive),
    Deployment(Deployment),
    Kill(ActorKill),
}

/// An instance as its latest lifecycle report leaves it.
#[derive(Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct InstanceChange {
    pub actor_name: String,
    pub actor_id: String,
    pub status: ActorResidency,
    pub occurred_at_ms: u64,
    pub reported_at_ms: u64,
    pub stale_at_ms: Option<u64>,
    pub last_active_at_ms: Option<u64>,
    pub waiting: Option<Vec<WaitingOperation>>,
}

#[derive(Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ConnectionOpened {
    pub connection_id: String,
    pub actor_name: String,
    pub actor_id: String,
    pub gateway_id: String,
    pub opened_at_ms: u64,
    pub metadata: Value,
}

#[derive(Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ConnectionClosed {
    pub connection_id: String,
    pub actor_name: String,
    pub actor_id: String,
    pub gateway_id: String,
    pub opened_at_ms: u64,
    pub closed_at_ms: u64,
    pub close_code: Option<u64>,
    pub close_reason: Option<String>,
    pub messages_in: Option<u64>,
    pub messages_out: Option<u64>,
}

#[derive(Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct GatewayAlive {
    pub gateway_id: String,
    pub at_ms: u64,
}

/// The actor classes a deployment leaves deployed; empty once it is removed.
#[derive(Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Deployment {
    pub at_ms: u64,
    pub actors: Vec<DeployedActor>,
}

#[derive(Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct DeployedActor {
    pub actor_name: String,
    pub methods: Vec<String>,
}

/// A sandbox that stopped under an actor without the platform asking it to. Only the stored
/// record names the sandbox: which one ran an actor is the platform's concern, not the observer's.
#[derive(Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ActorKill {
    pub actor_name: String,
    pub actor_id: String,
    pub at_ms: u64,
    pub reason: KillReason,
}

impl ObserveEvent {
    pub(super) fn name(&self) -> &'static str {
        match self {
            Self::Request(_) => "request",
            Self::Log(_) => "log",
            Self::Instance(_) => "instance",
            Self::ConnectionOpened(_) => "connectionOpened",
            Self::ConnectionClosed(_) => "connectionClosed",
            Self::GatewayAlive(_) => "gatewayAlive",
            Self::Deployment(_) => "deployment",
            Self::Kill(_) => "kill",
        }
    }
}

/// Consecutive events of one type, sent as one message.
#[derive(Debug, PartialEq)]
pub(super) struct EventBatch {
    pub name: &'static str,
    pub events: Vec<ObserveEvent>,
}

/// The project's events on the requested channels as the live feed delivers them.
pub(super) async fn subscribe(
    traces: &TraceStore,
    project: &str,
    query: &EventsQuery,
) -> Result<impl Stream<Item = EventBatch> + Send + use<>> {
    let mut channels = Vec::new();
    for signal in query.signals()? {
        let filter = query.log_filter();
        let updates = traces.subscribe(project, signal).await?;
        channels.push(
            updates
                .flat_map(move |payload| stream::iter(batches(decode(signal, &payload, &filter))))
                .boxed(),
        );
    }
    Ok(stream::select_all(channels))
}

pub(super) fn decode(signal: Signal, payload: &[u8], filter: &LogQuery) -> Vec<ObserveEvent> {
    match signal {
        Signal::Requests => requests(payload),
        Signal::Logs => logs(payload, filter),
        Signal::Platform => platform(payload),
    }
}

pub(super) fn batches(events: Vec<ObserveEvent>) -> Vec<EventBatch> {
    let mut batches: Vec<EventBatch> = Vec::new();
    for event in events {
        match batches.last_mut() {
            Some(batch) if batch.name == event.name() => batch.events.push(event),
            _ => batches.push(EventBatch {
                name: event.name(),
                events: vec![event],
            }),
        }
    }
    batches
}

fn requests(payload: &[u8]) -> Vec<ObserveEvent> {
    let Ok(request) = ExportTraceServiceRequest::decode(payload) else {
        return Vec::new();
    };
    otlp::trace_events(request)
        .into_iter()
        .map(|event| {
            ObserveEvent::Request(TraceRecord {
                sequence: event.trace.started_at_ms,
                event,
            })
        })
        .collect()
}

fn logs(payload: &[u8], filter: &LogQuery) -> Vec<ObserveEvent> {
    let Ok(request) = ExportLogsServiceRequest::decode(payload) else {
        return Vec::new();
    };
    otlp::actor_logs(request)
        .into_iter()
        .filter(|log| filter.matches(log))
        .map(ObserveEvent::Log)
        .collect()
}

fn platform(payload: &[u8]) -> Vec<ObserveEvent> {
    let Ok(request) = ExportLogsServiceRequest::decode(payload) else {
        return Vec::new();
    };
    otlp::platform_logs(request)
        .iter()
        .filter_map(platform_event)
        .collect()
}

pub(super) fn platform_event(log: &PlatformLog) -> Option<ObserveEvent> {
    match log.name.as_str() {
        lifecycle::REPORTED => instance_change(log),
        sockets::OPENED => connection_opened(log),
        sockets::CLOSED => connection_closed(log),
        sockets::ALIVE => Some(ObserveEvent::GatewayAlive(GatewayAlive {
            gateway_id: log.gateway_id.clone(),
            at_ms: log.at_ms,
        })),
        deployments::PUBLISHED | deployments::REMOVED => {
            Some(ObserveEvent::Deployment(Deployment {
                at_ms: log.at_ms,
                actors: deployed_actors(log.body.as_deref()),
            }))
        }
        sandboxes::KILLED => kill(log),
        _ => None,
    }
}

fn instance_change(log: &PlatformLog) -> Option<ObserveEvent> {
    let event: LifecycleEvent = serde_json::from_str(log.body.as_deref()?).ok()?;
    let (actor_name, occurred_at_ms) =
        (event.actor.actor_name.clone(), event.report.occurred_at_ms);
    let instance = instance_overview(event, SystemClock.now_ms().ok()?);
    Some(ObserveEvent::Instance(InstanceChange {
        actor_name,
        actor_id: instance.actor_id,
        status: instance.status,
        occurred_at_ms,
        reported_at_ms: instance.reported_at_ms?,
        stale_at_ms: instance.stale_at_ms,
        last_active_at_ms: instance.last_active_at_ms,
        waiting: instance.waiting,
    }))
}

fn connection_opened(log: &PlatformLog) -> Option<ObserveEvent> {
    Some(ObserveEvent::ConnectionOpened(ConnectionOpened {
        connection_id: log.string(otlp::CONNECTION_ID)?,
        actor_name: log.string(otlp::ACTOR_NAME)?,
        actor_id: log.string(otlp::ACTOR_ID)?,
        gateway_id: log.gateway_id.clone(),
        opened_at_ms: log.at_ms,
        metadata: log
            .string(otlp::METADATA)
            .and_then(|json| serde_json::from_str(&json).ok())
            .unwrap_or_default(),
    }))
}

fn connection_closed(log: &PlatformLog) -> Option<ObserveEvent> {
    Some(ObserveEvent::ConnectionClosed(ConnectionClosed {
        connection_id: log.string(otlp::CONNECTION_ID)?,
        actor_name: log.string(otlp::ACTOR_NAME)?,
        actor_id: log.string(otlp::ACTOR_ID)?,
        gateway_id: log.gateway_id.clone(),
        opened_at_ms: log
            .at_ms
            .saturating_sub(log.count(otlp::DURATION_MS).unwrap_or_default()),
        closed_at_ms: log.at_ms,
        close_code: log.count(otlp::CLOSE_CODE),
        close_reason: log.string(otlp::CLOSE_REASON),
        messages_in: log.count(otlp::MESSAGES_IN),
        messages_out: log.count(otlp::MESSAGES_OUT),
    }))
}

fn kill(log: &PlatformLog) -> Option<ObserveEvent> {
    Some(ObserveEvent::Kill(ActorKill {
        actor_name: log.string(otlp::ACTOR_NAME)?,
        actor_id: log.string(otlp::ACTOR_ID)?,
        at_ms: log.at_ms,
        reason: serde_json::from_value(Value::String(log.string(otlp::KILL_REASON)?)).ok()?,
    }))
}

fn deployed_actors(contract: Option<&str>) -> Vec<DeployedActor> {
    contract
        .and_then(|json| serde_json::from_str(json).ok())
        .and_then(|document| super::contracts::actor_methods(&document).ok())
        .unwrap_or_default()
        .into_iter()
        .map(|(actor_name, methods)| DeployedActor {
            actor_name,
            methods,
        })
        .collect()
}
