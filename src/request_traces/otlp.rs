use opentelemetry_proto::tonic::{
    collector::{logs::v1::ExportLogsServiceRequest, trace::v1::ExportTraceServiceRequest},
    common::v1::{AnyValue, KeyValue, any_value::Value},
    logs::v1::LogRecord,
    resource::v1::Resource,
    trace::v1::Span,
};
use serde::de::DeserializeOwned;

use super::{RequestTrace, TraceEvent, logs::ActorLog};

/// How long every OTLP exporter we run batches before sending, so observers see an event about
/// a tenth of a second after it happens.
pub(crate) const EXPORT_DELAY: std::time::Duration = std::time::Duration::from_millis(100);

pub(crate) const ORGANIZATION_ID: &str = "durable_actors.organization_id";
pub(crate) const PROJECT_ID: &str = "durable_actors.project_id";
pub(crate) const ACTOR_NAME: &str = "durable_actors.actor_name";
pub(crate) const ACTOR_ID: &str = "durable_actors.actor_id";
pub(crate) const HOST_ID: &str = "durable_actors.host_id";
pub(crate) const SESSION_ID: &str = "durable_actors.session_id";
pub(crate) const REQUEST_ID: &str = "durable_actors.request_id";
pub(crate) const LOG_ID: &str = "durable_actors.log_id";
pub(crate) const KIND: &str = "durable_actors.kind";
pub(crate) const CONNECTION_ID: &str = "durable_actors.connection_id";
pub(crate) const OUTCOME: &str = "durable_actors.outcome";
pub(crate) const QUEUE_WAIT_MS: &str = "durable_actors.queue_wait_ms";
pub(crate) const ROUTING_MS: &str = "durable_actors.routing_ms";
pub(crate) const HOST_STATE: &str = "durable_actors.host_state";
pub(crate) const STATE_VERSION: &str = "durable_actors.state_version";
pub(crate) const METADATA: &str = "durable_actors.metadata";
pub(crate) const GATEWAY_ID: &str = "durable_actors.gateway_id";
pub(crate) const CLOSE_CODE: &str = "durable_actors.close_code";
pub(crate) const CLOSE_REASON: &str = "durable_actors.close_reason";
pub(crate) const CLOSE_CLEAN: &str = "durable_actors.close_clean";
pub(crate) const MESSAGES_IN: &str = "durable_actors.messages_in";
pub(crate) const MESSAGES_OUT: &str = "durable_actors.messages_out";
pub(crate) const BYTES_IN: &str = "durable_actors.bytes_in";
pub(crate) const BYTES_OUT: &str = "durable_actors.bytes_out";
pub(crate) const DURATION_MS: &str = "durable_actors.duration_ms";
pub(crate) const SANDBOX_ID: &str = "durable_actors.sandbox_id";
pub(crate) const KILL_REASON: &str = "durable_actors.kill_reason";

const TENANT_PREFIX: &str = "durable_actors.";

#[derive(Clone, Copy)]
pub(crate) enum Signal {
    Requests,
    Logs,
    /// Platform events (lifecycle reports, WebSocket connections and deployments), which the
    /// control plane reports through the collector.
    Platform,
}

pub(crate) fn live_channel(project_id: &str, signal: Signal) -> String {
    let signal = match signal {
        Signal::Requests => "traces",
        Signal::Logs => "logs",
        Signal::Platform => "platform",
    };
    format!("durable-actors:v1:live:{project_id}:{signal}")
}

/// Verified host identity, written over whatever the host put in its resource.
pub(crate) struct Identity {
    pub organization_id: Option<String>,
    pub project_id: String,
    pub actor_name: String,
    pub actor_id: String,
    pub host_id: String,
    pub session_id: String,
}

impl Identity {
    pub(crate) fn stamp(&self, resource: &mut Option<Resource>) {
        let resource = resource.get_or_insert_with(Resource::default);
        resource
            .attributes
            .retain(|attribute| !attribute.key.starts_with(TENANT_PREFIX));
        let values = [
            (PROJECT_ID, Some(&self.project_id)),
            (ACTOR_NAME, Some(&self.actor_name)),
            (ACTOR_ID, Some(&self.actor_id)),
            (HOST_ID, Some(&self.host_id)),
            (SESSION_ID, Some(&self.session_id)),
            (ORGANIZATION_ID, self.organization_id.as_ref()),
        ];
        for (key, value) in values {
            if let Some(value) = value {
                resource.attributes.push(string_attribute(key, value));
            }
        }
    }
}

pub(crate) fn trace_events(request: ExportTraceServiceRequest) -> Vec<TraceEvent> {
    request
        .resource_spans
        .into_iter()
        .flat_map(|resource_spans| {
            let resource = resource_spans.resource.unwrap_or_default();
            resource_spans
                .scope_spans
                .into_iter()
                .flat_map(|scope| scope.spans)
                .filter_map(move |span| trace_event(&resource.attributes, span))
                .collect::<Vec<_>>()
        })
        .collect()
}

pub(crate) fn actor_logs(request: ExportLogsServiceRequest) -> Vec<ActorLog> {
    request
        .resource_logs
        .into_iter()
        .flat_map(|resource_logs| {
            let resource = resource_logs.resource.unwrap_or_default();
            resource_logs
                .scope_logs
                .into_iter()
                .flat_map(|scope| scope.log_records)
                .filter_map(move |record| actor_log(&resource.attributes, record))
                .collect::<Vec<_>>()
        })
        .collect()
}

/// One platform event (a connection change, a gateway heartbeat, a deployment) as the collector
/// publishes or stores it.
pub(crate) struct PlatformLog {
    pub name: String,
    pub gateway_id: String,
    pub at_ms: u64,
    pub attributes: Vec<KeyValue>,
    pub body: Option<String>,
}

impl PlatformLog {
    /// An event read back from a store, which keeps every attribute value as text.
    pub(crate) fn stored(
        name: String,
        gateway_id: String,
        at_ms: u64,
        attributes: impl IntoIterator<Item = (String, String)>,
        body: Option<String>,
    ) -> Self {
        Self {
            name,
            gateway_id,
            at_ms,
            attributes: attributes
                .into_iter()
                .map(|(key, value)| string_attribute(&key, &value))
                .collect(),
            body: body.filter(|body| !body.is_empty()),
        }
    }

    pub(crate) fn string(&self, key: &str) -> Option<String> {
        string(&self.attributes, key)
    }

    pub(crate) fn count(&self, key: &str) -> Option<u64> {
        integer(&self.attributes, key).and_then(|value| u64::try_from(value).ok())
    }
}

pub(crate) fn platform_logs(request: ExportLogsServiceRequest) -> Vec<PlatformLog> {
    request
        .resource_logs
        .into_iter()
        .flat_map(|resource_logs| {
            let gateway_id = resource_logs
                .resource
                .and_then(|resource| string(&resource.attributes, GATEWAY_ID))
                .unwrap_or_default();
            resource_logs
                .scope_logs
                .into_iter()
                .flat_map(|scope| scope.log_records)
                .map(move |record| PlatformLog {
                    name: record.event_name,
                    gateway_id: gateway_id.clone(),
                    at_ms: record.time_unix_nano / 1_000_000,
                    attributes: record.attributes,
                    body: record.body.and_then(text),
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

fn trace_event(resource: &[KeyValue], span: Span) -> Option<TraceEvent> {
    let attributes = &span.attributes;
    let started_at_ns = span.start_time_unix_nano;
    let duration_ns = span.end_time_unix_nano.saturating_sub(started_at_ns);
    Some(TraceEvent {
        event_id: hex(&span.span_id),
        host_id: string(resource, HOST_ID)?,
        session_id: string(resource, SESSION_ID)?,
        trace: RequestTrace {
            state_version: integer(attributes, STATE_VERSION).and_then(|v| u64::try_from(v).ok()),
            project_id: string(resource, PROJECT_ID)?,
            request_id: string(attributes, REQUEST_ID)?,
            actor_name: string(resource, ACTOR_NAME)?,
            actor_id: string(resource, ACTOR_ID)?,
            kind: parse(&string(attributes, KIND)?)?,
            operation: span.name,
            connection_id: string(attributes, CONNECTION_ID),
            started_at_ms: started_at_ns / 1_000_000,
            routing_ms: number(attributes, ROUTING_MS).unwrap_or_default(),
            duration_ms: duration_ns as f64 / 1e6,
            queue_wait_ms: number(attributes, QUEUE_WAIT_MS),
            host_state: string(attributes, HOST_STATE)
                .and_then(|value| parse(&value))
                .unwrap_or_default(),
            outcome: parse(&string(attributes, OUTCOME)?)?,
            metadata: string(attributes, METADATA)
                .and_then(|json| serde_json::from_str(&json).ok()),
        },
    })
}

fn actor_log(resource: &[KeyValue], record: LogRecord) -> Option<ActorLog> {
    let attributes = &record.attributes;
    let timestamp_ns = if record.time_unix_nano > 0 {
        record.time_unix_nano
    } else {
        record.observed_time_unix_nano
    };
    Some(ActorLog {
        log_id: string(attributes, LOG_ID)?,
        project_id: string(resource, PROJECT_ID)?,
        actor_name: string(resource, ACTOR_NAME)?,
        actor_id: string(resource, ACTOR_ID)?,
        host_id: string(resource, HOST_ID)?,
        request_id: string(attributes, REQUEST_ID),
        timestamp_ms: timestamp_ns / 1_000_000,
        level: record.severity_text.to_ascii_lowercase(),
        message: record.body.and_then(text).unwrap_or_default(),
    })
}

fn value<'a>(attributes: &'a [KeyValue], key: &str) -> Option<&'a Value> {
    attributes
        .iter()
        .find(|attribute| attribute.key == key)?
        .value
        .as_ref()?
        .value
        .as_ref()
}

fn string(attributes: &[KeyValue], key: &str) -> Option<String> {
    match value(attributes, key)? {
        Value::StringValue(value) => Some(value.clone()),
        _ => None,
    }
}

fn number(attributes: &[KeyValue], key: &str) -> Option<f64> {
    match value(attributes, key)? {
        Value::DoubleValue(value) => Some(*value),
        Value::IntValue(value) => Some(*value as f64),
        Value::StringValue(value) => value.parse().ok(),
        _ => None,
    }
    .filter(|value| value.is_finite() && *value >= 0.0)
}

fn integer(attributes: &[KeyValue], key: &str) -> Option<i64> {
    match value(attributes, key)? {
        Value::IntValue(value) => Some(*value),
        Value::StringValue(value) => value.parse().ok(),
        _ => None,
    }
}

fn text(body: AnyValue) -> Option<String> {
    match body.value? {
        Value::StringValue(value) => Some(value),
        _ => None,
    }
}

fn parse<T: DeserializeOwned>(value: &str) -> Option<T> {
    serde_json::from_value(serde_json::Value::String(value.to_owned())).ok()
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn string_attribute(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.into())),
        }),
        ..Default::default()
    }
}

#[cfg(test)]
#[path = "../../tests/unit/request_traces/otlp.rs"]
mod tests;
