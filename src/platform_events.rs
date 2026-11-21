//! Events the control plane reports on the collector's internal pipeline: lifecycle reports,
//! WebSocket connections, deployments, sandbox kills and developer feedback. The collector stores each in
//! `platform_events` and publishes it on its project's live channel. One process reports for many
//! projects, so each record carries its project.

use opentelemetry::logs::AnyValue;
use opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest,
    common::v1::{AnyValue as ProtoValue, KeyValue, any_value::Value as ProtoKind},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    resource::v1::Resource,
};

use crate::request_traces::otlp;

pub(crate) mod deployments;
pub(crate) mod feedback;
pub(crate) mod lifecycle;
pub(crate) mod sandboxes;
pub(crate) mod sockets;

/// An event observers read from the analytics store and the live feed.
pub(crate) trait PlatformEvent: Send + 'static {
    fn project_id(&self) -> &str;
    fn to_record(&self) -> PlatformRecord;
}

/// Records events without blocking or failing the caller; delivery is best effort.
pub(crate) trait PlatformEvents<E>: Send + Sync {
    fn record(&self, event: E);
}

/// For processes without platform telemetry configured.
pub(crate) struct NoPlatformEvents;

impl<E> PlatformEvents<E> for NoPlatformEvents {
    fn record(&self, _event: E) {}
}

/// One event as an OTLP log record.
#[derive(Clone)]
pub(crate) struct PlatformRecord {
    pub name: &'static str,
    pub at_ms: u64,
    pub attributes: Vec<(&'static str, AnyValue)>,
    pub body: Option<String>,
}

impl PlatformRecord {
    /// The record as the collector's Redis exporter publishes it, for processes without a
    /// collector.
    pub(crate) fn export_request(self, gateway_id: &str) -> ExportLogsServiceRequest {
        let record = LogRecord {
            time_unix_nano: self.at_ms * 1_000_000,
            event_name: self.name.to_owned(),
            attributes: self
                .attributes
                .into_iter()
                .map(|(key, value)| attribute(key, value))
                .collect(),
            body: self.body.map(|body| proto_value(AnyValue::from(body))),
            ..Default::default()
        };
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![attribute(
                        otlp::GATEWAY_ID,
                        AnyValue::from(gateway_id.to_owned()),
                    )],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    log_records: vec![record],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }
}

pub(crate) fn count(value: u64) -> AnyValue {
    AnyValue::from(i64::try_from(value).unwrap_or(i64::MAX))
}

fn attribute(key: &str, value: AnyValue) -> KeyValue {
    KeyValue {
        key: key.to_owned(),
        value: Some(proto_value(value)),
        ..Default::default()
    }
}

fn proto_value(value: AnyValue) -> ProtoValue {
    ProtoValue {
        value: match value {
            AnyValue::Int(value) => Some(ProtoKind::IntValue(value)),
            AnyValue::Boolean(value) => Some(ProtoKind::BoolValue(value)),
            AnyValue::String(value) => Some(ProtoKind::StringValue(value.to_string())),
            _ => None,
        },
    }
}
