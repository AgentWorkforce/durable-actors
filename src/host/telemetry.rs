use std::sync::Arc;

use anyhow::Result;
use opentelemetry::logs::{AnyValue, LogRecord, Logger, LoggerProvider, Severity};
use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::{LogExporter, SpanExporter, WithExportConfig, WithTonicConfig};
use opentelemetry_sdk::{
    Resource,
    logs::{SdkLogger, SdkLoggerProvider},
    trace::{SdkTracer, SdkTracerProvider},
};
use tonic::transport::ClientTlsConfig;

use crate::{
    actor::{ActorLogRecord, ActorLogSink, LogLevel, StdioLogSink},
    control_plane::HostAuthorization,
    request_traces::otlp::{LOG_ID, REQUEST_ID},
};

const ENDPOINT: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";
const SCOPE: &str = "durable-actors-host";

pub(crate) struct HostTelemetry {
    traces: SdkTracerProvider,
    logs: SdkLoggerProvider,
}

impl HostTelemetry {
    pub(crate) fn from_env(authorization: HostAuthorization) -> Result<Option<Self>> {
        match std::env::var(ENDPOINT) {
            Ok(endpoint) if !endpoint.is_empty() => {
                Ok(Some(Self::connect(&endpoint, authorization)?))
            }
            _ => Ok(None),
        }
    }

    pub(crate) fn tracer(&self) -> SdkTracer {
        self.traces.tracer(SCOPE)
    }

    pub(crate) fn log_sink(&self) -> Arc<dyn ActorLogSink> {
        Arc::new(OtelLogSink {
            logger: self.logs.logger(SCOPE),
        })
    }

    pub(crate) fn shutdown(&self) {
        if let Err(error) = self.traces.shutdown() {
            tracing::warn!(%error, "request span export did not flush");
        }
        if let Err(error) = self.logs.shutdown() {
            tracing::warn!(%error, "actor log export did not flush");
        }
    }

    fn connect(endpoint: &str, authorization: HostAuthorization) -> Result<Self> {
        let resource = Resource::builder().with_service_name(SCOPE).build();
        let spans = exporter(
            SpanExporter::builder().with_tonic(),
            endpoint,
            &authorization,
        )
        .build()?;
        let logs = exporter(
            LogExporter::builder().with_tonic(),
            endpoint,
            &authorization,
        )
        .build()?;
        Ok(Self {
            traces: SdkTracerProvider::builder()
                .with_resource(resource.clone())
                .with_batch_exporter(spans)
                .build(),
            logs: SdkLoggerProvider::builder()
                .with_resource(resource)
                .with_batch_exporter(logs)
                .build(),
        })
    }
}

fn exporter<B: WithExportConfig + WithTonicConfig>(
    builder: B,
    endpoint: &str,
    authorization: &HostAuthorization,
) -> B {
    let builder = builder
        .with_endpoint(endpoint)
        .with_interceptor(authorization.clone());
    if endpoint.starts_with("https:") {
        builder.with_tls_config(ClientTlsConfig::new().with_webpki_roots())
    } else {
        builder
    }
}

struct OtelLogSink {
    logger: SdkLogger,
}

impl ActorLogSink for OtelLogSink {
    fn emit(&self, record: ActorLogRecord) {
        StdioLogSink::write(&record);
        let mut log = self.logger.create_log_record();
        log.set_timestamp(std::time::SystemTime::now());
        log.set_severity_number(severity(record.level));
        log.set_severity_text(record.level.as_str());
        log.add_attribute(LOG_ID, uuid::Uuid::now_v7().to_string());
        if let Some(request_id) = record.request_id {
            log.add_attribute(REQUEST_ID, request_id);
        }
        log.set_body(AnyValue::from(record.message));
        self.logger.emit(log);
    }
}

fn severity(level: LogLevel) -> Severity {
    match level {
        LogLevel::Debug => Severity::Debug,
        LogLevel::Info => Severity::Info,
        LogLevel::Warn => Severity::Warn,
        LogLevel::Error => Severity::Error,
    }
}
