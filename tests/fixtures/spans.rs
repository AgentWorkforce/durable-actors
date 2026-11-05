use opentelemetry::trace::TracerProvider;
use opentelemetry_proto::{
    tonic::collector::trace::v1::ExportTraceServiceRequest,
    transform::{
        common::tonic::ResourceAttributesWithSchema,
        trace::tonic::group_spans_by_resource_and_scope,
    },
};
use opentelemetry_sdk::{
    Resource,
    error::OTelSdkResult,
    trace::{SdkTracer, SdkTracerProvider, SpanData, SpanExporter},
};
use tokio::sync::mpsc;

use crate::request_traces::{RequestTrace, otlp};

/// A tracer whose finished request spans arrive as decoded request traces.
pub(crate) fn request_spans() -> (SdkTracer, mpsc::UnboundedReceiver<RequestTrace>) {
    let (sender, receiver) = mpsc::unbounded_channel();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(Requests(sender))
        .build();
    (provider.tracer("test"), receiver)
}

#[derive(Debug)]
struct Requests(mpsc::UnboundedSender<RequestTrace>);

impl SpanExporter for Requests {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        let resource = ResourceAttributesWithSchema::from(&Resource::builder_empty().build());
        let mut resource_spans = group_spans_by_resource_and_scope(batch, &resource);
        for spans in &mut resource_spans {
            otlp::Identity {
                organization_id: None,
                project_id: "default".into(),
                actor_name: "counter".into(),
                actor_id: "one".into(),
                host_id: "host".into(),
                session_id: "session".into(),
            }
            .stamp(&mut spans.resource);
        }
        for event in otlp::trace_events(ExportTraceServiceRequest { resource_spans }) {
            let _ = self.0.send(event.trace);
        }
        Ok(())
    }
}
