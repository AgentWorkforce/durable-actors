use std::sync::Arc;

use opentelemetry_proto::tonic::collector::{
    logs::v1::{
        ExportLogsServiceRequest, ExportLogsServiceResponse,
        logs_service_server::{LogsService, LogsServiceServer},
    },
    trace::v1::{
        ExportTraceServiceRequest, ExportTraceServiceResponse,
        trace_service_server::{TraceService, TraceServiceServer},
    },
};
use prost::Message;
use tonic::{Request, Response, Status};

use super::ActorJwtVerifier;
use crate::request_traces::{
    live::InProcessFeed,
    otlp::{self, Identity, Signal},
    persistence::sqlite::SqliteTracePersistence,
};

/// The local stand-in for the OpenTelemetry Collector: it authenticates hosts the same way,
/// replaces payload identity with verified claims, and stores into SQLite.
#[derive(Clone)]
pub(super) struct LocalTelemetrySink {
    auth: ActorJwtVerifier,
    store: Arc<SqliteTracePersistence>,
    live: InProcessFeed,
}

impl LocalTelemetrySink {
    pub(super) fn new(
        auth: ActorJwtVerifier,
        store: Arc<SqliteTracePersistence>,
        live: InProcessFeed,
    ) -> Self {
        Self { auth, store, live }
    }

    pub(super) fn add_to(self, routes: tonic::service::Routes) -> tonic::service::Routes {
        routes
            .add_service(TraceServiceServer::new(self.clone()))
            .add_service(LogsServiceServer::new(self))
    }

    async fn identity<T>(&self, request: &Request<T>) -> Result<Identity, Status> {
        let principal = self.auth.authenticate(request).await?;
        Ok(Identity {
            organization_id: principal.organization_id,
            project_id: principal.actor.project_id,
            actor_name: principal.actor.actor_name,
            actor_id: principal.actor.actor_id,
            host_id: principal.host_id.as_str().to_owned(),
            session_id: principal.session_id,
        })
    }
}

#[tonic::async_trait]
impl TraceService for LocalTelemetrySink {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let identity = self.identity(&request).await?;
        let mut request = request.into_inner();
        for spans in &mut request.resource_spans {
            identity.stamp(&mut spans.resource);
        }
        self.store
            .append(&otlp::trace_events(request.clone()))
            .await
            .map_err(|error| Status::internal(format!("{error:#}")))?;
        self.live.publish(
            otlp::live_channel(&identity.project_id, Signal::Requests),
            request.encode_to_vec(),
        );
        Ok(Response::new(ExportTraceServiceResponse::default()))
    }
}

#[tonic::async_trait]
impl LogsService for LocalTelemetrySink {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let identity = self.identity(&request).await?;
        let mut request = request.into_inner();
        for logs in &mut request.resource_logs {
            identity.stamp(&mut logs.resource);
        }
        self.store
            .append_logs(otlp::actor_logs(request.clone()))
            .await
            .map_err(|error| Status::internal(format!("{error:#}")))?;
        self.live.publish(
            otlp::live_channel(&identity.project_id, Signal::Logs),
            request.encode_to_vec(),
        );
        Ok(Response::new(ExportLogsServiceResponse::default()))
    }
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/local_telemetry.rs"]
mod tests;
