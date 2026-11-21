use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use axum::{
    Router,
    body::Body,
    extract::{FromRequestParts, MatchedPath, RawPathParams, Request},
    http::{HeaderMap, Method, header, request::Parts},
    middleware::{self, Next},
    response::Response,
};
use http_body_util::BodyExt;

use super::{
    mcp::ORGANIZATION_HEADER,
    request_outcome::{Failure, Fault, log_by_fault, millis, nanos},
};

pub(super) fn logged(router: Router) -> Router {
    router.layer(middleware::from_fn(log_request))
}

async fn log_request(request: Request, next: Next) -> Response {
    if request.uri().path() == "/healthz" {
        return next.run(request).await;
    }
    let (mut parts, body) = request.into_parts();
    let log = HttpRequest::start(&mut parts).await;
    let (body, bytes_read) = counted(body);
    let response = next.run(Request::from_parts(parts, body)).await;
    log.finish(&response, bytes_read.load(Ordering::Relaxed));
    response
}

fn counted(body: Body) -> (Body, Arc<AtomicU64>) {
    let bytes_read = Arc::new(AtomicU64::new(0));
    let counter = bytes_read.clone();
    let body = body.map_frame(move |frame| {
        if let Some(data) = frame.data_ref() {
            counter.fetch_add(data.len() as u64, Ordering::Relaxed);
        }
        frame
    });
    (Body::new(body), bytes_read)
}

struct HttpRequest {
    started: Instant,
    method: Method,
    route: Option<String>,
    path: String,
    user_agent: Option<String>,
    request_id: Option<String>,
    organization_id: Option<String>,
    project_id: Option<String>,
    actor_name: Option<String>,
    actor_id: Option<String>,
}

impl HttpRequest {
    async fn start(parts: &mut Parts) -> Self {
        let params = RawPathParams::from_request_parts(parts, &()).await.ok();
        let param = |name: &str| {
            let (_, value) = params.as_ref()?.iter().find(|(key, _)| *key == name)?;
            Some(value.to_owned())
        };
        Self {
            started: Instant::now(),
            method: parts.method.clone(),
            route: parts
                .extensions
                .get::<MatchedPath>()
                .map(|route| route.as_str().to_owned()),
            path: parts.uri.path().to_owned(),
            user_agent: header_value(&parts.headers, header::USER_AGENT.as_str()),
            request_id: header_value(&parts.headers, "x-request-id"),
            organization_id: param("organization_id")
                .or_else(|| header_value(&parts.headers, ORGANIZATION_HEADER)),
            project_id: param("project_id"),
            actor_name: param("actor_name"),
            actor_id: param("actor_id"),
        }
    }

    fn finish(self, response: &Response, bytes_read: u64) {
        let elapsed = self.started.elapsed();
        let status = response.status();
        let failure = response.extensions().get::<Failure>();
        let fault = Fault::of_response(status, failure);
        let error_code = failure.map(|failure| failure.code.as_str());
        let error_message = failure
            .and_then(|failure| failure.message.as_deref())
            .filter(|_| fault == Fault::Server);
        log_by_fault!(
            fault,
            event = "http_request",
            http.method = %self.method,
            http.route = self.route,
            http.url_details.path = self.path,
            http.status_code = status.as_u16(),
            http.useragent = self.user_agent,
            network.bytes_read = bytes_read,
            duration = nanos(elapsed),
            duration_ms = millis(elapsed),
            outcome = fault.outcome(),
            fault = fault.as_str(),
            error_code,
            error.message = error_message,
            request_id = self.request_id,
            organization_id = self.organization_id,
            project_id = self.project_id,
            actor_name = self.actor_name,
            actor_id = self.actor_id,
            "{} {} {} {:.1}ms",
            self.method,
            self.path,
            status.as_u16(),
            millis(elapsed)
        );
    }
}

fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/request_log.rs"]
mod tests;
