use std::time::Instant;

use axum::http::request::Parts;
use rmcp::{
    ErrorData, RoleServer,
    model::{CallToolRequestParams, CallToolResponse, CallToolResult, ErrorCode},
    service::RequestContext,
};
use serde_json::Value;

use super::Organization;
use crate::control_plane::request_outcome::{Fault, log_by_fault, millis, nanos};

/// Names what a tool call touched and its payload sizes, never its contents: they carry
/// application data.
pub(super) struct ToolCall {
    started: Instant,
    tool: String,
    request_bytes: usize,
    request_id: Option<String>,
    organization_id: Option<String>,
    project_id: Option<String>,
    actor_name: Option<String>,
    actor_id: Option<String>,
    actor_method: Option<String>,
}

impl ToolCall {
    pub(super) fn start(
        request: &CallToolRequestParams,
        context: &RequestContext<RoleServer>,
    ) -> Self {
        let argument = |name: &str| {
            let value = request.arguments.as_ref()?.get(name)?.as_str()?;
            Some(value.to_owned())
        };
        let parts = context.extensions.get::<Parts>();
        Self {
            started: Instant::now(),
            tool: request.name.to_string(),
            request_bytes: request
                .arguments
                .as_ref()
                .and_then(|arguments| serde_json::to_vec(arguments).ok())
                .map_or(0, |bytes| bytes.len()),
            request_id: parts.and_then(request_id),
            organization_id: parts.and_then(organization),
            project_id: argument("projectId"),
            actor_name: argument("actorName"),
            actor_id: argument("actorId"),
            actor_method: argument("method"),
        }
    }

    pub(super) fn finish(self, response: &Result<CallToolResponse, ErrorData>) {
        let elapsed = self.started.elapsed();
        let failure = Failure::of(response);
        let fault = failure
            .as_ref()
            .map_or(Fault::None, |failure| failure.fault);
        let error_code = failure.as_ref().map(|failure| failure.code.as_str());
        let error_message = failure
            .as_ref()
            .and_then(|failure| failure.message)
            .filter(|_| fault == Fault::Server);
        log_by_fault!(
            fault,
            event = "mcp_tool_call",
            mcp.tool_name = self.tool,
            mcp.request_bytes = self.request_bytes,
            mcp.response_bytes = response_bytes(response),
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
            actor_method = self.actor_method,
            "mcp tool call completed"
        );
    }
}

struct Failure<'a> {
    code: String,
    fault: Fault,
    message: Option<&'a str>,
}

impl<'a> Failure<'a> {
    fn of(response: &'a Result<CallToolResponse, ErrorData>) -> Option<Self> {
        match response {
            Ok(CallToolResponse::Complete(result)) if result.is_error == Some(true) => {
                let code = tool_error_code(result).unwrap_or_else(|| "tool_error".into());
                Some(Self {
                    fault: Fault::of_code(&code),
                    code,
                    message: None,
                })
            }
            Ok(_) => None,
            Err(error) => Some(Self::protocol(error)),
        }
    }

    fn protocol(error: &'a ErrorData) -> Self {
        let (code, fault) = match error.code {
            ErrorCode::INTERNAL_ERROR => ("internal", Fault::Server),
            ErrorCode::INVALID_PARAMS => ("invalid_params", Fault::Client),
            ErrorCode::METHOD_NOT_FOUND => ("method_not_found", Fault::Client),
            _ => ("protocol_error", Fault::Client),
        };
        Self {
            code: code.into(),
            fault,
            message: Some(error.message.as_ref()),
        }
    }
}

/// Tool errors carry the API's error document, or an invocation view with the same `error`.
fn tool_error_code(result: &CallToolResult) -> Option<String> {
    let text = &result.content.first()?.as_text()?.text;
    let body: Value = serde_json::from_str(text).ok()?;
    body["error"]["code"].as_str().map(str::to_owned)
}

fn response_bytes(response: &Result<CallToolResponse, ErrorData>) -> usize {
    let Ok(CallToolResponse::Complete(result)) = response else {
        return 0;
    };
    result
        .content
        .iter()
        .filter_map(|content| content.as_text())
        .map(|content| content.text.len())
        .sum()
}

fn request_id(parts: &Parts) -> Option<String> {
    let value = parts.headers.get("x-request-id")?.to_str().ok()?;
    Some(value.to_owned())
}

fn organization(parts: &Parts) -> Option<String> {
    let Organization(organization) = parts.extensions.get::<Organization>()?;
    Some(organization.clone())
}
