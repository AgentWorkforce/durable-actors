//! Feedback a developer sends the Terse team through the MCP server.

use opentelemetry::logs::AnyValue;

use super::{PlatformEvent, PlatformRecord};
use crate::request_traces::otlp;

pub(crate) const SUBMITTED: &str = "durable_actors.feedback.submitted";

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Feedback {
    pub organization_id: String,
    pub project_id: String,
    pub message: String,
    pub at_ms: u64,
}

impl PlatformEvent for Feedback {
    fn project_id(&self) -> &str {
        &self.project_id
    }

    fn to_record(&self) -> PlatformRecord {
        PlatformRecord {
            name: SUBMITTED,
            at_ms: self.at_ms,
            attributes: vec![
                (
                    otlp::ORGANIZATION_ID,
                    AnyValue::from(self.organization_id.clone()),
                ),
                (otlp::PROJECT_ID, AnyValue::from(self.project_id.clone())),
            ],
            body: Some(self.message.clone()),
        }
    }
}
