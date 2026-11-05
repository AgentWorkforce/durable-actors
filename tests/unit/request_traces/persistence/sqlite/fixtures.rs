use crate::request_traces::{RequestKind, RequestOutcome, RequestTrace, TraceEvent};
use crate::request_tracking::HostState;

pub(super) fn event(id: &str) -> TraceEvent {
    TraceEvent {
        event_id: id.into(),
        host_id: "host".into(),
        session_id: "session".into(),
        trace: RequestTrace {
            state_version: None,
            project_id: "default".into(),
            request_id: "same-request".into(),
            actor_name: "Counter".into(),
            actor_id: "one".into(),
            kind: RequestKind::Method,
            operation: "increment".into(),
            connection_id: None,
            started_at_ms: 1,
            duration_ms: 1.0,
            queue_wait_ms: None,
            host_state: HostState::Warm,
            routing_ms: 25.0,
            outcome: RequestOutcome::Completed,
            metadata: None,
        },
    }
}
