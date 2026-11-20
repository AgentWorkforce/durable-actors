use std::time::Duration;

use axum::http::StatusCode;

/// Why a request failed, attached to its response for the request log.
#[derive(Clone)]
pub(super) struct Failure {
    pub(super) code: String,
    /// Platform text only: an actor's own failure message is application data.
    pub(super) message: Option<String>,
}

/// Who caused a failure, so monitors can alert on server faults alone.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Fault {
    None,
    Client,
    Server,
}

impl Fault {
    /// A successful response can still carry an actor's failure (invoke follows Lambda's
    /// convention), so its code decides the fault.
    pub(super) fn of_response(status: StatusCode, failure: Option<&Failure>) -> Self {
        if status.is_server_error() {
            Self::Server
        } else if status.is_client_error() {
            Self::Client
        } else {
            failure.map_or(Self::None, |failure| Self::of_code(&failure.code))
        }
    }

    pub(super) fn of_code(code: &str) -> Self {
        match code {
            "internal"
            | "unavailable"
            | "outcome_unknown"
            | "host_unavailable"
            | "host_unauthenticated"
            | "upstream_not_reached" => Self::Server,
            _ => Self::Client,
        }
    }

    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Client => "client",
            Self::Server => "server",
        }
    }

    pub(super) fn outcome(self) -> &'static str {
        match self {
            Self::None => "completed",
            Self::Client | Self::Server => "failed",
        }
    }
}

/// tracing fixes an event's level at its callsite, so each level needs its own call.
macro_rules! log_by_fault {
    ($fault:expr, $($fields:tt)+) => {
        if $fault == $crate::control_plane::request_outcome::Fault::Server {
            tracing::error!($($fields)+)
        } else {
            tracing::info!($($fields)+)
        }
    };
}
pub(super) use log_by_fault;

pub(super) fn nanos(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX)
}

pub(super) fn millis(elapsed: Duration) -> f64 {
    elapsed.as_secs_f64() * 1_000.0
}
