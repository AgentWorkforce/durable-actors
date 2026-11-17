//! Sandboxes that stopped under an actor without the platform asking, reported by the pool manager
//! from outside them so an actor that runs out of memory can't lose its own record.

use opentelemetry::logs::AnyValue;
use serde::{Deserialize, Serialize};

use super::{PlatformEvent, PlatformRecord};
use crate::{actor::ActorKey, request_traces::otlp};

pub(crate) const KILLED: &str = "durable_actors.sandbox.killed";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum KillReason {
    /// The actor used more memory than its limit.
    OutOfMemory,
    /// The actor used more disk than its limit.
    OutOfDisk,
    /// The node reclaimed the sandbox for something the node itself was short of.
    Evicted,
    /// SIGKILL that the platform didn't ask for.
    Killed,
}

impl KillReason {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::OutOfMemory => "out_of_memory",
            Self::OutOfDisk => "out_of_disk",
            Self::Evicted => "evicted",
            Self::Killed => "killed",
        }
    }
}

/// A sandbox that stopped under an actor without the platform asking it to.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SandboxKill {
    pub actor: ActorKey,
    pub sandbox_id: String,
    pub reason: KillReason,
    pub at_ms: u64,
}

impl PlatformEvent for SandboxKill {
    fn project_id(&self) -> &str {
        &self.actor.project_id
    }

    fn to_record(&self) -> PlatformRecord {
        PlatformRecord {
            name: KILLED,
            at_ms: self.at_ms,
            attributes: vec![
                (
                    otlp::PROJECT_ID,
                    AnyValue::from(self.actor.project_id.clone()),
                ),
                (
                    otlp::ACTOR_NAME,
                    AnyValue::from(self.actor.actor_name.clone()),
                ),
                (otlp::ACTOR_ID, AnyValue::from(self.actor.actor_id.clone())),
                (otlp::SANDBOX_ID, AnyValue::from(self.sandbox_id.clone())),
                (otlp::KILL_REASON, AnyValue::from(self.reason.as_str())),
            ],
            body: None,
        }
    }
}
