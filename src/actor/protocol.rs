use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{actor::ActorSocketEffect, actor_state::ActorStorageKey};

const MAX_ACTOR_NAME_BYTES: usize = 48;
const MAX_ACTOR_ID_BYTES: usize = 128;
const MAX_METHOD_BYTES: usize = 128;

/// The identity of one actor instance.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActorKey {
    pub project_id: String,
    pub actor_name: String,
    pub actor_id: String,
}

impl ActorKey {
    pub fn validate(&self) -> Result<()> {
        validate_component("project ID", &self.project_id, 64)?;
        validate_component("actor name", &self.actor_name, MAX_ACTOR_NAME_BYTES)?;
        validate_component("actor ID", &self.actor_id, MAX_ACTOR_ID_BYTES)?;
        self.storage_key().validate()
    }

    /// Stable, readable identity used for coordination records.
    pub fn storage_key(&self) -> ActorStorageKey {
        ActorStorageKey::new(format!(
            "object.v4.{}:{}:{}",
            self.project_id, self.actor_name, self.actor_id
        ))
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ActorInvocation {
    /// Correlation ID for this caller attempt. It is not an idempotency key.
    pub request_id: String,
    pub actor: ActorKey,
    pub method: String,
    pub args: Vec<Value>,
}

impl ActorInvocation {
    pub fn validate(&self) -> Result<()> {
        ensure!(!self.request_id.is_empty(), "request ID must not be empty");
        ensure!(
            self.request_id.len() <= 255,
            "request ID must be at most 255 bytes"
        );
        self.actor.validate()?;
        validate_component("actor method", &self.method, MAX_METHOD_BYTES)?;
        Ok(())
    }
}

/// An invocation an actor makes of another actor in its own project.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActorCall {
    pub request_id: String,
    pub actor_name: String,
    pub actor_id: String,
    pub method: String,
    pub args: Vec<Value>,
}

impl ActorCall {
    pub fn in_project(self, project_id: &str) -> ActorInvocation {
        ActorInvocation {
            request_id: self.request_id,
            actor: ActorKey {
                project_id: project_id.to_owned(),
                actor_name: self.actor_name,
                actor_id: self.actor_id,
            },
            method: self.method,
            args: self.args,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorInvocationFailure {
    pub code: String,
    pub message: String,
}

impl ActorInvocationFailure {
    pub(crate) fn outcome_unknown_after_execution() -> Self {
        Self {
            code: "outcome_unknown".into(),
            message: "actor execution completed, but its durable publication outcome could not be confirmed".into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum ActorExecutionResult {
    Completed {
        result: Value,
        effects: Vec<ActorSocketEffect>,
    },
    Failed {
        failure: ActorInvocationFailure,
    },
    HostUnavailable,
}

fn validate_component(name: &str, value: &str, max_bytes: usize) -> Result<()> {
    ensure!(!value.is_empty(), "{name} must not be empty");
    ensure!(
        value.len() <= max_bytes,
        "{name} must be at most {max_bytes} bytes"
    );
    ensure!(
        value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')),
        "{name} may contain only ASCII letters, digits, '.', '-', and '_'"
    );
    Ok(())
}

#[cfg(test)]
#[path = "../../tests/unit/actor/protocol.rs"]
mod tests;
