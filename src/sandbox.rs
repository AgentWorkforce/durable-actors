use std::ops::RangeInclusive;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::host::HostId;

pub(crate) mod assignment;
pub(crate) mod connections;
pub(crate) mod gke;
mod kube_retry;
mod local;
mod local_store;
pub(crate) mod modal;
pub(crate) mod pool;
mod remote;
pub(crate) mod secrets;

pub(crate) use local::LocalSandboxProvider;

/// Kubernetes-style resources: `cpu_millis` and `memory_mib` are requests; an unset limit
/// leaves that resource uncapped.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceLimits {
    pub cpu_millis: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_limit_millis: Option<u32>,
    pub memory_mib: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_limit_mib: Option<u32>,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            cpu_millis: 250,
            cpu_limit_millis: None,
            memory_mib: 256,
            memory_limit_mib: Some(256),
        }
    }
}

/// Values a deployment may request through `@Compute`. Operator defaults are not held to them.
#[derive(Clone, Debug)]
pub struct ResourceBounds {
    pub cpu_millis: RangeInclusive<u32>,
    pub memory_mib: RangeInclusive<u32>,
}

impl Default for ResourceBounds {
    fn default() -> Self {
        Self {
            cpu_millis: 125..=4000,
            memory_mib: 256..=4096,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SpareHandle {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub control_route: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub control_token: String,
    pub name: String,
    pub resource_id: String,
    pub route: String,
    pub canonical_region: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SpareKind {
    Actor,
}

impl SpareKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Actor => "actor",
        }
    }
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateSpareRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub control_plane_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub otlp_endpoint: Option<String>,
    pub kind: SpareKind,
    pub name: String,
    pub image_ref: String,
    pub canonical_region: String,
    pub resources: ResourceLimits,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EnsureHostRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) initial_operation: Option<assignment::InitialOperation>,
    pub actor_is_new: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner_hint: Option<String>,
    pub actor: Option<crate::actor::ActorKey>,
    pub code_snapshot: Option<String>,
    pub spare: Option<SpareHandle>,
    pub resources: ResourceLimits,
    pub runtime_config: Option<String>,

    pub host_config_key: String,
    pub canonical_region: String,
    pub host_id: HostId,
    pub session_id: String,
    pub host_token: String,
    pub jwt_public_keys: String,
    pub control_plane_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub otlp_endpoint: Option<String>,
    pub jwt_issuer: String,
    pub invocation_jwt_audience: String,
    pub image_ref: String,
    pub working_directory: String,
    pub actor_entrypoint: Option<String>,
    pub secret_refs: Vec<String>,
    pub host_idle_timeout_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorHostHandle {
    #[serde(default)]
    pub(crate) initial_reply: Option<serde_json::Value>,
    pub lease: Option<crate::host_leases::HostLease>,
    #[serde(default)]
    pub owner_epoch: u64,
    pub host_id: HostId,
    pub route: String,
    pub canonical_region: String,
    pub provisioning: Option<ActorHostProvisioning>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorHostProvisioning {
    pub provider: String,
    pub resource_id: String,
    pub reused: bool,
    pub started_at_ms: u64,
    pub input_parsed_at_ms: Option<u64>,
    pub sdk_loaded_at_ms: Option<u64>,
    pub resources_resolved_at_ms: Option<u64>,
    pub sandbox_scheduled_at_ms: Option<u64>,
    pub host_ready_observed_at_ms: Option<u64>,
    pub route_read_at_ms: Option<u64>,
    pub completed_at_ms: u64,
    #[serde(default)]
    pub command_spawned_at_ms: Option<u64>,
    #[serde(default)]
    pub request_written_at_ms: Option<u64>,
    #[serde(default)]
    pub process_completed_at_ms: Option<u64>,
    #[serde(default)]
    pub response_decoded_at_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TerminateHostsRequest {
    pub host_config_key: String,
    pub canonical_regions: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HostTermination {
    pub provider: String,
    pub resource_ids: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct HostNotReady;

impl std::fmt::Display for HostNotReady {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("actor host is not ready")
    }
}

impl std::error::Error for HostNotReady {}

#[async_trait]
pub trait SandboxProvider: Send + Sync {
    async fn prewarm_spares(&self, _spares: &[SpareHandle]) {}

    async fn wait_ready(&self, _host: &HostId) -> Result<()> {
        Ok(())
    }

    async fn create_spare(&self, _request: &CreateSpareRequest) -> Result<SpareHandle> {
        anyhow::bail!("provider does not support generic spares")
    }
    async fn retire_spare(&self, _request: &SpareHandle) -> Result<()> {
        anyhow::bail!("provider does not support generic spares")
    }
    async fn stopped_spares(&self, spares: &[SpareHandle]) -> Result<Vec<String>>;
    /// Deletes finished sandboxes; providers whose sandboxes disappear on exit need nothing.
    async fn reap_finished(&self) -> Result<()> {
        Ok(())
    }

    async fn ensure_host(&self, request: &EnsureHostRequest) -> Result<ActorHostHandle>;
    async fn terminate_hosts(&self, request: &TerminateHostsRequest) -> Result<HostTermination>;
}

#[derive(Clone)]
pub struct HostSandboxRuntimeConfig {
    pub control_plane_url: String,
    pub otlp_endpoint: Option<String>,
    pub jwt_issuer: String,
    pub invocation_jwt_audience: String,
    pub host_idle_timeout_ms: u64,
}

#[cfg(test)]
#[path = "../tests/support/sandbox.rs"]
pub(crate) mod testing;
