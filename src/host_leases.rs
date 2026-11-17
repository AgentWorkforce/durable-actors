use crate::{actor::ActorKey, host::HostId};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

pub const MAX_HOST_LEASE_DURATION_MS: u64 = 60_000;

pub const LOCAL_HOST_LEASE_VARIABLE: &str = "DURABLE_ACTORS_LOCAL_HOST_LEASE_MS";

/// The lease a local runtime gives its hosts, so that a host held at a debugger breakpoint, which
/// cannot renew, keeps its actor. The runtime passes the variable on to its hosts.
pub fn local_host_lease_ms() -> Result<Option<u64>> {
    std::env::var(LOCAL_HOST_LEASE_VARIABLE)
        .ok()
        .map(|milliseconds| {
            milliseconds.parse().with_context(|| {
                format!("{LOCAL_HOST_LEASE_VARIABLE} must be a number of milliseconds")
            })
        })
        .transpose()
}

pub fn max_host_lease_duration_ms() -> Result<u64> {
    Ok(
        local_host_lease_ms()?.map_or(MAX_HOST_LEASE_DURATION_MS, |lease| {
            lease.max(MAX_HOST_LEASE_DURATION_MS)
        }),
    )
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostLease {
    pub id: HostId,
    pub session_id: String,
    pub route: String,
    pub expires_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostLeaseRequest {
    pub id: HostId,
    pub session_id: String,
    pub route: String,
    pub duration_ms: u64,
}

impl HostLeaseRequest {
    pub fn validate_duration(&self) -> Result<()> {
        ensure!(self.duration_ms > 0, "host lease duration must be positive");
        let limit = max_host_lease_duration_ms()?;
        ensure!(
            self.duration_ms <= limit,
            "host lease duration must not exceed {limit}ms"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ActorSocketInventory {
    pub actor: ActorKey,
    pub connections: Vec<crate::actor::ActorSocketConnection>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WaitingOperation {
    pub id: String,
    pub operation: String,
}

#[async_trait]
pub trait HostLeaseRegistry: Send + Sync {
    async fn register(&self, request: &HostLeaseRequest) -> Result<HostLease>;
    async fn unregister(&self, id: &HostId, session_id: &str) -> Result<()>;
}

/// Ends the lease of a host that can no longer give it up.
#[async_trait]
pub trait AbandonedLeases: Send + Sync {
    async fn release(&self, actor: &ActorKey, host: &HostId, session_id: &str) -> Result<()>;
}
