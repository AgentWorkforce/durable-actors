#[cfg(test)]
#[path = "../tests/support/placement.rs"]
pub(crate) mod testing;

use anyhow::{Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{actor_state::ActorStorageKey, host::HostId};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectPlacement {
    pub lease: crate::host_leases::HostLease,
    pub object: ActorStorageKey,
    pub owner: HostId,
    pub owner_epoch: u64,
    pub home_region: String,
    pub state_version: u64,
    pub state_object: Option<String>,
    pub last_request_id: Option<String>,
}

#[async_trait]
pub trait ObjectPlacementStore: Send + Sync {
    async fn get_owner(&self, object: &ActorStorageKey) -> Result<Option<ObjectPlacement>> {
        Ok(self.get_owner_with_hint(object).await?.0)
    }

    async fn get_owner_with_hint(
        &self,
        object: &ActorStorageKey,
    ) -> Result<(
        Option<ObjectPlacement>,
        Option<crate::bucket::OwnershipHint>,
    )>;

    async fn get(&self, object: &ActorStorageKey) -> Result<Option<ObjectPlacement>>;
}

pub fn validate_region(region: &str) -> Result<()> {
    ensure!(
        !region.is_empty()
            && region.len() <= 64
            && region.bytes().all(|byte| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || matches!(byte, b'.' | b'_' | b'-')
            }),
        "sandbox region is invalid"
    );
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ActorResidency {
    Live,
    Dormant,
    Unknown,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorInstanceOverview {
    pub actor_id: String,
    pub status: ActorResidency,
    pub last_active_at_ms: Option<u64>,
    /// When the control plane received the instance's latest report.
    pub reported_at_ms: Option<u64>,
    /// When a live or dormant status turns unknown unless another report arrives; stopped
    /// instances stay dormant.
    pub stale_at_ms: Option<u64>,
    pub connections: Vec<ActorConnectionInventory>,
    pub waiting: Option<Vec<crate::host_leases::WaitingOperation>>,
}

#[derive(Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorConnectionInventory {
    pub id: String,
    pub gateway_id: String,
    pub metadata: serde_json::Value,
}

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorInventory {
    pub actor_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub methods: Option<Vec<String>>,
    pub live: u64,
    pub dormant: u64,
    pub unknown: u64,
    pub instances: Vec<ActorInstanceOverview>,
}

#[async_trait]
pub trait ActorInventoryReader: Send + Sync {
    async fn actor_inventory(&self, project_id: &str) -> Result<ActorInventorySnapshot>;

    async fn residency_history(
        &self,
        _project_id: &str,
        _query: &ResidencyQuery,
    ) -> Result<ResidencyHistory> {
        Ok(ResidencyHistory::default())
    }
}

/// One actor class's instances, or one instance when `actor_id` is set, over `[from_ms, to_ms]`.
#[derive(Clone, Debug)]
pub struct ResidencyQuery {
    pub actor_name: String,
    pub actor_id: Option<String>,
    pub from_ms: u64,
    pub to_ms: u64,
}

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResidencyHistory {
    pub periods: Vec<ResidencyPeriod>,
    pub truncated: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResidencyPeriod {
    pub actor_id: String,
    pub status: ActorResidency,
    pub from_ms: u64,
    pub to_ms: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorInventorySnapshot {
    pub actors: Vec<ActorInventory>,
    pub connections_complete: bool,
    /// When each gateway holding an open connection last showed it was alive.
    pub gateways: std::collections::BTreeMap<String, u64>,
}
