//! One actor's resource usage over a time range: the CPU and memory of the sandboxes it ran in,
//! and the size of its SQLite database.

use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};

use crate::sandbox::ResourceLimits;

const MAX_BUCKETS: u64 = 1_000;

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ResourceQuery {
    pub actor_name: String,
    pub actor_id: String,
    pub from_ms: u64,
    pub to_ms: u64,
    pub bucket_ms: u64,
}

impl ResourceQuery {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(self.from_ms < self.to_ms, "fromMs must be before toMs");
        ensure!(
            self.bucket_ms > 0
                && (self.to_ms - self.from_ms).div_ceil(self.bucket_ms) <= MAX_BUCKETS,
            "bucketMs must split the range into 1 to {MAX_BUCKETS} buckets"
        );
        super::history::HistoryQuery {
            actor_name: Some(self.actor_name.clone()),
            actor_id: Some(self.actor_id.clone()),
            from_ms: Some(self.from_ms),
            to_ms: Some(self.to_ms),
            ..Default::default()
        }
        .validate()
    }
}

/// Each bucket holds the highest sample within it and is named by when it starts.
#[derive(Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ResourceUsage {
    /// The resources in force over the range: the sandbox running at its start, then each later one.
    /// A sandbox keeps its resources for its whole life.
    pub sandboxes: Vec<SandboxResources>,
    pub usage: Vec<UsageBucket>,
    pub database: Vec<DatabaseBucket>,
    /// The latest reported size, however long ago it was reported.
    pub database_bytes: Option<u64>,
}

#[derive(Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SandboxResources {
    pub claimed_at_ms: u64,
    #[serde(flatten)]
    pub resources: ResourceLimits,
}

#[derive(Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UsageBucket {
    pub at_ms: u64,
    pub cpu_cores: Option<f64>,
    pub memory_bytes: Option<u64>,
}

#[derive(Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DatabaseBucket {
    pub at_ms: u64,
    pub bytes: u64,
}
