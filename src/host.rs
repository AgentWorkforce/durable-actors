mod actor_database;
mod actor_host;
mod assignment;
mod config;
pub(crate) mod http;
mod lease_maintenance;
mod lifecycle;
mod persistence;
mod process;
pub(crate) mod sockets;
mod spare;
pub(crate) mod storage;
mod telemetry;

use serde::{Deserialize, Serialize};
use std::fmt;

pub(crate) use self::config::host_idle_timeout_ms;
pub use self::process::serve_actor_host;
pub use self::spare::serve_spare;
pub(crate) use self::{
    actor_database::HostDatabase,
    actor_host::ActorHost,
    lease_maintenance::{HostLeaseMaintainer, LeaseRenewalTask},
};

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct HostId(String);

impl HostId {
    pub fn new<S>(id: S) -> Self
    where
        S: Into<String>,
    {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for HostId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HostEndpoint {
    pub id: HostId,
    pub route: String,
}

pub(super) fn protect_runtime_credentials() -> anyhow::Result<()> {
    // Customer code shares the pod but must not inspect the Rust process's credentials.
    #[cfg(target_os = "linux")]
    nix::sys::prctl::set_dumpable(false)?;
    Ok(())
}

#[cfg(all(test, target_os = "linux"))]
#[path = "../tests/unit/host/credentials.rs"]
mod credentials_tests;
