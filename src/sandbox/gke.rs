use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;

use super::{
    pool::claims::ClaimedActors,
    remote::{RemoteSandboxProvider, SandboxCluster},
    secrets::SecretResolver,
    *,
};
use crate::platform_events::{PlatformEvents, sandboxes::SandboxKill};

mod kills;
mod kubernetes;
pub(crate) use super::remote::validate_image;
pub(crate) use kubernetes::{GkeConfig, PodScheduling};

/// What reporting sandbox kills takes: the actor each sandbox was claimed for, and where its
/// kill is recorded.
pub(crate) struct KillReporting {
    pub claims: Arc<dyn ClaimedActors>,
    pub events: Arc<dyn PlatformEvents<SandboxKill>>,
}

pub(crate) async fn provider(
    config: GkeConfig,
    secrets: Arc<dyn SecretResolver>,
    connections: connections::HostConnections,
    kills: Option<KillReporting>,
) -> Result<RemoteSandboxProvider> {
    let cluster = kubernetes::Kubernetes::new(kube::Client::try_default().await?, config, kills);
    RemoteSandboxProvider::new("gke", Arc::new(cluster), secrets, connections)
}
