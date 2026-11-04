use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;

use super::{
    remote::{RemoteSandboxProvider, SandboxCluster},
    secrets::SecretResolver,
    *,
};

mod kubernetes;
pub(crate) use super::remote::validate_image;
pub(crate) use kubernetes::{GkeConfig, PodScheduling};

pub(crate) async fn provider(
    config: GkeConfig,
    secrets: Arc<dyn SecretResolver>,
    connections: connections::HostConnections,
) -> Result<RemoteSandboxProvider> {
    let cluster = kubernetes::Kubernetes::new(kube::Client::try_default().await?, config);
    RemoteSandboxProvider::new("gke", Arc::new(cluster), secrets, connections)
}
