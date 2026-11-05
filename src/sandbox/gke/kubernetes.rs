use std::collections::BTreeMap;

use futures_util::StreamExt;
use k8s_openapi::api::core::v1::Pod;
use kube::{
    Api, Client, ResourceExt,
    api::{DeleteParams, PostParams, Preconditions},
    runtime::{
        WatchStreamExt,
        reflector::{self, ObjectRef, Store, store::Writer},
        watcher,
    },
};
use serde_json::json;
use tokio::sync::watch;
use tokio_util::task::AbortOnDropHandle;

use super::*;
use crate::sandbox::kube_retry::retry_throttled;

const MANAGED: &str = "app.kubernetes.io/managed-by=terse";

#[derive(Clone, Debug)]
pub(crate) struct GkeConfig {
    pub namespace: String,
    pub zones: BTreeMap<String, Vec<String>>,
    pub reap_concurrency: usize,
}

/// Reads pod state from one shared watch so provisioning costs a single API call per pod.
pub(super) struct Kubernetes {
    pods: Api<Pod>,
    observed: Store<Pod>,
    changes: watch::Receiver<()>,
    config: GkeConfig,
    _watch: AbortOnDropHandle<()>,
}
impl Kubernetes {
    pub fn new(client: Client, config: GkeConfig) -> Self {
        let pods = Api::namespaced(client, &config.namespace);
        let (observed, writer) = reflector::store();
        let (changed, changes) = watch::channel(());
        let watch = AbortOnDropHandle::new(tokio::spawn(observe(pods.clone(), writer, changed)));
        Self {
            pods,
            observed,
            changes,
            config,
            _watch: watch,
        }
    }

    async fn start(&self, pod: Pod) -> Result<Arc<Pod>> {
        let params = PostParams::default();
        let created = retry_throttled(|| self.pods.create(&params, &pod)).await?;
        let mut cleanup = PodCleanup::new(self.pods.clone(), &created)?;
        let ready = tokio::time::timeout(Duration::from_secs(120), self.settled(&created))
            .await
            .context("sandbox pod readiness timed out")??;
        ensure!(is_ready(&ready), "sandbox pod failed: {:?}", ready.status);
        cleanup.disarm();
        Ok(ready)
    }

    async fn settled(&self, created: &Pod) -> Result<Arc<Pod>> {
        let mut changes = self.changes.clone();
        let mut seen = false;
        loop {
            match self.observed_pod(&created.name_any(), created.uid().as_deref()) {
                Some(pod) if terminal_or_ready(&pod) => return Ok(pod),
                Some(_) => seen = true,
                None => ensure!(!seen, "sandbox pod disappeared"),
            }
            changes
                .changed()
                .await
                .context("sandbox pod watch stopped")?;
        }
    }

    async fn is_live(&self, name: &str, uid: &str) -> Result<bool> {
        let pod = match self.observed_pod(name, Some(uid)) {
            Some(pod) => Some(pod),
            // The watch can trail a pod another controller just registered.
            None => retry_throttled(|| self.pods.get_opt(name))
                .await?
                .map(Arc::new),
        };
        Ok(pod.is_some_and(|pod| pod.uid().as_deref() == Some(uid) && !is_terminal(&pod)))
    }

    fn observed_pod(&self, name: &str, uid: Option<&str>) -> Option<Arc<Pod>> {
        self.observed
            .get(&ObjectRef::new(name).within(&self.config.namespace))
            .filter(|pod| pod.uid().as_deref() == uid)
    }

    fn zones(&self, region: &str) -> Result<&[String]> {
        self.config
            .zones
            .get(region)
            .map(Vec::as_slice)
            .with_context(|| format!("no GKE zone configured for {region}"))
    }
}

#[async_trait]
impl SandboxCluster for Kubernetes {
    async fn create_spare(&self, request: &CreateSpareRequest) -> Result<SpareHandle> {
        let token = format!(
            "{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        let pod = spare_pod(request, self.zones(&request.canonical_region)?, &token)?;
        let ready = self.start(pod).await?;
        let ip: std::net::IpAddr = ready
            .status
            .as_ref()
            .and_then(|status| status.pod_ip.as_deref())
            .context("ready pod has no IP")?
            .parse()?;
        let authority = if ip.is_ipv6() {
            format!("[{ip}]")
        } else {
            ip.to_string()
        };
        Ok(SpareHandle {
            name: ready.name_any(),
            resource_id: format!(
                "{}/{}/{}",
                self.config.namespace,
                ready.name_any(),
                ready.uid().context("pod UID missing")?
            ),
            control_route: format!("http://{authority}:7102"),
            control_token: token,
            route: format!("http://{authority}:7101"),
            canonical_region: request.canonical_region.clone(),
        })
    }

    async fn retire_spare(&self, spare: &SpareHandle) -> Result<()> {
        if spare.resource_id.is_empty() {
            let Some(pod) = retry_throttled(|| self.pods.get_opt(&spare.name)).await? else {
                return Ok(());
            };
            ensure!(
                pod.labels()
                    .get("app.kubernetes.io/managed-by")
                    .is_some_and(|owner| owner == "terse"),
                "refusing to retire an unmanaged pod"
            );
            return delete_pod(
                &self.pods,
                &spare.name,
                &pod.uid().context("pod UID missing")?,
            )
            .await;
        }
        let (namespace, name, uid) = resource_identity(&spare.resource_id)?;
        ensure!(
            namespace == self.config.namespace && name == spare.name,
            "sandbox resource identity mismatch"
        );
        delete_pod(&self.pods, name, uid).await
    }

    async fn stopped_spares(&self, spares: &[SpareHandle]) -> Result<Vec<String>> {
        self.observed.wait_until_ready().await?;
        let mut stopped = Vec::new();
        for spare in spares {
            let (namespace, name, uid) = resource_identity(&spare.resource_id)?;
            ensure!(
                namespace == self.config.namespace && name == spare.name,
                "sandbox resource identity mismatch"
            );
            if !self.is_live(name, uid).await? {
                stopped.push(spare.resource_id.clone());
            }
        }
        Ok(stopped)
    }

    // Each finished pod costs one DELETE; finding them in the watched copy makes no API calls.
    async fn reap_finished(&self) -> Result<()> {
        let finished = self
            .observed
            .state()
            .into_iter()
            .filter(|pod| is_terminal(pod) && pod.metadata.deletion_timestamp.is_none());
        futures_util::stream::iter(finished)
            .for_each_concurrent(self.config.reap_concurrency, |pod| async move {
                let deleted = match pod.uid() {
                    Some(uid) => delete_pod(&self.pods, &pod.name_any(), &uid).await,
                    None => Err(anyhow::anyhow!("pod UID missing")),
                };
                if let Err(error) = deleted {
                    tracing::warn!(%error, name = %pod.name_any(), "finished pod cleanup failed");
                }
            })
            .await;
        Ok(())
    }
}

async fn observe(pods: Api<Pod>, writer: Writer<Pod>, changed: watch::Sender<()>) {
    watcher(pods, watcher::Config::default().labels(MANAGED))
        .default_backoff()
        .modify(|pod| {
            pod.managed_fields_mut().clear();
            pod.spec = None;
        })
        .reflect(writer)
        .for_each(|event| {
            match event {
                Ok(_) => {
                    changed.send_replace(());
                }
                Err(error) => tracing::warn!(%error, "sandbox pod watch failed"),
            }
            std::future::ready(())
        })
        .await;
}

fn spare_pod(request: &CreateSpareRequest, zones: &[String], token: &str) -> Result<Pod> {
    let mut pod = base_pod(&request.name, &request.image_ref, zones, &request.resources);
    pod["metadata"]["annotations"] =
        json!({"cluster-autoscaler.kubernetes.io/safe-to-evict": "true"});
    let mut env = json!([
        {"name":"DURABLE_ACTORS_PROCESS_ROLE", "value":"spare"},
        {"name":"DURABLE_ACTORS_SPARE_TOKEN", "value":token},
        {"name":"DURABLE_ACTORS_CONTROL_PLANE_URL", "value":request.control_plane_url.as_ref().context("spare control plane URL required")?}
    ]);
    if let Some(endpoint) = &request.otlp_endpoint {
        env.as_array_mut()
            .expect("env list")
            .push(json!({"name":"OTEL_EXPORTER_OTLP_ENDPOINT", "value":endpoint}));
    }
    pod["spec"]["containers"][0]["env"] = env;
    pod["spec"]["containers"][0]["readinessProbe"] = json!({"exec":{"command":["/usr/bin/test", "-f", "/tmp/durable-actors-spare-ready"]}, "periodSeconds":1, "failureThreshold":3});
    Ok(serde_json::from_value(pod)?)
}

fn base_pod(
    name: &str,
    image: &str,
    zones: &[String],
    resources: &ResourceLimits,
) -> serde_json::Value {
    json!({
        "apiVersion":"v1", "kind":"Pod", "metadata":{"name":name, "labels":{"app.kubernetes.io/managed-by":"terse", "terse.ai/purpose":"actor"}},
        "spec":{
            "runtimeClassName":"gvisor", "automountServiceAccountToken":false, "serviceAccountName":"sandbox",
            "restartPolicy":"Never", "terminationGracePeriodSeconds":45,
            "nodeSelector":{"sandbox.gke.io/runtime":"gvisor"},
            "affinity":{"nodeAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":{"nodeSelectorTerms":[{"matchExpressions":[{"key":"topology.kubernetes.io/zone","operator":"In","values":zones}]}]}}},
            "topologySpreadConstraints":[{"maxSkew":1,"topologyKey":"topology.kubernetes.io/zone","whenUnsatisfiable":"ScheduleAnyway","labelSelector":{"matchLabels":{"terse.ai/purpose":"actor"}}}],
            "securityContext":{"runAsNonRoot":true, "runAsUser":10000, "runAsGroup":10000, "fsGroup":10000},
            "containers":[{"name":"runtime", "image":image, "imagePullPolicy":"IfNotPresent", "command":["/usr/local/bin/durable-actors"],
                "securityContext":{"allowPrivilegeEscalation":false, "readOnlyRootFilesystem":true, "capabilities":{"drop":["ALL"]}},
                "resources":container_resources(resources),
                "volumeMounts":[{"name":"tmp","mountPath":"/tmp"},{"name":"customer","mountPath":"/customer"}]
            }], "volumes":[{"name":"tmp","emptyDir":{"sizeLimit":"7Gi"}},{"name":"customer","emptyDir":{"sizeLimit":"1Gi"}}]
        }
    })
}

fn container_resources(resources: &ResourceLimits) -> serde_json::Value {
    let mut limits = json!({"ephemeral-storage":"8Gi"});
    if let Some(cpu) = resources.cpu_limit_millis {
        limits["cpu"] = json!(format!("{cpu}m"));
    }
    if let Some(memory) = resources.memory_limit_mib {
        limits["memory"] = json!(format!("{memory}Mi"));
    }
    json!({
        "requests":{"ephemeral-storage":"256Mi","cpu":format!("{}m", resources.cpu_millis),"memory":format!("{}Mi", resources.memory_mib)},
        "limits":limits
    })
}

fn terminal_or_ready(pod: &Pod) -> bool {
    is_ready(pod) || is_terminal(pod)
}
fn is_terminal(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|status| status.phase.as_deref())
        .is_some_and(|phase| matches!(phase, "Failed" | "Succeeded"))
}
fn is_ready(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|status| status.conditions.as_ref())
        .is_some_and(|conditions| {
            conditions
                .iter()
                .any(|condition| condition.type_ == "Ready" && condition.status == "True")
        })
}
fn resource_identity(resource: &str) -> Result<(&str, &str, &str)> {
    let mut parts = resource.split('/');
    let namespace = parts.next().context("namespace missing")?;
    let name = parts.next().context("pod name missing")?;
    let uid = parts.next().context("pod UID missing")?;
    ensure!(
        !namespace.is_empty() && !name.is_empty() && !uid.is_empty() && parts.next().is_none(),
        "invalid pod identity"
    );
    Ok((namespace, name, uid))
}
async fn delete_pod(pods: &Api<Pod>, name: &str, uid: &str) -> Result<()> {
    let params = DeleteParams {
        preconditions: Some(Preconditions {
            uid: Some(uid.into()),
            resource_version: None,
        }),
        ..DeleteParams::default()
    };
    match retry_throttled(|| pods.delete(name, &params)).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(error)) if error.code == 404 => Ok(()),
        Err(error) => Err(error.into()),
    }
}
struct PodCleanup {
    pods: Api<Pod>,
    identity: Option<(String, String)>,
}
impl PodCleanup {
    fn new(pods: Api<Pod>, pod: &Pod) -> Result<Self> {
        Ok(Self {
            pods,
            identity: Some((pod.name_any(), pod.uid().context("pod UID missing")?)),
        })
    }
    fn disarm(&mut self) {
        self.identity = None;
    }
}
impl Drop for PodCleanup {
    fn drop(&mut self) {
        if let Some((name, uid)) = self.identity.take() {
            let pods = self.pods.clone();
            tokio::spawn(async move {
                if let Err(error) = delete_pod(&pods, &name, &uid).await {
                    tracing::warn!(%error, %name, "sandbox cleanup failed");
                }
            });
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/sandbox/kubernetes.rs"]
mod tests;
