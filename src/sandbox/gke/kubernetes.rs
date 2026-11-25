use std::collections::BTreeMap;

use futures_util::StreamExt;
use k8s_openapi::{
    api::core::v1::{
        Container, EmptyDirVolumeSource, EnvVar, NodeSelectorRequirement, Pod, PodSpec, Volume,
        VolumeMount,
    },
    apimachinery::pkg::api::resource::Quantity,
};
use kube::{
    Api, Client, ResourceExt,
    api::{DeleteParams, Patch, PatchParams, PostParams, Preconditions},
    runtime::{
        WatchStreamExt,
        reflector::{self, ObjectRef, Store, store::Writer},
        watcher,
    },
};
use tokio::sync::watch;
use tokio_util::task::AbortOnDropHandle;

use super::{kills::KillReports, *};
use crate::sandbox::kube_retry::retry_throttled;

const MANAGED: &str = "app.kubernetes.io/managed-by=terse";
/// The RuntimeClass GKE provides for gVisor.
pub(crate) const GKE_RUNTIME_CLASS: &str = "gvisor";
/// The volume the node cache moves an actor's database in and out of from outside the sandbox.
const SLOT_VOLUME: &str = "slot";
/// Names a spare's pool, one image at one size. Only unclaimed spares carry it.
const SPARE_POOL: &str = "terse.ai/spare-pool";

#[derive(Clone, Debug)]
pub(crate) struct GkeConfig {
    pub namespace: String,
    pub zones: BTreeMap<String, Vec<String>>,
    pub reap_concurrency: usize,
    pub node_cache_url: Option<String>,
    /// The size of each spare's database slot, which kubelet enforces and deletes with the pod.
    pub sqlite_slot_gib: Option<u32>,
    pub runtime_class: String,
    /// Added to every spare pod, for cluster tooling such as log agents.
    pub pod_annotations: BTreeMap<String, String>,
}

/// Reads pod state from one shared watch so provisioning costs a single API call per pod.
pub(super) struct Kubernetes {
    pods: Api<Pod>,
    observed: Store<Pod>,
    changes: watch::Receiver<()>,
    config: GkeConfig,
    kills: Option<Arc<KillReports>>,
    _watch: AbortOnDropHandle<()>,
}
impl Kubernetes {
    pub fn new(client: Client, config: GkeConfig, kills: Option<KillReporting>) -> Self {
        let pods = Api::namespaced(client, &config.namespace);
        let (observed, writer) = reflector::store();
        let (changed, changes) = watch::channel(());
        let kills = kills.map(|reporting| Arc::new(KillReports::new(observed.clone(), reporting)));
        let watch = AbortOnDropHandle::new(tokio::spawn(observe(
            pods.clone(),
            writer,
            changed,
            kills.clone(),
        )));
        Self {
            pods,
            observed,
            changes,
            config,
            kills,
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
        let pod = spare_pod(
            request,
            self.zones(&request.canonical_region)?,
            &token,
            self.config.node_cache_url.as_deref(),
            self.config.sqlite_slot_gib,
            &self.config.runtime_class,
            &self.config.pod_annotations,
        )?;
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
            node: ready
                .spec
                .as_ref()
                .and_then(|spec| spec.node_name.clone())
                .unwrap_or_default(),
        })
    }

    fn claimed(&self, spare: &SpareHandle) {
        let (pods, name) = (self.pods.clone(), spare.name.clone());
        tokio::spawn(async move {
            if let Err(error) = leave_pool(&pods, &name).await {
                tracing::warn!(%error, %name, "claimed sandbox still counts as a spare");
            }
        });
    }

    async fn retire_spare(&self, spare: &SpareHandle) -> Result<()> {
        if let Some(kills) = &self.kills {
            kills.keep_claim(&spare.name).await;
        }
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

async fn observe(
    pods: Api<Pod>,
    writer: Writer<Pod>,
    changed: watch::Sender<()>,
    kills: Option<Arc<KillReports>>,
) {
    let kills = kills.as_deref();
    watcher(pods, watcher::Config::default().labels(MANAGED))
        .default_backoff()
        .modify(|pod| {
            pod.managed_fields_mut().clear();
            pod.spec = pod.spec.take().map(|spec| PodSpec {
                node_name: spec.node_name,
                ..PodSpec::default()
            });
        })
        // Ahead of the store, as `KillReports::report` needs.
        .then(|event| async move {
            if let (Some(kills), Ok(watcher::Event::Apply(pod) | watcher::Event::InitApply(pod))) =
                (kills, &event)
            {
                kills.report(pod).await;
            }
            event
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

/// The spare sandbox manifest, kept as YAML so its isolation settings read like any other.
const SPARE_POD: &str = include_str!("spare-pod.yaml");

fn spare_pod(
    request: &CreateSpareRequest,
    zones: &[String],
    token: &str,
    node_cache_url: Option<&str>,
    sqlite_slot_gib: Option<u32>,
    runtime_class: &str,
    annotations: &BTreeMap<String, String>,
) -> Result<Pod> {
    let mut pod: Pod = serde_yaml::from_str(SPARE_POD).context("spare pod manifest")?;
    pod.metadata.name = Some(request.name.clone());
    if !annotations.is_empty() {
        pod.metadata
            .annotations
            .get_or_insert_with(BTreeMap::new)
            .extend(annotations.clone());
    }
    pod.metadata
        .labels
        .get_or_insert_with(BTreeMap::new)
        .insert(
            SPARE_POOL.into(),
            crate::sandbox::spare_pool_id(&request.image_ref, &request.resources)?,
        );
    let spec = pod
        .spec
        .as_mut()
        .context("spare pod manifest has no spec")?;
    zone_requirement(spec)?.values = Some(zones.to_vec());
    for container in spec
        .init_containers
        .iter_mut()
        .flatten()
        .chain(&mut spec.containers)
    {
        container.image = Some(request.image_ref.clone());
        add_compute(container, &request.resources);
    }
    spec.containers[0]
        .env
        .get_or_insert_with(Vec::new)
        .extend(spare_env(request, token, node_cache_url)?);
    let slot = node_cache_url.and(sqlite_slot_gib);
    if let Some(gib) = slot {
        add_slot(spec, gib);
    }
    if runtime_class == GKE_RUNTIME_CLASS {
        if slot.is_some() {
            pod.metadata
                .annotations
                .get_or_insert_with(BTreeMap::new)
                .insert(
                    format!("dev.gvisor.empty-dir.{SLOT_VOLUME}.force-shared"),
                    "true".into(),
                );
        }
    } else {
        spec.runtime_class_name = Some(runtime_class.to_owned());
        let mounts = mount_annotations(spec);
        pod.metadata
            .annotations
            .get_or_insert_with(BTreeMap::new)
            .extend(mounts);
    }
    Ok(pod)
}

/// The scratch volume the actor's database lives in, which the node cache finds by the pod's UID
/// in the host token. Kubelet caps it and deletes it with the pod, along with any database the
/// host never parked.
fn add_slot(spec: &mut PodSpec, gib: u32) {
    spec.volumes.get_or_insert_with(Vec::new).push(Volume {
        name: SLOT_VOLUME.into(),
        empty_dir: Some(EmptyDirVolumeSource {
            size_limit: Some(Quantity(format!("{gib}Gi"))),
            ..Default::default()
        }),
        ..Default::default()
    });
    spec.containers[0]
        .volume_mounts
        .get_or_insert_with(Vec::new)
        .push(VolumeMount {
            name: SLOT_VOLUME.into(),
            mount_path: "/slot".into(),
            ..Default::default()
        });
}

fn zone_requirement(spec: &mut PodSpec) -> Result<&mut NodeSelectorRequirement> {
    spec.affinity
        .as_mut()
        .and_then(|affinity| affinity.node_affinity.as_mut())
        .and_then(|node| {
            node.required_during_scheduling_ignored_during_execution
                .as_mut()
        })
        .and_then(|required| required.node_selector_terms.first_mut())
        .and_then(|term| term.match_expressions.as_mut())
        .and_then(|requirements| requirements.first_mut())
        .context("spare pod manifest has no zone affinity")
}

fn add_compute(container: &mut Container, resources: &ResourceLimits) {
    let requirements = container.resources.get_or_insert_with(Default::default);
    let requests = requirements.requests.get_or_insert_with(Default::default);
    requests.insert("cpu".into(), Quantity(format!("{}m", resources.cpu_millis)));
    requests.insert(
        "memory".into(),
        Quantity(format!("{}Mi", resources.memory_mib)),
    );
    let limits = requirements.limits.get_or_insert_with(Default::default);
    if let Some(cpu) = resources.cpu_limit_millis {
        limits.insert("cpu".into(), Quantity(format!("{cpu}m")));
    }
    if let Some(memory) = resources.memory_limit_mib {
        limits.insert("memory".into(), Quantity(format!("{memory}Mi")));
    }
}

fn spare_env(
    request: &CreateSpareRequest,
    token: &str,
    node_cache_url: Option<&str>,
) -> Result<Vec<EnvVar>> {
    let control_plane = request
        .control_plane_url
        .as_deref()
        .context("spare control plane URL required")?;
    let mut env = vec![
        env_var("DURABLE_ACTORS_SPARE_TOKEN", token),
        env_var("DURABLE_ACTORS_CONTROL_PLANE_URL", control_plane),
    ];
    if let Some(endpoint) = &request.otlp_endpoint {
        env.push(env_var("OTEL_EXPORTER_OTLP_ENDPOINT", endpoint));
    }
    if let Some(url) = node_cache_url {
        env.push(env_var("DURABLE_ACTORS_NODE_CACHE_URL", url));
    }
    Ok(env)
}

fn env_var(name: &str, value: &str) -> EnvVar {
    EnvVar {
        name: name.into(),
        value: Some(value.into()),
        ..Default::default()
    }
}

/// GKE tells gVisor how to mount a pod's volumes, but only for pods under its own class. A pod
/// under another class has to say the same itself, or its volumes are mounted straight from the
/// node's disk.
fn mount_annotations(spec: &PodSpec) -> BTreeMap<String, String> {
    spec.volumes
        .iter()
        .flatten()
        .filter(|volume| volume.empty_dir.is_some())
        .flat_map(|volume| {
            let share = match mounting_containers(spec, &volume.name) {
                _ if volume.name == SLOT_VOLUME => "shared",
                1 => "container",
                _ => "pod",
            };
            [
                ("type", "bind"),
                ("share", share),
                ("options", "rw,rprivate"),
            ]
            .map(|(hint, value)| {
                (
                    format!("dev.gvisor.spec.mount.{}.{hint}", volume.name),
                    value.to_owned(),
                )
            })
        })
        .collect()
}

fn mounting_containers(spec: &PodSpec, volume: &str) -> usize {
    spec.init_containers
        .iter()
        .flatten()
        .chain(&spec.containers)
        .filter(|container| {
            container
                .volume_mounts
                .iter()
                .flatten()
                .any(|mount| mount.name == volume)
        })
        .count()
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
async fn leave_pool(pods: &Api<Pod>, name: &str) -> Result<()> {
    let patch = Patch::Merge(serde_json::json!({"metadata": {"labels": {SPARE_POOL: null}}}));
    let params = PatchParams::default();
    match retry_throttled(|| pods.patch(name, &params, &patch)).await {
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
