//! Reports pods that were killed while they ran an actor. A stopped pod cannot say which actor
//! it ran; the claim that handed it to the actor does.

use std::{sync::Arc, time::Duration};

use k8s_openapi::{
    api::core::v1::{ContainerStateTerminated, Pod, PodStatus},
    apimachinery::pkg::apis::meta::v1::Time,
};
use kube::{
    ResourceExt,
    runtime::reflector::{ObjectRef, Store},
};
use moka::future::Cache;

use super::KillReporting;
use crate::{
    actor::ActorKey,
    clock::{Clock, SystemClock},
    platform_events::{
        PlatformEvents,
        sandboxes::{KillReason, SandboxKill},
    },
    sandbox::pool::claims::ClaimedActors,
};

/// Provisioning waits on the pod watch, which waits on each claim read.
const CLAIM_READ_TIMEOUT: Duration = Duration::from_secs(2);
/// Longer than a deleted pod takes to show how it stopped.
const RETIRED_CLAIM_TTL: Duration = Duration::from_secs(300);
const RETIRED_CLAIMS: u64 = 10_000;

/// Reports each sandbox's kill once, under the actor that claimed the sandbox. A killed pod
/// shows its kill on every later update and again when the watch relists.
pub(super) struct KillReports {
    observed: Store<Pod>,
    claims: Arc<dyn ClaimedActors>,
    /// The pool forgets a claim once its sandbox is retired, which can be before the watch
    /// shows how the sandbox stopped.
    retired: Cache<String, ActorKey>,
    events: Arc<dyn PlatformEvents<SandboxKill>>,
}

impl KillReports {
    pub fn new(observed: Store<Pod>, reporting: KillReporting) -> Self {
        Self {
            observed,
            claims: reporting.claims,
            retired: Cache::builder()
                .max_capacity(RETIRED_CLAIMS)
                .time_to_live(RETIRED_CLAIM_TTL)
                .build(),
            events: reporting.events,
        }
    }

    /// Runs before `observed` learns of `pod`: it still holds the pod as it was, and the pool
    /// keeps the claim of every sandbox it shows running.
    pub async fn report(&self, pod: &Pod) {
        let Some(kill) = kill(pod) else {
            return;
        };
        if self.reported(pod, &kill) {
            return;
        }
        if let Some(actor) = self.claimed_actor(&pod.name_any()).await {
            self.events.record(kill.under(actor));
        }
    }

    /// Runs before `sandbox` is retired, while the pool still holds its claim.
    pub async fn keep_claim(&self, sandbox: &str) {
        if let Some(actor) = self.read_claim(sandbox).await {
            self.retired.insert(sandbox.to_owned(), actor).await;
        }
    }

    fn reported(&self, pod: &Pod, kill: &PodKill) -> bool {
        self.observed
            .get(&ObjectRef::from_obj(pod))
            .and_then(|before| self::kill(&before))
            .is_some_and(|before| before.sandbox_id == kill.sandbox_id)
    }

    async fn claimed_actor(&self, sandbox: &str) -> Option<ActorKey> {
        match self.retired.get(sandbox).await {
            Some(actor) => Some(actor),
            None => self.read_claim(sandbox).await,
        }
    }

    async fn read_claim(&self, sandbox: &str) -> Option<ActorKey> {
        match tokio::time::timeout(CLAIM_READ_TIMEOUT, self.claims.actor_of(sandbox)).await {
            Ok(Ok(actor)) => actor,
            Ok(Err(error)) => {
                tracing::warn!(error = %format!("{error:#}"), sandbox, "sandbox claim read failed");
                None
            }
            Err(_) => {
                tracing::warn!(sandbox, "sandbox claim read timed out");
                None
            }
        }
    }
}

/// How a pod's status shows it was killed.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct PodKill {
    pub sandbox_id: String,
    pub reason: KillReason,
    pub at_ms: u64,
}

impl PodKill {
    fn under(self, actor: ActorKey) -> SandboxKill {
        SandboxKill {
            actor,
            sandbox_id: self.sandbox_id,
            reason: self.reason,
            at_ms: self.at_ms,
        }
    }
}

pub(super) fn kill(pod: &Pod) -> Option<PodKill> {
    let status = pod.status.as_ref()?;
    let terminated = terminated(status);
    Some(PodKill {
        sandbox_id: sandbox_id(pod)?,
        reason: reason(pod, status, terminated)?,
        at_ms: stopped_at(status, terminated)?,
    })
}

fn sandbox_id(pod: &Pod) -> Option<String> {
    Some(format!(
        "{}/{}/{}",
        pod.namespace()?,
        pod.name_any(),
        pod.uid()?
    ))
}

/// Pods we delete end with SIGKILL too, so a plain 137 only counts when nothing asked the pod to
/// stop. An out-of-memory kill counts even then: retiring a pod whose assignment just failed can
/// beat the kubelet's report of why it failed.
fn reason(
    pod: &Pod,
    status: &PodStatus,
    terminated: Option<&ContainerStateTerminated>,
) -> Option<KillReason> {
    if status.reason.as_deref() == Some("Evicted") {
        Some(eviction(status.message.as_deref().unwrap_or_default()))
    } else if terminated.is_some_and(|state| state.reason.as_deref() == Some("OOMKilled")) {
        Some(KillReason::OutOfMemory)
    } else if terminated.is_some_and(|state| state.exit_code == 137)
        && pod.metadata.deletion_timestamp.is_none()
    {
        Some(KillReason::Killed)
    } else {
        None
    }
}

/// What the kubelet says when it evicts a pod for passing one of the pod's own disk limits: a
/// container's, the pod's total, or a volume's. Any other eviction is the node reclaiming what
/// the node is short of.
const DISK_LIMITS: [&str; 3] = [
    "exceeded its local ephemeral storage limit",
    "ephemeral local storage usage exceeds the total limit",
    "Usage of EmptyDir volume",
];

fn eviction(message: &str) -> KillReason {
    if DISK_LIMITS.iter().any(|limit| message.contains(limit)) {
        KillReason::OutOfDisk
    } else {
        KillReason::Evicted
    }
}

fn terminated(status: &PodStatus) -> Option<&ContainerStateTerminated> {
    status
        .container_statuses
        .iter()
        .flatten()
        .find_map(|container| container.state.as_ref()?.terminated.as_ref())
}

/// Evicted pods have no finished container. Their last condition change marks the stop and stays
/// the same when the watch replays the pod, so a replay doesn't move the kill.
fn stopped_at(status: &PodStatus, terminated: Option<&ContainerStateTerminated>) -> Option<u64> {
    terminated
        .and_then(|state| state.finished_at.as_ref())
        .or_else(|| last_transition(status))
        .and_then(|time| u64::try_from(time.0.timestamp_millis()).ok())
        .or_else(|| SystemClock.now_ms().ok())
}

fn last_transition(status: &PodStatus) -> Option<&Time> {
    status
        .conditions
        .iter()
        .flatten()
        .filter_map(|condition| condition.last_transition_time.as_ref())
        .max_by_key(|time| time.0)
}

#[cfg(test)]
#[path = "../../../tests/unit/sandbox/kills.rs"]
mod tests;
