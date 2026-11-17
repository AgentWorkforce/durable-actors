use serde_json::json;

use super::*;

#[test]
fn a_stopped_container_is_a_kill_unless_the_platform_stopped_it() {
    let deleting = |reason, exit_code| {
        let mut pod = stopped_json(reason, exit_code);
        pod["metadata"]["deletionTimestamp"] = json!("2026-01-01T00:00:05Z");
        parse(pod)
    };
    assert_eq!(
        kill(&stopped("OOMKilled", 137)).unwrap(),
        PodKill {
            sandbox_id: "sandboxes/actor-pod/actor-uid".into(),
            reason: KillReason::OutOfMemory,
            at_ms: 1_767_225_600_000,
        }
    );
    let reason = |pod: Pod| kill(&pod).map(|kill| kill.reason);
    assert_eq!(
        reason(deleting("OOMKilled", 137)),
        Some(KillReason::OutOfMemory)
    );
    assert_eq!(reason(stopped("Error", 137)), Some(KillReason::Killed));
    assert_eq!(reason(deleting("Error", 137)), None);
    assert_eq!(reason(stopped("Completed", 0)), None);
}

/// The messages are the kubelet's own, from `pkg/kubelet/eviction`.
#[test]
fn an_eviction_past_the_pods_own_disk_limit_is_out_of_disk() {
    let mut pod = pod_json(json!({
        "phase": "Failed",
        "reason": "Evicted",
        "conditions": [
            {"type": "Ready", "status": "False", "lastTransitionTime": "2026-01-01T00:00:03Z"},
            {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2025-12-31T23:00:00Z"},
        ],
    }));
    for (message, reason) in [
        (
            "Usage of EmptyDir volume \"tmp\" exceeds the limit \"7Gi\". ",
            KillReason::OutOfDisk,
        ),
        (
            "Container runtime exceeded its local ephemeral storage limit \"8Gi\". ",
            KillReason::OutOfDisk,
        ),
        (
            "Pod ephemeral local storage usage exceeds the total limit of containers 8Gi. ",
            KillReason::OutOfDisk,
        ),
        (
            "The node was low on resource: ephemeral-storage. Threshold quantity: 10Gi, available: 9Gi. ",
            KillReason::Evicted,
        ),
    ] {
        pod["status"]["message"] = json!(message);
        let evicted = kill(&parse(pod.clone())).unwrap();
        assert_eq!(evicted.reason, reason, "{message}");
        assert_eq!(evicted.at_ms, 1_767_225_603_000);
    }
    pod["status"].as_object_mut().unwrap().remove("message");
    assert_eq!(kill(&parse(pod)).unwrap().reason, KillReason::Evicted);
}

struct HungClaims;

#[async_trait::async_trait]
impl ClaimedActors for HungClaims {
    async fn actor_of(&self, _: &str) -> anyhow::Result<Option<ActorKey>> {
        std::future::pending().await
    }
}

#[tokio::test(start_paused = true)]
async fn a_claim_read_that_hangs_is_given_up_so_the_watch_moves_on() {
    let reports = KillReports::new(
        kube::runtime::reflector::store().0,
        KillReporting {
            claims: Arc::new(HungClaims),
            events: Arc::new(crate::platform_events::NoPlatformEvents),
        },
    );
    tokio::time::timeout(
        CLAIM_READ_TIMEOUT * 2,
        reports.report(&stopped("OOMKilled", 137)),
    )
    .await
    .expect("the report gave up on the claim");
}

fn stopped(reason: &str, exit_code: i32) -> Pod {
    parse(stopped_json(reason, exit_code))
}

fn stopped_json(reason: &str, exit_code: i32) -> serde_json::Value {
    pod_json(json!({
        "phase": "Failed",
        "containerStatuses": [{
            "name": "runtime",
            "image": "runtime",
            "imageID": "",
            "ready": false,
            "restartCount": 0,
            "state": {"terminated": {
                "exitCode": exit_code,
                "reason": reason,
                "finishedAt": "2026-01-01T00:00:00Z",
            }},
        }],
    }))
}

fn pod_json(status: serde_json::Value) -> serde_json::Value {
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "actor-pod",
            "namespace": "sandboxes",
            "uid": "actor-uid",
        },
        "status": status,
    })
}

fn parse(pod: serde_json::Value) -> Pod {
    serde_json::from_value(pod).unwrap()
}
