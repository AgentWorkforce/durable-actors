use std::sync::Mutex;

use serde_json::{Value, json};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use super::*;
use crate::{
    actor::ActorKey,
    platform_events::{
        NoPlatformEvents,
        feedback::Feedback,
        sandboxes::{KillReason, SandboxKill},
    },
};

#[tokio::test]
async fn a_recorded_kill_is_published_as_observers_see_it_under_a_stable_id() -> Result<()> {
    let (sender, mut published) = mpsc::unbounded_channel();
    let recorded = Arc::new(Recorded::default());
    let events = PublishedEvents::new(recorded.clone(), Arc::new(Sent(sender)));

    let first = publish(&events, &mut published, "sandbox-1").await?;
    let repeated = publish(&events, &mut published, "sandbox-1").await?;
    let another = publish(&events, &mut published, "sandbox-2").await?;

    assert_eq!(
        first,
        json!({
            "eventId": first["eventId"],
            "projectId": "project",
            "type": "kill",
            "item": {
                "actorName": "Room",
                "actorId": "one",
                "atMs": 1_700_000_000_000_u64,
                "reason": "out_of_memory",
            },
        })
    );
    assert_eq!(first["eventId"].as_str().map(str::len), Some(64));
    assert_eq!(repeated, first);
    assert_ne!(another["eventId"], first["eventId"]);
    assert_eq!(recorded.0.lock().unwrap().len(), 3);
    Ok(())
}

#[tokio::test]
async fn feedback_is_published_with_its_organization() -> Result<()> {
    let (sender, mut published) = mpsc::unbounded_channel();
    let events = PublishedEvents::new(Arc::new(NoPlatformEvents), Arc::new(Sent(sender)));

    events.record(Feedback {
        organization_id: "org".into(),
        project_id: "project".into(),
        message: "Deploys are slow".into(),
        at_ms: 1_700_000_000_000,
    });

    let event = published.recv().await.context("published event")?;
    assert_eq!(
        event,
        json!({
            "eventId": event["eventId"],
            "projectId": "project",
            "type": "feedback",
            "item": {
                "organizationId": "org",
                "atMs": 1_700_000_000_000_u64,
                "message": "Deploys are slow",
            },
        })
    );
    Ok(())
}

async fn publish(
    events: &PublishedEvents<SandboxKill>,
    published: &mut UnboundedReceiver<Value>,
    sandbox: &str,
) -> Result<Value> {
    events.record(SandboxKill {
        actor: ActorKey {
            project_id: "project".into(),
            actor_name: "Room".into(),
            actor_id: "one".into(),
        },
        sandbox_id: sandbox.into(),
        reason: KillReason::OutOfMemory,
        at_ms: 1_700_000_000_000,
    });
    published.recv().await.context("published event")
}

#[derive(Default)]
struct Recorded(Mutex<Vec<SandboxKill>>);

impl PlatformEvents<SandboxKill> for Recorded {
    fn record(&self, event: SandboxKill) {
        self.0.lock().unwrap().push(event);
    }
}

struct Sent(UnboundedSender<Value>);

#[async_trait]
impl EventTopic for Sent {
    async fn publish(&self, event: &TopicEvent) -> Result<()> {
        self.0.send(serde_json::to_value(event)?)?;
        Ok(())
    }
}
