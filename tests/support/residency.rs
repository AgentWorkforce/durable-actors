use super::*;

/// Reports from up to three instances of `Room`, with handoffs, clocks that go backwards, gaps longer
/// than a report lasts, stops, and every `resident` value.
pub(crate) fn random_events(seed: u64, project: &str) -> Vec<LifecycleEvent> {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut random = |bound: u64| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state % bound
    };
    let mut events = Vec::new();
    for actor in 0..=random(3) {
        let mut at = 100_000 + random(60_000);
        for epoch in 1..=random(3) + 1 {
            for sequence in 1..=random(12) + 1 {
                at = (at + random(50_000)).saturating_sub(30_000 * u64::from(random(10) == 0));
                let kind = match (sequence, random(5)) {
                    (1, _) => LifecycleKind::Started,
                    (_, 0) => LifecycleKind::Stopped,
                    _ => LifecycleKind::Heartbeat,
                };
                let resident = [Some(true), Some(false), None][random(3) as usize];
                events.push(report(project, actor, epoch, sequence, at, kind, resident));
            }
        }
    }
    events
}

pub(crate) fn report(
    project: &str,
    actor: u64,
    epoch: u64,
    sequence: u64,
    occurred_at_ms: u64,
    kind: LifecycleKind,
    resident: Option<bool>,
) -> LifecycleEvent {
    LifecycleEvent {
        organization_id: None,
        event_id: format!("{actor}:{epoch}:{sequence}"),
        actor: ActorKey {
            project_id: project.into(),
            actor_name: "Room".into(),
            actor_id: format!("room-{actor}"),
        },
        host_id: format!("host-{epoch}"),
        session_id: format!("session-{epoch}"),
        region: "us-east".into(),
        received_at_ms: occurred_at_ms,
        report: LifecycleReport {
            kind,
            owner_epoch: epoch,
            sequence,
            started_at_ms: 0,
            occurred_at_ms,
            resources: Default::default(),
            resident,
            last_active_at_ms: None,
            database_bytes: None,
            waiting: Vec::new(),
        },
    }
}
