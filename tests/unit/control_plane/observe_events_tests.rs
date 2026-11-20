use super::*;

fn usage(actor_id: &str, at_ms: u64) -> ObserveEvent {
    ObserveEvent::Usage(ActorUsage {
        actor_name: "Room".into(),
        actor_id: actor_id.into(),
        at_ms,
        cpu_cores: None,
        memory_bytes: None,
    })
}

fn alive() -> ObserveEvent {
    ObserveEvent::GatewayAlive(GatewayAlive {
        gateway_id: "gateway".into(),
        at_ms: 1,
    })
}

#[test]
fn windows_keep_each_actors_latest_snapshot_where_it_arrived() {
    assert_eq!(
        latest_snapshots(vec![usage("a", 1), usage("b", 1), alive(), usage("a", 2)]),
        [usage("b", 1), alive(), usage("a", 2)]
    );
}
