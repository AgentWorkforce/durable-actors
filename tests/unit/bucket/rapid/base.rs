use super::*;

const SNAPSHOT: &str = "durable-actors/v3/snapshots/63/cHJvamVjdA/Q291bnRlcg/aWQ/000000000000000000000000000000a1/42.json";

fn reference(object: &str, state_version: u64) -> SnapshotRef {
    SnapshotRef {
        object: object.into(),
        state_version,
        request_id: "request".into(),
        digest: "digest".into(),
    }
}

#[test]
fn a_base_name_records_its_epoch_version_transaction_and_size() {
    let key = key(&reference(SNAPSHOT, 42), 7, 4096).unwrap();
    assert!(key.starts_with(SNAPSHOTS), "{key}");
    assert!(
        key.ends_with("~bases~000000000000000000000000000000a1~42~7~4096.sqlite"),
        "{key}"
    );
    let name = BaseName {
        owner_epoch: 0xa1,
        version: 42,
        txid: 7,
        bytes: 4096,
    };
    assert_eq!(parse(&key), Some(name));
}

#[test]
fn every_base_of_an_actor_shares_one_prefix() {
    let key = key(&reference(SNAPSHOT, 42), 7, 4096).unwrap();
    assert!(key.starts_with(&actor_prefix(&reference(SNAPSHOT, 42)).unwrap()));
    // Host tokens grant the actor's snapshot names, so they cover its bases without a new rule.
    let actor = "durable-actors/v3/snapshots/63/cHJvamVjdA/Q291bnRlcg/aWQ/";
    assert!(key.starts_with(&object_name(actor).unwrap()));
    let other = "durable-actors/v3/snapshots/63/cHJvamVjdA/Q291bnRlcg/b3RoZXI/";
    assert!(!key.starts_with(&prefix(other).unwrap()));
}

#[test]
fn ignores_names_that_are_not_bases() {
    assert_eq!(
        parse("durable-actors-v3-snapshots-abc~bases~01~2~3.sqlite"),
        None
    );
    assert_eq!(
        parse("durable-actors-v3-snapshots-abc~01~2~3~4.sqlite"),
        None
    );
    assert_eq!(
        parse("durable-actors-v3-snapshots-abc~bases~zz~2~3~4.sqlite"),
        None
    );
    assert_eq!(parse("durable-actors-v3-snapshots-abc~01~2.json"), None);
}
