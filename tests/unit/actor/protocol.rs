use super::*;

#[test]
fn actor_identity_uses_actor_name_on_the_wire() {
    let wire = serde_json::json!({
        "project_id": "my-app", "actor_name": "Counter", "actor_id": "one"
    });
    let key: ActorKey = serde_json::from_value(wire.clone()).expect("actor_name identity");
    key.validate().expect("valid actor identity");
    assert_eq!(serde_json::to_value(key).unwrap(), wire);
}

#[test]
fn actor_identity_requires_project_id() {
    assert!(
        serde_json::from_value::<ActorKey>(serde_json::json!({
            "actor_name": "Counter", "actor_id": "one"
        }))
        .is_err()
    );
    let key = ActorKey {
        project_id: String::new(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
    };
    assert!(key.validate().is_err());
}

#[test]
fn maps_an_actor_to_one_safe_object_id() {
    let key = ActorKey {
        project_id: "default".into(),
        actor_name: "counter".into(),
        actor_id: "customer.123".into(),
    };

    key.validate().expect("actor key");
    assert_eq!(
        key.storage_key().as_str(),
        "object.v4.default:counter:customer.123"
    );
}

#[test]
fn rejects_components_that_can_reshape_storage_paths() {
    let mut key = ActorKey {
        project_id: "default".into(),
        actor_name: "counter".into(),
        actor_id: "../other".into(),
    };
    assert!(key.validate().is_err());

    key.actor_id = "valid".into();
    key.actor_name = "counter/type".into();
    assert!(key.validate().is_err());
}

#[derive(Deserialize)]
struct Received {
    #[serde(deserialize_with = "ActorArgs::deserialize_text")]
    args: ActorArgs,
}

#[test]
fn args_keep_the_text_they_arrived_as() {
    let received: Received = serde_json::from_str(r#"{"args": [1, {"b": 2.50}]}"#).unwrap();
    assert_eq!(received.args.text().get(), r#"[1, {"b": 2.50}]"#);
    assert!(serde_json::from_str::<Received>(r#"{"args":{}}"#).is_err());
}

#[test]
fn args_decode_inside_an_assignment() {
    let request = crate::sandbox::assignment::InitialRequest::Invoke {
        invocation: ActorInvocation {
            request_id: "first".into(),
            actor: serde_json::from_value(serde_json::json!({
                "project_id": "default", "actor_name": "Counter", "actor_id": "one"
            }))
            .unwrap(),
            method: "increment".into(),
            args: vec![serde_json::json!({"by": 2})].into(),
        },
        grant: None,
    };
    let wire = serde_json::to_string(&request).unwrap();
    assert_eq!(
        serde_json::from_str::<crate::sandbox::assignment::InitialRequest>(&wire).unwrap(),
        request
    );
}

#[test]
fn project_identity_separates_same_named_actors() {
    let actor = |project: &str| -> ActorKey {
        serde_json::from_value(serde_json::json!({
            "project_id": project, "actor_name": "Counter", "actor_id": "shared"
        }))
        .unwrap()
    };
    let first = actor("team-a");
    let second = actor("team-b");
    assert_ne!(first.storage_key(), second.storage_key());
    assert_ne!(
        crate::storage_paths::snapshots(&first).unwrap(),
        crate::storage_paths::snapshots(&second).unwrap()
    );
    assert_eq!(
        crate::storage_paths::actor_from_key(&first.storage_key()).unwrap(),
        first
    );
}
