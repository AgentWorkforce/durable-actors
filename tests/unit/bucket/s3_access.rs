use super::*;

fn rule(bucket: &str, prefix: &str, permissions: Permissions) -> Rule {
    Rule {
        bucket: bucket.into(),
        prefix: prefix.into(),
        permissions,
    }
}

fn statements(policy: &str) -> Vec<Value> {
    serde_json::from_str::<Value>(policy).unwrap()["Statement"]
        .as_array()
        .unwrap()
        .clone()
}

#[test]
fn owner_rules_allow_read_list_and_replace_of_one_prefix() -> Result<()> {
    let policy = session_policy(
        &[rule(
            "state",
            "durable-actors/v3/owners/abc.json",
            Permissions::OBJECT_USER,
        )],
        true,
    )?;
    let statements = statements(&policy);
    assert_eq!(
        statements,
        vec![
            json!({"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::state/durable-actors/v3/owners/abc.json*"}),
            json!({"Effect":"Allow","Action":"s3:ListBucket","Resource":"arn:aws:s3:::state",
                "Condition":{"StringLike":{"s3:prefix":"durable-actors/v3/owners/abc.json*"}}}),
            json!({"Effect":"Allow","Action":"s3:PutObject","Resource":"arn:aws:s3:::state/durable-actors/v3/owners/abc.json*"}),
        ]
    );
    Ok(())
}

#[test]
fn snapshot_rules_are_create_only_and_code_is_read_only() -> Result<()> {
    let policy = session_policy(
        &[
            rule(
                "state",
                "durable-actors/v3/snapshots/x/",
                Permissions::OBJECT_VIEWER | Permissions::OBJECT_CREATOR,
            ),
            rule(
                "code",
                "durable-actors/v3/artifacts/d/",
                Permissions::OBJECT_VIEWER,
            ),
        ],
        true,
    )?;
    let granted = statements(&policy);
    let puts: Vec<_> = granted
        .iter()
        .filter(|statement| statement["Action"] == "s3:PutObject")
        .collect();
    assert_eq!(puts.len(), 1, "code is never writable");
    assert_eq!(
        puts[0]["Resource"],
        "arn:aws:s3:::state/durable-actors/v3/snapshots/x/*"
    );
    assert_eq!(
        puts[0]["Condition"]["StringEquals"]["s3:if-none-match"],
        "*"
    );
    assert!(granted.iter().any(|statement| statement["Resource"]
        == "arn:aws:s3:::code/durable-actors/v3/artifacts/d/*"
        && statement["Action"] == "s3:GetObject"));

    let relaxed = session_policy(
        &[rule(
            "state",
            "p/",
            Permissions::OBJECT_VIEWER | Permissions::OBJECT_CREATOR,
        )],
        false,
    )?;
    assert!(
        statements(&relaxed)
            .iter()
            .all(|statement| statement.get("Condition").is_none()
                || statement["Action"] == "s3:ListBucket")
    );
    Ok(())
}

#[test]
fn policies_for_the_longest_identifiers_fit_the_sts_limit() -> Result<()> {
    let actor = crate::actor::ActorKey {
        project_id: "p".repeat(64),
        actor_name: "n".repeat(48),
        actor_id: "i".repeat(128),
    };
    let bucket = "b".repeat(63);
    let owner = crate::storage_paths::owner(&actor.storage_key())?;
    let snapshots = crate::storage_paths::snapshots(&actor)?;
    for rules in [
        vec![
            rule(&bucket, &owner, Permissions::OBJECT_USER),
            rule(
                &bucket,
                "durable-actors/v3/artifacts/12345678-1234-4234-8234-123456789012/",
                Permissions::OBJECT_VIEWER,
            ),
        ],
        vec![rule(
            &bucket,
            &snapshots,
            Permissions::OBJECT_VIEWER | Permissions::OBJECT_CREATOR,
        )],
    ] {
        let policy = session_policy(&rules, true)?;
        assert!(policy.len() <= MAX_POLICY_BYTES, "{}", policy.len());
    }
    assert!(session_policy(&[], true).is_err());
    Ok(())
}

#[test]
fn session_names_are_valid_and_unique() {
    let (first, second) = (session_name(), session_name());
    assert_ne!(first, second);
    assert!(first.len() <= 64);
    assert!(
        first
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    );
}
