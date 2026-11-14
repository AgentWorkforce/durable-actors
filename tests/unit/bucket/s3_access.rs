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
            json!({"Effect":"Allow","Action":["s3:GetObject","s3:PutObject"],"Resource":"arn:aws:s3:::state/durable-actors/v3/owners/abc.json*"}),
            json!({"Effect":"Allow","Action":"s3:ListBucket","Resource":"arn:aws:s3:::state",
                "Condition":{"StringLike":{"s3:prefix":"durable-actors/v3/owners/abc.json*"}}}),
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

#[test]
fn tokens_carry_expiring_session_credentials() -> Result<()> {
    let expiry = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_900_000_000);
    let issued = token("AKIA", "secret", "session", Some(expiry))?;
    assert_eq!(issued.expires_at_ms, 1_900_000_000_000);
    assert!(issued.access_token.is_empty());
    let aws = issued.aws.unwrap();
    assert_eq!(
        (aws.access_key_id.as_str(), aws.session_token.as_str()),
        ("AKIA", "session")
    );
    let static_keys = token("AKIA", "secret", "", None)?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as u64;
    assert!(static_keys.expires_at_ms > now && static_keys.expires_at_ms <= now + 900_000);
    Ok(())
}

#[test]
fn directory_buckets_get_one_create_session_grant_instead_of_object_actions() -> Result<()> {
    let express = "actors--usw2-az1--x-s3";
    let owner = "durable-actors/v3/owners/abc.json";
    let ownership = statements(&session_policy(
        &[
            rule(express, owner, Permissions::OBJECT_USER),
            rule("checkpoint", owner, Permissions::OBJECT_USER),
        ],
        true,
    )?);
    assert!(
        ownership.iter().all(|statement| !statement["Resource"]
            .as_str()
            .unwrap()
            .contains(express)
            || statement["Action"] == "s3express:CreateSession"),
        "no s3:* actions on the directory bucket: {ownership:?}"
    );
    assert_eq!(
        ownership.last().unwrap(),
        &json!({"Effect":"Allow","Action":"s3express:CreateSession",
            "Resource":"arn:aws:s3express:*:*:bucket/actors--usw2-az1--x-s3"})
    );
    // The regional checkpoint copy keeps its prefix-scoped object grants.
    assert!(ownership.iter().any(|statement| statement["Resource"]
        == "arn:aws:s3:::checkpoint/durable-actors/v3/owners/abc.json*"
        && statement["Action"] == json!(["s3:GetObject", "s3:PutObject"])));

    let snapshots = "durable-actors/v3/snapshots/x/";
    let state = statements(&session_policy(
        &[
            rule(
                express,
                snapshots,
                Permissions::OBJECT_VIEWER | Permissions::OBJECT_CREATOR,
            ),
            rule(
                express,
                "durable-actors/v3/snapshots/y/",
                Permissions::OBJECT_VIEWER,
            ),
        ],
        true,
    )?);
    assert_eq!(state.len(), 1, "one grant per directory bucket: {state:?}");
    assert!(state[0].get("Condition").is_none(), "writes need ReadWrite");
    Ok(())
}

#[test]
fn read_only_rules_on_a_directory_bucket_allow_only_read_only_sessions() -> Result<()> {
    let policy = statements(&session_policy(
        &[rule(
            "code--use1-az4--x-s3",
            "durable-actors/v3/artifacts/d/",
            Permissions::OBJECT_VIEWER,
        )],
        true,
    )?);
    assert_eq!(
        policy,
        vec![json!({"Effect":"Allow","Action":"s3express:CreateSession",
            "Resource":"arn:aws:s3express:*:*:bucket/code--use1-az4--x-s3",
            "Condition":{"StringEquals":{"s3express:SessionMode":"ReadOnly"}}})]
    );
    Ok(())
}

#[test]
fn checkpointed_express_policies_fit_the_sts_limit() -> Result<()> {
    let actor = crate::actor::ActorKey {
        project_id: "p".repeat(64),
        actor_name: "n".repeat(48),
        actor_id: "i".repeat(128),
    };
    let express = format!("{}--usw2-az1--x-s3", "e".repeat(45));
    let checkpoint = "c".repeat(63);
    let owner = crate::storage_paths::owner(&actor.storage_key())?;
    let snapshots = crate::storage_paths::snapshots(&actor)?;
    for rules in [
        vec![
            rule(&express, &owner, Permissions::OBJECT_USER),
            rule(
                &checkpoint,
                "durable-actors/v3/artifacts/12345678-1234-4234-8234-123456789012/",
                Permissions::OBJECT_VIEWER,
            ),
            rule(&checkpoint, &owner, Permissions::OBJECT_USER),
        ],
        vec![
            rule(
                &express,
                &snapshots,
                Permissions::OBJECT_VIEWER | Permissions::OBJECT_CREATOR,
            ),
            rule(
                &checkpoint,
                &snapshots,
                Permissions::OBJECT_VIEWER | Permissions::OBJECT_CREATOR,
            ),
        ],
    ] {
        let policy = session_policy(&rules, true)?;
        assert!(policy.len() <= MAX_POLICY_BYTES, "{}", policy.len());
    }
    Ok(())
}
