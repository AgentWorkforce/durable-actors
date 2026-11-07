use super::*;
use anyhow::Result;
use serde_json::json;

#[test]
fn reads_legacy_results_and_encodes_only_the_current_format() -> Result<()> {
    for result in [
        Value::Null,
        json!(false),
        json!(3),
        json!("saved"),
        json!([1, null]),
        json!({"count": 3}),
    ] {
        let stored = legacy_record(result.clone())?;
        let snapshot = StateSnapshot::decode(&serde_json::to_vec(&stored)?)?;
        assert_eq!(snapshot.completion.as_ref().unwrap().result, result);
        let encoded = serde_json::to_value(StateSnapshot::decode(&snapshot.encode()?)?)?;
        let mut expected = stored;
        expected.as_object_mut().unwrap().remove("result");
        expected["completion"] = json!({"result": result});
        assert_eq!(encoded, expected);
    }
    Ok(())
}

#[test]
fn rejects_records_containing_both_completion_formats() -> Result<()> {
    for completion in [Value::Null, json!({"result": null}), json!({"result": 3})] {
        let mut stored = legacy_record(Value::Null)?;
        stored["completion"] = completion;
        assert!(StateSnapshot::decode(&serde_json::to_vec(&stored)?).is_err());
    }
    Ok(())
}

#[test]
fn legacy_records_still_require_valid_metadata_and_sqlite() -> Result<()> {
    let valid = legacy_record(json!(3))?;
    for (field, value) in [
        ("stateVersion", json!(0)),
        ("ownerEpoch", json!(0)),
        ("requestId", json!("")),
        ("unexpected", json!(true)),
        ("sqlite", json!({"txid": 0, "files": []})),
    ] {
        let mut invalid = valid.clone();
        invalid[field] = value;
        assert!(StateSnapshot::decode(&serde_json::to_vec(&invalid)?).is_err());
    }
    Ok(())
}

#[test]
fn legacy_snapshot_references_use_the_original_bytes() -> Result<()> {
    let bytes = serde_json::to_vec(&legacy_record(json!(3))?)?;
    let snapshot = StateSnapshot::decode(&bytes)?;
    let stream = crate::storage::StateStream {
        session: "session".into(),
        prefix: "snapshots/".into(),
        owner_epoch: 2,
        base_version: 0,
    };
    let reference = stream.snapshot(&bytes)?;
    reference.verify(&bytes)?;
    assert!(reference.verify(&snapshot.encode()?).is_err());
    Ok(())
}

fn legacy_record(result: Value) -> Result<Value> {
    Ok(json!({
        "stateVersion": 1,
        "ownerEpoch": 2,
        "requestId": "request",
        "sqlite": crate::test_sqlite::snapshot(json!({"count": 3}))?,
        "result": result,
        "attribution": {
            "operation": "increment", "connectionId": null,
            "committedAtMs": 1234, "interleaved": false
        }
    }))
}
