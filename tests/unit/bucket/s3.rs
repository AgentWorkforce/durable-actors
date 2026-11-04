use std::collections::HashMap;

use super::*;

#[test]
fn lost_preconditions_are_conflicts_not_errors() {
    for status in [404, 409, 412] {
        assert!(lost_precondition(Some(status)), "{status}");
    }
    for status in [None, Some(200), Some(400), Some(403), Some(500), Some(503)] {
        assert!(!lost_precondition(status), "{status:?}");
    }
}

#[test]
fn generations_come_from_object_metadata() -> Result<()> {
    let metadata = |value: &str| HashMap::from([(GENERATION.to_owned(), value.to_owned())]);
    assert_eq!(generation(Some(&metadata("7")))?, 7);
    for invalid in ["0", "-1", "seven", ""] {
        assert!(generation(Some(&metadata(invalid))).is_err(), "{invalid}");
    }
    assert!(generation(None).is_err());
    assert!(generation(Some(&HashMap::new())).is_err());
    Ok(())
}

#[test]
fn bucket_names_follow_s3_rules() {
    for valid in ["abc", "pippen-sbx-actors-proof-643255347601", "a.b-c"] {
        assert!(validate_bucket(valid).is_ok(), "{valid}");
    }
    for invalid in [
        "ab",
        "Upper",
        "-lead",
        "trail-",
        "has/slash",
        "under_score",
        &"a".repeat(64),
    ] {
        assert!(validate_bucket(invalid).is_err(), "{invalid}");
    }
}

#[test]
fn endpoints_round_trip_through_host_configuration() -> Result<()> {
    let endpoint = S3Endpoint {
        region: "us-west-2".into(),
        endpoint: Some("http://minio:9000".into()),
        force_path_style: true,
    };
    let encoded = serde_json::to_value(&endpoint)?;
    assert_eq!(
        encoded,
        serde_json::json!({"region":"us-west-2","endpoint":"http://minio:9000","forcePathStyle":true})
    );
    assert_eq!(serde_json::from_value::<S3Endpoint>(encoded)?, endpoint);
    let aws: S3Endpoint = serde_json::from_value(serde_json::json!({"region":"us-west-2"}))?;
    assert_eq!(aws.endpoint, None);
    assert!(!aws.force_path_style);
    Ok(())
}
