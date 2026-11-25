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

/// An S3 stand-in that refuses every GET with 403, as S3 does for a missing key when the caller
/// may list the bucket only under its own prefixes, and lists `listed` for any prefix.
async fn refusing_s3(listed: Vec<&'static str>) -> Result<(S3Bucket, tokio::task::JoinHandle<()>)> {
    use axum::{Router, extract::Query, http::StatusCode, response::IntoResponse, routing::get};
    let app = Router::new().fallback(get(move |Query(query): Query<HashMap<String, String>>| {
        let listed = listed.clone();
        async move {
            if query.get("list-type").map(String::as_str) != Some("2") {
                return (StatusCode::FORBIDDEN, "<Error><Code>AccessDenied</Code></Error>").into_response();
            }
            let contents: String = listed
                .iter()
                .map(|key| format!("<Contents><Key>{key}</Key></Contents>"))
                .collect();
            (
                [("content-type", "application/xml")],
                format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult><Name>b</Name><KeyCount>{}</KeyCount><IsTruncated>false</IsTruncated>{contents}</ListBucketResult>", listed.len()),
            )
                .into_response()
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let endpoint = S3Endpoint {
        region: "us-east-1".into(),
        endpoint: Some(format!("http://{address}")),
        force_path_style: true,
    };
    let credentials = SharedCredentialsProvider::new(aws_credential_types::Credentials::from_keys(
        "test", "test", None,
    ));
    Ok((
        S3Bucket::with_credentials("actor-bucket", &endpoint, credentials)?,
        server,
    ))
}

#[tokio::test]
async fn a_refused_read_of_a_key_its_listing_lacks_is_absent() -> Result<()> {
    use crate::bucket::Bucket;
    let (bucket, server) = refusing_s3(vec![]).await?;
    assert!(bucket.get("snapshots/a/2.json.checkpoint").await?.is_none());
    server.abort();
    Ok(())
}

#[tokio::test]
async fn a_refused_read_of_a_listed_key_stays_an_error() -> Result<()> {
    use crate::bucket::Bucket;
    let (bucket, server) = refusing_s3(vec!["snapshots/a/2.json"]).await?;
    assert!(bucket.get("snapshots/a/2.json").await.is_err());
    server.abort();
    Ok(())
}
