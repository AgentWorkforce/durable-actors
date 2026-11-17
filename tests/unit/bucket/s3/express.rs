use super::*;
use crate::bucket::RapidPlacement;

#[tokio::test]
async fn fencing_a_segment_stops_its_previous_writer() -> Result<()> {
    let Ok(bucket) = std::env::var("DURABLE_ACTORS_TEST_EXPRESS_BUCKET") else {
        return Ok(());
    };
    let region = std::env::var("DURABLE_ACTORS_TEST_EXPRESS_REGION")?;
    let sdk = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(region))
        .load()
        .await;
    let zone = ExpressZone {
        bucket,
        logs: Arc::new(S3ExpressLogs::new(aws_sdk_s3::Client::new(&sdk))),
        http: reqwest::Client::new(),
    };
    let segment = format!("test-{}.segment", uuid::Uuid::new_v4());
    let (replica, mut writer) = zone.open(&segment).await?;
    assert_eq!(
        writer.append_and_flush(Bytes::from_static(b"one")).await?,
        3
    );
    assert_eq!(
        writer.append_and_flush(Bytes::from_static(b"two")).await?,
        6
    );
    assert_eq!(
        zone.read_range(&replica, 2, 3).await?,
        Bytes::from_static(b"etw")
    );
    // Crosses the first batch of upload URLs, so the background batch is used.
    for _ in 0..UPLOADS_PER_REQUEST {
        writer.append_and_flush(Bytes::from_static(b"x")).await?;
    }
    let written = [b"onetwo".as_slice(), &[b'x'; UPLOADS_PER_REQUEST as usize]].concat();

    assert_eq!(zone.read(&replica, true).await?, written);
    assert!(
        writer
            .append_and_flush(Bytes::from_static(b"late"))
            .await
            .is_err()
    );
    assert!(zone.open(&segment).await.is_err());
    assert_eq!(zone.read(&replica, false).await?, written);
    zone.delete(&replica).await
}

#[test]
fn one_log_zone_or_nearest_placement_leaves_a_single_zone() {
    let zone = |bucket: &str, zone: &str| RapidBucket {
        bucket: bucket.into(),
        zone: zone.into(),
    };
    let rapid = |buckets, placement| SnapshotStoreConfig::Rapid {
        buckets,
        archive_bucket: "archive".into(),
        archive_batch: Default::default(),
        placement,
    };
    let two = vec![
        zone("logs--use2-az1--x-s3", "use2-az1"),
        zone("logs--use2-az2--x-s3", "use2-az2"),
    ];
    assert!(!single_zone(&rapid(two.clone(), RapidPlacement::All)));
    assert!(single_zone(&rapid(two, RapidPlacement::Nearest)));
    assert!(single_zone(&rapid(
        vec![zone("logs--use2-az1--x-s3", "use2-az1")],
        RapidPlacement::All
    )));
    assert!(!single_zone(&SnapshotStoreConfig::Local));
}

#[tokio::test]
async fn validation_opens_a_session_on_each_log_bucket() -> Result<()> {
    let Ok(bucket) = std::env::var("DURABLE_ACTORS_TEST_EXPRESS_BUCKET") else {
        return Ok(());
    };
    let region = std::env::var("DURABLE_ACTORS_TEST_EXPRESS_REGION")?;
    let sdk = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(region))
        .load()
        .await;
    let client = aws_sdk_s3::Client::new(&sdk);
    let zone_id = bucket
        .strip_suffix("--x-s3")
        .and_then(|name| name.rsplit("--").next())
        .unwrap_or_default()
        .to_owned();
    let config = |bucket: String| SnapshotStoreConfig::Rapid {
        buckets: vec![RapidBucket {
            bucket,
            zone: zone_id.clone(),
        }],
        archive_bucket: "archive".into(),
        archive_batch: Default::default(),
        placement: RapidPlacement::All,
    };
    validate(&config(bucket), &client).await?;
    let missing = format!("missing-{}--{zone_id}--x-s3", uuid::Uuid::new_v4().simple());
    let error = validate(&config(missing), &client).await.unwrap_err();
    assert!(format!("{error:#}").contains("s3express:CreateSession"));
    Ok(())
}
