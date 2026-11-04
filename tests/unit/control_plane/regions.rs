use super::*;

#[test]
fn aws_availability_zones_map_to_their_region() -> anyhow::Result<()> {
    assert_eq!(storage_region("us-west-2a")?, "north-america-west");
    assert_eq!(storage_region("us-east-1f")?, "north-america-east");
    assert_eq!(storage_region("eu-central-1b")?, "europe-west");
    assert_eq!(storage_region("ap-southeast-1c")?, "asia-southeast");
    assert_eq!(storage_region("ca-central-1a")?, "canada");
    assert!(storage_region("us-west-2-lax-1a").is_err());
    assert!(storage_region("mars-north-1a").is_err());
    Ok(())
}

#[test]
fn gke_zones_and_regions_are_unchanged() -> anyhow::Result<()> {
    assert_eq!(storage_region("us-west4-a")?, "north-america-west");
    assert_eq!(storage_region("us-central1-c")?, "north-america-central");
    assert_eq!(storage_region("us-west-2")?, "north-america-west");
    assert_eq!(storage_region("north-america-west")?, "north-america-west");
    Ok(())
}
