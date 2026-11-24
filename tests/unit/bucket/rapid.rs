use super::*;
use crate::{
    actor::ActorKey,
    bucket::{BucketSnapshots, FileBucket},
    state_log::StateSnapshot,
    storage::StateStream,
};
use anyhow::ensure;
use std::{
    collections::BTreeMap,
    sync::{
        Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

#[path = "rapid/batch.rs"]
mod batch;

#[tokio::test]
async fn hot_writes_reuse_streams_and_a_fresh_reader_discovers_acknowledged_state() -> Result<()> {
    let f = Fixture::new()?;
    let store = f.store()?;
    store.start(&f.stream, None).await?;
    for version in 1..=3 {
        store
            .put(&f.stream.object(version), state(version, 1)?)
            .await?;
    }
    assert!(
        f.zones
            .iter()
            .all(|zone| zone.opens.load(Ordering::SeqCst) == 1)
    );
    assert_eq!(
        f.store()?.latest(&f.stream.prefix).await?,
        Some((f.stream.object(3), state(3, 1)?))
    );
    assert_eq!(
        f.store()?.get(&f.stream.object(2)).await?,
        Some(state(2, 1)?)
    );
    Ok(())
}

#[tokio::test]
async fn single_zone_writes_recover_after_an_interrupted_flush() -> Result<()> {
    let mut f = Fixture::new()?;
    f.zones.truncate(1);
    let store = f.store()?;
    store.start(&f.stream, None).await?;
    store.put(&f.stream.object(1), state(1, 1)?).await?;
    assert_eq!(
        f.store()?.latest(&f.stream.prefix).await?,
        Some((f.stream.object(1), state(1, 1)?))
    );
    f.zones[0].stalled.store(true, Ordering::SeqCst);
    assert!(
        tokio::time::timeout(
            Duration::from_millis(30),
            store.put(&f.stream.object(2), state(2, 1)?)
        )
        .await
        .is_err()
    );
    f.zones[0].stalled.store(false, Ordering::SeqCst);
    assert!(store.put(&f.stream.object(2), state(2, 1)?).await.is_err());
    drop(store);
    let reader = f.store()?;
    assert_eq!(
        reader.recover(&f.stream.prefix).await?,
        Some((f.stream.object(1), state(1, 1)?))
    );
    f.zones[0].offline.store(true, Ordering::SeqCst);
    assert_eq!(reader.get(&f.stream.object(1)).await?, Some(state(1, 1)?));
    Ok(())
}

fn state(version: u64, epoch: u64) -> Result<Payload> {
    // LTX files contain timestamps; reuse captures for byte-for-byte recovery assertions.
    static STATES: OnceLock<Mutex<BTreeMap<(u64, u64), Payload>>> = OnceLock::new();
    let mut states = STATES.get_or_init(Mutex::default).lock().unwrap();
    if let Some(bytes) = states.get(&(version, epoch)) {
        return Ok(bytes.clone());
    }
    let bytes = StateSnapshot::new(
        version,
        epoch,
        format!("request-{version}"),
        crate::test_sqlite::snapshot(serde_json::json!({"count": version}))?,
        serde_json::json!(version),
    )?
    .encode()?;
    states.insert((version, epoch), bytes.clone());
    Ok(bytes)
}

#[tokio::test]
async fn actor_assignment_opens_both_connections_before_activation_and_reuses_them() -> Result<()> {
    let f = Fixture::new()?;
    let store = f.store()?;
    let actor = crate::storage_paths::actor_from_snapshot(&f.stream.object(1))?;
    store.prepare(&actor)?;
    tokio::time::timeout(Duration::from_secs(1), async {
        while f
            .zones
            .iter()
            .any(|zone| zone.opens.load(Ordering::SeqCst) != 1)
        {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert!(f.archive.list("").await?.is_empty());
    store.start(&f.stream, None).await?;
    store.put(&f.stream.object(1), state(1, 1)?).await?;
    assert!(
        f.zones
            .iter()
            .all(|zone| zone.opens.load(Ordering::SeqCst) == 1)
    );
    assert_eq!(
        f.store()?.latest(&f.stream.prefix).await?,
        Some((f.stream.object(1), state(1, 1)?))
    );
    Ok(())
}

#[tokio::test]
async fn nearest_placement_writes_to_the_first_zone_to_open_and_discards_the_rest() -> Result<()> {
    let mut f = Fixture::new()?;
    f.placement = crate::bucket::RapidPlacement::Nearest;
    f.zones[0].open_delay_ms.store(20, Ordering::SeqCst);
    let store = f.store()?;
    store.prepare(&crate::storage_paths::actor_from_snapshot(
        &f.stream.object(1),
    )?)?;
    store.start(&f.stream, None).await?;
    for version in 1..=2 {
        store
            .put(&f.stream.object(version), state(version, 1)?)
            .await?;
    }
    assert!(written(&f.zones[1]) && !written(&f.zones[0]));
    f.zones[0].discarded().await?;
    assert_eq!(
        f.store()?.latest(&f.stream.prefix).await?,
        Some((f.stream.object(2), state(2, 1)?))
    );
    Ok(())
}

#[tokio::test]
async fn nearest_placement_skips_an_unavailable_zone() -> Result<()> {
    let mut f = Fixture::new()?;
    f.placement = crate::bucket::RapidPlacement::Nearest;
    f.zones[0].offline.store(true, Ordering::SeqCst);
    let store = f.store()?;
    store.start(&f.stream, None).await?;
    store.put(&f.stream.object(1), state(1, 1)?).await?;
    assert!(written(&f.zones[1]));
    Ok(())
}

#[tokio::test]
async fn nearest_placement_rotates_within_the_chosen_zone() -> Result<()> {
    let mut f = Fixture::new()?;
    f.placement = crate::bucket::RapidPlacement::Nearest;
    f.zones[0].open_delay_ms.store(20, Ordering::SeqCst);
    let store = f.store()?;
    store.start(&f.stream, None).await?;
    store.put(&f.stream.object(1), state(1, 1)?).await?;
    f.zones[0].discarded().await?;
    f.zones[0].open_delay_ms.store(0, Ordering::SeqCst);
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(61)).await;
    tokio::time::resume();
    tokio::time::timeout(Duration::from_secs(2), async {
        while manifests(&f).await.len() < 2 {
            let mut session = store.session.lock().await;
            session
                .as_mut()
                .unwrap()
                .rotate(store.storage.clone())
                .await;
            drop(session);
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await?;
    store.put(&f.stream.object(2), state(2, 1)?).await?;
    assert_eq!(f.zones[0].opens.load(Ordering::SeqCst), 1);
    assert!(
        manifests(&f)
            .await
            .iter()
            .all(|m| m.replicas.len() == 1 && m.replicas[0].bucket == "b")
    );
    assert_eq!(
        f.store()?.latest(&f.stream.prefix).await?,
        Some((f.stream.object(2), state(2, 1)?))
    );
    Ok(())
}

#[tokio::test]
async fn logs_stay_recoverable_when_placement_changes() -> Result<()> {
    use crate::bucket::RapidPlacement::{All, Nearest};
    for (writer, reader) in [(All, Nearest), (Nearest, All)] {
        let mut f = Fixture::new()?;
        f.placement = writer;
        let store = f.store()?;
        store.start(&f.stream, None).await?;
        store.put(&f.stream.object(1), state(1, 1)?).await?;
        drop(store);
        f.placement = reader;
        assert_eq!(
            f.store()?.recover(&f.stream.prefix).await?,
            Some((f.stream.object(1), state(1, 1)?))
        );
    }
    Ok(())
}

#[tokio::test]
async fn manifests_must_name_distinct_configured_zones() -> Result<()> {
    let tampers: [fn(&mut Vec<Replica>); 2] = [
        |replicas| replicas.push(replicas[0].clone()),
        |replicas| replicas[0].bucket = "unconfigured".into(),
    ];
    for tamper in tampers {
        let f = Fixture::new()?;
        let store = f.store()?;
        store.start(&f.stream, None).await?;
        store.put(&f.stream.object(1), state(1, 1)?).await?;
        let prefix = crate::bucket::rapid::object_name(&f.stream.prefix)?;
        let key = f
            .archive
            .list(&prefix)
            .await?
            .into_iter()
            .find(|key| key.ends_with(".manifest"))
            .unwrap();
        let stored = f.archive.get(&key).await?.unwrap();
        let mut manifest: Manifest = serde_json::from_slice(&stored.bytes.to_bytes()?)?;
        tamper(&mut manifest.replicas);
        let tampered = crate::payload::encode(&manifest)?;
        assert!(
            f.archive
                .compare_and_swap(&key, Some(stored.generation), tampered)
                .await?
        );
        assert!(f.store()?.latest(&f.stream.prefix).await.is_err());
    }
    Ok(())
}

fn written(zone: &MemoryZone) -> bool {
    zone.objects
        .lock()
        .unwrap()
        .values()
        .any(|object| !object.bytes.is_empty())
}

async fn manifests(f: &Fixture) -> Vec<Manifest> {
    let mut manifests = Vec::new();
    for key in f.archive.list("").await.unwrap() {
        if key.ends_with(".manifest") {
            let bytes = f.archive.get(&key).await.unwrap().unwrap().bytes;
            manifests.push(serde_json::from_slice(&bytes.to_bytes().unwrap()).unwrap());
        }
    }
    manifests
}

#[tokio::test]
async fn foreign_prepared_connections_are_rejected() -> Result<()> {
    let f = Fixture::new()?;
    let store = f.store()?;
    let mut actor = crate::storage_paths::actor_from_snapshot(&f.stream.object(1))?;
    actor.actor_id = "different".into();
    store.prepare(&actor)?;
    store.start(&f.stream, None).await?;
    assert!(store.put(&f.stream.object(1), state(1, 1)?).await.is_err());
    assert!(f.zones.iter().all(|z| {
        z.objects
            .lock()
            .unwrap()
            .values()
            .all(|o| o.bytes.is_empty())
    }));
    Ok(())
}

#[tokio::test]
async fn failed_manifest_publication_never_acknowledges_an_undiscoverable_log() -> Result<()> {
    struct UnavailableArchive;
    #[async_trait]
    impl Bucket for UnavailableArchive {
        async fn range(
            &self,
            key: &str,
            start: u64,
            length: u64,
        ) -> Result<Option<crate::bucket::BucketObject>> {
            let Some(mut object) = self.get(key).await? else {
                return Ok(None);
            };
            let end = start
                .checked_add(length)
                .ok_or_else(|| anyhow::anyhow!("range overflow"))?;
            anyhow::ensure!(end <= object.bytes.len() as u64, "incomplete range");
            object.bytes = object.bytes.slice(start as usize..end as usize);
            Ok(Some(object))
        }

        async fn get(&self, _key: &str) -> Result<Option<crate::bucket::BucketObject>> {
            anyhow::bail!("archive unavailable")
        }
        async fn list(&self, _prefix: &str) -> Result<Vec<String>> {
            anyhow::bail!("archive unavailable")
        }
        async fn compare_and_swap(
            &self,
            _key: &str,
            _generation: Option<i64>,
            _bytes: Payload,
        ) -> Result<bool> {
            anyhow::bail!("archive unavailable")
        }
    }
    let f = Fixture::new()?;
    let store = RapidSnapshots::new(
        Arc::new(UnavailableArchive),
        Arc::new(BucketSnapshots(f.archive.clone())),
        f.zones
            .iter()
            .map(|zone| zone.clone() as Arc<dyn LogZone>)
            .collect(),
        f.placement,
        crate::bucket::ArchiveBatchConfig::default(),
        CancellationToken::new(),
    )?;
    store.start(&f.stream, None).await?;
    assert!(store.put(&f.stream.object(1), state(1, 1)?).await.is_err());
    assert!(f.zones.iter().all(|zone| {
        zone.objects
            .lock()
            .unwrap()
            .values()
            .all(|object| object.bytes.is_empty())
    }));
    Ok(())
}

#[tokio::test]
async fn runtime_crash_recovery_advances_ownership_before_enabling_new_writes() -> Result<()> {
    use crate::{
        bucket::{RapidBucket, RuntimeStorage, SnapshotStoreConfig},
        clock::Clock,
        host::HostId,
        host_leases::HostLeaseRequest,
        state_transport::SnapshotWriter,
    };
    struct TestClock(AtomicU64);
    impl Clock for TestClock {
        fn now_ms(&self) -> Result<u64> {
            Ok(self.0.load(Ordering::SeqCst))
        }
    }
    let f = Fixture::new()?;
    let clock = Arc::new(TestClock(AtomicU64::new(1)));
    let config = SnapshotStoreConfig::Rapid {
        archive_batch: Default::default(),
        placement: Default::default(),
        archive_bucket: "archive".into(),
        buckets: vec![
            RapidBucket {
                bucket: "a".into(),
                zone: "us-west4-a".into(),
            },
            RapidBucket {
                bucket: "b".into(),
                zone: "us-west4-b".into(),
            },
        ],
    };
    let actor = crate::storage_paths::actor_from_snapshot(&f.stream.object(1))?;
    let runtime = || {
        RuntimeStorage::new(f.archive.clone(), clock.clone())?
            .with_persistence(config.clone(), Arc::new(f.store()?))
    };
    let first = runtime()?;
    let request = |name: &str| HostLeaseRequest {
        id: HostId::new(name),
        session_id: name.into(),
        route: format!("http://{name}"),
        duration_ms: 1000,
    };
    let active = first
        .register_activation(&actor, &request("first"), "us-west", true, None)
        .await?;
    let plan = first
        .prepare_actor_write(&actor, &active.placement.lease, 1, 1)
        .await?;
    first.write_snapshot(&plan, state(1, 1)?).await?;
    assert!(
        runtime()?
            .register_activation(&actor, &request("early"), "us-west", false, None)
            .await
            .is_err()
    );
    first
        .write_snapshot(
            &first
                .prepare_actor_write(&actor, &active.placement.lease, 1, 2)
                .await?,
            state(2, 1)?,
        )
        .await?;
    clock.0.store(2000, Ordering::SeqCst);
    f.zones[1].offline.store(true, Ordering::SeqCst);
    let second = runtime()?;
    let recovered = second
        .register_activation(&actor, &request("second"), "us-west", false, None)
        .await?;
    assert_eq!(recovered.placement.owner_epoch, 2);
    assert_eq!(
        recovered.state.map(|snapshot| snapshot.bytes),
        Some(state(2, 1)?)
    );
    assert!(
        first
            .write_snapshot(
                &first
                    .prepare_actor_write(&actor, &active.placement.lease, 1, 3)
                    .await?,
                state(3, 1)?
            )
            .await
            .is_err()
    );
    let plan = second
        .prepare_actor_write(&actor, &recovered.placement.lease, 2, 3)
        .await?;
    second.write_snapshot(&plan, state(3, 2)?).await?;
    let checkpoint = second
        .drain_activation(
            &actor,
            &recovered.placement.owner,
            &recovered.placement.lease.session_id,
            tokio::time::Instant::now() + std::time::Duration::from_secs(30),
        )
        .await?;

    second
        .release_with_checkpoint(
            &actor,
            &recovered.placement.owner,
            &recovered.placement.lease.session_id,
            Some(checkpoint),
        )
        .await?;
    let third = runtime()?
        .register_activation(&actor, &request("third"), "us-west", false, None)
        .await?;
    assert_eq!(third.placement.owner_epoch, 3);
    assert_eq!(
        third.state.map(|snapshot| snapshot.bytes),
        Some(state(3, 2)?)
    );
    Ok(())
}

#[tokio::test]
async fn control_plane_archives_and_seals_an_abandoned_activation() -> Result<()> {
    use crate::{
        bucket::{RapidBucket, RuntimeStorage, RuntimeStorageReader, SnapshotStoreConfig},
        clock::Clock,
        host::HostId,
        host_leases::HostLeaseRequest,
        state_transport::SnapshotWriter,
    };
    struct TestClock(AtomicU64);
    impl Clock for TestClock {
        fn now_ms(&self) -> Result<u64> {
            Ok(self.0.load(Ordering::SeqCst))
        }
    }
    let f = Fixture::new()?;
    let clock = Arc::new(TestClock(AtomicU64::new(1)));
    let config = SnapshotStoreConfig::Rapid {
        archive_batch: Default::default(),
        placement: Default::default(),
        archive_bucket: "archive".into(),
        buckets: vec![
            RapidBucket {
                bucket: "a".into(),
                zone: "us-west4-a".into(),
            },
            RapidBucket {
                bucket: "b".into(),
                zone: "us-west4-b".into(),
            },
        ],
    };
    let actor = crate::storage_paths::actor_from_snapshot(&f.stream.object(1))?;
    let runtime = || {
        RuntimeStorage::new(f.archive.clone(), clock.clone())?
            .with_persistence(config.clone(), Arc::new(f.store()?))
    };
    let request = |name: &str| HostLeaseRequest {
        id: HostId::new(name),
        session_id: name.into(),
        route: format!("http://{name}"),
        duration_ms: 1000,
    };
    let control_plane = RuntimeStorageReader::new(
        f.archive.clone(),
        clock.clone(),
        Arc::new(crate::litestream::EmbeddedRestore),
    )?
    .with_persistence(config.clone(), Arc::new(f.store()?))?;
    let host = runtime()?;
    let active = host
        .register_activation(&actor, &request("crashed"), "us-west", true, None)
        .await?;
    let lease = &active.placement.lease;
    host.write_snapshot(
        &host.prepare_actor_write(&actor, lease, 1, 1).await?,
        state(1, 1)?,
    )
    .await?;
    control_plane.seal_abandoned().await?;
    host.write_snapshot(
        &host.prepare_actor_write(&actor, lease, 1, 2).await?,
        state(2, 1)?,
    )
    .await?;

    clock.0.store(1_000_000, Ordering::SeqCst);
    control_plane.seal_abandoned().await?;
    let fenced = host
        .write_snapshot(
            &host.prepare_actor_write(&actor, lease, 1, 3).await?,
            state(3, 1)?,
        )
        .await;
    assert!(fenced.is_err());
    for zone in &f.zones {
        zone.objects.lock().unwrap().clear();
    }
    let next = runtime()?
        .register_activation(&actor, &request("next"), "us-west", false, None)
        .await?;
    assert_eq!(next.placement.owner_epoch, 2);
    assert_eq!(
        next.state.map(|snapshot| snapshot.bytes),
        Some(state(2, 1)?)
    );
    Ok(())
}

#[tokio::test]
async fn large_segments_reuse_streams_and_keep_their_history() -> Result<()> {
    let f = Fixture::new()?;
    let store = f.store()?;
    store.start(&f.stream, None).await?;
    let snapshots = large_snapshots()?;
    for (version, bytes) in &snapshots {
        store.put(&f.stream.object(*version), bytes.clone()).await?;
    }
    for zone in &f.zones {
        assert_eq!(zone.opens.load(Ordering::SeqCst), 1);
        let objects = zone.objects.lock().unwrap();
        assert_eq!(objects.len(), 1);
        assert!(objects.values().next().unwrap().bytes.len() > 8 * 1024 * 1024);
    }
    assert_eq!(
        f.store()?.latest(&f.stream.prefix).await?,
        Some((f.stream.object(10), snapshots[&10].clone()))
    );
    store
        .finish(
            &f.stream,
            tokio::time::Instant::now() + Duration::from_secs(30),
        )
        .await?;
    let reader = f.store()?;
    assert_eq!(reader.list(&f.stream.prefix).await?.len(), snapshots.len());
    for (version, bytes) in snapshots {
        assert_eq!(reader.get(&f.stream.object(version)).await?, Some(bytes));
    }
    Ok(())
}

fn large_snapshots() -> Result<BTreeMap<u64, Payload>> {
    (1..=10)
        .map(|version| {
            let mut snapshot = StateSnapshot::decode(&state(version, 1)?)?;
            snapshot.completion.as_mut().unwrap().result =
                serde_json::json!({"data": "x".repeat(1_000_000)}).into();
            Ok((version, snapshot.encode()?))
        })
        .collect()
}

#[test]
fn lifecycle_rules_never_delete_logs() -> Result<()> {
    use google_cloud_storage::model::bucket::{
        Lifecycle,
        lifecycle::{
            Rule,
            rule::{Action, Condition},
        },
    };
    let bucket = |prefixes: Vec<&str>, age: Option<i32>| {
        google_cloud_storage::model::Bucket::new().set_lifecycle(
            Lifecycle::new().set_rule([Rule::new()
                .set_action(Action::new().set_type("Delete"))
                .set_condition(
                    Condition::new()
                        .set_or_clear_age_days(age)
                        .set_matches_prefix(prefixes),
                )]),
        )
    };
    let validate = super::gcs::validate_retention;
    validate(&bucket(vec!["durable-actors-v3-snapshots-"], Some(1)))?;
    assert!(validate(&bucket(vec![], Some(7))).is_err());
    assert!(validate(&bucket(vec!["durable-actors-v3-logs-"], Some(30))).is_err());
    assert!(validate(&bucket(vec!["durable-actors-v3-"], Some(6))).is_err());
    assert!(validate(&bucket(vec!["durable-actors-v3-logs-tenant"], None)).is_err());
    Ok(())
}

#[tokio::test]
async fn large_records_and_subsequent_small_writes_stay_on_rapid() -> Result<()> {
    let f = Fixture::new()?;
    let store = f.store()?;
    store.start(&f.stream, None).await?;
    store.put(&f.stream.object(1), state(1, 1)?).await?;
    let mut snapshot = StateSnapshot::decode(&state(2, 1)?)?;
    snapshot.completion.as_mut().unwrap().result =
        serde_json::json!({"data": "x".repeat(5 * 1024 * 1024)}).into();
    let bytes = snapshot.encode()?;
    store.put(&f.stream.object(2), bytes.clone()).await?;
    store.put(&f.stream.object(3), state(3, 1)?).await?;
    for zone in &f.zones {
        let objects = zone.objects.lock().unwrap();
        let records = frame::decode(&Payload::from(
            objects.values().next().unwrap().bytes.clone(),
        ))?;
        assert_eq!(
            records.iter().map(|r| r.version).collect::<Vec<_>>(),
            [1, 2, 3]
        );
        assert_eq!(records[1].state, bytes);
    }
    assert!(f.archive.get(&f.stream.object(2)).await?.is_none());
    assert!(f.archive.get(&f.stream.object(3)).await?.is_none());
    drop(store);
    f.zones[1].offline.store(true, Ordering::SeqCst);
    let reader = f.store()?;
    assert_eq!(
        reader.recover(&f.stream.prefix).await?,
        Some((f.stream.object(3), state(3, 1)?))
    );
    f.zones[1].offline.store(false, Ordering::SeqCst);
    f.zones[0].offline.store(true, Ordering::SeqCst);
    assert_eq!(reader.get(&f.stream.object(2)).await?, Some(bytes));
    assert_eq!(reader.get(&f.stream.object(1)).await?, Some(state(1, 1)?));
    Ok(())
}

#[tokio::test]
async fn recovery_with_one_zone_unavailable_copies_missing_commits_to_standard() -> Result<()> {
    let f = Fixture::new()?;
    let old = f.store()?;
    old.start(&f.stream, None).await?;
    let snapshots = large_snapshots()?;
    for (version, bytes) in &snapshots {
        old.put(&f.stream.object(*version), bytes.clone()).await?;
    }
    f.zones[1].offline.store(true, Ordering::SeqCst);
    let restored = f.store()?.recover(&f.stream.prefix).await?;
    assert_eq!(
        restored,
        Some((f.stream.object(10), snapshots[&10].clone()))
    );
    assert_eq!(
        f.archive.get(&f.stream.object(10)).await?.unwrap().bytes,
        snapshots[&10]
    );
    Ok(())
}

#[tokio::test]
async fn sleep_keeps_history_on_rapid_and_seals_the_head() -> Result<()> {
    for zone_count in [1, 2] {
        let mut f = Fixture::new()?;
        f.zones.truncate(zone_count);
        let store = f.store()?;
        store.start(&f.stream, None).await?;
        for v in 1..=3 {
            store.put(&f.stream.object(v), state(v, 1)?).await?;
        }
        store
            .finish(
                &f.stream,
                tokio::time::Instant::now() + Duration::from_secs(30),
            )
            .await?;
        assert!(
            f.zones
                .iter()
                .all(|z| !z.objects.lock().unwrap().is_empty())
        );
        assert_eq!(
            f.archive.get(&f.stream.object(3)).await?.map(|o| o.bytes),
            Some(state(3, 1)?)
        );
        assert!(
            f.archive
                .list("")
                .await?
                .iter()
                .all(|key| !key.ends_with(".batch"))
        );
        let reader = f.store()?;
        assert_eq!(
            reader.latest(&f.stream.prefix).await?,
            Some((f.stream.object(3), state(3, 1)?))
        );
        assert_eq!(reader.list(&f.stream.prefix).await?.len(), 3);
        assert_eq!(reader.get(&f.stream.object(1)).await?, Some(state(1, 1)?));
    }
    Ok(())
}

#[tokio::test]
async fn recovery_ignores_incomplete_tail_but_rejects_corrupt_complete_records() -> Result<()> {
    let f = Fixture::new()?;
    let store = f.store()?;
    store.start(&f.stream, None).await?;
    store.put(&f.stream.object(1), state(1, 1)?).await?;
    let mut snapshot = StateSnapshot::decode(&state(2, 1)?)?;
    snapshot.completion.as_mut().unwrap().result =
        serde_json::json!({"data": "x".repeat(5 * 1024 * 1024)}).into();
    let mut interrupted = Vec::new();
    Record {
        version: 2,
        state: snapshot.encode()?,
    }
    .write_to(&mut interrupted)?;
    f.zones[0]
        .objects
        .lock()
        .unwrap()
        .values_mut()
        .next()
        .unwrap()
        .bytes
        .extend_from_slice(&interrupted[..crate::payload::IO_BUFFER_BYTES]);
    assert_eq!(
        f.store()?.latest(&f.stream.prefix).await?,
        Some((f.stream.object(1), state(1, 1)?))
    );
    f.zones[0]
        .objects
        .lock()
        .unwrap()
        .values_mut()
        .next()
        .unwrap()
        .bytes[48] ^= 1;
    assert!(f.store()?.latest(&f.stream.prefix).await.is_err());
    Ok(())
}

#[tokio::test]
async fn unavailable_rapid_at_activation_uses_standard_without_losing_state() -> Result<()> {
    let f = Fixture::new()?;
    f.zones[0].offline.store(true, Ordering::SeqCst);
    let store = f.store()?;
    store.start(&f.stream, None).await?;
    store.put(&f.stream.object(1), state(1, 1)?).await?;
    store
        .finish(
            &f.stream,
            tokio::time::Instant::now() + Duration::from_secs(30),
        )
        .await?;
    assert_eq!(
        f.archive.get(&f.stream.object(1)).await?.unwrap().bytes,
        state(1, 1)?
    );
    assert_eq!(
        f.store()?.latest(&f.stream.prefix).await?,
        Some((f.stream.object(1), state(1, 1)?))
    );
    Ok(())
}

#[tokio::test]
async fn writes_require_an_activated_epoch_and_consecutive_versions() -> Result<()> {
    let f = Fixture::new()?;
    let store = f.store()?;
    assert!(store.put(&f.stream.object(1), state(1, 1)?).await.is_err());
    store.start(&f.stream, None).await?;
    assert!(store.put(&f.stream.object(2), state(2, 1)?).await.is_err());
    assert!(store.put(&f.stream.object(1), state(1, 2)?).await.is_err());
    store.put(&f.stream.object(1), state(1, 1)?).await?;
    store.put(&f.stream.object(1), state(1, 1)?).await?;
    assert_eq!(f.zones[0].opens.load(Ordering::SeqCst), 1);
    Ok(())
}

struct Fixture {
    _directory: tempfile::TempDir,
    archive: Arc<FileBucket>,
    zones: Vec<Arc<MemoryZone>>,
    placement: crate::bucket::RapidPlacement,
    stream: StateStream,
}

#[tokio::test]
async fn manifests_cannot_redirect_a_reader_to_another_actors_log() -> Result<()> {
    let f = Fixture::new()?;
    let first = f.store()?;
    first.start(&f.stream, None).await?;
    first.put(&f.stream.object(1), state(1, 1)?).await?;
    let actor = ActorKey {
        project_id: "log-tests".into(),
        actor_name: "Counter".into(),
        actor_id: "other".into(),
    };
    let object = crate::storage::snapshot_object_name(&actor, 1, &format!("{:032x}", 1))?;
    let other_stream = StateStream {
        prefix: object.strip_suffix("1.json").unwrap().into(),
        ..f.stream.clone()
    };
    let other = f.store()?;
    other.start(&other_stream, None).await?;
    other.put(&object, state(1, 1)?).await?;
    let manifests = |stream: &StateStream| crate::bucket::rapid::object_name(&stream.prefix);
    let left = f.archive.list(&manifests(&f.stream)?).await?.pop().unwrap();
    let right = f
        .archive
        .list(&manifests(&other_stream)?)
        .await?
        .pop()
        .unwrap();
    let stored = f.archive.get(&left).await?.unwrap();
    let mut manifest: Manifest = serde_json::from_slice(&stored.bytes.to_bytes()?)?;
    let foreign: Manifest =
        serde_json::from_slice(&f.archive.get(&right).await?.unwrap().bytes.to_bytes()?)?;
    manifest.replicas = foreign.replicas;
    assert!(
        f.archive
            .compare_and_swap(
                &left,
                Some(stored.generation),
                crate::payload::encode(&manifest)?
            )
            .await?
    );
    assert!(f.store()?.latest(&f.stream.prefix).await.is_err());
    Ok(())
}
impl Fixture {
    async fn cleaned(&self) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(2), async {
            while self
                .zones
                .iter()
                .any(|z| !z.objects.lock().unwrap().is_empty())
            {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await?;
        Ok(())
    }
    fn new() -> Result<Self> {
        let directory = tempfile::tempdir()?;
        let archive = Arc::new(FileBucket::new(directory.path().into())?);
        let actor = ActorKey {
            project_id: "log-tests".into(),
            actor_name: "Counter".into(),
            actor_id: "one".into(),
        };
        let object = crate::storage::snapshot_object_name(&actor, 1, &format!("{:032x}", 1))?;
        Ok(Self {
            _directory: directory,
            archive,
            zones: vec![
                Arc::new(MemoryZone::new("a")),
                Arc::new(MemoryZone::new("b")),
            ],
            placement: Default::default(),
            stream: StateStream {
                session: "session".into(),
                prefix: object.strip_suffix("1.json").unwrap().into(),
                owner_epoch: 1,
                base_version: 0,
            },
        })
    }
    fn store(&self) -> Result<RapidSnapshots> {
        RapidSnapshots::new(
            self.archive.clone(),
            Arc::new(BucketSnapshots(self.archive.clone())),
            self.zones
                .iter()
                .map(|z| z.clone() as Arc<dyn LogZone>)
                .collect(),
            self.placement,
            crate::bucket::ArchiveBatchConfig::default(),
            CancellationToken::new(),
        )
    }
}

#[derive(Default)]
struct MemoryObject {
    fence: u64,
    bytes: Vec<u8>,
}
struct MemoryZone {
    name: String,
    objects: Arc<Mutex<BTreeMap<String, MemoryObject>>>,
    offline: Arc<AtomicBool>,
    stalled: Arc<AtomicBool>,
    opens: AtomicU64,
    open_delay_ms: AtomicU64,
    range_reads: AtomicU64,
}
impl MemoryZone {
    fn new(name: &str) -> Self {
        Self {
            name: name.into(),
            objects: Default::default(),
            offline: Default::default(),
            stalled: Default::default(),
            opens: AtomicU64::new(0),
            open_delay_ms: AtomicU64::new(0),
            range_reads: AtomicU64::new(0),
        }
    }
    async fn discarded(&self) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(2), async {
            while self.opens.load(Ordering::SeqCst) == 0 || !self.objects.lock().unwrap().is_empty()
            {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await?;
        Ok(())
    }
}
#[async_trait]
impl LogZone for MemoryZone {
    fn bucket(&self) -> &str {
        &self.name
    }
    async fn open(&self, object: &str) -> Result<(Replica, Box<dyn LogWriter>)> {
        ensure!(!self.offline.load(Ordering::SeqCst), "zone offline");
        let delay = self.open_delay_ms.load(Ordering::SeqCst);
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
        let mut objects = self.objects.lock().unwrap();
        ensure!(!objects.contains_key(object), "already exists");
        objects.insert(object.into(), MemoryObject::default());
        self.opens.fetch_add(1, Ordering::SeqCst);
        Ok((
            Replica {
                bucket: self.name.clone(),
                object: object.into(),
                generation: 1,
            },
            Box::new(MemoryWriter {
                objects: self.objects.clone(),
                offline: self.offline.clone(),
                stalled: self.stalled.clone(),
                object: object.into(),
                fence: 0,
            }),
        ))
    }
    async fn read(&self, replica: &Replica, fence: bool) -> Result<Payload> {
        ensure!(!self.offline.load(Ordering::SeqCst), "zone offline");
        let mut objects = self.objects.lock().unwrap();
        let object = objects
            .get_mut(&replica.object)
            .ok_or_else(|| anyhow::anyhow!("missing replica"))?;
        if fence {
            object.fence += 1;
        }
        crate::payload::copy(object.bytes.as_slice())
    }
    async fn read_range(&self, replica: &Replica, start: u64, length: u64) -> Result<Payload> {
        self.range_reads.fetch_add(1, Ordering::SeqCst);
        let bytes = self.read(replica, false).await?;
        let end = start.checked_add(length).context("range overflow")? as usize;
        ensure!(end <= bytes.len(), "incomplete range");
        Ok(bytes.slice(start as usize..end))
    }
    async fn delete(&self, replica: &Replica) -> Result<()> {
        ensure!(!self.offline.load(Ordering::SeqCst), "zone offline");
        self.objects.lock().unwrap().remove(&replica.object);
        Ok(())
    }
}
struct MemoryWriter {
    objects: Arc<Mutex<BTreeMap<String, MemoryObject>>>,
    offline: Arc<AtomicBool>,
    stalled: Arc<AtomicBool>,
    object: String,
    fence: u64,
}
#[async_trait]
impl LogWriter for MemoryWriter {
    async fn append_and_flush(&mut self, header: Bytes, state: Payload) -> Result<u64> {
        let bytes = [header, state.to_bytes()?].concat();
        ensure!(!self.offline.load(Ordering::SeqCst), "zone offline");
        while self.stalled.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let mut objects = self.objects.lock().unwrap();
        let object = objects
            .get_mut(&self.object)
            .ok_or_else(|| anyhow::anyhow!("missing replica"))?;
        ensure!(object.fence == self.fence, "stream fenced");
        object.bytes.extend_from_slice(&bytes);
        Ok(object.bytes.len() as u64)
    }
}
