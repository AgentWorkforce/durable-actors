use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

fn actor() -> ActorKey {
    ActorKey {
        project_id: "project".into(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
    }
}

fn claimed(actor: ActorKey, revision: u64) -> ClaimedAlarm {
    ClaimedAlarm { actor, revision }
}

async fn with_each_store(test: impl AsyncFn(&dyn AlarmStore) -> Result<()>) -> Result<()> {
    test(&MemoryAlarmStore::default()).await?;
    test(&SqliteAlarmStore::open(":memory:".into()).await?).await?;
    crate::postgres::testing::with_postgres(async |database| {
        test(&PostgresAlarmStore::new(PostgresDatabase::lazy(
            &database.url,
        )?))
        .await
    })
    .await
}

#[tokio::test]
async fn due_alarms_are_claimed_once_while_queued() -> Result<()> {
    with_each_store(async |store| {
        store.update(&actor(), Some(100)).await?;

        assert!(store.claim_due(99, 0, 100).await?.is_empty());
        assert_eq!(
            store.claim_due(100, 0, 100).await?,
            vec![claimed(actor(), 1)]
        );
        assert!(store.claim_due(200, 99, 100).await?.is_empty());
        assert_eq!(store.get(&actor()).await?, Some(100));
        Ok(())
    })
    .await
}

#[tokio::test]
async fn queued_alarms_are_reclaimed_after_their_window_expires() -> Result<()> {
    with_each_store(async |store| {
        store.update(&actor(), Some(100)).await?;
        store.claim_due(100, 0, 100).await?;

        assert_eq!(
            store.claim_due(400, 100, 100).await?,
            vec![claimed(actor(), 1)]
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn latest_update_replaces_or_deletes_an_alarm() -> Result<()> {
    with_each_store(async |store| {
        store.update(&actor(), Some(200)).await?;
        store.update(&actor(), Some(100)).await?;
        assert_eq!(store.get(&actor()).await?, Some(100));

        store.update(&actor(), None).await?;
        assert_eq!(store.get(&actor()).await?, None);
        assert!(store.claim_due(200, 0, 100).await?.is_empty());
        Ok(())
    })
    .await
}

#[tokio::test]
async fn finishing_a_claim_clears_the_alarm() -> Result<()> {
    with_each_store(async |store| {
        store.update(&actor(), Some(100)).await?;
        let alarm = store.claim_due(100, 0, 100).await?.remove(0);

        store.finish(&alarm).await?;

        assert_eq!(store.get(&actor()).await?, None);
        assert!(store.claim_due(1_000, 1_000, 100).await?.is_empty());
        Ok(())
    })
    .await
}

#[tokio::test]
async fn an_alarm_replaced_during_delivery_survives_finish_and_is_claimable() -> Result<()> {
    with_each_store(async |store| {
        store.update(&actor(), Some(100)).await?;
        let alarm = store.claim_due(100, 0, 100).await?.remove(0);

        store.update(&actor(), Some(150)).await?;
        store.finish(&alarm).await?;

        assert_eq!(store.get(&actor()).await?, Some(150));
        assert_eq!(
            store.claim_due(150, 0, 100).await?,
            vec![claimed(actor(), 2)]
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn sqlite_alarms_survive_a_restart_and_release_interrupted_claims() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("alarms.sqlite3");
    let store = SqliteAlarmStore::open(path.clone()).await?;
    store.update(&actor(), Some(100)).await?;
    store.claim_due(100, 0, 100).await?;
    drop(store);

    let store = SqliteAlarmStore::open(path).await?;

    assert_eq!(store.get(&actor()).await?, Some(100));
    assert_eq!(
        store.claim_due(100, 0, 100).await?,
        vec![claimed(actor(), 1)]
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn delivery_retries_in_memory_until_an_attempt_succeeds() -> Result<()> {
    let store = MemoryAlarmStore::default();
    let alarm = due_alarm(&store).await?;
    let delivery = FailingDelivery::new(1);

    deliver(&store, &delivery, &alarm).await;

    assert_eq!(delivery.attempts(), 2);
    assert_eq!(store.get(&actor()).await?, None);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn delivery_gives_up_and_finishes_after_max_attempts() -> Result<()> {
    let store = MemoryAlarmStore::default();
    let alarm = due_alarm(&store).await?;
    let delivery = FailingDelivery::new(usize::MAX);

    deliver(&store, &delivery, &alarm).await;

    assert_eq!(delivery.attempts(), MAX_ATTEMPTS);
    assert_eq!(store.get(&actor()).await?, None);
    Ok(())
}

#[tokio::test]
async fn suspended_deliveries_preserve_the_alarm_without_retries() -> Result<()> {
    struct SuspendedDelivery(AtomicUsize);
    #[async_trait]
    impl AlarmDelivery for SuspendedDelivery {
        async fn attempt(&self, _: &ClaimedAlarm) -> Result<()> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(crate::control_plane::access::AccessDisabled.into())
        }
    }
    let store = MemoryAlarmStore::default();
    let alarm = due_alarm(&store).await?;
    let delivery = SuspendedDelivery(AtomicUsize::new(0));
    deliver(&store, &delivery, &alarm).await;
    assert_eq!(delivery.0.load(Ordering::SeqCst), 1);
    assert_eq!(store.get(&actor()).await?, Some(100));
    assert_eq!(store.claim_due(400, 100, 1).await?, vec![alarm]);
    Ok(())
}

async fn due_alarm(store: &MemoryAlarmStore) -> Result<ClaimedAlarm> {
    store.update(&actor(), Some(100)).await?;
    Ok(store.claim_due(100, 0, 1).await?.remove(0))
}

struct FailingDelivery {
    failures: usize,
    attempts: AtomicUsize,
}

impl FailingDelivery {
    fn new(failures: usize) -> Self {
        Self {
            failures,
            attempts: AtomicUsize::new(0),
        }
    }

    fn attempts(&self) -> usize {
        self.attempts.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl AlarmDelivery for FailingDelivery {
    async fn attempt(&self, _alarm: &ClaimedAlarm) -> Result<()> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
        anyhow::ensure!(attempt >= self.failures, "actor host is unavailable");
        Ok(())
    }
}
