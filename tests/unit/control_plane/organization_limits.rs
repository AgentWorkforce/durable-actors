use super::*;
use crate::{
    actor_lifecycle::{LifecycleKind, LifecycleReport, LifecycleStore},
    clock::Clock,
    postgres::testing::with_postgres,
    redis_client::{Redis, RedisConfig},
};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

const RETENTION: Duration = Duration::from_secs(86_460);

#[tokio::test]
async fn routing_preserves_suspension_and_admission_errors() -> Result<()> {
    use crate::control_plane::{access::AccessDisabled, public_api::ApiError};
    use axum::{body::to_bytes, response::IntoResponse};

    for (error, status, code, retry_after) in [
        (
            anyhow::Error::new(AccessDisabled),
            403,
            "access_disabled",
            None,
        ),
        (
            anyhow::Error::new(AdmissionRejected::Quota { limit: 500 }).context(AdmissionFailure),
            429,
            "actor_quota_exceeded",
            None,
        ),
        (
            anyhow::Error::new(AdmissionRejected::Rate {
                retry_after_seconds: 2,
            })
            .context(AdmissionFailure),
            429,
            "actor_start_rate_limited",
            Some("2"),
        ),
    ] {
        let response = ApiError::routing(error).into_response();
        assert_eq!(response.status().as_u16(), status);
        assert_eq!(
            response
                .headers()
                .get("retry-after")
                .map(|v| v.to_str().unwrap()),
            retry_after
        );
        if status == 429 {
            assert_eq!(response.headers()["cache-control"], "no-store");
        }
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body)?["error"]["code"],
            code
        );
    }
    Ok(())
}

#[test]
fn limits_are_enabled_hosted_and_opt_in_locally() -> Result<()> {
    assert_eq!(
        OrganizationLimits::from_lookup(&mut |_| None, true)?,
        Some(limits(500, 50, 100))
    );
    assert!(local_admission(&mut |_| None)?.is_none());
    assert!(
        local_admission(
            &mut |key| (key == "DURABLE_ACTORS_ORGANIZATION_LIMITS_ENABLED").then(|| "true".into())
        )?
        .is_some()
    );
    Ok(())
}

#[tokio::test]
async fn organization_projects_share_capacity_until_the_actor_stops() -> Result<()> {
    for store in stores()? {
        let limits = limits(1, 50, 100);
        let first = activation("o", "p", "one", "first");
        let second = activation("o", "q", "two", "second");
        store.reserve(&first, limits).await?;
        store.reserve(&first, limits).await?;
        assert_quota(store.reserve(&second, limits).await, 1);
        let lifecycle = LifecycleStore::default()
            .with_observer(Arc::new(ActorAdmission::new(limits, store.clone())));
        lifecycle
            .record(event(&first, LifecycleKind::Stopped))
            .await?;
        store.reserve(&second, limits).await?;
        store
            .reserve(&activation("other", "p", "one", "first"), limits)
            .await?;
    }
    Ok(())
}

#[tokio::test]
async fn lifecycle_reports_without_an_organization_are_rejected() -> Result<()> {
    let store = Arc::new(MemoryReservations::new(
        Arc::new(TestClock(AtomicU64::new(1000))),
        RETENTION,
    ));
    let admission = ActorAdmission::new(limits(1, 50, 100), store);
    let mut orphan = event(
        &activation("o", "p", "one", "first"),
        LifecycleKind::Started,
    );
    orphan.organization_id = None;
    let error = admission.observe(&orphan).await.unwrap_err();
    assert_eq!(error.to_string(), "lifecycle report has no organization");
    Ok(())
}

#[tokio::test]
async fn releasing_a_failed_launch_frees_capacity() -> Result<()> {
    for store in stores()? {
        let limits = limits(1, 50, 100);
        let failed = activation("o", "p", "one", "failed");
        let next = activation("o", "p", "two", "next");
        store.reserve(&failed, limits).await?;
        assert_quota(store.reserve(&next, limits).await, 1);
        store.release(&failed).await?;
        store.reserve(&next, limits).await?;
    }
    Ok(())
}

#[tokio::test]
async fn stopping_frees_capacity_but_waking_again_consumes_another_start_token() -> Result<()> {
    let clock = Arc::new(TestClock(AtomicU64::new(1000)));
    let store = MemoryReservations::new(clock.clone(), RETENTION);
    let limits = limits(1, 1, 1);
    let first = activation("o", "p", "same-actor", "first");
    let second = activation("o", "p", "same-actor", "second");
    store.reserve(&first, limits).await?;
    store
        .observe(&first, &event(&first, LifecycleKind::Stopped).report)
        .await?;
    assert_rate(store.reserve(&second, limits).await);
    clock.0.store(2000, Ordering::SeqCst);
    store.reserve(&second, limits).await?;
    Ok(())
}

#[tokio::test]
async fn slots_expire_unless_reports_renew_them() -> Result<()> {
    let clock = Arc::new(TestClock(AtomicU64::new(0)));
    let store = MemoryReservations::new(clock.clone(), RETENTION);
    let limits = limits(1, 50, 100);
    let live = activation("o", "p", "one", "live");
    let lost = activation("o", "p", "two", "lost");
    let next = activation("o", "p", "three", "next");
    store.reserve(&live, limits).await?;
    clock.0.store(LAUNCH_LEASE_MS - 1, Ordering::SeqCst);
    store
        .observe(&live, &event(&live, LifecycleKind::Heartbeat).report)
        .await?;
    clock.0.store(LAUNCH_LEASE_MS + 1, Ordering::SeqCst);
    assert_quota(store.reserve(&next, limits).await, 1);
    clock
        .0
        .store(LAUNCH_LEASE_MS - 1 + REPORT_LEASE_MS, Ordering::SeqCst);
    store.reserve(&lost, limits).await?;
    clock.0.fetch_add(LAUNCH_LEASE_MS, Ordering::SeqCst);
    store.reserve(&next, limits).await?;
    Ok(())
}

#[tokio::test]
async fn postgres_overrides_inherit_unset_defaults() -> Result<()> {
    with_postgres(async |fixture| {
        let database = PostgresDatabase::connect(&fixture.url).await?;
        let defaults = OrganizationLimits::default();
        let reservations = MemoryReservations::new(Arc::new(crate::clock::SystemClock), RETENTION);
        let admission = ActorAdmission::new(defaults, Arc::new(reservations)).with_database(database.clone());
        database.execute("INSERT INTO durable_actors_organization_limits (organization_id, max_active_actors) VALUES ('o', 20000)", &[]).await?;
        assert_eq!(admission.limits("o").await?, limits(20_000, 50, 100));
        assert_eq!(admission.limits("other").await?, defaults);
        Ok(())
    }).await
}

fn stores() -> Result<Vec<Arc<dyn ReservationStore>>> {
    let mut stores: Vec<Arc<dyn ReservationStore>> = vec![Arc::new(MemoryReservations::new(
        Arc::new(TestClock(AtomicU64::new(1000))),
        RETENTION,
    ))];
    if let Ok(url) = std::env::var("DURABLE_ACTORS_TEST_REDIS_URL") {
        let redis = Redis::connect(&RedisConfig { url, ca_cert: None })?;
        stores.push(Arc::new(RedisReservations::new(
            redis,
            &format!("admission-test:{}", uuid::Uuid::new_v4()),
            RETENTION,
        )));
    }
    Ok(stores)
}

fn limits(max_active_actors: u32, starts_per_second: u32, start_burst: u32) -> OrganizationLimits {
    OrganizationLimits {
        max_active_actors,
        starts_per_second,
        start_burst,
    }
}

fn activation(organization: &str, project: &str, actor: &str, session: &str) -> Activation {
    Activation::new(
        organization,
        &ActorKey {
            project_id: project.into(),
            actor_name: "Counter".into(),
            actor_id: actor.into(),
        },
        &format!("host-{session}"),
        session,
    )
}

fn event(activation: &Activation, kind: LifecycleKind) -> LifecycleEvent {
    LifecycleEvent {
        organization_id: Some(activation.organization.clone()),
        event_id: format!("{}:{kind:?}", activation.session),
        actor: activation.actor.clone(),
        host_id: activation.host.clone(),
        session_id: activation.session.clone(),
        region: "north-america-east".into(),
        received_at_ms: 1000,
        report: LifecycleReport {
            kind,
            owner_epoch: 1,
            sequence: 1,
            started_at_ms: 1000,
            occurred_at_ms: 1000,
            resources: Default::default(),
            resident: Some(true),
            last_active_at_ms: None,
            database_bytes: None,
            waiting: vec![],
        },
    }
}

fn assert_quota(result: Result<()>, limit: u32) {
    assert!(
        matches!(result.unwrap_err().downcast_ref(), Some(AdmissionRejected::Quota { limit: actual }) if *actual == limit)
    );
}

fn assert_rate(result: Result<()>) {
    assert!(matches!(
        result.unwrap_err().downcast_ref(),
        Some(AdmissionRejected::Rate {
            retry_after_seconds: 1
        })
    ));
}

struct TestClock(AtomicU64);
impl Clock for TestClock {
    fn now_ms(&self) -> Result<u64> {
        Ok(self.0.load(Ordering::SeqCst))
    }
}
