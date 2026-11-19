use super::*;
use crate::actor_lifecycle::{LifecycleKind, LifecycleReport};
use crate::control_plane::routes::{RedisRoutes, RouteEntry, RouteStore};

#[tokio::test]
async fn shutdown_reports_hold_discovery_until_the_host_stops() -> Result<()> {
    handoff(None).await
}

#[tokio::test]
async fn redis_shutdown_reports_reach_another_control_plane() -> Result<()> {
    let Ok(url) = std::env::var("DURABLE_ACTORS_TEST_REDIS_URL") else {
        return Ok(());
    };
    let redis = crate::redis_client::Redis::connect(&crate::redis_client::RedisConfig {
        url,
        ca_cert: None,
    })?;
    handoff(Some(Arc::new(RedisRoutes::new(
        redis,
        &format!("test:routes:{}", uuid::Uuid::new_v4()),
    ))))
    .await
}

async fn handoff(routes: Option<Arc<dyn RouteStore>>) -> Result<()> {
    let fixture = Fixture::start_with_routes(vec![], true, routes).await?;
    let target = fixture.find_actor().await?;
    let token = target["token"].as_str().unwrap();
    let principal = fixture
        .verifier
        .authenticate_authorization(&format!("Bearer {token}"))?;
    report(&fixture, &principal, LifecycleKind::Draining, 1).await?;

    let mut pending = Box::pin(post_actor(
        &fixture.replica,
        "find-actor",
        "api-key",
        json!({}),
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(350), &mut pending)
            .await
            .is_err()
    );

    report(&fixture, &principal, LifecycleKind::Stopped, 2).await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(350), &mut pending)
            .await
            .is_err()
    );
    let key = principal.actor.storage_key();
    let mut placement = fixture.placements.get_owner(&key).await?.unwrap();
    placement.lease.expires_at_ms = 0;
    fixture
        .placements
        .set_owner(&key, placement.lease, &placement.home_region)?;

    let replacement: Value = tokio::time::timeout(Duration::from_secs(3), pending)
        .await??
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(replacement, json!({"homeRegion":"north-america-east"}));
    let target = fixture.find_actor().await?;
    let next = fixture
        .verifier
        .authenticate_authorization(&format!("Bearer {}", target["token"].as_str().unwrap()))?;
    assert_ne!(principal.session_id, next.session_id);

    report(&fixture, &principal, LifecycleKind::Heartbeat, 1).await?;
    report(&fixture, &principal, LifecycleKind::Draining, 1).await?;
    assert!(matches!(
        fixture.service.routes.get(&principal.actor).await,
        Some(RouteEntry::Active(route)) if route.placement.lease.session_id == next.session_id
    ));
    Ok(())
}

async fn report(
    fixture: &Fixture,
    principal: &ActorPrincipal,
    kind: LifecycleKind,
    sequence: u64,
) -> Result<()> {
    fixture
        .service
        .execute_command(
            principal,
            ControlPlaneCommand::Lifecycle {
                report: LifecycleReport {
                    kind,
                    owner_epoch: 1,
                    sequence,
                    started_at_ms: 1000,
                    occurred_at_ms: sequence * 1000,
                    resources: Default::default(),
                    resident: Some(kind != LifecycleKind::Stopped),
                    last_active_at_ms: None,
                    database_bytes: None,
                    waiting: vec![],
                },
            },
        )
        .await?;
    Ok(())
}
