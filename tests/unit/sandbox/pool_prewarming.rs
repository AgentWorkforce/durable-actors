use super::*;
use crate::{postgres::testing::with_postgres, sandbox::*};

struct Provider(tokio::sync::mpsc::UnboundedSender<Vec<String>>);

#[async_trait::async_trait]
impl SandboxProvider for Provider {
    async fn prewarm_spares(&self, spares: &[SpareHandle]) {
        if !spares.is_empty() {
            let _ = self
                .0
                .send(spares.iter().map(|spare| spare.name.clone()).collect());
        }
    }

    async fn stopped_spares(&self, _: &[SpareHandle]) -> Result<Vec<String>> {
        Ok(Vec::new())
    }

    async fn ensure_host(&self, _: &EnsureHostRequest) -> Result<ActorHostHandle> {
        anyhow::bail!("unexpected assignment")
    }

    async fn terminate_hosts(&self, _: &TerminateHostsRequest) -> Result<HostTermination> {
        anyhow::bail!("unexpected termination")
    }
}

#[tokio::test]
async fn every_replica_warms_ready_spares_without_claiming_them() -> Result<()> {
    with_postgres(async |fixture| {
        let database = PostgresDatabase::connect(&fixture.url).await?;
        let config = PoolConfig {
            otlp_endpoint: None,
            control_plane_url: None,
            kind: SpareKind::Actor,
            idle: 2,
            fleet_maximum: 4,
            max_starting: 2,
            idle_ttl_seconds: 600,
            refill_interval: Duration::from_secs(1), retire_concurrency: 8, reap_interval: Duration::from_secs(30),
            pod_reap_interval: Duration::from_secs(1),
            regions: vec!["north-america-west".into()],
            resources: ResourceLimits::default(),
        };
        let (send, mut receive) = tokio::sync::mpsc::unbounded_channel();
        let pool = SparePool::new(database.clone(), Arc::new(Provider(send)), config.clone());
        let (other_send, mut other_receive) = tokio::sync::mpsc::unbounded_channel();
        let other = SparePool::new(database.clone(), Arc::new(Provider(other_send)), config);
        let stop = CancellationToken::new();
        let first = tokio::spawn(pool.clone().prewarm(stop.child_token()));
        let second = tokio::spawn(other.prewarm(stop.child_token()));
        let handle = SpareHandle {
            name: "spare".into(),
            resource_id: "sandbox".into(),
            route: "https://invoke.test".into(),
            control_route: "https://control.test".into(),
            control_token: "secret".into(),
            canonical_region: "north-america-west".into(),
        };
        for (name, status, ttl) in [("spare", "ready", 600), ("claimed", "claimed", 600), ("expired", "ready", -1)] {
            let handle = SpareHandle { name: name.into(), ..handle.clone() };
            database.execute(
                "INSERT INTO durable_actors_spares (name, pool_key, kind, status, handle, expires_at) VALUES ($1, 'test', 'actor', $2, $3, clock_timestamp() + make_interval(secs => $4))",
                &[&name, &status, &serde_json::to_string(&handle)?, &f64::from(ttl)],
            ).await?;
        }
        assert_eq!(tokio::time::timeout(Duration::from_secs(3), receive.recv()).await?.unwrap(), ["spare"]);
        assert_eq!(tokio::time::timeout(Duration::from_secs(3), other_receive.recv()).await?.unwrap(), ["spare"]);
        let row = database.query_opt("SELECT status FROM durable_actors_spares WHERE name = 'spare'", &[]).await?.unwrap();
        assert_eq!(row.get::<_, &str>(0), "ready");
        stop.cancel();
        first.await?;
        second.await?;
        Ok(())
    }).await
}
