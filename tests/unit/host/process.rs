use super::*;

fn busy() -> ActorStatus {
    ActorStatus {
        idle_since: None,
        ..Default::default()
    }
}

#[tokio::test]
async fn admitted_requests_prevent_idle_host_shutdown() -> Result<()> {
    let mut server = Box::pin(std::future::pending::<Result<()>>());
    let mut executor = Box::pin(std::future::pending::<Result<()>>());
    let mut shutdown = Box::pin(std::future::pending::<()>());
    let mut javascript = tokio::process::Command::new("sleep")
        .arg("60")
        .kill_on_drop(true)
        .spawn()?;
    let (_lease_sender, mut lease) = tokio::sync::watch::channel(false);
    let (actor, mut status) = tokio::sync::watch::channel(busy());
    let mut stopped = Box::pin(wait_for_host_stop(
        server.as_mut(),
        executor.as_mut(),
        &mut javascript,
        shutdown.as_mut(),
        &mut lease,
        &mut status,
        Duration::from_millis(100),
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(150), stopped.as_mut())
            .await
            .is_err()
    );
    actor.send_modify(|status| status.idle_since = Some(tokio::time::Instant::now()));
    tokio::time::timeout(Duration::from_secs(1), stopped).await??;
    Ok(())
}

#[tokio::test]
async fn failed_activation_stops_a_busy_host() -> Result<()> {
    let mut server = Box::pin(std::future::pending::<Result<()>>());
    let mut executor = Box::pin(std::future::pending::<Result<()>>());
    let mut shutdown = Box::pin(std::future::pending::<()>());
    let mut javascript = tokio::process::Command::new("sleep")
        .arg("60")
        .kill_on_drop(true)
        .spawn()?;
    let (_lease_sender, mut lease) = tokio::sync::watch::channel(false);
    let (actor, mut status) = tokio::sync::watch::channel(busy());
    let mut stopped = Box::pin(wait_for_host_stop(
        server.as_mut(),
        executor.as_mut(),
        &mut javascript,
        shutdown.as_mut(),
        &mut lease,
        &mut status,
        Duration::from_millis(10),
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), stopped.as_mut())
            .await
            .is_err()
    );
    actor.send_modify(|status| status.phase = ActorPhase::Stopped);
    let error = tokio::time::timeout(Duration::from_secs(1), stopped.as_mut())
        .await?
        .unwrap_err();
    assert!(error.to_string().contains("activation stopped"));
    drop(stopped);
    javascript.kill().await?;
    Ok(())
}

#[tokio::test]
async fn an_executor_that_stopped_the_host_is_not_awaited_again() -> Result<()> {
    let mut server = Box::pin(std::future::pending::<Result<()>>());
    let mut executor = Box::pin(async { anyhow::Ok(()) }.fuse());
    let mut shutdown = Box::pin(std::future::pending::<()>());
    let mut javascript = tokio::process::Command::new("sleep")
        .arg("60")
        .kill_on_drop(true)
        .spawn()?;
    let (_lease_sender, mut lease) = tokio::sync::watch::channel(false);
    let (_actor, mut status) = tokio::sync::watch::channel(busy());
    wait_for_host_stop(
        server.as_mut(),
        executor.as_mut(),
        &mut javascript,
        shutdown.as_mut(),
        &mut lease,
        &mut status,
        Duration::from_secs(60),
    )
    .await?;

    tokio::time::timeout(Duration::from_millis(500), executor_stopped(executor)).await?;
    Ok(())
}

#[test]
fn executor_outlasts_a_rapid_fallback() {
    let command = executor_command(true, "/tmp/executor.sock", ActorRuntime::Typescript);
    let startup = command
        .as_std()
        .get_envs()
        .find(|(name, _)| *name == "DURABLE_ACTORS_HOST_STARTUP_MS")
        .and_then(|(_, value)| value);
    assert_eq!(startup, Some(std::ffi::OsStr::new("30000")));
}
