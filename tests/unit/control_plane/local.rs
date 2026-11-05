use super::*;
use crate::request_tracking::HostState;

#[test]
fn startup_message_has_clear_hierarchy_and_next_step() {
    let message = local_ready_message(
        "http://127.0.0.1:7100",
        Path::new("/projects/chat/.durable-actors"),
        "local",
    );

    assert!(message.starts_with("durable actors / local\n\n  Ready"));
    assert!(message.contains("Connect your application"));
    assert!(message.contains("Project  local"));
    assert!(message.contains("DURABLE_ACTORS_CONTROL_PLANE_URL=http://127.0.0.1:7100"));
    assert!(message.contains("Authentication is disabled"));
    assert!(
        message.find("1. Configure your client").unwrap()
            < message.find("2. Generate your client").unwrap()
    );
    assert!(!message.contains("cat --"));
    assert!(!message.contains("API key"));
    assert!(!message.contains("export "));
}

#[test]
fn startup_command_uses_the_configured_project_and_port() {
    let message = local_ready_message(
        "http://127.0.0.1:8123",
        Path::new("/projects/chat/.durable-actors"),
        "my-project",
    );
    assert!(message.contains("DURABLE_ACTORS_PROJECT_ID=my-project"));
    assert!(message.contains("DURABLE_ACTORS_CONTROL_PLANE_URL=http://127.0.0.1:8123"));
    assert!(message.contains("     durable-actors generate\n"));
}

#[test]
fn startup_reports_configured_authentication_without_exposing_the_secret() {
    let message = format_local_ready_message(
        "http://127.0.0.1:7100",
        Path::new("/projects/chat/.durable-actors"),
        "local",
        Some("Sam's-$secret #1"),
        LocalReadyStyles::default(),
    );
    assert!(message.contains("Authentication is enabled"));
    assert!(message.contains("DURABLE_ACTORS_SECRET"));
    assert!(!message.contains("Sam's-$secret #1"));
}

#[tokio::test]
async fn relative_state_directories_are_absolute_in_host_configuration() -> Result<()> {
    let cwd = std::env::current_dir()?;
    let directory = tempfile::tempdir_in(&cwd)?;
    let relative = directory.path().strip_prefix(&cwd)?;
    let options = DevOptions {
        watch: false,
        project_id: "default".into(),
        api_key: Some("test-key".into()),
        project: cwd.clone(),
        port: 0,
        data_dir: Some(relative.into()),
        entrypoint: Some("actors.ts".into()),
        storage: DevStorage::Local,
        ready_fd: None,
        sdk_host: None,
    };
    let state = local_storage(&options, relative).await?;
    let config: crate::bucket::access::HostStorageConfig = serde_json::from_str(
        &state
            .access
            .bootstrap(
                &state.region,
                &crate::actor::ActorKey {
                    project_id: "default".into(),
                    actor_name: "Counter".into(),
                    actor_id: "one".into(),
                },
                None,
            )
            .await?,
    )?;
    let BucketLocation::File {
        directory: configured,
    } = config.bucket
    else {
        panic!("expected file bucket")
    };
    assert_eq!(configured, directory.path().canonicalize()?.join("objects"));
    state
        .telemetry
        .0
        .append(&[crate::request_traces::TraceEvent {
            event_id: "event".into(),
            host_id: "host".into(),
            session_id: "session".into(),
            trace: crate::request_traces::RequestTrace {
                state_version: None,
                project_id: "default".into(),
                request_id: "request".into(),
                actor_name: "Counter".into(),
                actor_id: "one".into(),
                kind: crate::request_traces::RequestKind::Method,
                operation: "increment".into(),
                connection_id: None,
                started_at_ms: 1,
                duration_ms: 1.0,
                queue_wait_ms: None,
                host_state: HostState::Warm,
                routing_ms: 0.0,
                outcome: crate::request_traces::RequestOutcome::Completed,
                metadata: None,
            },
        }])
        .await?;
    let event_id = state
        .traces
        .replay("default", &Default::default())
        .await?
        .records[0]
        .event
        .event_id
        .clone();
    drop(state);
    let restored = local_storage(&options, relative).await?;
    assert_eq!(
        restored
            .traces
            .replay("default", &Default::default())
            .await?
            .records[0]
            .event
            .event_id,
        event_id
    );
    Ok(())
}

#[test]
fn actor_source_discovery_supports_both_languages_and_project_layouts() -> Result<()> {
    for entrypoint in ["src/actors.ts", "src/actors.py", "actors.ts", "actors.py"] {
        let root = tempfile::tempdir()?;
        let file = root.path().join(entrypoint);
        std::fs::create_dir_all(file.parent().unwrap())?;
        std::fs::write(file, "")?;
        assert_eq!(resolve_entrypoint(root.path(), None)?, entrypoint);
    }
    let root = tempfile::tempdir()?;
    assert!(resolve_entrypoint(root.path(), None).is_err());
    std::fs::write(root.path().join("actors.ts"), "")?;
    std::fs::write(root.path().join("actors.py"), "")?;
    assert!(
        resolve_entrypoint(root.path(), None)
            .unwrap_err()
            .to_string()
            .contains("Multiple actor sources")
    );
    assert_eq!(
        resolve_entrypoint(root.path(), Some("actors.py"))?,
        "actors.py"
    );
    Ok(())
}
