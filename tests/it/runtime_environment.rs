use crate::local_project;

use std::{process::Stdio, time::Duration};

use anyhow::{Context, Result, ensure};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, BufReader},
    process::Command,
    time::timeout,
};

fn runtime() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_durable-actors"));
    for (name, _) in std::env::vars_os() {
        if name
            .to_str()
            .is_some_and(|name| name.starts_with("DURABLE_ACTORS_"))
        {
            command.env_remove(name);
        }
    }
    command.env("RUST_LOG", "info");
    command
}

#[tokio::test]
async fn process_role_rejects_unsupported_roles() -> Result<()> {
    let output = runtime()
        .env("DURABLE_ACTORS_PROCESS_ROLE", "invalid-role")
        .output()
        .await?;
    let log: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert!(log["error"].as_str().unwrap().contains("invalid-role"));
    Ok(())
}

#[tokio::test]
async fn control_plane_rejects_invalid_bind_addresses() -> Result<()> {
    let output = runtime()
        .envs([
            ("DURABLE_ACTORS_PROCESS_ROLE", "control_plane"),
            ("DURABLE_ACTORS_CONTROL_PLANE_BIND", "invalid-bind"),
        ])
        .output()
        .await?;
    let log: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert!(
        log["error"]
            .as_str()
            .unwrap()
            .contains("must be a socket address")
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires bun run --bun --cwd sdk build and Bun"]
async fn local_runtime_uses_configured_options_secret_and_parent_lifetime() -> Result<()> {
    let project = tempfile::tempdir()?;
    local_project::write_actor(project.path(), "async read(): Promise<number> { return 1 }")?;
    let mut child = runtime()
        .arg("dev")
        .arg("--sdk-host")
        .arg(local_project::sdk_host())
        .env("DURABLE_ACTORS_PROJECT", project.path())
        .envs([
            ("DURABLE_ACTORS_PROJECT_ID", "test-project"),
            ("DURABLE_ACTORS_PORT", "0"),
            ("DURABLE_ACTORS_ENTRYPOINT", "actors.ts"),
            ("DURABLE_ACTORS_PARENT_LIFETIME_STDIN", "1"),
            ("DURABLE_ACTORS_SECRET", "preferred-secret"),
            ("DURABLE_ACTORS_API_KEY", "alternate-key"),
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let mut output = BufReader::new(child.stdout.take().context("capture runtime output")?);
    let origin = timeout(Duration::from_secs(10), async {
        let mut line = String::new();
        let mut startup = String::new();
        loop {
            ensure!(
                output.read_line(&mut line).await? != 0,
                "runtime exited: {startup}"
            );
            if let Some((_, origin)) = line.split_once("  Ready  ") {
                return Ok::<_, anyhow::Error>(origin.trim().to_owned());
            }
            startup.push_str(&line);
            line.clear();
        }
    })
    .await??;
    let client = reqwest::Client::new();
    for (key, authorized) in [("preferred-secret", true), ("alternate-key", false)] {
        let response = client
            .get(format!(
                "{origin}/v1/projects/test-project/deployment/contract"
            ))
            .bearer_auth(key)
            .send()
            .await?;
        assert_eq!(response.status().is_success(), authorized);
        if !authorized {
            assert_eq!(response.status(), 401);
        }
    }
    drop(child.stdin.take());
    let mut remaining = String::new();
    let (status, _) = timeout(Duration::from_secs(10), async {
        tokio::try_join!(child.wait(), output.read_to_string(&mut remaining))
    })
    .await??;
    ensure!(
        status.success(),
        "runtime exited with {status}: {remaining}"
    );
    Ok(())
}
