use crate::local_project;

use std::{process::Stdio, time::Duration};

use anyhow::{Context, Result, ensure};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, BufReader},
    process::{Child, ChildStdout, Command},
    time::timeout,
};

#[tokio::test]
#[ignore = "requires bun run --bun --cwd sdk build and Bun"]
async fn local_requests_are_concise_and_human_readable_by_default() -> Result<()> {
    let runtime = LocalRuntime::start(None).await?;
    let client = reqwest::Client::new();
    for (method, path, status) in [
        (reqwest::Method::GET, "/healthz", 200),
        (
            reqwest::Method::POST,
            "/v1/projects/default/actors/Counter/one/find-actor",
            401,
        ),
        (
            reqwest::Method::GET,
            "/v1/projects/default/observe/actors",
            200,
        ),
        (reqwest::Method::GET, "/missing", 404),
    ] {
        let response = client
            .request(
                method,
                format!("{}{path}?token=query-secret", runtime.origin),
            )
            .bearer_auth("header-secret")
            .json(&serde_json::json!({
                "homeRegion": "body-secret"
            }))
            .send()
            .await?;
        assert_eq!(response.status().as_u16(), status, "{path}");
    }
    let output = runtime.stop().await?;
    let requests = request_logs(&output);
    assert_eq!(requests.len(), 4, "missing terminal request logs: {output}");
    for (log, (method, path, status)) in requests.iter().zip([
        ("GET", "/healthz", 200),
        (
            "POST",
            "/v1/projects/default/actors/Counter/one/find-actor",
            401,
        ),
        ("GET", "/v1/projects/default/observe/actors", 200),
        ("GET", "/missing", 404),
    ]) {
        assert!(log.contains("INFO "), "missing level: {log}");
        assert!(
            log.contains(&format!("method={method}")),
            "missing method: {log}"
        );
        assert!(log.contains(&format!("path={path}")), "missing path: {log}");
        assert!(
            log.contains(&format!("status={status}")),
            "missing status: {log}"
        );
        assert!(log.contains("latency_ms="), "missing latency: {log}");
        assert!(
            !log.trim_start().starts_with('{'),
            "development log is JSON: {log}"
        );
    }
    for secret in ["query-secret", "header-secret", "body-secret"] {
        assert!(!output.contains(secret), "request log leaked {secret}");
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires bun run --bun --cwd sdk build and Bun"]
async fn local_request_logs_respect_rust_log() -> Result<()> {
    let runtime = LocalRuntime::start(Some("warn")).await?;
    reqwest::get(format!("{}/healthz", runtime.origin))
        .await?
        .error_for_status()?;
    let output = runtime.stop().await?;
    assert!(request_logs(&output).is_empty(), "{output}");
    Ok(())
}

#[tokio::test]
async fn service_process_logs_remain_structured() -> Result<()> {
    let output = Command::new(env!("CARGO_BIN_EXE_durable-actors"))
        .env("DURABLE_ACTORS_PROCESS_ROLE", "invalid")
        .env_remove("DURABLE_ACTORS_LOG_MODE")
        .env_remove("RUST_LOG")
        .output()
        .await?;
    assert!(!output.status.success());
    let log: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(log["level"], "ERROR");
    assert_eq!(log["message"], "durable-actors process failed");
    assert!(log["error"].as_str().unwrap().contains("unsupported"));
    Ok(())
}

fn request_logs(output: &str) -> Vec<&str> {
    output
        .lines()
        .filter(|line| line.contains("request completed"))
        .collect()
}

struct LocalRuntime {
    _project: tempfile::TempDir,
    child: Child,
    output: BufReader<ChildStdout>,
    origin: String,
}

impl LocalRuntime {
    async fn start(filter: Option<&str>) -> Result<Self> {
        let project = tempfile::tempdir()?;
        local_project::write_actor(project.path(), "async read(): Promise<number> { return 1 }")?;
        let mut command = Command::new(env!("CARGO_BIN_EXE_durable-actors"));
        command
            .args([
                "dev",
                "--project-id",
                "default",
                "--port",
                "0",
                "--entrypoint",
                "actors.ts",
            ])
            .arg("--sdk-host")
            .arg(local_project::sdk_host())
            .arg("--project")
            .arg(project.path())
            .env("DURABLE_ACTORS_PARENT_LIFETIME_STDIN", "1")
            .env("DURABLE_ACTORS_SECRET", "test-key")
            .env_remove("RUST_LOG")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true);
        if let Some(filter) = filter {
            command.env("RUST_LOG", filter);
        }
        let mut child = command.spawn()?;
        let mut output = BufReader::new(child.stdout.take().context("capture runtime output")?);
        let origin = timeout(Duration::from_secs(20), wait_until_ready(&mut output)).await??;
        Ok(Self {
            _project: project,
            child,
            output,
            origin,
        })
    }

    async fn stop(mut self) -> Result<String> {
        drop(self.child.stdin.take());
        let mut output = String::new();
        let (status, _) = timeout(Duration::from_secs(5), async {
            tokio::try_join!(self.child.wait(), self.output.read_to_string(&mut output))
        })
        .await??;
        ensure!(status.success(), "runtime exited with {status}: {output}");
        Ok(output)
    }
}

async fn wait_until_ready(output: &mut BufReader<ChildStdout>) -> Result<String> {
    let mut line = String::new();
    loop {
        ensure!(
            output.read_line(&mut line).await? != 0,
            "runtime exited before readiness: {line}"
        );
        if let Some((_, origin)) = line.split_once("  Ready  ") {
            return Ok(origin.trim().to_owned());
        }
        line.clear();
    }
}
