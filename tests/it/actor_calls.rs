use std::time::Duration;

#[tokio::test]
#[ignore = "requires bun run --bun --cwd sdk build and Bun"]
async fn actors_call_actors_in_their_project_end_to_end() -> anyhow::Result<()> {
    let result = tokio::time::timeout(
        Duration::from_secs(240),
        tokio::process::Command::new("bun")
            .arg("test")
            .arg("--timeout")
            .arg("120000")
            .arg("sdk/tests/scripts/actor-calls.test.mjs")
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .env(
                "DURABLE_ACTORS_BINARY",
                env!("CARGO_BIN_EXE_durable-actors"),
            )
            .kill_on_drop(true)
            .status(),
    )
    .await??;
    anyhow::ensure!(result.success(), "actor call integration test failed");
    Ok(())
}
