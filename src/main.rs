use std::io::IsTerminal;

use anyhow::Result;
use clap::{Parser, Subcommand};
use durable_actors::{
    control_plane::{
        ControlPlaneProcessConfig, DevOptions, serve_control_plane, serve_local, serve_pool_manager,
    },
    host::serve_actor_host,
};
use tokio::sync::oneshot;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let development_logs = cli.command.is_some()
        || std::env::var("DURABLE_ACTORS_LOG_MODE").as_deref() == Ok("development");
    init_logging(development_logs);
    if let Err(error) = run(cli).await {
        if development_logs {
            error!(error = %format!("{error:#}"), "local actor runtime failed");
        } else {
            error!(error = %format!("{error:#}"), "durable-actors process failed");
        }
        std::process::exit(1);
    }
}

fn init_logging(development: bool) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new(if development {
            "durable_actors=error,durable_actors::dev=info"
        } else {
            "info"
        })
    });
    if development {
        tracing_subscriber::fmt()
            .compact()
            .without_time()
            .with_target(false)
            .with_ansi(std::io::stdout().is_terminal())
            .with_env_filter(filter)
            .init();
    } else {
        tracing_subscriber::fmt()
            .json()
            .flatten_event(true)
            .with_env_filter(filter)
            .init();
    }
}

async fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Some(Commands::Dev(options)) => return serve_local(options, shutdown_signal()).await,
        Some(Commands::CheckpointRestore(options)) => return checkpoint_restore(options).await,
        None => {}
    }
    let shutdown = shutdown_signal();
    match std::env::var("DURABLE_ACTORS_PROCESS_ROLE")
        .as_deref()
        .unwrap_or("host")
    {
        "control_plane" => {
            serve_control_plane(ControlPlaneProcessConfig::from_env()?, shutdown).await
        }
        "pool_manager" => {
            serve_pool_manager(ControlPlaneProcessConfig::from_env()?, shutdown).await
        }
        "spare" => durable_actors::host::serve_spare(shutdown).await,
        "host" => serve_actor_host(shutdown).await,
        role => anyhow::bail!("unsupported DURABLE_ACTORS_PROCESS_ROLE {role:?}"),
    }
}

#[derive(Parser)]
#[command(
    version,
    about = "Run durable TypeScript and Python actors locally or in the cloud"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    #[command(about = "Start local actors with persistent file storage")]
    Dev(DevOptions),
    #[command(
        about = "Copy actor state from the regional S3 checkpoint bucket into an S3 Express bucket"
    )]
    CheckpointRestore(RestoreOptions),
}

#[derive(clap::Args)]
struct RestoreOptions {
    /// The regional (S3 Standard) checkpoint bucket.
    #[arg(long)]
    from: String,
    /// The bucket to restore into, normally a new S3 Express directory bucket in a healthy zone.
    #[arg(long)]
    to: String,
    #[arg(long, env = "DURABLE_ACTORS_S3_REGION")]
    region: String,
    /// Restore only this prefix, e.g. one project's or one actor's objects.
    #[arg(long, default_value = durable_actors::storage_paths::ROOT)]
    prefix: String,
    #[arg(long, default_value_t = 32)]
    concurrency: usize,
}

/// Run with every host stopped (scale actor capacity to zero) so nothing writes `--to` meanwhile;
/// re-running is safe, objects already present at the same or a later generation are kept.
async fn checkpoint_restore(options: RestoreOptions) -> Result<()> {
    use durable_actors::bucket::{S3Bucket, S3Endpoint, checkpoint::restore};
    let endpoint = S3Endpoint {
        region: options.region,
        endpoint: std::env::var("DURABLE_ACTORS_S3_ENDPOINT").ok(),
        force_path_style: std::env::var("DURABLE_ACTORS_S3_FORCE_PATH_STYLE").as_deref()
            == Ok("true"),
    };
    let source = S3Bucket::from_environment(&options.from, &endpoint).await?;
    let target = S3Bucket::from_environment(&options.to, &endpoint).await?;
    let report = restore(&source, &target, &options.prefix, options.concurrency).await?;
    info!(
        event = "checkpoint_restore",
        from = options.from,
        to = options.to,
        prefix = options.prefix,
        listed = report.listed,
        copied = report.copied,
        missing = report.missing,
    );
    println!(
        "restored {} of {} objects under {} ({} vanished while listing)",
        report.copied, report.listed, options.prefix, report.missing
    );
    Ok(())
}

async fn shutdown_signal() {
    if std::env::var_os("DURABLE_ACTORS_PARENT_LIFETIME_STDIN").is_none() {
        wait_for_signal().await;
        info!("shutdown signal received");
        return;
    }
    tokio::select! {
        _ = wait_for_signal() => info!("shutdown signal received"),
        _ = wait_for_parent_stdin_close() => info!("parent process exited"),
    }
}

async fn wait_for_signal() {
    #[cfg(unix)]
    {
        if let Ok(mut terminate) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

async fn wait_for_parent_stdin_close() {
    let (closed, receiver) = oneshot::channel();
    // Tokio's stdin read cannot be cancelled and would block runtime shutdown after a signal.
    std::thread::spawn(move || {
        let _ = std::io::copy(&mut std::io::stdin().lock(), &mut std::io::sink());
        let _ = closed.send(());
    });
    let _ = receiver.await;
}
