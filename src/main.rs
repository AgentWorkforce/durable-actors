use anyhow::Result;
use clap::{Parser, Subcommand};
use durable_actors::{
    control_plane::{
        ControlPlaneProcessConfig, DevOptions, serve_control_plane, serve_local, serve_pool_manager,
    },
    entrypoint::{init_logging, shutdown_signal},
};
use tracing::error;

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
        "node_cache" => durable_actors::node_cache::serve_node_cache(shutdown).await,
        role => durable_actors::host::serve_role(role, shutdown).await,
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
