use axum::{Router, extract::WebSocketUpgrade, response::Response, routing::get};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::oneshot;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use super::*;
use std::collections::HashMap;

#[test]
fn usage_publishing_is_opt_in() -> Result<()> {
    let mut values = process_environment();
    let parse = |values: &HashMap<&str, &str>| {
        ControlPlaneProcessConfig::from_lookup(|name| values.get(name).map(|value| (*value).into()))
    };
    assert!(parse(&values)?.usage_topic.is_none());
    values.insert(
        "DURABLE_ACTORS_USAGE_TOPIC",
        "projects/example/topics/actor-usage",
    );
    assert_eq!(
        parse(&values)?.usage_topic.as_deref(),
        Some("projects/example/topics/actor-usage")
    );
    Ok(())
}

#[test]
fn analytics_defaults_to_postgres_and_clickhouse_is_opt_in() -> Result<()> {
    let mut values = process_environment();
    let parse = |values: &HashMap<&str, &str>| {
        ControlPlaneProcessConfig::from_lookup(|name| values.get(name).map(|value| (*value).into()))
    };
    assert!(matches!(
        parse(&values)?.storage.analytics,
        AnalyticsBackend::Postgres
    ));
    values.insert("DURABLE_ACTORS_ANALYTICS_BACKEND", "clickhouse");
    assert!(parse(&values).is_err());
    values.extend([
        (
            "DURABLE_ACTORS_CLICKHOUSE_URL",
            "https://clickhouse.example:8443",
        ),
        ("DURABLE_ACTORS_CLICKHOUSE_DATABASE", "traces"),
        ("DURABLE_ACTORS_CLICKHOUSE_USER", "writer"),
        ("DURABLE_ACTORS_CLICKHOUSE_PASSWORD", "test-only"),
    ]);
    assert!(matches!(
        parse(&values)?.storage.analytics,
        AnalyticsBackend::ClickHouse(_)
    ));
    values.insert("DURABLE_ACTORS_ANALYTICS_BACKEND", "unknown");
    assert!(parse(&values).is_err());
    Ok(())
}

#[test]
fn configures_gateway_connection_limit() -> Result<()> {
    let mut values = process_environment();
    let parse = |values: &HashMap<&str, &str>| {
        ControlPlaneProcessConfig::from_lookup(|name| values.get(name).map(|value| (*value).into()))
    };
    assert_eq!(parse(&values)?.max_socket_connections, 32768);
    values.insert("DURABLE_ACTORS_SOCKET_MAX_CONNECTIONS", "4096");
    let config = parse(&values)?;
    assert_eq!(config.max_socket_connections, 4096);
    values.insert("DURABLE_ACTORS_SOCKET_MAX_CONNECTIONS", "0");
    assert!(parse(&values).is_err());
    Ok(())
}

#[tokio::test]
async fn server_carries_websocket_upgrades() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let routes = tonic::service::Routes::from(Router::new().route("/socket", get(echo_websocket)));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(serve_routes(listener, routes, async {
        let _ = shutdown_rx.await;
    }));

    let (mut socket, _) = connect_async(format!("ws://{address}/socket")).await?;
    socket.send(Message::Text("hello".into())).await?;
    assert_eq!(
        socket.next().await.transpose()?,
        Some(Message::Text("hello".into()))
    );
    socket.close(None).await?;
    let _ = shutdown_tx.send(());
    server.await??;
    Ok(())
}

#[test]
fn parses_the_minimal_storage_configuration() -> Result<()> {
    let values = process_environment();
    let config = ControlPlaneProcessConfig::from_lookup(|name| {
        values.get(name).map(|value| (*value).into())
    })?;
    assert_eq!(config.storage.bucket, "actor-state-test");
    let resources = &config.sandbox_provider.pool.resources;
    assert_eq!((resources.cpu_millis, resources.memory_mib), (250, 256));
    assert_eq!(resources, &crate::sandbox::ResourceLimits::default());
    assert_eq!(config.sandbox_provider.runtime.host_idle_timeout_ms, 10_000);
    assert_eq!(config.jwt_max_lifetime, Duration::from_secs(86_400));
    assert_eq!(config.api_key.as_deref(), Some("api-key"));
    Ok(())
}

#[test]
fn pool_capacity_is_configurable_and_validated() -> Result<()> {
    let mut values = process_environment();
    let parse = |values: &HashMap<&str, &str>| {
        ControlPlaneProcessConfig::from_lookup(|name| values.get(name).map(|v| (*v).into()))
    };
    let defaults = parse(&values)?.sandbox_provider.pool;
    assert_eq!(
        (defaults.idle, defaults.fleet_maximum, defaults.max_starting),
        (64, 256, 32)
    );
    assert_eq!(defaults.refill_interval, Duration::from_secs(1));
    values.extend([
        ("DURABLE_ACTORS_SPARE_IDLE", "128"),
        ("DURABLE_ACTORS_SPARE_FLEET_MAX", "512"),
        ("DURABLE_ACTORS_SPARE_MAX_STARTING", "4"),
        ("DURABLE_ACTORS_SPARE_REFILL_INTERVAL_MS", "250"),
        ("DURABLE_ACTORS_HOST_CPU_MILLIS", "500"),
    ]);
    let configured = parse(&values)?.sandbox_provider.pool;
    assert_eq!(configured.resources.cpu_millis, 500);
    assert_eq!(configured.refill_interval, Duration::from_millis(250));
    assert_eq!(
        (
            configured.idle,
            configured.fleet_maximum,
            configured.max_starting
        ),
        (128, 512, 4)
    );
    values.insert("DURABLE_ACTORS_SPARE_FLEET_MAX", "1");
    assert!(parse(&values).is_err());
    values.insert("DURABLE_ACTORS_SPARE_FLEET_MAX", "512");
    values.insert("DURABLE_ACTORS_SPARE_MAX_STARTING", "0");
    assert!(parse(&values).is_err());
    values.insert("DURABLE_ACTORS_SPARE_MAX_STARTING", "4");
    values.insert("DURABLE_ACTORS_SPARE_REFILL_INTERVAL_MS", "10");
    assert!(parse(&values).is_err());
    Ok(())
}

#[test]
fn pool_drain_rates_are_configurable_and_validated() -> Result<()> {
    let mut values = process_environment();
    values.insert("DURABLE_ACTORS_SANDBOX_PROVIDER", "gke");
    let parse = |values: &HashMap<&str, &str>| {
        ControlPlaneProcessConfig::from_lookup(|name| values.get(name).map(|v| (*v).into()))
    };
    let defaults = parse(&values)?;
    assert_eq!(defaults.sandbox_provider.pool.retire_concurrency, 64);
    assert_eq!(
        defaults.sandbox_provider.pool.reap_interval,
        Duration::from_secs(30)
    );
    assert_eq!(
        defaults.sandbox_provider.pool.pod_reap_interval,
        Duration::from_secs(1)
    );
    assert_eq!(gke(&defaults).reap_concurrency, 64);
    values.extend([
        ("DURABLE_ACTORS_SPARE_RETIRE_CONCURRENCY", "128"),
        ("DURABLE_ACTORS_SPARE_REAP_INTERVAL_MS", "2000"),
        ("DURABLE_ACTORS_SPARE_POD_REAP_INTERVAL_MS", "500"),
        ("DURABLE_ACTORS_GKE_REAP_CONCURRENCY", "16"),
    ]);
    let configured = parse(&values)?;
    assert_eq!(configured.sandbox_provider.pool.retire_concurrency, 128);
    assert_eq!(
        configured.sandbox_provider.pool.reap_interval,
        Duration::from_secs(2)
    );
    assert_eq!(
        configured.sandbox_provider.pool.pod_reap_interval,
        Duration::from_millis(500)
    );
    assert_eq!(gke(&configured).reap_concurrency, 16);
    for (name, value) in [
        ("DURABLE_ACTORS_SPARE_RETIRE_CONCURRENCY", "0"),
        ("DURABLE_ACTORS_GKE_REAP_CONCURRENCY", "0"),
        ("DURABLE_ACTORS_SPARE_POD_REAP_INTERVAL_MS", "10"),
    ] {
        let mut invalid = values.clone();
        invalid.insert(name, value);
        assert!(parse(&invalid).is_err(), "{name}={value}");
    }
    Ok(())
}

#[test]
fn host_idle_timeout_is_configurable_and_bounded() -> Result<()> {
    for value in ["1", "120000", "86400000"] {
        let mut values = process_environment();
        values.insert("DURABLE_ACTORS_HOST_IDLE_TIMEOUT_MS", value);
        let config = ControlPlaneProcessConfig::from_lookup(|name| {
            values.get(name).map(|value| (*value).into())
        })?;
        assert_eq!(
            config.sandbox_provider.runtime.host_idle_timeout_ms,
            value.parse::<u64>()?
        );
    }
    for value in ["0", "-1", "1.5", "86400001", "not-a-number"] {
        let mut values = process_environment();
        values.insert("DURABLE_ACTORS_HOST_IDLE_TIMEOUT_MS", value);
        assert!(
            ControlPlaneProcessConfig::from_lookup(|name| {
                values.get(name).map(|value| (*value).into())
            })
            .is_err()
        );
    }
    Ok(())
}

#[test]
fn server_configuration_allows_an_unset_secret_on_any_address() -> Result<()> {
    for bind in [
        "127.0.0.1:7100",
        "[::1]:7100",
        "0.0.0.0:7100",
        "[::]:7100",
        "192.168.1.1:7100",
    ] {
        let mut values = process_environment();
        values.remove("DURABLE_ACTORS_SECRET");
        values.insert("DURABLE_ACTORS_CONTROL_PLANE_BIND", bind);
        let config = ControlPlaneProcessConfig::from_lookup(|name| {
            values.get(name).map(|value| (*value).into())
        })?;
        assert!(config.api_key.is_none());
        assert_eq!(config.bind, bind.parse::<SocketAddr>()?);
    }
    Ok(())
}

#[test]
fn server_rejects_an_empty_or_untrimmed_shared_secret() {
    for secret in ["", " ", " key", "key "] {
        let mut values = process_environment();
        values.insert("DURABLE_ACTORS_SECRET", secret);
        let result = ControlPlaneProcessConfig::from_lookup(|name| {
            values.get(name).map(|value| (*value).into())
        });
        assert!(result.is_err(), "server accepted an invalid secret");
    }
}

#[test]
fn invoke_api_key_is_optional_and_must_differ_from_the_shared_secret() -> Result<()> {
    let mut values = process_environment();
    let config = ControlPlaneProcessConfig::from_lookup(|name| {
        values.get(name).map(|value| (*value).into())
    })?;
    assert!(config.invoke_api_key.is_none());
    values.insert("DURABLE_ACTORS_INVOKE_API_KEY", "invoke-key");
    let config = ControlPlaneProcessConfig::from_lookup(|name| {
        values.get(name).map(|value| (*value).into())
    })?;
    assert_eq!(config.invoke_api_key.as_deref(), Some("invoke-key"));
    for invoke_key in ["", " ", " key", "key ", "api-key"] {
        values.insert("DURABLE_ACTORS_INVOKE_API_KEY", invoke_key);
        let result = ControlPlaneProcessConfig::from_lookup(|name| {
            values.get(name).map(|value| (*value).into())
        });
        assert!(result.is_err(), "server accepted invoke key {invoke_key:?}");
    }
    Ok(())
}

#[test]
fn authentication_warning_depends_on_the_listening_address_and_secret() -> Result<()> {
    for (bind, exposed) in [
        ("127.0.0.1:7100", false),
        ("127.0.0.2:7100", false),
        ("[::1]:7100", false),
        ("0.0.0.0:7100", true),
        ("[::]:7100", true),
        ("192.168.1.1:7100", true),
    ] {
        for secret in [None, Some("configured-secret")] {
            let output = tempfile::NamedTempFile::new()?;
            let subscriber = tracing_subscriber::fmt()
                .without_time()
                .with_ansi(false)
                .with_max_level(tracing::Level::WARN)
                .with_writer(output.reopen()?)
                .finish();
            tracing::subscriber::with_default(subscriber, || {
                warn_if_authentication_disabled(bind.parse().unwrap(), secret);
            });
            let logs = std::fs::read_to_string(output.path())?;
            if exposed && secret.is_none() {
                assert!(
                    logs.contains("Authentication is disabled"),
                    "{bind}: {logs}"
                );
                assert!(logs.contains(bind));
                assert!(logs.contains("DURABLE_ACTORS_SECRET"));
            } else {
                assert!(logs.is_empty(), "unexpected warning at {bind}: {logs}");
            }
        }
    }
    Ok(())
}

#[test]
fn rapid_configuration_uses_two_zones_and_gke() -> Result<()> {
    let values = process_environment();
    let config = ControlPlaneProcessConfig::from_lookup(|name| {
        values.get(name).map(|value| (*value).into())
    })?;
    assert!(matches!(
        config.storage.persistence,
        crate::bucket::SnapshotStoreConfig::Rapid { .. }
    ));
    assert_eq!(gke(&config).zones["north-america-west"], vec!["us-west4-a"]);
    Ok(())
}

#[test]
fn compute_region_accepts_multiple_zones_and_rejects_empty_or_mismatched_sets() -> Result<()> {
    let mut values = process_environment();
    values.insert(
        "DURABLE_ACTORS_GKE_ZONES",
        r#"{"north-america-west":["us-west4-a","us-west4-b","us-west4-c"]}"#,
    );
    let parse = |values: &HashMap<&str, &str>| {
        ControlPlaneProcessConfig::from_lookup(|name| values.get(name).map(|value| (*value).into()))
    };
    parse(&values)?;
    for zones in [
        r#"{"north-america-west":[]}"#,
        r#"{"north-america-west":["us-west4-a","us-east4-b"]}"#,
        r#"{"north-america-west":["us-west4-a","us-west4-a"]}"#,
    ] {
        values.insert("DURABLE_ACTORS_GKE_ZONES", zones);
        assert!(parse(&values).is_err(), "{zones}");
    }
    Ok(())
}

#[test]
fn configures_socket_events_without_a_separate_key() -> Result<()> {
    let mut complete = HashMap::from([(
        "DURABLE_ACTORS_SOCKET_EVENT_URL",
        "https://api.example.com/events",
    )]);
    let sink =
        socket_event_sink_config(&mut |name| complete.get(name).map(|value| (*value).into()))?
            .context("socket event sink was not configured")?;
    assert_eq!(sink.url, "https://api.example.com/events");
    complete.remove("DURABLE_ACTORS_SOCKET_EVENT_URL");
    assert!(
        socket_event_sink_config(&mut |name| complete.get(name).map(|value| (*value).into()))?
            .is_none()
    );
    Ok(())
}

async fn echo_websocket(upgrade: WebSocketUpgrade) -> Response {
    upgrade.on_upgrade(async |mut socket| {
        if let Some(Ok(message)) = socket.recv().await {
            let _ = socket.send(message).await;
        }
    })
}

fn process_environment() -> HashMap<&'static str, &'static str> {
    HashMap::from([
        ("DURABLE_ACTORS_PERSISTENCE", "rapid"),
        ("DURABLE_ACTORS_JWT_SIGNING_KEY", "c2lnbmluZw=="),
        ("DURABLE_ACTORS_SECRET", "api-key"),
        ("DURABLE_ACTORS_BUCKET", "actor-state-test"),
        (
            "DURABLE_ACTORS_TYPESCRIPT_IMAGE",
            "registry.example/runtime@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ),
        (
            "DURABLE_ACTORS_PYTHON_IMAGE",
            "registry.example/python@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        ),
        ("DURABLE_ACTORS_ARTIFACT_BUCKET", "customer-code"),
        ("DURABLE_ACTORS_ARCHIVE_BUCKET", "actor-archive"),
        (
            "DURABLE_ACTORS_RAPID_BUCKETS",
            r#"[{"bucket":"rapid-test-a","zone":"us-west4-a"},{"bucket":"rapid-test-b","zone":"us-west4-b"}]"#,
        ),
        (
            "DURABLE_ACTORS_GKE_ZONES",
            r#"{"north-america-west":"us-west4-a"}"#,
        ),
        ("DURABLE_ACTORS_PUBLIC_URL", "https://actors.example.com"),
        (
            "DURABLE_ACTORS_CONTROL_PLANE_URL",
            "https://objects.example.com",
        ),
        (
            "DURABLE_ACTORS_POSTGRES_URL",
            "postgresql://localhost/actors",
        ),
    ])
}

#[test]
fn postgres_pool_size_is_configurable_and_bounded() -> Result<()> {
    let mut values = process_environment();
    let parse = |values: &HashMap<&str, &str>| {
        ControlPlaneProcessConfig::from_lookup(|name| values.get(name).map(|value| (*value).into()))
    };
    assert_eq!(parse(&values)?.storage.postgres_max_connections, 8);
    for (value, expected) in [("1", 1), ("64", 64), ("1000", 1000)] {
        values.insert("DURABLE_ACTORS_POSTGRES_MAX_CONNECTIONS", value);
        assert_eq!(parse(&values)?.storage.postgres_max_connections, expected);
    }
    for invalid in ["0", "-1", "1.5", "1001", "", "oops", "18446744073709551616"] {
        values.insert("DURABLE_ACTORS_POSTGRES_MAX_CONNECTIONS", invalid);
        let error = parse(&values)
            .err()
            .expect("invalid pool size was accepted");
        assert!(
            error
                .to_string()
                .contains("DURABLE_ACTORS_POSTGRES_MAX_CONNECTIONS")
        );
    }
    Ok(())
}

#[test]
fn analytics_retention_is_configurable_and_bounded() -> Result<()> {
    let mut values = process_environment();
    let parse = |values: &HashMap<&str, &str>| {
        ControlPlaneProcessConfig::from_lookup(|name| values.get(name).map(|value| (*value).into()))
    };
    assert_eq!(
        parse(&values)?.storage.trace_retention,
        Duration::from_secs(30 * 86400)
    );
    values.insert("DURABLE_ACTORS_ANALYTICS_RETENTION_DAYS", "7");
    assert_eq!(
        parse(&values)?.storage.trace_retention,
        Duration::from_secs(7 * 86400)
    );
    for invalid in ["0", "-1", "1.5", "3651", ""] {
        values.insert("DURABLE_ACTORS_ANALYTICS_RETENTION_DAYS", invalid);
        assert!(parse(&values).is_err());
    }
    Ok(())
}

#[test]
fn archive_batch_triggers_are_configurable_and_positive() -> Result<()> {
    let mut values = process_environment();
    let parse = |values: &HashMap<&str, &str>| {
        ControlPlaneProcessConfig::from_lookup(|name| values.get(name).map(|value| (*value).into()))
    };
    let defaults = parse(&values)?.storage.persistence;
    let crate::bucket::SnapshotStoreConfig::Rapid { archive_batch, .. } = defaults.clone() else {
        panic!()
    };
    assert_eq!(archive_batch.bytes, 16 * 1024 * 1024);
    assert_eq!(archive_batch.interval_ms, 10_000);
    values.insert("DURABLE_ACTORS_ARCHIVE_BATCH_BYTES", "2097152");
    values.insert("DURABLE_ACTORS_ARCHIVE_BATCH_INTERVAL_MS", "250");
    let configured = parse(&values)?.storage.persistence;
    let crate::bucket::SnapshotStoreConfig::Rapid { archive_batch, .. } = configured.clone() else {
        panic!()
    };
    assert_eq!(archive_batch.bytes, 2 * 1024 * 1024);
    assert_eq!(archive_batch.interval_ms, 250);
    assert!(configured.same_backend(&defaults));
    assert_eq!(
        serde_json::from_value::<crate::bucket::SnapshotStoreConfig>(serde_json::to_value(
            &configured
        )?)?,
        configured
    );
    for name in [
        "DURABLE_ACTORS_ARCHIVE_BATCH_BYTES",
        "DURABLE_ACTORS_ARCHIVE_BATCH_INTERVAL_MS",
    ] {
        for invalid in ["0", "-1", "1.5", ""] {
            let mut bad = values.clone();
            bad.insert(name, invalid);
            assert!(parse(&bad).is_err(), "accepted {name}={invalid}");
        }
    }
    Ok(())
}

#[test]
fn default_region_requires_a_configured_compute_zone() -> Result<()> {
    let mut values = process_environment();
    let parse = |values: &HashMap<&str, &str>| {
        ControlPlaneProcessConfig::from_lookup(|name| values.get(name).map(|value| (*value).into()))
    };
    assert_eq!(
        parse(&values)?.region.as_deref(),
        Some("north-america-west")
    );
    values.insert("DURABLE_ACTORS_REGION", "north-america-east");
    assert!(parse(&values).is_err());
    values.remove("DURABLE_ACTORS_REGION");
    values.insert(
        "DURABLE_ACTORS_GKE_ZONES",
        r#"{"north-america-west":"us-west4-b"}"#,
    );
    assert!(parse(&values).is_ok());
    Ok(())
}

#[test]
fn standard_gcs_starts_with_one_bucket_and_no_rapid_configuration() -> Result<()> {
    let mut values = process_environment();
    values.remove("DURABLE_ACTORS_PERSISTENCE");
    values.remove("DURABLE_ACTORS_ARCHIVE_BUCKET");
    values.remove("DURABLE_ACTORS_RAPID_BUCKETS");
    values.remove("DURABLE_ACTORS_ARTIFACT_BUCKET");
    let config = ControlPlaneProcessConfig::from_lookup(|name| {
        values.get(name).map(|value| (*value).into())
    })?;
    assert_eq!(config.storage.artifact_bucket, config.storage.bucket);
    assert_eq!(
        config.storage.persistence,
        crate::bucket::SnapshotStoreConfig::Local
    );
    Ok(())
}

#[test]
fn persistence_selection_rejects_typos_and_conflicting_rapid_configuration() {
    for mode in ["standrad", "", "standard"] {
        let mut values = process_environment();
        values.insert("DURABLE_ACTORS_PERSISTENCE", mode);
        assert!(
            ControlPlaneProcessConfig::from_lookup(|name| {
                values.get(name).map(|value| (*value).into())
            })
            .is_err()
        );
    }
}

#[test]
fn rapid_placement_defaults_to_every_zone_and_accepts_nearest() -> Result<()> {
    let mut values = process_environment();
    let parse = |values: &HashMap<&str, &str>| {
        ControlPlaneProcessConfig::from_lookup(|name| values.get(name).map(|value| (*value).into()))
    };
    let placement = |values: &HashMap<&str, &str>| -> Result<_> {
        match parse(values)?.storage.persistence {
            crate::bucket::SnapshotStoreConfig::Rapid { placement, .. } => Ok(placement),
            crate::bucket::SnapshotStoreConfig::Local => anyhow::bail!("expected Rapid"),
        }
    };
    assert_eq!(placement(&values)?, crate::bucket::RapidPlacement::All);
    values.insert("DURABLE_ACTORS_RAPID_PLACEMENT", "nearest");
    assert_eq!(placement(&values)?, crate::bucket::RapidPlacement::Nearest);
    values.insert("DURABLE_ACTORS_RAPID_PLACEMENT", "closest");
    assert!(parse(&values).is_err());
    values.insert("DURABLE_ACTORS_RAPID_PLACEMENT", "nearest");
    values.insert(
        "DURABLE_ACTORS_RAPID_BUCKETS",
        r#"[{"bucket":"rapid-test-a","zone":"us-west4-a"}]"#,
    );
    assert!(parse(&values).is_err());
    for name in [
        "DURABLE_ACTORS_PERSISTENCE",
        "DURABLE_ACTORS_ARCHIVE_BUCKET",
        "DURABLE_ACTORS_RAPID_BUCKETS",
    ] {
        values.remove(name);
    }
    assert!(parse(&values).is_err());
    Ok(())
}

#[test]
fn single_gke_zone_infers_the_actor_region() -> Result<()> {
    let mut values = process_environment();
    values.remove("DURABLE_ACTORS_GKE_ZONES");
    values.insert("DURABLE_ACTORS_GKE_ZONE", "us-east4-a");
    let config =
        ControlPlaneProcessConfig::from_lookup(|name| values.get(name).map(|v| (*v).into()))?;
    assert_eq!(config.region.as_deref(), Some("north-america-east"));
    assert_eq!(gke(&config).zones["north-america-east"], ["us-east4-a"]);
    values.insert(
        "DURABLE_ACTORS_GKE_ZONES",
        r#"{"north-america-west":"us-west4-a"}"#,
    );
    assert!(
        ControlPlaneProcessConfig::from_lookup(|name| values.get(name).map(|v| (*v).into()))
            .is_err()
    );
    Ok(())
}

#[test]
fn compiled_artifacts_select_their_language_image() -> Result<()> {
    let values = process_environment();
    let config = ControlPlaneProcessConfig::from_lookup(|name| {
        values.get(name).map(|value| (*value).into())
    })?;
    let images = &config.sandbox_provider.runtime_images;
    assert_eq!(
        images.for_entrypoint("actors.mjs")?,
        values["DURABLE_ACTORS_TYPESCRIPT_IMAGE"]
    );
    assert_eq!(
        images.for_entrypoint("actors.pyz")?,
        values["DURABLE_ACTORS_PYTHON_IMAGE"]
    );
    assert!(images.for_entrypoint("actors.ts").is_err());
    Ok(())
}

#[test]
fn replicas_share_caches_through_redis_when_configured() -> Result<()> {
    let mut values = process_environment();
    let parse = |values: &HashMap<&str, &str>| {
        ControlPlaneProcessConfig::from_lookup(|name| values.get(name).map(|value| (*value).into()))
    };
    assert!(parse(&values)?.redis.is_none());

    values.insert("DURABLE_ACTORS_REDIS_URL", "rediss://:secret@10.0.0.5:6378");
    values.insert(
        "DURABLE_ACTORS_REDIS_CA_CERT",
        "-----BEGIN CERTIFICATE-----",
    );
    let redis = parse(&values)?.redis.unwrap();
    assert_eq!(redis.url, "rediss://:secret@10.0.0.5:6378");
    assert_eq!(
        redis.ca_cert.as_deref(),
        Some("-----BEGIN CERTIFICATE-----")
    );

    values.insert("DURABLE_ACTORS_REDIS_URL", "redis://:secret@10.0.0.5:6379");
    let error = format!("{:#}", parse(&values).err().unwrap());
    assert!(
        error.contains("rediss://") && !error.contains("secret"),
        "{error}"
    );
    values.insert("DURABLE_ACTORS_REDIS_URL", "http://:secret@10.0.0.5");
    let error = format!("{:#}", parse(&values).err().unwrap());
    assert!(!error.contains("secret"), "{error}");
    values.insert("DURABLE_ACTORS_REDIS_URL", " ");
    assert!(parse(&values).is_err());
    values.remove("DURABLE_ACTORS_REDIS_CA_CERT");
    assert!(parse(&values)?.redis.is_none());
    Ok(())
}

fn gke(config: &ControlPlaneProcessConfig) -> &GkeConfig {
    match &config.sandbox_provider.backend {
        SandboxBackend::Gke(gke) => gke,
        _ => panic!("expected GKE"),
    }
}

#[test]
fn modal_configuration_does_not_require_kubernetes() -> Result<()> {
    let mut values = process_environment();
    values.remove("DURABLE_ACTORS_GKE_ZONES");
    values.remove("DURABLE_ACTORS_GKE_ZONE");
    values.extend([
        ("DURABLE_ACTORS_SANDBOX_PROVIDER", "modal"),
        ("DURABLE_ACTORS_MODAL_APP", "modal-test"),
        (
            "DURABLE_ACTORS_CONTROL_PLANE_URL",
            "https://control.example.com",
        ),
        (
            "DURABLE_ACTORS_MODAL_REGIONS",
            r#"{"north-america-west":"us-west"}"#,
        ),
    ]);
    let parse = |values: &HashMap<&str, &str>| {
        ControlPlaneProcessConfig::from_lookup(|name| values.get(name).map(|v| (*v).into()))
    };
    let config = parse(&values)?;
    assert_eq!(config.region.as_deref(), Some("north-america-west"));
    assert_eq!(config.sandbox_provider.pool.regions, ["north-america-west"]);
    assert_eq!(config.sandbox_provider.secret_namespace, "terse-sandboxes");
    let SandboxBackend::Modal(modal) = config.sandbox_provider.backend else {
        panic!("expected Modal")
    };
    assert_eq!(modal.app_name, "modal-test");
    assert_eq!(modal.timeout, Duration::from_secs(3600));
    values.insert("DURABLE_ACTORS_GKE_NAMESPACE", "project-secrets");
    assert_eq!(
        parse(&values)?.sandbox_provider.secret_namespace,
        "project-secrets"
    );
    for (key, value) in [
        ("DURABLE_ACTORS_SANDBOX_PROVIDER", "unknown"),
        ("DURABLE_ACTORS_CONTROL_PLANE_URL", "http://localhost:7100"),
        (
            "DURABLE_ACTORS_CONTROL_PLANE_URL",
            "https://control.example.com/path",
        ),
        ("DURABLE_ACTORS_MODAL_REGIONS", "{}"),
        (
            "DURABLE_ACTORS_MODAL_REGIONS",
            r#"{"north-america-west":"us-east"}"#,
        ),
        ("DURABLE_ACTORS_MODAL_TIMEOUT_SECONDS", "0"),
    ] {
        let mut invalid = values.clone();
        invalid.insert(key, value);
        assert!(parse(&invalid).is_err(), "accepted {key}={value}");
    }
    Ok(())
}
