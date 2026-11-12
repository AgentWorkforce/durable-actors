use std::panic::AssertUnwindSafe;

use futures_util::FutureExt;

use super::*;
use testing::{TestDatabase, with_postgres, with_postgres_schema};

#[tokio::test]
async fn fixtures_isolate_migrations_data_and_reconnections() -> Result<()> {
    with_postgres(async |first| {
        with_postgres(async |second| {
            let first_client = first.pool.get().await?;
            let another_client = first.pool.get().await?;
            let second_client = second.pool.get().await?;
            let first_schema = current_schema(&first_client).await?;
            let second_schema = current_schema(&second_client).await?;
            assert_eq!(first_schema, current_schema(&another_client).await?);
            assert_ne!(first_schema, second_schema);
            check_isolated_rows(&first_client, &second_client).await?;
            check_isolated_migrations(&first_client, &second_client).await?;
            check_reconnected_fixture(&second.url, &second_schema).await
        })
        .await
    })
    .await
}

#[tokio::test]
async fn fixtures_cleanup_after_success_error_and_panic() -> Result<()> {
    with_postgres(async |observer| {
        let client = observer.pool.get().await?;
        for outcome in ["success", "error", "panic"] {
            check_fixture_cleanup(&client, outcome).await?;
        }
        Ok(())
    })
    .await
}

#[test]
fn embedded_migrations_have_no_version_gaps() {
    let runner = embedded::migrations::runner();
    let mut versions: Vec<_> = runner
        .get_migrations()
        .iter()
        .map(|migration| migration.version())
        .collect();
    versions.sort_unstable();
    let mut expected: Vec<_> = (1..=versions.len() as i32).collect();
    expected.sort_unstable();
    assert_eq!(versions, expected);
}

#[tokio::test]
async fn contract_schema_keeps_one_contract_per_project() -> Result<()> {
    with_postgres(async |database| {
        let client = database.pool.get().await?;
        client
            .batch_execute(
                "INSERT INTO durable_actors_deployment (project_id, image_ref, working_directory, organization_id)
             VALUES ('team-a', 'image', '/app', 'org'), ('team-b', 'image', '/app', 'org');
             INSERT INTO durable_actors_contracts (project_id, contract_hash, contract_json)
             VALUES ('team-a', 'hash', '{}'), ('team-b', 'hash', '{}');",
            )
            .await?;
        let duplicate = client
            .execute(
                "INSERT INTO durable_actors_contracts (project_id, contract_hash, contract_json)
             VALUES ('team-a', 'other-hash', '{}')",
                &[],
            )
            .await
            .unwrap_err();
        assert_eq!(
            duplicate.code(),
            Some(&tokio_postgres::error::SqlState::UNIQUE_VIOLATION)
        );
        client
            .execute(
                "DELETE FROM durable_actors_deployment WHERE project_id = 'team-a'",
                &[],
            )
            .await?;
        let project: String = client
            .query_one("SELECT project_id FROM durable_actors_contracts", &[])
            .await?
            .get(0);
        assert_eq!(project, "team-b");
        Ok(())
    })
    .await
}

#[tokio::test]
async fn organization_limits_migration_requires_deployment_organizations() -> Result<()> {
    with_postgres_schema(async |database| {
        let mut client = database.pool.get().await?;
        embedded::migrations::runner()
            .set_target(refinery::Target::Version(24))
            .run_async(&mut **client)
            .await?;
        client
            .batch_execute(
                "INSERT INTO durable_actors_deployment (project_id, image_ref, working_directory, organization_id)
             VALUES ('legacy', 'image', '/app', NULL), ('a', 'image', '/app', 'org'), ('b', 'image', '/app', 'org');
             INSERT INTO durable_actors_project_limits VALUES ('a', 20000, NULL, 1000), ('b', 5000, 500, NULL);",
            )
            .await?;
        let preflight = embedded::migrations::runner()
            .run_async(&mut **client)
            .await
            .map_err(anyhow::Error::new)
            .unwrap_err();
        let preflight = format!("{preflight:#}");
        assert!(preflight.contains("deployments without a valid organization_id: legacy"));
        assert!(preflight.contains("HINT: Redeploy each project with organizationId"));
        // Stands in for the redeploy, which would also replace the project's hosts.
        client
            .batch_execute("UPDATE durable_actors_deployment SET organization_id = 'org' WHERE project_id = 'legacy'")
            .await?;
        embedded::migrations::runner()
            .run_async(&mut **client)
            .await?;
        let merged = client
            .query_one("SELECT max_active_actors, actor_starts_per_second, actor_start_burst FROM durable_actors_organization_limits WHERE organization_id = 'org'", &[])
            .await?;
        assert_eq!(
            (merged.get::<_, i64>(0), merged.get::<_, i64>(1), merged.get::<_, i64>(2)),
            (20_000, 500, 1000)
        );
        for (organization, code) in [
            ("NULL", tokio_postgres::error::SqlState::NOT_NULL_VIOLATION),
            ("''", tokio_postgres::error::SqlState::CHECK_VIOLATION),
        ] {
            let rejected = client
                .execute(&format!("UPDATE durable_actors_deployment SET organization_id = {organization}"), &[])
                .await
                .unwrap_err();
            assert_eq!(rejected.code(), Some(&code));
        }
        Ok(())
    })
    .await
}

#[tokio::test]
async fn concurrent_connections_migrate_fresh_and_existing_schemas_once() -> Result<()> {
    let latest = embedded::migrations::runner()
        .get_migrations()
        .iter()
        .map(|m| m.version())
        .max()
        .unwrap();
    for version in [0, latest] {
        with_postgres_schema(async |database| check_concurrent_migrations(database, version).await)
            .await?;
    }
    Ok(())
}

#[test]
fn runtime_startup_does_not_connect_to_postgres() -> Result<()> {
    let database = PostgresDatabase::lazy("postgresql://localhost:1/unavailable?sslmode=disable")?;
    assert_eq!(database.pool.status().size, 0);
    assert_eq!(database.pool.status().max_size, 8);
    Ok(())
}

#[test]
fn configured_pool_size_does_not_connect_to_postgres() -> Result<()> {
    let database = PostgresDatabase::lazy_with_max_connections(
        "postgresql://localhost:1/unavailable?sslmode=disable",
        64,
    )?;
    assert_eq!(database.pool.status().size, 0);
    assert_eq!(database.pool.status().max_size, 64);
    Ok(())
}

#[tokio::test]
async fn independent_queries_can_use_different_database_connections() -> Result<()> {
    with_postgres(async |fixture| {
        let database = PostgresDatabase::connect(&fixture.url).await?;
        let (first, second) = tokio::try_join!(
            database.query_one("SELECT pg_backend_pid(), pg_sleep(0.05)", &[]),
            database.query_one("SELECT pg_backend_pid(), pg_sleep(0.05)", &[]),
        )?;
        assert_ne!(first.get::<_, i32>(0), second.get::<_, i32>(0));
        Ok(())
    })
    .await
}

async fn current_schema(client: &tokio_postgres::Client) -> Result<String> {
    Ok(client
        .query_one("SELECT current_schema()", &[])
        .await?
        .get(0))
}

async fn check_isolated_rows(
    first: &tokio_postgres::Client,
    second: &tokio_postgres::Client,
) -> Result<()> {
    let insert = "INSERT INTO durable_actors_deployment (project_id, image_ref, working_directory, organization_id) VALUES ('project', 'image', '/app', 'org')";
    first.execute(insert, &[]).await?;
    let count: i64 = second
        .query_one("SELECT count(*) FROM durable_actors_deployment", &[])
        .await?
        .get(0);
    assert_eq!(count, 0);
    second.execute(insert, &[]).await?;
    Ok(())
}

async fn check_isolated_migrations(
    first: &tokio_postgres::Client,
    second: &tokio_postgres::Client,
) -> Result<()> {
    first
        .execute("DELETE FROM refinery_schema_history WHERE version = 3", &[])
        .await?;
    let count: i64 = second
        .query_one(
            "SELECT count(*) FROM refinery_schema_history WHERE version = 3",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(count, 1);
    Ok(())
}

async fn check_reconnected_fixture(url: &str, expected_schema: &str) -> Result<()> {
    let reopened = PostgresDatabase::connect(url).await?;
    let client = reopened.connection().await?;
    assert_eq!(current_schema(&client).await?, expected_schema);
    let count: i64 = client
        .query_one("SELECT count(*) FROM durable_actors_deployment", &[])
        .await?
        .get(0);
    assert_eq!(count, 1);
    Ok(())
}

async fn check_fixture_cleanup(client: &tokio_postgres::Client, outcome: &str) -> Result<()> {
    let mut schema = String::new();
    let result = AssertUnwindSafe(with_postgres(async |fixture| {
        schema = current_schema(&*fixture.pool.get().await?).await?;
        match outcome {
            "error" => anyhow::bail!("test failure"),
            "panic" => panic!("test panic"),
            _ => Ok(()),
        }
    }))
    .catch_unwind()
    .await;
    match outcome {
        "error" => assert_eq!(result.unwrap().unwrap_err().to_string(), "test failure"),
        "panic" => assert!(result.is_err()),
        _ => result.unwrap()?,
    }
    let exists: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = $1)",
            &[&schema],
        )
        .await?
        .get(0);
    assert!(!exists, "fixture schema remained after {outcome}");
    Ok(())
}

async fn check_concurrent_migrations(database: &TestDatabase, version: i32) -> Result<()> {
    let mut client = database.pool.get().await?;
    if version > 0 {
        embedded::migrations::runner()
            .set_target(refinery::Target::Version(version))
            .run_async(&mut **client)
            .await?;
    }
    check_concurrent_connections(&database.url).await?;
    let versions: Vec<i32> = client
        .query(
            "SELECT version FROM refinery_schema_history ORDER BY version",
            &[],
        )
        .await?
        .iter()
        .map(|row| row.get(0))
        .collect();
    let mut expected: Vec<_> = embedded::migrations::runner()
        .get_migrations()
        .iter()
        .map(|m| m.version())
        .collect();
    expected.sort_unstable();
    assert_eq!(versions, expected);
    Ok(())
}

async fn check_concurrent_connections(url: &str) -> Result<()> {
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(8));
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let url = url.to_owned();
        let barrier = barrier.clone();
        tasks.spawn(async move {
            barrier.wait().await;
            PostgresDatabase::connect(&url).await
        });
    }
    let mut failures = Vec::new();
    while let Some(result) = tasks.join_next().await {
        if let Err(error) = result? {
            failures.push(format!("{error:#}"));
        }
    }
    assert!(
        failures.is_empty(),
        "concurrent migrations failed: {failures:#?}"
    );
    Ok(())
}

#[tokio::test]
async fn notifications_recover_from_initial_connection_failure_and_reach_another_instance()
-> Result<()> {
    testing::with_postgres(async |fixture| {
        let stop = tokio_util::sync::CancellationToken::new();
        let _guard = stop.clone().drop_guard();
        let database = PostgresDatabase::connect(&fixture.url).await?;
        let first =
            notifications::ChangeFeed::postgres(database.clone(), &fixture.url, stop.clone())
                .await?;
        let (proxy_url, _proxy) = reset_first_connection(&fixture.url).await?;
        let second = notifications::ChangeFeed::postgres(database, &proxy_url, stop).await?;
        let mut changes = second.subscribe();
        first.notify().await;
        tokio::time::timeout(Duration::from_secs(2), changes.changed()).await??;
        Ok(())
    })
    .await
}

async fn reset_first_connection(
    url: &str,
) -> Result<(String, tokio_util::task::AbortOnDropHandle<Result<()>>)> {
    let mut url = reqwest::Url::parse(url)?;
    let target = format!(
        "{}:{}",
        url.host_str().context("PostgreSQL host")?,
        url.port().unwrap_or(5432)
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    url.set_host(Some("127.0.0.1"))?;
    url.set_port(Some(listener.local_addr()?.port())).unwrap();
    let task = tokio::spawn(async move {
        drop(listener.accept().await?.0);
        let mut connections = tokio::task::JoinSet::new();
        while let Ok((mut downstream, _)) = listener.accept().await {
            let mut upstream = tokio::net::TcpStream::connect(&target).await?;
            connections.spawn(async move {
                tokio::io::copy_bidirectional(&mut downstream, &mut upstream).await
            });
        }
        Ok(())
    });
    Ok((
        url.to_string(),
        tokio_util::task::AbortOnDropHandle::new(task),
    ))
}

#[test]
fn libpq_tls_params_are_taken_out_of_url_connection_strings() -> Result<()> {
    let (url, verify, root) = take_libpq_tls_params(
        "postgres://u:p@db.example:5432/app?sslmode=verify-full&sslrootcert=%2Fetc%2Fpostgres-ca%2Fca.crt&application_name=x",
    )?;
    assert_eq!(
        url,
        "postgres://u:p@db.example:5432/app?sslmode=require&application_name=x"
    );
    assert_eq!(verify, Verify::ChainAndHostname);
    assert_eq!(root.as_deref(), Some("/etc/postgres-ca/ca.crt"));

    let (url, verify, root) =
        take_libpq_tls_params("postgresql://db/app?sslrootcert=/ca.pem&sslmode=verify-ca")?;
    assert_eq!(url, "postgresql://db/app?sslmode=require");
    assert_eq!(verify, Verify::Chain);
    assert_eq!(root.as_deref(), Some("/ca.pem"));

    let (url, _, root) = take_libpq_tls_params("postgres://db/app?sslrootcert=/ca.pem")?;
    assert_eq!(url, "postgres://db/app");
    assert_eq!(root.as_deref(), Some("/ca.pem"));

    for repeated in [
        "postgres://db/app?sslmode=verify-ca&sslmode=verify-full",
        "postgres://db/app?sslmode=verify-full&sslmode=disable",
        "postgres://db/app?sslrootcert=/a.pem&sslrootcert=/b.pem",
    ] {
        let error = take_libpq_tls_params(repeated)
            .err()
            .expect("a repeated TLS parameter must be rejected");
        assert!(error.to_string().contains("more than once"), "{error}");
    }

    for unchanged in [
        "postgres://db/app",
        "postgres://db/app?sslmode=disable",
        "host=db sslmode=require",
    ] {
        assert_eq!(take_libpq_tls_params(unchanged)?.0, unchanged);
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn verify_full_settings_require_tls_and_read_the_ca_bundle() -> Result<()> {
    let tls = TestTls::new(&["localhost"])?;
    let settings = ConnectionSettings::parse(
        &format!(
            "postgres://u@localhost/app?sslmode=verify-full&sslrootcert={}",
            tls.ca_path.display()
        ),
        None,
    )?;
    assert_eq!(settings.config.get_ssl_mode(), SslMode::Require);
    assert!(settings.tls.is_some());

    let plaintext = ConnectionSettings::parse(
        "postgres://u@localhost/app?sslmode=disable",
        Some(tls.ca_path.display().to_string()),
    )?;
    assert!(plaintext.tls.is_none());

    let missing = ConnectionSettings::parse(
        "postgres://u@localhost/app?sslmode=verify-full&sslrootcert=/nonexistent/ca.crt",
        None,
    )
    .err()
    .expect("a missing CA bundle must fail");
    assert!(format!("{missing:#}").contains("/nonexistent/ca.crt"));

    let empty = tls.dir.path().join("empty.crt");
    std::fs::write(&empty, "")?;
    assert!(
        ConnectionSettings::parse(
            &format!("postgres://u@localhost/app?sslrootcert={}", empty.display()),
            None
        )
        .is_err()
    );
    Ok(())
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn verify_full_accepts_the_configured_ca_and_matching_hostname() -> Result<()> {
    let tls = TestTls::new(&["localhost"])?;
    let url = |path: &std::path::Path| {
        format!(
            "postgres://u@localhost:{{port}}/app?sslmode=verify-full&sslrootcert={}",
            path.display()
        )
    };
    assert!(handshake(&tls, &url(&tls.ca_path), None).await?);

    // PGSSLROOTCERT is honoured when the URL does not name a bundle.
    assert!(
        handshake(
            &tls,
            "postgres://u@localhost:{port}/app?sslmode=verify-full",
            Some(tls.ca_path.display().to_string()),
        )
        .await?
    );

    // A bundle from another CA is rejected, and without a bundle the platform roots do not
    // trust the private CA.
    let other = TestTls::new(&["localhost"])?;
    assert!(!handshake(&tls, &url(&other.ca_path), None).await?);
    assert!(
        !handshake(
            &tls,
            "postgres://u@localhost:{port}/app?sslmode=verify-full",
            None
        )
        .await?
    );
    Ok(())
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn verify_full_rejects_a_hostname_mismatch_that_verify_ca_allows() -> Result<()> {
    let tls = TestTls::new(&["db.example.internal"])?;
    let ca = tls.ca_path.display().to_string();
    assert!(
        !handshake(
            &tls,
            &format!("postgres://u@localhost:{{port}}/app?sslmode=verify-full&sslrootcert={ca}"),
            None,
        )
        .await?
    );
    assert!(
        handshake(
            &tls,
            &format!("postgres://u@localhost:{{port}}/app?sslmode=verify-ca&sslrootcert={ca}"),
            None,
        )
        .await?
    );
    Ok(())
}

#[cfg(target_os = "linux")]
struct TestTls {
    dir: tempfile::TempDir,
    ca_path: std::path::PathBuf,
    identity: native_tls::Identity,
}

#[cfg(target_os = "linux")]
impl TestTls {
    /// A fresh CA and a server certificate it signed for `server_names`, valid for one day.
    fn new(server_names: &[&str]) -> Result<Self> {
        use openssl::{
            asn1::Asn1Time,
            bn::BigNum,
            ec::{EcGroup, EcKey},
            hash::MessageDigest,
            nid::Nid,
            pkey::{PKey, Private},
            x509::{
                X509, X509Name,
                extension::{BasicConstraints, SubjectAlternativeName},
            },
        };

        fn key() -> Result<PKey<Private>> {
            let group = EcGroup::from_curve_name(Nid::X9_62_PRIME256V1)?;
            Ok(PKey::from_ec_key(EcKey::generate(&group)?)?)
        }
        fn name(common_name: &str) -> Result<X509Name> {
            let mut name = X509Name::builder()?;
            name.append_entry_by_nid(Nid::COMMONNAME, common_name)?;
            Ok(name.build())
        }
        fn certificate(
            subject: &X509Name,
            issuer: &X509Name,
            public: &PKey<Private>,
            signer: &PKey<Private>,
            serial: u32,
            extend: impl FnOnce(&mut openssl::x509::X509Builder) -> Result<()>,
        ) -> Result<X509> {
            let mut builder = X509::builder()?;
            builder.set_version(2)?;
            let serial = BigNum::from_u32(serial)?.to_asn1_integer()?;
            builder.set_serial_number(&serial)?;
            builder.set_subject_name(subject)?;
            builder.set_issuer_name(issuer)?;
            builder.set_pubkey(public)?;
            let not_before = Asn1Time::days_from_now(0)?;
            builder.set_not_before(&not_before)?;
            let not_after = Asn1Time::days_from_now(1)?;
            builder.set_not_after(&not_after)?;
            extend(&mut builder)?;
            builder.sign(signer, MessageDigest::sha256())?;
            Ok(builder.build())
        }

        let ca_key = key()?;
        let ca_name = name("durable-actors test CA")?;
        let ca = certificate(&ca_name, &ca_name, &ca_key, &ca_key, 1, |builder| {
            builder.append_extension(BasicConstraints::new().critical().ca().build()?)?;
            Ok(())
        })?;
        let server_key = key()?;
        let server = certificate(
            &name(server_names[0])?,
            &ca_name,
            &server_key,
            &ca_key,
            2,
            |builder| {
                let mut names = SubjectAlternativeName::new();
                for server_name in server_names {
                    names.dns(server_name);
                }
                let names = names.build(&builder.x509v3_context(Some(&ca), None))?;
                builder.append_extension(names)?;
                Ok(())
            },
        )?;
        let dir = tempfile::tempdir()?;
        let ca_path = dir.path().join("ca.crt");
        std::fs::write(&ca_path, ca.to_pem()?)?;
        let identity = native_tls::Identity::from_pkcs8(
            &server.to_pem()?,
            &server_key.private_key_to_pem_pkcs8()?,
        )?;
        Ok(Self {
            dir,
            ca_path,
            identity,
        })
    }
}

#[cfg(target_os = "linux")]
/// Answers one PostgreSQL SSLRequest with TLS and reports whether the client completed the
/// handshake. `{port}` in `url` is replaced by the listener port; `url` must have a query.
async fn handshake(tls: &TestTls, url: &str, root_cert_env: Option<String>) -> Result<bool> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    // Verify against the URL's host name but dial only the listener: `localhost` may also resolve to
    // ::1, and tokio-postgres reports the last address's error.
    let url = format!(
        "{}&hostaddr=127.0.0.1",
        url.replace("{port}", &port.to_string())
    );
    let settings = ConnectionSettings::parse(&url, root_cert_env)?;
    let mut config = settings.config;
    config.connect_timeout(Duration::from_secs(5));
    let acceptor =
        tokio_native_tls::TlsAcceptor::from(native_tls::TlsAcceptor::new(tls.identity.clone())?);
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        let mut request = [0_u8; 8];
        socket.read_exact(&mut request).await?;
        anyhow::ensure!(
            request == [0, 0, 0, 8, 4, 210, 22, 47],
            "expected an SSLRequest"
        );
        socket.write_all(b"S").await?;
        anyhow::Ok(acceptor.accept(socket).await.is_ok())
    });
    let client = config.connect(settings.tls.expect("TLS is enabled")).await;
    // The fake server never speaks the PostgreSQL protocol after TLS, so the client always fails.
    let error = client.err().expect("the fake server never authenticates");
    let accepted = tokio::time::timeout(Duration::from_secs(10), server).await???;
    if !accepted {
        assert!(error.to_string().contains("TLS"), "{error}");
    }
    Ok(accepted)
}
