use std::{str::FromStr, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use deadpool_postgres::{Manager, Pool, Runtime};
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use tokio_postgres::{Config, NoTls, Row, config::SslMode, types::ToSql};

pub(crate) mod notifications;

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("migrations");
}

#[cfg(test)]
#[path = "../tests/support/postgres.rs"]
pub(crate) mod testing;

pub(crate) const DEFAULT_MAX_CONNECTIONS: usize = 8;

#[derive(Clone)]
pub(crate) struct PostgresDatabase {
    pool: Pool,
    initialized: Arc<tokio::sync::OnceCell<()>>,
}

impl PostgresDatabase {
    pub(crate) async fn connection(&self) -> Result<deadpool_postgres::Object> {
        self.initialize().await?;
        self.pool
            .get()
            .await
            .context("acquire PostgreSQL connection")
    }

    #[cfg(test)]
    pub(crate) async fn connect(url: &str) -> Result<Self> {
        let database = Self::lazy(url)?;
        database.initialize().await?;
        Ok(database)
    }

    #[cfg(test)]
    pub(crate) fn lazy(url: &str) -> Result<Self> {
        Self::lazy_with_max_connections(url, DEFAULT_MAX_CONNECTIONS)
    }

    pub(crate) fn lazy_with_max_connections(url: &str, max_connections: usize) -> Result<Self> {
        Ok(Self {
            pool: connection_pool(url, max_connections)?,
            initialized: Arc::new(tokio::sync::OnceCell::new()),
        })
    }

    async fn initialize(&self) -> Result<()> {
        self.initialized
            .get_or_try_init(|| migrate(&self.pool))
            .await?;
        Ok(())
    }

    pub(crate) async fn query_opt(
        &self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>> {
        self.initialize().await?;
        let client = self
            .pool
            .get()
            .await
            .context("acquire PostgreSQL connection")?;
        let statement = client.prepare_cached(query).await?;
        Ok(client.query_opt(&statement, params).await?)
    }

    #[cfg(test)]
    pub(crate) async fn query_one(
        &self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row> {
        self.initialize().await?;
        let client = self
            .pool
            .get()
            .await
            .context("acquire PostgreSQL connection")?;
        let statement = client.prepare_cached(query).await?;
        Ok(client.query_one(&statement, params).await?)
    }

    pub(crate) async fn execute(&self, query: &str, params: &[&(dyn ToSql + Sync)]) -> Result<u64> {
        self.initialize().await?;
        let client = self
            .pool
            .get()
            .await
            .context("acquire PostgreSQL connection")?;
        let statement = client.prepare_cached(query).await?;
        Ok(client.execute(&statement, params).await?)
    }
}

async fn migrate(pool: &Pool) -> Result<()> {
    let client = pool.get().await.context("connect to PostgreSQL")?;
    // Closing this dedicated session releases the lock even on errors or cancellation.
    let mut client = deadpool_postgres::Object::take(client);
    client
        .query_one(
            "SELECT pg_advisory_lock(hashtext('durable-actors'), hashtext('schema-migrations'))",
            &[],
        )
        .await
        .context("lock durable-actors PostgreSQL migrations")?;
    embedded::migrations::runner()
        .run_async(&mut *client)
        .await
        .context("run durable-actors PostgreSQL migrations")?;
    Ok(())
}

fn connection_pool(url: &str, max_connections: usize) -> Result<Pool> {
    let settings = ConnectionSettings::from_url(url)?;
    let manager = match settings.tls {
        None => Manager::new(settings.config, NoTls),
        Some(tls) => Manager::new(settings.config, tls),
    };
    Ok(Pool::builder(manager)
        .max_size(max_connections)
        .runtime(Runtime::Tokio1)
        .wait_timeout(Some(Duration::from_secs(5)))
        .create_timeout(Some(Duration::from_secs(5)))
        .build()?)
}

/// Environment variable naming a PEM CA bundle, as in libpq. A `sslrootcert` URL parameter wins.
const ROOT_CERT_ENV: &str = "PGSSLROOTCERT";

/// A parsed PostgreSQL URL plus the TLS connector it asks for (`None` means plaintext).
///
/// `tokio-postgres` only understands `sslmode=disable|prefer|require` and rejects `sslrootcert`, so
/// the libpq parameters it lacks are taken out of a `postgres://` URL here before it is parsed:
///
/// - `sslrootcert=<path>` (or `PGSSLROOTCERT`) trusts only the PEM certificates in that file;
///   `sslrootcert=system` keeps the platform trust store.
/// - `sslmode=verify-full` requires TLS and verifies the chain and the server hostname.
/// - `sslmode=verify-ca` requires TLS and verifies the chain but not the hostname.
///
/// `prefer` and `require` keep their existing behaviour here: when TLS is used the chain and the
/// hostname are verified against the trust store above.
pub(crate) struct ConnectionSettings {
    pub(crate) config: Config,
    pub(crate) tls: Option<MakeTlsConnector>,
}

impl ConnectionSettings {
    pub(crate) fn from_url(url: &str) -> Result<Self> {
        Self::parse(url, std::env::var(ROOT_CERT_ENV).ok())
    }

    fn parse(url: &str, root_cert_env: Option<String>) -> Result<Self> {
        let (url, verify, root_cert) = take_libpq_tls_params(url)?;
        let config = Config::from_str(&url).context("parse PostgreSQL connection URL")?;
        if config.get_ssl_mode() == SslMode::Disable {
            return Ok(Self { config, tls: None });
        }
        let root_cert = root_cert.or(root_cert_env.filter(|path| !path.is_empty()));
        let mut builder = TlsConnector::builder();
        if verify == Verify::Chain {
            builder.danger_accept_invalid_hostnames(true);
        }
        if let Some(path) = root_cert.filter(|path| path != "system") {
            let pem = std::fs::read(&path)
                .with_context(|| format!("read PostgreSQL CA bundle {path}"))?;
            let certificates = native_tls::Certificate::stack_from_pem(&pem)
                .with_context(|| format!("parse PostgreSQL CA bundle {path}"))?;
            anyhow::ensure!(
                !certificates.is_empty(),
                "PostgreSQL CA bundle {path} contains no certificates"
            );
            builder.disable_built_in_roots(true);
            for certificate in certificates {
                builder.add_root_certificate(certificate);
            }
        }
        let connector = builder.build().context("build PostgreSQL TLS connector")?;
        Ok(Self {
            config,
            tls: Some(MakeTlsConnector::new(connector)),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verify {
    ChainAndHostname,
    Chain,
}

/// Removes `sslrootcert` and rewrites `sslmode=verify-ca|verify-full` to `require` in a URL-form
/// connection string. Key/value connection strings are returned unchanged. Values are
/// percent-decoded exactly as `tokio-postgres` decodes the rest of the query. A repeated `sslmode`
/// or `sslrootcert` is rejected, so the effective mode and the verification policy cannot differ.
fn take_libpq_tls_params(url: &str) -> Result<(String, Verify, Option<String>)> {
    let mut verify = Verify::ChainAndHostname;
    let mut root_cert = None;
    let is_url = url.starts_with("postgres://") || url.starts_with("postgresql://");
    let Some((base, query)) = url.split_once('?').filter(|_| is_url) else {
        return Ok((url.to_owned(), verify, root_cert));
    };
    let mut kept = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for pair in query.split('&').filter(|pair| !pair.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let decode = |part: &str| -> Result<String> {
            Ok(percent_encoding::percent_decode_str(part)
                .decode_utf8()
                .context("decode PostgreSQL connection URL parameter")?
                .into_owned())
        };
        let key = decode(key)?;
        if matches!(key.as_str(), "sslmode" | "sslrootcert") {
            anyhow::ensure!(
                seen.insert(key.clone()),
                "PostgreSQL connection URL sets {key} more than once"
            );
        }
        match (key.as_str(), decode(value)?.as_str()) {
            ("sslrootcert", path) => root_cert = Some(path.to_owned()),
            ("sslmode", "verify-full") => kept.push("sslmode=require".to_owned()),
            ("sslmode", "verify-ca") => {
                verify = Verify::Chain;
                kept.push("sslmode=require".to_owned());
            }
            _ => kept.push(pair.to_owned()),
        }
    }
    let url = if kept.is_empty() {
        base.to_owned()
    } else {
        format!("{base}?{}", kept.join("&"))
    };
    Ok((url, verify, root_cert))
}

#[cfg(test)]
#[path = "../tests/unit/postgres.rs"]
mod tests;
