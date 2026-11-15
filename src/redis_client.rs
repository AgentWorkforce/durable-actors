use anyhow::{Context, Result, anyhow};
use redis::aio::{ConnectionManager, ConnectionManagerConfig, PubSub};
use std::{future::Future, time::Duration};

pub struct RedisConfig {
    pub url: String,
    pub ca_cert: Option<String>,
}

const COMMAND_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Clone)]
pub(crate) struct Redis {
    client: redis::Client,
    connection: ConnectionManager,
    timeout: Duration,
}

impl Redis {
    pub(crate) fn connect(config: &RedisConfig) -> Result<Self> {
        let client = match &config.ca_cert {
            Some(ca) => redis::Client::build_with_tls(
                config.url.as_str(),
                redis::TlsCertificates {
                    client_tls: None,
                    root_cert: Some(ca.as_bytes().to_vec()),
                },
            ),
            None => redis::Client::open(config.url.as_str()),
        }
        .context("configure Redis client")?;
        Self::open(client, COMMAND_TIMEOUT)
    }

    /// A connection of its own, so a slow command there does not hold up the quick ones here.
    pub(crate) fn with_timeout(&self, timeout: Duration) -> Result<Self> {
        Self::open(self.client.clone(), timeout)
    }

    fn open(client: redis::Client, timeout: Duration) -> Result<Self> {
        let connection = ConnectionManager::new_lazy_with_config(
            client.clone(),
            ConnectionManagerConfig::new()
                .set_connection_timeout(Some(Duration::from_secs(1)))
                .set_response_timeout(Some(timeout))
                .set_number_of_retries(1),
        )
        .context("configure Redis connection")?;
        Ok(Self {
            client,
            connection,
            timeout,
        })
    }

    pub(crate) async fn pubsub(&self) -> Result<PubSub> {
        tokio::time::timeout(Duration::from_secs(5), self.client.get_async_pubsub())
            .await
            .context("Redis pub/sub connection timed out")?
            .context("open Redis pub/sub connection")
    }

    pub(crate) async fn run<T: redis::FromRedisValue>(&self, command: redis::Cmd) -> Result<T> {
        let mut connection = self.connection.clone();
        self.answered(command.query_async(&mut connection)).await
    }

    /// Sends the commands as one request: they run in order, before anything sent here later.
    pub(crate) async fn run_all<T: redis::FromRedisValue>(
        &self,
        commands: redis::Pipeline,
    ) -> Result<T> {
        let mut connection = self.connection.clone();
        self.answered(commands.query_async(&mut connection)).await
    }

    async fn answered<T>(&self, reply: impl Future<Output = redis::RedisResult<T>>) -> Result<T> {
        let reply = tokio::time::timeout(self.timeout, reply)
            .await
            .map_err(|_| anyhow!("Redis did not answer within {:?}", self.timeout))?;
        Ok(reply?)
    }
}
