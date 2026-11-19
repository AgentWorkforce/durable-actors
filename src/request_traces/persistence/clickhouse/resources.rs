use super::*;
use crate::{
    request_traces::resources::{
        DatabaseBucket, ResourceQuery, ResourceUsage, SandboxResources, UsageBucket,
    },
    sandbox::ResourceLimits,
};

impl ClickHouseTracePersistence {
    pub(super) async fn read_resources(
        &self,
        project: &str,
        query: &ResourceQuery,
    ) -> Result<ResourceUsage> {
        let (usage, database, database_bytes, sandboxes) =
            tokio::time::timeout(Duration::from_secs(15), async {
                tokio::try_join!(
                    self.resource_query(include_str!("resource_usage.sql"), project, query)
                        .fetch_all::<UsageRow>(),
                    self.resource_query(include_str!("resource_database.sql"), project, query)
                        .fetch_all::<DatabaseRow>(),
                    self.resource_query(include_str!("latest_database_size.sql"), project, query)
                        .fetch_optional::<DatabaseSizeRow>(),
                    self.resource_query(include_str!("resource_sandboxes.sql"), project, query)
                        .fetch_all::<SandboxRow>(),
                )
            })
            .await??;
        Ok(ResourceUsage {
            sandboxes: sandboxes.into_iter().map(SandboxResources::from).collect(),
            usage: usage.into_iter().map(|row| row.bucket(query)).collect(),
            database: database.into_iter().map(|row| row.bucket(query)).collect(),
            database_bytes: database_bytes.map(|row| row.database_bytes),
        })
    }

    fn resource_query(
        &self,
        sql: &str,
        project: &str,
        query: &ResourceQuery,
    ) -> clickhouse::query::Query {
        self.client
            .query(sql)
            .param("project", project)
            .param("actor_name", query.actor_name.as_str())
            .param("actor_id", query.actor_id.as_str())
            .param("from", i64::try_from(query.from_ms).unwrap_or(i64::MAX))
            .param("to", i64::try_from(query.to_ms).unwrap_or(i64::MAX))
            .param("bucket", i64::try_from(query.bucket_ms).unwrap_or(i64::MAX))
    }
}

fn bucket_start(query: &ResourceQuery, bucket: u64) -> u64 {
    query.from_ms + bucket * query.bucket_ms
}

#[derive(Row, Deserialize)]
struct UsageRow {
    bucket: u64,
    cpu_cores: Option<f64>,
    memory_bytes: Option<u64>,
}

impl UsageRow {
    fn bucket(self, query: &ResourceQuery) -> UsageBucket {
        UsageBucket {
            at_ms: bucket_start(query, self.bucket),
            cpu_cores: self.cpu_cores,
            memory_bytes: self.memory_bytes,
        }
    }
}

#[derive(Row, Deserialize)]
struct DatabaseRow {
    bucket: u64,
    bytes: u64,
}

impl DatabaseRow {
    fn bucket(self, query: &ResourceQuery) -> DatabaseBucket {
        DatabaseBucket {
            at_ms: bucket_start(query, self.bucket),
            bytes: self.bytes,
        }
    }
}

#[derive(Row, Deserialize)]
struct DatabaseSizeRow {
    database_bytes: u64,
}

#[derive(Row, Deserialize)]
struct SandboxRow {
    claimed_at_ms: u64,
    cpu_millis: u32,
    cpu_limit_millis: Option<u32>,
    memory_mib: u32,
    memory_limit_mib: Option<u32>,
}

impl From<SandboxRow> for SandboxResources {
    fn from(row: SandboxRow) -> Self {
        Self {
            claimed_at_ms: row.claimed_at_ms,
            resources: ResourceLimits {
                cpu_millis: row.cpu_millis,
                cpu_limit_millis: row.cpu_limit_millis,
                memory_mib: row.memory_mib,
                memory_limit_mib: row.memory_limit_mib,
            },
        }
    }
}
