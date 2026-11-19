-- Columns match the OpenTelemetry Collector ClickHouse exporter (v0.163.0), which inserts by name.
-- Tenant columns come from resource attributes the collector stamps from verified host claims.
-- Partitioning and retention use the server's insert time. Partitioning by host timestamps would
-- let one host's clock fail inserts that carry other projects' rows, and whole partitions expire cheaply.
CREATE TABLE IF NOT EXISTS otel_traces (
    Timestamp DateTime64(9) CODEC(Delta, ZSTD(1)),
    TraceId String CODEC(ZSTD(1)),
    SpanId String CODEC(ZSTD(1)),
    ParentSpanId String CODEC(ZSTD(1)),
    TraceState String CODEC(ZSTD(1)),
    SpanName LowCardinality(String) CODEC(ZSTD(1)),
    SpanKind LowCardinality(String) CODEC(ZSTD(1)),
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeName String CODEC(ZSTD(1)),
    ScopeVersion String CODEC(ZSTD(1)),
    SpanAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    Duration UInt64 CODEC(ZSTD(1)),
    StatusCode LowCardinality(String) CODEC(ZSTD(1)),
    StatusMessage String CODEC(ZSTD(1)),
    Events Nested (
        Timestamp DateTime64(9),
        Name LowCardinality(String),
        Attributes Map(LowCardinality(String), String)
    ) CODEC(ZSTD(1)),
    Links Nested (
        TraceId String,
        SpanId String,
        TraceState String,
        Attributes Map(LowCardinality(String), String)
    ) CODEC(ZSTD(1)),
    ProjectId LowCardinality(String) MATERIALIZED ResourceAttributes['durable_actors.project_id'],
    ActorName LowCardinality(String) MATERIALIZED ResourceAttributes['durable_actors.actor_name'],
    ActorId String MATERIALIZED ResourceAttributes['durable_actors.actor_id'],
    HostId String MATERIALIZED ResourceAttributes['durable_actors.host_id'],
    SessionId String MATERIALIZED ResourceAttributes['durable_actors.session_id'],
    RequestId String MATERIALIZED SpanAttributes['durable_actors.request_id'],
    ReceivedAt DateTime64(3) DEFAULT now64(3),
    INDEX idx_request_id RequestId TYPE bloom_filter(0.001) GRANULARITY 1
) ENGINE = MergeTree
PARTITION BY toDate(ReceivedAt)
ORDER BY (ProjectId, ActorName, ActorId, Timestamp)
TTL toDateTime(ReceivedAt) + INTERVAL RETENTION_SECONDS SECOND
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;

CREATE TABLE IF NOT EXISTS otel_logs (
    Timestamp DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    TraceId String CODEC(ZSTD(1)),
    SpanId String CODEC(ZSTD(1)),
    TraceFlags UInt8,
    SeverityText LowCardinality(String) CODEC(ZSTD(1)),
    SeverityNumber UInt8,
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    Body String CODEC(ZSTD(1)),
    ResourceSchemaUrl LowCardinality(String) CODEC(ZSTD(1)),
    ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeSchemaUrl LowCardinality(String) CODEC(ZSTD(1)),
    ScopeName String CODEC(ZSTD(1)),
    ScopeVersion LowCardinality(String) CODEC(ZSTD(1)),
    ScopeAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    LogAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    EventName String CODEC(ZSTD(1)),
    ProjectId LowCardinality(String) MATERIALIZED ResourceAttributes['durable_actors.project_id'],
    ActorName LowCardinality(String) MATERIALIZED ResourceAttributes['durable_actors.actor_name'],
    ActorId String MATERIALIZED ResourceAttributes['durable_actors.actor_id'],
    HostId String MATERIALIZED ResourceAttributes['durable_actors.host_id'],
    RequestId String MATERIALIZED LogAttributes['durable_actors.request_id'],
    LogId String MATERIALIZED LogAttributes['durable_actors.log_id'],
    ReceivedAt DateTime64(3) DEFAULT now64(3),
    INDEX idx_request_id RequestId TYPE bloom_filter(0.001) GRANULARITY 1
) ENGINE = MergeTree
PARTITION BY toDate(ReceivedAt)
ORDER BY (ProjectId, ActorName, ActorId, Timestamp)
TTL toDateTime(ReceivedAt) + INTERVAL RETENTION_SECONDS SECOND
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;

ALTER TABLE otel_traces MODIFY TTL toDateTime(ReceivedAt) + INTERVAL RETENTION_SECONDS SECOND;
ALTER TABLE otel_logs MODIFY TTL toDateTime(ReceivedAt) + INTERVAL RETENTION_SECONDS SECOND;
CREATE TABLE IF NOT EXISTS platform_events (
    Timestamp DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    TraceId String CODEC(ZSTD(1)),
    SpanId String CODEC(ZSTD(1)),
    TraceFlags UInt8,
    SeverityText LowCardinality(String) CODEC(ZSTD(1)),
    SeverityNumber UInt8,
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    Body String CODEC(ZSTD(1)),
    ResourceSchemaUrl LowCardinality(String) CODEC(ZSTD(1)),
    ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeSchemaUrl LowCardinality(String) CODEC(ZSTD(1)),
    ScopeName String CODEC(ZSTD(1)),
    ScopeVersion LowCardinality(String) CODEC(ZSTD(1)),
    ScopeAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    LogAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    EventName LowCardinality(String) CODEC(ZSTD(1)),
    -- Gateways report many projects under one resource, so these come from each record.
    ProjectId LowCardinality(String) MATERIALIZED LogAttributes['durable_actors.project_id'],
    ActorName LowCardinality(String) MATERIALIZED LogAttributes['durable_actors.actor_name'],
    ActorId String MATERIALIZED LogAttributes['durable_actors.actor_id'],
    ConnectionId String MATERIALIZED LogAttributes['durable_actors.connection_id'],
    GatewayId String MATERIALIZED ResourceAttributes['durable_actors.gateway_id'],
    ReceivedAt DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree
PARTITION BY toDate(ReceivedAt)
ORDER BY (ProjectId, ActorName, ActorId, ConnectionId, Timestamp)
TTL toDateTime(ReceivedAt) + INTERVAL RETENTION_SECONDS SECOND
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;
ALTER TABLE platform_events MODIFY TTL toDateTime(ReceivedAt) + INTERVAL RETENTION_SECONDS SECOND;

-- Raw events expire, so each project's latest contract is kept here without a TTL. A removed
-- deployment has an empty body.
CREATE TABLE IF NOT EXISTS latest_deployments (
    ProjectId LowCardinality(String),
    Contract AggregateFunction(argMax, String, DateTime64(9))
) ENGINE = AggregatingMergeTree
ORDER BY ProjectId;

CREATE MATERIALIZED VIEW IF NOT EXISTS latest_deployment_updates TO latest_deployments AS
SELECT LogAttributes['durable_actors.project_id'] AS ProjectId,
       argMaxState(Body, Timestamp) AS Contract
FROM platform_events
WHERE EventName IN ('durable_actors.deployment.published', 'durable_actors.deployment.removed')
GROUP BY ProjectId;

CREATE TABLE IF NOT EXISTS actor_lifecycle (
    project_id String,
    actor_name String,
    actor_id String,
    event_id String,
    owner_epoch UInt64,
    sequence UInt64,
    event_json String,
    received_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree
ORDER BY (project_id, actor_name, actor_id, event_id)
TTL received_at + INTERVAL RETENTION_SECONDS SECOND;

CREATE TABLE IF NOT EXISTS actor_inventory (
    project_id String,
    actor_name String,
    actor_id String,
    event AggregateFunction(argMax, String, Tuple(UInt64, UInt64))
) ENGINE = AggregatingMergeTree
ORDER BY (project_id, actor_name, actor_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS actor_inventory_updates TO actor_inventory AS
SELECT project_id, actor_name, actor_id,
       argMaxState(event_json, tuple(owner_epoch, sequence)) AS event
FROM actor_lifecycle
GROUP BY project_id, actor_name, actor_id;

ALTER TABLE actor_lifecycle MODIFY TTL received_at + INTERVAL RETENTION_SECONDS SECOND;

-- Residency reads one time range of a class, so time leads the key. Rows written before this
-- table existed come from scripts/backfill-residency-reports.sh.
CREATE TABLE IF NOT EXISTS actor_residency_reports (
    project_id String,
    actor_name String,
    actor_id String,
    event_id String,
    owner_epoch UInt64,
    sequence UInt64,
    occurred_at_ms UInt64,
    kind LowCardinality(String),
    resident Nullable(Bool),
    received_at DateTime
) ENGINE = ReplacingMergeTree
ORDER BY (project_id, actor_name, occurred_at_ms, actor_id, event_id)
TTL received_at + INTERVAL RETENTION_SECONDS SECOND;

CREATE MATERIALIZED VIEW IF NOT EXISTS actor_residency_report_updates TO actor_residency_reports AS
SELECT project_id, actor_name, actor_id, event_id, owner_epoch, sequence,
       JSONExtractUInt(event_json, 'occurredAtMs') AS occurred_at_ms,
       JSONExtractString(event_json, 'kind') AS kind,
       JSONExtract(event_json, 'resident', 'Nullable(Bool)') AS resident,
       received_at
FROM actor_lifecycle;

ALTER TABLE actor_residency_reports MODIFY TTL received_at + INTERVAL RETENTION_SECONDS SECOND;

-- Lifecycle reports arrive as platform events, and the inventory view above follows from here.
CREATE MATERIALIZED VIEW IF NOT EXISTS actor_lifecycle_reports TO actor_lifecycle AS
SELECT LogAttributes['durable_actors.project_id'] AS project_id,
       LogAttributes['durable_actors.actor_name'] AS actor_name,
       LogAttributes['durable_actors.actor_id'] AS actor_id,
       JSONExtractString(Body, 'eventId') AS event_id,
       JSONExtractUInt(Body, 'ownerEpoch') AS owner_epoch,
       JSONExtractUInt(Body, 'sequence') AS sequence,
       Body AS event_json
FROM platform_events
WHERE EventName = 'durable_actors.actor.reported';

-- The CPU and memory of each sandbox's runtime container, from the node collector. The collector's
-- ClickHouse exporter inserts these columns by name (v0.163.0). A pod runs one actor, so reads look
-- samples up by pod.
CREATE TABLE IF NOT EXISTS sandbox_usage (
    ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ResourceSchemaUrl String CODEC(ZSTD(1)),
    ScopeName String CODEC(ZSTD(1)),
    ScopeVersion String CODEC(ZSTD(1)),
    ScopeAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeDroppedAttrCount UInt32 CODEC(ZSTD(1)),
    ScopeSchemaUrl String CODEC(ZSTD(1)),
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    MetricName LowCardinality(String) CODEC(ZSTD(1)),
    MetricDescription String CODEC(ZSTD(1)),
    MetricUnit String CODEC(ZSTD(1)),
    Attributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    StartTimeUnix DateTime CODEC(Delta, ZSTD(1)),
    TimeUnix DateTime CODEC(Delta, ZSTD(1)),
    Value Float64 CODEC(ZSTD(1)),
    Flags UInt32 CODEC(ZSTD(1)),
    Exemplars Nested (
        FilteredAttributes Map(LowCardinality(String), String),
        TimeUnix DateTime,
        Value Float64,
        SpanId String,
        TraceId String
    ) CODEC(ZSTD(1)),
    PodName String MATERIALIZED ResourceAttributes['k8s.pod.name'],
    ReceivedAt DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree
PARTITION BY toDate(ReceivedAt)
ORDER BY (PodName, MetricName, TimeUnix)
TTL toDateTime(ReceivedAt) + INTERVAL RETENTION_SECONDS SECOND
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;

ALTER TABLE sandbox_usage MODIFY TTL toDateTime(ReceivedAt) + INTERVAL RETENTION_SECONDS SECOND;

-- The actor each sandbox was claimed for, with the sandbox's resources.
CREATE TABLE IF NOT EXISTS actor_sandboxes (
    project_id String,
    actor_name String,
    actor_id String,
    sandbox_id String,
    claimed_at_ms UInt64,
    cpu_millis UInt32,
    cpu_limit_millis Nullable(UInt32),
    memory_mib UInt32,
    memory_limit_mib Nullable(UInt32),
    received_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree
ORDER BY (project_id, actor_name, actor_id, claimed_at_ms, sandbox_id)
TTL received_at + INTERVAL RETENTION_SECONDS SECOND;

CREATE MATERIALIZED VIEW IF NOT EXISTS actor_sandbox_claims TO actor_sandboxes AS
SELECT LogAttributes['durable_actors.project_id'] AS project_id,
       LogAttributes['durable_actors.actor_name'] AS actor_name,
       LogAttributes['durable_actors.actor_id'] AS actor_id,
       LogAttributes['durable_actors.sandbox_id'] AS sandbox_id,
       toUInt64(toUnixTimestamp64Milli(Timestamp)) AS claimed_at_ms,
       toUInt32(JSONExtractUInt(Body, 'cpuMillis')) AS cpu_millis,
       JSONExtract(Body, 'cpuLimitMillis', 'Nullable(UInt32)') AS cpu_limit_millis,
       toUInt32(JSONExtractUInt(Body, 'memoryMib')) AS memory_mib,
       JSONExtract(Body, 'memoryLimitMib', 'Nullable(UInt32)') AS memory_limit_mib
FROM platform_events
WHERE EventName = 'durable_actors.sandbox.claimed';

ALTER TABLE actor_sandboxes MODIFY TTL received_at + INTERVAL RETENTION_SECONDS SECOND;

-- Each lifecycle report's database size, for one actor's storage over time.
CREATE TABLE IF NOT EXISTS actor_database_sizes (
    project_id String,
    actor_name String,
    actor_id String,
    occurred_at_ms UInt64,
    database_bytes UInt64,
    received_at DateTime DEFAULT now()
) ENGINE = MergeTree
ORDER BY (project_id, actor_name, actor_id, occurred_at_ms)
TTL received_at + INTERVAL RETENTION_SECONDS SECOND;

CREATE MATERIALIZED VIEW IF NOT EXISTS actor_database_size_reports TO actor_database_sizes AS
SELECT project_id, actor_name, actor_id,
       JSONExtractUInt(event_json, 'occurredAtMs') AS occurred_at_ms,
       JSONExtractUInt(event_json, 'databaseBytes') AS database_bytes
FROM actor_lifecycle
WHERE JSONHas(event_json, 'databaseBytes');

ALTER TABLE actor_database_sizes MODIFY TTL received_at + INTERVAL RETENTION_SECONDS SECOND;
