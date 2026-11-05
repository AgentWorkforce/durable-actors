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
