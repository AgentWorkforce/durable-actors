SELECT connection_id, actor_name, actor_id,
    argMin(host_id, (operation != 'onConnect', started_at_ms, event_id)) AS host_id,
    minOrNullIf(started_at_ms, operation = 'onConnect') AS opened_at_ms,
    maxOrNullIf(started_at_ms, operation = 'onDisconnect') AS closed_at_ms,
    max(started_at_ms) AS last_seen_ms,
    countIf(operation = 'onMessage') AS messages,
    countIf(outcome NOT IN ('completed', 'rerouted')) AS failures,
    argMinIf(metadata, (started_at_ms, event_id), operation = 'onConnect') AS metadata
FROM (
    SELECT SpanAttributes['durable_actors.connection_id'] AS connection_id,
        ActorName AS actor_name, ActorId AS actor_id, HostId AS host_id, SpanName AS operation,
        toUInt64(toUnixTimestamp64Milli(Timestamp)) AS started_at_ms, SpanId AS event_id,
        SpanAttributes['durable_actors.outcome'] AS outcome,
        SpanAttributes['durable_actors.metadata'] AS metadata
    FROM otel_traces
    WHERE ProjectId = {project:String}
        AND ReceivedAt > now64(3) - toIntervalSecond({retention:UInt64})
        AND SpanAttributes['durable_actors.kind'] = 'websocket'
)
WHERE connection_id != ''
GROUP BY connection_id, actor_name, actor_id
HAVING max(started_at_ms) >= {from:UInt64} AND min(started_at_ms) <= {to:UInt64}
ORDER BY coalesce(opened_at_ms, last_seen_ms) DESC, connection_id
LIMIT 500
