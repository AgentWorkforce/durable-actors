SELECT * FROM (
    SELECT SpanId AS event_id, HostId AS host_id, SessionId AS session_id, ProjectId AS project_id,
        RequestId AS request_id, ActorName AS actor_name, ActorId AS actor_id,
        SpanAttributes['durable_actors.kind'] AS kind, SpanName AS operation,
        SpanAttributes['durable_actors.connection_id'] AS connection_id,
        toUInt64(toUnixTimestamp64Milli(Timestamp)) AS started_at_ms,
        toFloat64OrZero(SpanAttributes['durable_actors.routing_ms']) AS routing_ms,
        Duration / 1e6 AS duration_ms,
        toFloat64OrNull(SpanAttributes['durable_actors.queue_wait_ms']) AS queue_wait_ms,
        SpanAttributes['durable_actors.host_state'] AS host_state,
        SpanAttributes['durable_actors.outcome'] AS outcome,
        toUInt64OrNull(SpanAttributes['durable_actors.state_version']) AS state_version,
        SpanAttributes['durable_actors.metadata'] AS metadata
    FROM otel_traces
    WHERE ProjectId = {project:String}
        AND ReceivedAt > now64(3) - toIntervalSecond({retention:UInt64})
        AND ({actor_name:String} = '' OR ActorName = {actor_name:String})
        AND ({actor_id:String} = '' OR ActorId = {actor_id:String})
        AND ({request_id:String} = '' OR RequestId = {request_id:String})
        AND toUnixTimestamp64Milli(Timestamp) BETWEEN {from:Int64} AND {to:Int64}
)
WHERE ({connection_id:String} = '' OR connection_id = {connection_id:String})
    AND ({outcome:String} = '' OR outcome = {outcome:String})
    AND (NOT {has_cursor:Bool} OR (started_at_ms, event_id) < ({after_time:UInt64}, {after_id:String}))
ORDER BY started_at_ms DESC, event_id DESC
LIMIT {limit:UInt64}
