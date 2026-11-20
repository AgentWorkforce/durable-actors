SELECT bucket, outcome, count() AS requests,
    sum(ifNull(wait_ms, 0)) AS queue_wait_ms,
    count(wait_ms) AS queue_waits,
    sum(duration_ms - ifNull(wait_ms, 0)) AS run_ms
FROM (
    SELECT toUInt64(intDiv(toUnixTimestamp64Milli(Timestamp) - {from:Int64}, {bucket:Int64})) AS bucket,
        SpanAttributes['durable_actors.outcome'] AS outcome,
        toFloat64OrNull(SpanAttributes['durable_actors.queue_wait_ms']) AS wait_ms,
        Duration / 1e6 AS duration_ms
    FROM otel_traces
    WHERE ProjectId = {project:String}
        AND ReceivedAt > now64(3) - toIntervalSecond({retention:UInt64})
        AND ({actor_name:String} = '' OR ActorName = {actor_name:String})
        AND ({actor_id:String} = '' OR ActorId = {actor_id:String})
        AND toUnixTimestamp64Milli(Timestamp) >= {from:Int64}
        AND toUnixTimestamp64Milli(Timestamp) < {to:Int64}
)
WHERE outcome != 'rerouted'
GROUP BY bucket, outcome
