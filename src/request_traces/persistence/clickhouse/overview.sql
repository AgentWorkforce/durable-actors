SELECT actor_name, count() AS count,
    countIf(outcome != 'rerouted') AS attempts,
    countIf(outcome = 'completed') AS completed,
    toFloat64(quantileTDigestOrNullIf(0.95)(duration_ms, outcome != 'rerouted')) AS p95,
    toFloat64(quantileTDigestOrNullIf(0.95)(queue_wait_ms, outcome != 'rerouted')) AS queue_p95,
    toUInt8(grouping(actor_name)) AS is_total
FROM (
    SELECT ActorName AS actor_name, Duration / 1e6 AS duration_ms,
        SpanAttributes['durable_actors.outcome'] AS outcome,
        toFloat64OrNull(SpanAttributes['durable_actors.queue_wait_ms']) AS queue_wait_ms
    FROM otel_traces
    WHERE ProjectId = {project:String}
        AND ReceivedAt > now64(3) - toIntervalSecond({retention:UInt64})
        AND toUnixTimestamp64Milli(Timestamp) BETWEEN {from:Int64} AND {to:Int64}
)
GROUP BY GROUPING SETS ((actor_name), ())
ORDER BY is_total DESC, actor_name
LIMIT 500
