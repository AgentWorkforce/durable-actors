SELECT actor_name, actor_id, count() AS admitted,
    avg(assumeNotNull(queue_wait_ms)) AS average_ms,
    max(assumeNotNull(queue_wait_ms)) AS max_ms
FROM (
    SELECT ActorName AS actor_name, ActorId AS actor_id,
        SpanAttributes['durable_actors.outcome'] AS outcome,
        toFloat64OrNull(SpanAttributes['durable_actors.queue_wait_ms']) AS queue_wait_ms
    FROM otel_traces
    WHERE ProjectId = {project:String}
        AND ReceivedAt > now64(3) - toIntervalSecond({retention:UInt64})
        AND toUnixTimestamp64Milli(Timestamp) BETWEEN {from:Int64} AND {to:Int64}
        AND ({actor_name:String} = '' OR ActorName = {actor_name:String})
)
WHERE outcome != 'rerouted' AND queue_wait_ms IS NOT NULL
GROUP BY actor_name, actor_id
ORDER BY admitted DESC, actor_name, actor_id
LIMIT 500
