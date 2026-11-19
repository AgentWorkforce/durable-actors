-- A sandbox's samples count once it was claimed; before that it was a spare.
WITH claims AS (
    SELECT sandbox_id, min(claimed_at_ms) AS claimed_ms
    FROM actor_sandboxes
    WHERE project_id = {project:String} AND actor_name = {actor_name:String}
        AND actor_id = {actor_id:String} AND claimed_at_ms < {to:Int64}
    GROUP BY sandbox_id
)
SELECT toUInt64(intDiv(toInt64(toUnixTimestamp(usage.TimeUnix)) * 1000 - {from:Int64}, {bucket:Int64})) AS bucket,
    maxIfOrNull(usage.Value, usage.MetricName = 'container.cpu.usage') AS cpu_cores,
    CAST(maxIfOrNull(usage.Value, usage.MetricName = 'container.memory.working_set') AS Nullable(UInt64)) AS memory_bytes
FROM sandbox_usage AS usage
INNER JOIN claims ON usage.PodName = claims.sandbox_id
WHERE usage.PodName IN (SELECT sandbox_id FROM claims)
    AND usage.MetricName IN ('container.cpu.usage', 'container.memory.working_set')
    AND toInt64(toUnixTimestamp(usage.TimeUnix)) * 1000 >= greatest({from:Int64}, toInt64(claims.claimed_ms))
    AND toInt64(toUnixTimestamp(usage.TimeUnix)) * 1000 < {to:Int64}
GROUP BY bucket
ORDER BY bucket
