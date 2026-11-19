SELECT toUInt64(intDiv(toInt64(occurred_at_ms) - {from:Int64}, {bucket:Int64})) AS bucket,
    max(database_bytes) AS bytes
FROM actor_database_sizes
WHERE project_id = {project:String} AND actor_name = {actor_name:String}
    AND actor_id = {actor_id:String}
    AND occurred_at_ms >= {from:Int64} AND occurred_at_ms < {to:Int64}
GROUP BY bucket
ORDER BY bucket
