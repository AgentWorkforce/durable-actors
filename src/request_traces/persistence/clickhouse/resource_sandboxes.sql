-- The sandbox running when the range starts, then each one claimed during it.
SELECT claimed_at_ms, cpu_millis, cpu_limit_millis, memory_mib, memory_limit_mib
FROM actor_sandboxes
WHERE project_id = {project:String} AND actor_name = {actor_name:String}
    AND actor_id = {actor_id:String} AND claimed_at_ms < {to:Int64}
    AND claimed_at_ms >= (
        SELECT max(claimed_at_ms) FROM actor_sandboxes
        WHERE project_id = {project:String} AND actor_name = {actor_name:String}
            AND actor_id = {actor_id:String} AND claimed_at_ms <= {from:Int64}
    )
ORDER BY claimed_at_ms
LIMIT 1 BY claimed_at_ms, sandbox_id
LIMIT 1000
