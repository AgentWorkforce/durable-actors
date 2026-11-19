SELECT database_bytes
FROM actor_database_sizes
WHERE project_id = {project:String} AND actor_name = {actor_name:String}
    AND actor_id = {actor_id:String}
ORDER BY occurred_at_ms DESC
LIMIT 1
