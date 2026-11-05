SELECT LogId AS log_id, ProjectId AS project_id, ActorName AS actor_name, ActorId AS actor_id,
    HostId AS host_id, RequestId AS request_id,
    toUInt64(toUnixTimestamp64Milli(Timestamp)) AS timestamp_ms,
    lower(SeverityText) AS level, Body AS message
FROM otel_logs
WHERE ProjectId = {project:String}
    AND ReceivedAt > now64(3) - toIntervalSecond({retention:UInt64})
    AND LogId != ''
    AND ({actor_name:String} = '' OR ActorName = {actor_name:String})
    AND ({actor_id:String} = '' OR ActorId = {actor_id:String})
    AND ({request_id:String} = '' OR RequestId = {request_id:String})
    AND ({levels:String} = '' OR has(splitByChar(',', {levels:String}), lower(SeverityText)))
    AND ({search:String} = '' OR positionCaseInsensitiveUTF8(Body, {search:String}) > 0)
    AND toUnixTimestamp64Milli(Timestamp) BETWEEN {from:Int64} AND {to:Int64}
    AND (NOT {has_cursor:Bool} OR (timestamp_ms, log_id) < ({after_time:UInt64}, {after_id:String}))
ORDER BY timestamp_ms DESC, log_id DESC
LIMIT {limit:UInt64}
