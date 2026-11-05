SELECT toUInt64(intDiv(toUnixTimestamp64Milli(Timestamp) - {from:Int64}, {bucket:Int64})) AS bucket,
    lower(SeverityText) AS level, count() AS lines
FROM otel_logs
WHERE ProjectId = {project:String}
    AND ReceivedAt > now64(3) - toIntervalSecond({retention:UInt64})
    AND LogId != ''
    AND ({actor_name:String} = '' OR ActorName = {actor_name:String})
    AND ({actor_id:String} = '' OR ActorId = {actor_id:String})
    AND ({request_id:String} = '' OR RequestId = {request_id:String})
    AND ({search:String} = '' OR positionCaseInsensitiveUTF8(Body, {search:String}) > 0)
    AND toUnixTimestamp64Milli(Timestamp) >= {from:Int64}
    AND toUnixTimestamp64Milli(Timestamp) < {to:Int64}
GROUP BY bucket, level
