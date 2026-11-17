-- Delivery can repeat a record, and a control plane that restarts reports what it still sees, so
-- one name, subject and time reads once.
SELECT EventName AS name,
    GatewayId AS gateway_id,
    toUInt64(toUnixTimestamp64Milli(Timestamp)) AS at_ms,
    LogAttributes AS attributes,
    Body AS body
FROM platform_events
WHERE ProjectId = {project:String}
    AND has(splitByChar(',', {names:String}), EventName)
    AND ({actor_name:String} = '' OR ActorName = {actor_name:String})
    AND ({actor_id:String} = '' OR ActorId = {actor_id:String})
    AND toUnixTimestamp64Milli(Timestamp) BETWEEN {from:Int64} AND {to:Int64}
    AND ReceivedAt > now64(3) - toIntervalSecond({retention:UInt64})
ORDER BY Timestamp DESC, EventName, ActorName, ActorId, ConnectionId, ReceivedAt
LIMIT 1 BY EventName, ActorName, ActorId, ConnectionId, Timestamp
LIMIT {limit:UInt64}
