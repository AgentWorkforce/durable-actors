-- Redelivered reports are left in: a copy has its original's status and time, so it can't start a run.
WITH reports AS (
    SELECT actor_id, owner_epoch, sequence, occurred_at_ms,
           multiIf(kind = 'stopped' OR resident = false, 2, resident = true, 1, 0) AS status,
           kind = 'stopped' AS stopped
    FROM actor_residency_reports
    WHERE project_id = {project:String} AND actor_name = {actor_name:String}
      AND occurred_at_ms BETWEEN {from:UInt64} AND {to:UInt64}
      AND ({actor_id:String} = '' OR actor_id = {actor_id:String})
), ordered AS (
    SELECT *, max(occurred_at_ms) OVER (
        PARTITION BY actor_id ORDER BY owner_epoch, sequence
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS latest_before
    FROM reports
), kept AS (
    SELECT *,
           lagInFrame(status, 1, 255) OVER actor AS previous_status,
           lagInFrame(occurred_at_ms) OVER actor AS previous_at,
           lagInFrame(stopped) OVER actor AS previous_stopped
    FROM ordered
    WHERE occurred_at_ms >= latest_before
    WINDOW actor AS (
        PARTITION BY actor_id ORDER BY owner_epoch, sequence
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
), numbered AS (
    SELECT *, sum(
        status != previous_status
        OR (occurred_at_ms - previous_at > {stale:UInt64} AND NOT previous_stopped)
    ) OVER (
        PARTITION BY actor_id ORDER BY owner_epoch, sequence ROWS UNBOUNDED PRECEDING
    ) AS run
    FROM kept
)
SELECT actor_id,
       argMin(owner_epoch, tuple(owner_epoch, sequence)) AS first_epoch,
       argMin(sequence, tuple(owner_epoch, sequence)) AS first_sequence,
       toUInt8(any(status)) AS run_status,
       min(occurred_at_ms) AS started_at_ms,
       max(occurred_at_ms) AS last_at_ms,
       toBool(argMax(stopped, tuple(owner_epoch, sequence))) AS last_stopped
FROM numbered
GROUP BY actor_id, run
ORDER BY started_at_ms DESC, first_epoch DESC, first_sequence DESC
LIMIT {limit:UInt64}
