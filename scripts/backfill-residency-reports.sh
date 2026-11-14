#!/usr/bin/env bash
# Copies lifecycle reports that predate actor_residency_reports into it. Run once after the release
# that creates the table; its materialized view stores every later report. Each statement copies one
# actor class, the leading key of actor_lifecycle, so the run reads that table once. Re-running is
# safe: the table and the residency query both collapse duplicates.
#
#   DURABLE_ACTORS_CLICKHOUSE_URL=http://127.0.0.1:8123 DURABLE_ACTORS_CLICKHOUSE_DATABASE=... \
#   DURABLE_ACTORS_CLICKHOUSE_USER=... DURABLE_ACTORS_CLICKHOUSE_PASSWORD=... \
#     scripts/backfill-residency-reports.sh
set -euo pipefail

# Keeps the password out of the process list, and prints ClickHouse's error before exiting.
clickhouse() {
    local output
    if ! output=$(curl -sS --fail-with-body \
        -K <(printf 'user = "%s:%s"\n' "$DURABLE_ACTORS_CLICKHOUSE_USER" "$DURABLE_ACTORS_CLICKHOUSE_PASSWORD") \
        "$DURABLE_ACTORS_CLICKHOUSE_URL/?database=$DURABLE_ACTORS_CLICKHOUSE_DATABASE&max_execution_time=3600&max_threads=2&max_memory_usage=2000000000$2" \
        --data-binary "$1"); then
        echo "$output" >&2
        exit 1
    fi
    echo "$output"
}

classes=$(clickhouse "SELECT DISTINCT project_id, actor_name FROM actor_lifecycle ORDER BY project_id, actor_name FORMAT TSV" "")
while IFS=$'\t' read -r project actor; do
    # IDs are limited to [A-Za-z0-9._-], so they go into the URL unencoded.
    [[ $project$actor =~ ^[A-Za-z0-9._-]+$ ]] || { echo "unexpected characters in $project $actor" >&2; exit 1; }
    clickhouse "INSERT INTO actor_residency_reports
        (project_id, actor_name, actor_id, event_id, owner_epoch, sequence, occurred_at_ms, kind, resident, received_at)
        SELECT project_id, actor_name, actor_id, event_id, owner_epoch, sequence,
               JSONExtractUInt(event_json, 'occurredAtMs'), JSONExtractString(event_json, 'kind'),
               JSONExtract(event_json, 'resident', 'Nullable(Bool)'), received_at
        FROM actor_lifecycle WHERE project_id = {project:String} AND actor_name = {actor:String}" \
        "&param_project=$project&param_actor=$actor" >/dev/null
    echo "copied $project $actor"
done <<<"$classes"
