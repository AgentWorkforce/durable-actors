# Environment variables

Configuration uses environment variables. Precedence is to use exported variables followed by reading a .env in the folder durable-actors executes in. There is also support for specifying an env file using the `--env-file` switch.

## Connecting to the control plane

| Variable                           | Default                                 | Description                                                                                                                 |
| ---------------------------------- | --------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| `DURABLE_ACTORS_PROJECT_ID`        | `local` on localhost; required remotely | Actor project ID; use the same value in the server registration and backend.                                                |
| `DURABLE_ACTORS_CONTROL_PLANE_URL` | `http://127.0.0.1:7100`                 | HTTP(S) server origin. Paths, query strings, fragments, and embedded credentials are not allowed.                           |
| `DURABLE_ACTORS_SECRET`            | Unset                                   | Optional shared secret. Set the same value on the server and backend to enable authentication. Keep it out of browser code. |
| `DURABLE_ACTORS_INVOKE_API_KEY`    | Unset                                   | Optional long-lived key for backends that only call actors. Accepted solely by `invoke`, `find-actor`, and `find-websocket`; it cannot deploy, issue sessions, read contracts, observe, or publish socket effects. Invocations are limited to published RPC methods like a session; WebSocket URLs carry the metadata the backend supplies, as with the secret. The server requires `DURABLE_ACTORS_SECRET` alongside it, and the two must differ. On a backend, set it instead of `DURABLE_ACTORS_SECRET`; the SDKs send it when no secret is set. |

## Diagnostics and runtime overrides

| Variable                   | Default                   | Description                                                                                      |
| -------------------------- | ------------------------- | ------------------------------------------------------------------------------------------------ |
| `RUST_LOG`                 | `info`                    | Runtime log filter, such as `warn` or `debug`; the packaged container supplies its own filter.   |
| `DURABLE_ACTORS_TELEMETRY` | Disabled                  | Set to `1` to enable SDK invocation telemetry on standard error; unset or `0` keeps it disabled. |
| `DURABLE_ACTORS_BINARY`    | Downloaded runtime        | Use an existing native executable. Relative paths resolve from the working directory.            |
| `DURABLE_ACTORS_CACHE_DIR` | `~/.cache/durable-actors` | Runtime download cache; ignored when `DURABLE_ACTORS_BINARY` is set.                             |

## Local development

| Variable                    | Default                            | Description                                                                                                           |
| --------------------------- | ---------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `DURABLE_ACTORS_PROJECT`    | `.`                                | Actor project directory.                                                                                              |
| `DURABLE_ACTORS_ENTRYPOINT` | Auto-detected | Actor source file, relative to the project. Discovers `actors.ts` or `actors.py` in `src/` or the project root; multiple matches require an explicit setting. |
| `DURABLE_ACTORS_PORT`       | `7100`                             | Listening port; `0` selects a free port. `--port` overrides it for one run.                                           |
| `DURABLE_ACTORS_DATA_DIR`   | `<project>/.durable-actors`        | Persistent local state directory.                                                                                     |
| `DURABLE_ACTORS_STORAGE`    | `local`                            | `local` for file storage or `gcs` for a GCS bucket. GCS also requires `DURABLE_ACTORS_BUCKET` and Google credentials. |
| `DURABLE_ACTORS_LOCAL_HOST_BINARY` | The runtime | Executable run for each actor host; it must not exit before the host does. |
| `DURABLE_ACTORS_LOCAL_HOST_READY_TIMEOUT_MS` | `30000` | Time a new actor host has to become ready. |
| `DURABLE_ACTORS_LOCAL_HOST_LEASE_MS` | Unset | Actor host lease, which may exceed 60000 ms for debugging. Leases of exited hosts are then released. |

## WebSockets

| Variable                                | Default | Description                                                                                                                          |
| --------------------------------------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `DURABLE_ACTORS_SOCKET_MAX_CONNECTIONS` | `32768` | Per-actor connection limit on each process; 1–32,768, including pending connect handlers. Actual capacity depends on workload and gateway resources. |

## Capacity and placement

| Variable                                | Default | Description                                                                                                                                                                                                                                                                                 |
| --------------------------------------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DURABLE_ACTORS_HOST_IDLE_TIMEOUT_MS`   | `10000` | Actor idle time before eviction; 1–86400000 ms. Applies locally too. Method calls and WebSocket handlers reset the timer; running handlers defer eviction. Open WebSockets remain at the gateway and do not keep the sandbox alive. Automatic gateway replies do not reset actor idle time. |
| `DURABLE_ACTORS_HOST_STARTUP_MS`        | `30000` | Executor startup timeout in milliseconds. Actor hosts set it to three times the Rapid operation timeout.                                                                                                                                                                                    |
| `DURABLE_ACTORS_SPARE_IDLE` | `64` | Ready actor sandboxes of the default actor size per image and configured compute region; must not exceed the fleet budget. Zero creates hosts on demand. Control-plane replicas must share pool settings. Customer secrets are installed at assignment. |
| `DURABLE_ACTORS_SPARE_TIERS` | Unset | Spare sizes as a JSON list, replacing `DURABLE_ACTORS_SPARE_IDLE`: `[{"cpuMillis":250,"cpuLimitMillis":null,"memoryMiB":256,"memoryLimitMiB":256,"warm":2},{"cpuMillis":250,"cpuLimitMillis":null,"memoryMiB":512,"memoryLimitMiB":512,"warm":1}]`. Each tier keeps `warm` ready sandboxes per image and region. Every field is required; a `null` `cpuLimitMillis` leaves CPU uncapped. An actor starts on a spare only when its requests and limits equal a tier, counting the limits it inherits from the default actor size; other sizes start a new sandbox. Tiers must differ, and their `warm` counts together must not exceed the fleet budget. |
| `DURABLE_ACTORS_SPARE_FLEET_MAX` | `256` | Maximum unassigned spares across pools and tiers. Active actors do not count against this budget. |
| `DURABLE_ACTORS_SPARE_MAX_STARTING` | `32` | Maximum simultaneous spare starts across pools and tiers. The pool that has waited longest starts first, so a busy tier cannot starve another. |
| `DURABLE_ACTORS_SPARE_REFILL_INTERVAL_MS` | `1000` | How often the pool manager refills spare pools; 50–60000 ms. Shorter intervals replace claimed spares sooner and query PostgreSQL more often. |
| `DURABLE_ACTORS_SPARE_RETIRE_CONCURRENCY` | `64` | Retired spares the pool manager deletes at once; 1–1024. Retiring spares count against the fleet maximum until deleted. |
| `DURABLE_ACTORS_SPARE_REAP_INTERVAL_MS` | `30000` | How often the pool manager drops stopped spares and hosts from the registry; 500–600000 ms. Each pass inspects every registered sandbox: a Modal API call, but only a local read on GKE. |
| `DURABLE_ACTORS_SPARE_POD_REAP_INTERVAL_MS` | `1000` | How often the pool manager deletes finished sandboxes; 100–60000 ms. On GKE each pass reads the shared pod watch and only finished pods cost an API call; Modal sandboxes end on their own. |
| `DURABLE_ACTORS_GKE_REAP_CONCURRENCY` | `64` | Finished GKE pods the pool manager deletes at once; 1–1024. |
| `DURABLE_ACTORS_SPARE_TTL_SECONDS`      | `600`   | Unassigned host lifetime; 30–3600 seconds.                                                                                                                                                                                                                                                  |
| `DURABLE_ACTORS_HOST_CPU_MILLIS` | `250` | Default actor CPU request in millicores, for actors that set none. Spares use it unless tiers are set. |
| `DURABLE_ACTORS_HOST_CPU_LIMIT_MILLIS`  | Unset   | Default actor CPU limit; at least the request. Unset leaves CPU uncapped.                                                                                                                                                                                                                   |
| `DURABLE_ACTORS_HOST_MEMORY_MIB` | `256` | Default actor memory request in MiB, for actors that set none. Spares use it unless tiers are set. |
| `DURABLE_ACTORS_HOST_MEMORY_LIMIT_MIB`  | Request | Default actor memory limit; at least the request.                                                                                                                                                                                                                                           |
| `DURABLE_ACTORS_SANDBOX_MIN_CPU_MILLIS` | `125`   | Smallest `@Compute` CPU request or limit a deployment may set, in millicores.                                                                                                                                                                                                               |
| `DURABLE_ACTORS_SANDBOX_MAX_CPU_MILLIS` | `4000`  | Largest `@Compute` CPU request or limit a deployment may set, in millicores.                                                                                                                                                                                                                |
| `DURABLE_ACTORS_SANDBOX_MIN_MEMORY_MIB` | `256`   | Smallest `@Compute` memory request or limit a deployment may set, in MiB.                                                                                                                                                                                                                   |
| `DURABLE_ACTORS_SANDBOX_MAX_MEMORY_MIB` | `4096`  | Largest `@Compute` memory request or limit a deployment may set, in MiB.                                                                                                                                                                                                                    |
| `DURABLE_ACTORS_REGION`                 | Unset   | Default region for new actors. Without a decorator region override, explicit assignments must match it; existing actors keep their saved home.                                                                                                                                              |
| `DURABLE_ACTORS_HOME_REGION`            | Unset   | Region requested by a trusted backend. Omit to use the actor's saved home or the server default.                                                                                                                                                                                            |

## Organization actor limits

Hosted control planes enable per-organization limits by default, shared by every project whose deployment names that `organizationId`; local development requires explicit opt-in. Controllers share Redis for enforcement, or use memory when Redis is absent (single controller only).

| Variable | Default | Description |
| --- | --- | --- |
| `DURABLE_ACTORS_ORGANIZATION_LIMITS_ENABLED` | `true` hosted; `false` local | Enable activation limits. |
| `DURABLE_ACTORS_ORGANIZATION_MAX_ACTIVE_ACTORS` | `500` | Maximum starting, running, and draining activations. |
| `DURABLE_ACTORS_ORGANIZATION_ACTOR_STARTS_PER_SECOND` | `50` | Sustained start rate, including waking dormant actors. |
| `DURABLE_ACTORS_ORGANIZATION_ACTOR_START_BURST` | `100` | Start-token capacity, initially full. |

Positive integer limits also map to the Helm chart's `organizationLimits` values. Each invocation, WebSocket, or alarm that starts a sandbox consumes one start token and active slot; warm deliveries consume neither. A confirmed lifecycle stop releases its slot; a slot also expires 35 seconds after its host's last report, or 3 minutes after launch if the host never reports. Rejections return `429` with `actor_quota_exceeded` or `actor_start_rate_limited`; rate rejections include `Retry-After`. Backend failures block new starts with `503`.

Operators can override hosted defaults directly in PostgreSQL without restarting:

```sql
INSERT INTO durable_actors_organization_limits
    (organization_id, max_active_actors, actor_starts_per_second, actor_start_burst)
VALUES ('my-organization', 20000, 500, 1000)
ON CONFLICT (organization_id) DO UPDATE SET
    max_active_actors = EXCLUDED.max_active_actors,
    actor_starts_per_second = EXCLUDED.actor_starts_per_second,
    actor_start_burst = EXCLUDED.actor_start_burst;
```

`NULL` inherits the environment default; deleting the row restores all defaults. Lowering capacity blocks additional starts while existing hosts continue serving. Overrides are operator-only; local opt-in uses environment defaults.

Every deployment must have a valid `organization_id`; local development uses `local`. Upgrading to this schema copies each project override to its organization, keeping the highest value per setting, and drops `durable_actors_project_limits`. Startup fails without changes, listing the affected projects, while any deployment lacks a valid organization or any project override lacks a deployment. Redeploy those projects with `organizationId` before upgrading; a redeploy replaces their hosts, so none keep credentials without an organization.

## Authentication

| Variable                                | Default                        | Description                                                                        |
| --------------------------------------- | ------------------------------ | ---------------------------------------------------------------------------------- |
| `DURABLE_ACTORS_JWT_KEY_ID`             | `primary`                      | Signing key identifier.                                                            |
| `DURABLE_ACTORS_JWT_ISSUER`             | `durable-actors-control-plane` | Token issuer. Changing it invalidates tokens that running hosts and sessions hold. |
| `DURABLE_ACTORS_AUTHORITY_JWT_AUDIENCE` | `durable-actors-authority`     | Server authentication audience.                                                    |
| `DURABLE_ACTORS_INVOKE_JWT_AUDIENCE`    | `durable-actors-invoke`        | Actor-call audience.                                                               |
| `DURABLE_ACTORS_JWT_MAX_TTL_SECONDS`    | `86400`                        | Positive maximum credential lifetime in seconds.                                   |

## Kubernetes hosting

Use the [self-hosting guide](../self-hosting.md) and [Helm chart](../../charts/durable-actors/README.md) to run on GKE Sandbox with PostgreSQL and one Standard GCS bucket. One controller handles HTTP and WebSockets, and actor pods start on demand. The chart sets `DURABLE_ACTORS_SPARE_IDLE=0`, or `DURABLE_ACTORS_SPARE_TIERS` from `actors.tiers`; the runtime's standalone default above remains 64.

| Variable | Description |
| --- | --- |
| `DURABLE_ACTORS_PROCESS_ROLE` | `control_plane` serves the API; `pool_manager` fills, trims and cleans up the warm pool with the same settings and must run as exactly one replica. In Rapid mode the pool manager also deletes history that bases cover. The provider assigns actor/spare roles. |
| `DURABLE_ACTORS_CONTROL_PLANE_BIND` | Listen address, `0.0.0.0:7100` in the chart. |
| `DURABLE_ACTORS_ALARM_POLL_INTERVAL_MS` | Interval for checking due alarms, default `100` milliseconds. The chart sets it from `alarms.pollIntervalMs`. Shorter intervals reduce polling delay and increase database load. |
| `DURABLE_ACTORS_CONTROL_PLANE_URL` | Private Kubernetes Service origin reachable from sandboxes. |
| `DURABLE_ACTORS_GATEWAY_ACCEPT_CONNECTIONS` | Whether this process accepts browser WebSockets; defaults to `true`. |
| `DURABLE_ACTORS_PUBLIC_URL` | Public HTTPS origin for client invocation and socket routing. |
| `DURABLE_ACTORS_POSTGRES_URL` | Registry and spare bookkeeping database; the user needs migration privileges. |
| `DURABLE_ACTORS_ANALYTICS_BACKEND` | Telemetry backend: `none` (default) or `clickhouse`. ClickHouse stores request spans, actor logs, and actor inventory; with `none`, request and log endpoints return empty results. PostgreSQL remains required for control-plane bookkeeping. |
| `DURABLE_ACTORS_OTLP_ENDPOINT` | OpenTelemetry Collector gRPC endpoint passed to actor hosts as `OTEL_EXPORTER_OTLP_ENDPOINT`. Hosts export request spans and actor logs there with their host token; unset disables export. |
| `DURABLE_ACTORS_PLATFORM_TELEMETRY_ENDPOINT` | Collector gRPC endpoint where the control plane reports deployments, WebSocket opens and closes, and a gateway heartbeat every 10 seconds. It republishes every current deployment on startup. Requires `DURABLE_ACTORS_PLATFORM_TELEMETRY_TOKEN_FILE`, a projected service-account token sent on every export. The chart sets both when `telemetry.collector.platform.issuer` is set; unset disables reporting. |
| `DURABLE_ACTORS_CLICKHOUSE_URL` | ClickHouse HTTP(S) endpoint, required for the ClickHouse backend. |
| `DURABLE_ACTORS_CLICKHOUSE_DATABASE` | Existing ClickHouse database. The application user needs SELECT, INSERT, CREATE TABLE, CREATE VIEW, and ALTER TABLE privileges in it. |
| `DURABLE_ACTORS_CLICKHOUSE_USER` | ClickHouse application user. |
| `DURABLE_ACTORS_CLICKHOUSE_PASSWORD` | ClickHouse application password. Supply through a secret. |
| `DURABLE_ACTORS_USAGE_TOPIC` | Optional Pub/Sub topic, `projects/<gcp-project>/topics/<topic>`, for actor activation usage. Unset disables publishing. The control plane uses Application Default Credentials with publisher access to this topic. |
| `DURABLE_ACTORS_EVENTS_TOPIC` | Optional Pub/Sub topic, `projects/<gcp-project>/topics/<topic>`, for [platform events](#published-events): sandbox kills and MCP feedback. Unset disables publishing. The pool manager and control plane use Application Default Credentials with publisher access to this topic. |
| `DURABLE_ACTORS_POSTGRES_MAX_CONNECTIONS` | PostgreSQL pool size per process, default `8`, range 1–1,000. The chart sets it from `postgres.maxConnections`. Invocation routing and alarm polling share the pool. Budget the database's connection limit for this pool size times the controller replica count, plus other database clients. |
| `DURABLE_ACTORS_REDIS_URL` | Redis that carries socket messages between processes; required with more than one. `redis://host:6379`, or `rediss://:password@host:6378` for TLS. If the connection drops, the process closes its sockets with `1012` and browsers reconnect. Unset, a single process keeps everything in memory. The chart reads it from the key named by `redis.urlSecretKey`. |
| `DURABLE_ACTORS_REDIS_CA_CERT` | PEM CA for a `rediss://` server with a private CA, such as Memorystore. The chart reads it from the key named by `redis.caSecretKey`. |
| `DURABLE_ACTORS_PERSISTENCE` | `standard` by default. Select `rapid` for Rapid append logs with SQLite bases in Standard. |
| `DURABLE_ACTORS_BUCKET` | Standard GCS bucket for ownership and, in Standard mode, immutable state snapshots. |
| `DURABLE_ACTORS_ARTIFACT_BUCKET` | Immutable compiled actor code. Defaults to `DURABLE_ACTORS_BUCKET` in Standard mode; the chart uses that bucket in both modes. |
| `DURABLE_ACTORS_ARCHIVE_BUCKET` | Required only in Rapid mode: the Standard GCS bucket for log manifests, sealed heads and SQLite bases. The chart uses `storage.bucket`. |
| `DURABLE_ACTORS_RAPID_BUCKETS` | Required only in Rapid mode: JSON array of one or two `{ "bucket": "name", "zone": "us-west4-a" }` placements in distinct zones. Every configured zone must durably flush before acknowledgment. With one zone, changes since an actor's last base are unavailable while that zone is down. |
| `DURABLE_ACTORS_RAPID_PLACEMENT` | Rapid only: `all` (default) writes every configured zone. `nearest` needs two zones, usually in different regions: at assignment the host opens its log in both, keeps the first to open, and writes only there for the session. The chart uses `storage.rapid.placement`. |
| `DURABLE_ACTORS_ARCHIVE_BATCH_BYTES` | Largest byte range a restore reads at once from log batches older hosts archived, default `16777216`. |
| `DURABLE_ACTORS_NODE_CACHE_BASE_BUCKET` | Node cache only: the Standard bucket it uploads parked databases to as bases when their hosts find one due, and downloads each restoring actor's newest base from with its own identity. Unset, it uploads nothing and restores read the base with the host's token. The chart sets it to `storage.bucket` in Rapid mode with `nodeCache.googleServiceAccount`. |
| `DURABLE_ACTORS_SWEEP_MIN_AGE_HOURS` | Pool manager, Rapid mode: how old Rapid history, archived history and superseded bases must be before the sweeper deletes what a base covers, default `720`. The chart uses `historySweeper.minAgeHours`. |
| `DURABLE_ACTORS_SWEEP_INTERVAL_SECONDS` | Pool manager, Rapid mode: time between passes, default `3600`. The chart uses `historySweeper.intervalSeconds`. |
| `DURABLE_ACTORS_SWEEP_DRY_RUN` | Pool manager, Rapid mode: `true` logs what each pass would delete without deleting it. The default is `true`. The chart uses `historySweeper.dryRun`. |
| `DURABLE_ACTORS_GKE_NAMESPACE` | Dedicated sandbox namespace; the chart defaults to `<release-namespace>-<release-name>`. |
| `DURABLE_ACTORS_GKE_ZONE` | Actor placement zone, for example `us-west4-a`; the runtime infers the canonical compute region. The chart sets this from `placement.zone`. |
| `DURABLE_ACTORS_GKE_ZONES` | Advanced alternative to `GKE_ZONE`: JSON map from canonical compute region to eligible Google zones, such as `{"north-america-west":["us-west4-a","us-west4-b"]}`. A single zone string per region is also accepted. Configure only one of the two placement variables. |
| `DURABLE_ACTORS_GKE_RUNTIME_CLASS` | RuntimeClass of sandbox pods, default `gvisor` (the class GKE provides). gVisor's own memory counts against a pod's limit, and GKE's class declares no overhead for it. To add it on top of the limit, create a RuntimeClass with the `gvisor` handler, the same scheduling as GKE's class and an `overhead.podFixed.memory`, and name it here. Under any other class than `gvisor` the pods carry the volume mount annotations GKE adds only for its own. |
| `DURABLE_ACTORS_TYPESCRIPT_IMAGE` | TypeScript actor OCI image pinned by SHA-256 digest; selected for compiled `.mjs` artifacts. |
| `DURABLE_ACTORS_PYTHON_IMAGE` | Python actor OCI image pinned by SHA-256 digest; selected for compiled `.pyz` artifacts. |
| `DURABLE_ACTORS_EXECUTOR_RUNTIME` | Set by actor images to `typescript` or `python`; selects the executor before a warm spare is assigned. Native hosts infer it from the compiled entrypoint. |
| `DURABLE_ACTORS_JWT_SIGNING_KEY` | Shared base64 Ed25519 PKCS#8 key; stable across restarts. |
| `GOOGLE_APPLICATION_CREDENTIALS` | Optional ADC file; use Workload Identity on GKE. |

Standard mode rejects Rapid bucket and archive settings. Storage mode and bucket identities are fixed for existing actor ownership records; changing them requires a planned data migration. See [Rapid configuration](../self-hosting.md#speed-up-writes-with-gcs-rapid) for bucket creation, IAM, and chart values.

### Host lifecycle reports

Upgraded hosts report `started` after activation, `heartbeat` every 10 seconds, `draining` before executor shutdown, and `stopped` after the storage shutdown attempt. Reports contain the ownership epoch, a per-session sequence, activation/report timestamps, requested resources, residency, and waiting operations. The sandbox provider supplies resources through the internal `DURABLE_ACTORS_HOST_RESOURCES` JSON environment variable. Each report has a one-second timeout and is best effort.

The control plane derives project, actor, host, session, and region from the authenticated host token without reading ownership. This allows a stop report after ownership release. All dashboard inventory comes from lifecycle reports. Without ClickHouse, and in local development, inventory uses a bounded in-memory cache (10,000 actors, 24-hour idle expiry). This POC cache is local to each control-plane process, resets on restart, and contains only reports received by that replica. Use ClickHouse for shared, persistent inventory across replicas.

With ClickHouse enabled, dashboard inventory and request analytics come from ClickHouse, deployed actor classes from the deployments the control plane reports, and WebSocket connections from the events gateways report. PostgreSQL stays the source of truth for running deployments. A connection without a recorded close is open while its gateway keeps reporting, and lost once it has been silent for 30 seconds. Inventory reads never fall back to GCS in any mode: an actor without a report is absent from lifecycle inventory, while a ClickHouse read error is returned to the dashboard. A report older than 35 seconds is shown as unknown unless it is a confirmed `stopped` report, which remains dormant. Explicit state inspection and authoritative ownership still use GCS.

Hosts register ownership immediately and renew on the configured lease schedule. Residency and waiting operations travel only in lifecycle reports, not in ownership records, so changes to them cause no GCS renewals. The dashboard applies each lifecycle report from its live event stream.

`draining` and `stopped` reports also record routing hints in the configured Redis, independently of analytics persistence. Hints identify the actor, host, session, and ownership epoch; late heartbeats cannot clear them, and a late draining report cannot undo a stop. All control-plane replicas must use the same Redis. Hints expire after the configured maximum JWT lifetime plus 60 seconds. Without Redis, hints use a process-local cache capped at 100,000 sessions; a Redis error logs a warning and uses the existing lease and host checks.

Discovery and cached HTTP/socket routes check these hints. A cached HTTP invocation to a shutting-down session returns `not_executed` with `host_unavailable`; sockets discard the cached route and resolve again. Discovery waits for handoff for up to 30 seconds, watching the hint while draining and rechecking authoritative ownership after a stop report or the observed lease expiry. A stop report never releases ownership or permits replacement while its lease remains active. Missing shutdown reports retain the existing host-rejection/retry behavior.

Deploy lifecycle-capable control planes before upgraded host images, then enable ClickHouse inventory once hosts can report. Existing ClickHouse installations switch inventory readers when upgrading to this version. Actors appear as their upgraded hosts report; there is no historical backfill. See the [chart configuration](../../charts/durable-actors/README.md#clickhouse-analytics) for credentials and rollout details.

### Usage events

Deployment registration requires `organizationId` from the upstream service. The control plane persists it with the deployment and binds it into host credentials. Lifecycle analytics and usage updates take it from the authenticated host, including refreshed credentials and estimated stops, without a per-event deployment lookup. Pub/Sub also includes it as an `organizationId` message attribute alongside `projectId`. Changing the organization changes the host configuration; existing activations retain their original attribution.

Coordinate this API upgrade with the upstream deployment service: older control planes reject the new field, and the upgraded API rejects requests without it. Hold deployment registrations and secret rotations while upgrading the control plane and configured host images, then upgrade the upstream service and resume registrations. If that service pins the runtime version, update its pin to the published release before deploying it.

Existing deployments have null organization attribution until re-registered; there is no inferred ownership or historical backfill. Consumers must exclude unattributed events from organization billing. Local development uses `local`.

Setting `DURABLE_ACTORS_USAGE_TOPIC` publishes `started` and `stopped` reports to Pub/Sub, independently of the analytics backend. Heartbeats and draining reports are published as usage only when correcting an estimated stop. Both warm-spare and on-demand activations report through the same host lifecycle path. Usage covers activation through shutdown, including idle time while assigned; it excludes time spent waiting as an unassigned spare and measures allocated resources, not actual CPU utilization. Token grants and usage enforcement belong to the external consumer.

Each message contains the lifecycle event with four additional fields:

| Field | Meaning |
| --- | --- |
| `organizationId` | Owning organization supplied at deployment. |
| `usageId` | Stable `<hostId>:<sessionId>` activation ID, shared by its start and stop. |
| `revision` | `2 * sequence` for a host report; `2 * sequence + 1` for an estimated stop based on that report. Higher revisions replace lower revisions for the same activation. |
| `endedAtMs` | `null` for start or a correction from a live host; the real or estimated stop timestamp for stop. |
| `estimated` | `true` when silence caused an estimated stop; `false` for host reports. |

The event includes `eventId`, `actor` (`project_id`, `actor_name`, `actor_id`), `hostId`, `sessionId`, `ownerEpoch`, `region`, `kind`, `sequence`, `startedAtMs`, `occurredAtMs`, `receivedAtMs`, `resources`, `resident`, and `waiting`. Resources use `cpuMillis` and `memoryMib` for requests, plus optional `cpuLimitMillis` and `memoryLimitMib`. Pub/Sub attributes `projectId` and `eventId` support filtering and deduplication. The topic's Google Cloud project can differ from the actor's project.

Consumers should upsert by actor project, actor identity, and `usageId`, accepting only a higher `revision`. Messages can be duplicated or arrive out of order; do not sum every message. A stop contains the complete interval even if its start was missed: duration is `endedAtMs - startedAtMs`. For example, 3,000 ms at 500 millicores is 1.5 allocated CPU-seconds; at 768 MiB it is 2,304 MiB-seconds. Sum completed activations to roll up usage by actor, project, and organization.

Publishing runs in the background, waits up to five seconds for acknowledgment, and logs failures without blocking lifecycle recording or routing. This POC has no durable outbox or replay; a controller exit or failed publication can lose events. No history is backfilled.

When usage publishing is enabled, controllers retain each session's latest report in shared Redis and check for missing stops every 10 seconds, claiming up to 128 overdue sessions per controller per pass. After 35 seconds without a report, an estimated stop uses the last report's `occurredAtMs` as its end time. It retains that report's sequence, appends `:estimated` to its event ID, and uses the reconciliation time as `receivedAtMs`. It does not count the silence window as usage.

The next newer heartbeat or draining report reopens the usage interval (`endedAtMs: null`, `estimated: false`); a real stop replaces the estimate with the actual end time. Consumers must replace or retract the previous contribution when processing a higher revision, including reopening a previously closed interval. Duplicate and older reports do not extend the freshness window. Redis atomically coordinates reports and expiry across replicas; session records expire after 24 hours without an update. Enable Redis persistence to retain this state across Redis restarts. Without Redis, reconciliation uses a process-local cache capped at 100,000 sessions that resets on restart.

Estimates affect only usage publications. ClickHouse retains the actual lifecycle reports, stale dashboard entries stay unknown, and estimated stops neither create routing hints nor release ownership. Reconciliation performs no GCS or PostgreSQL reads. Redis errors log warnings; start/stop publishing and the other lifecycle paths continue, but reconciliation and heartbeat corrections require the session state to be available.

### Published events

Setting `DURABLE_ACTORS_EVENTS_TOPIC` publishes platform events to Pub/Sub for services outside the control plane. Each message is one event as the `observe/events` stream sends it, with its project and an ID:

```json
{
  "eventId": "5f0c7a…",
  "projectId": "project-1",
  "type": "kill",
  "item": {
    "actorName": "Room",
    "actorId": "lobby",
    "atMs": 1700000000000,
    "reason": "out_of_memory"
  }
}
```

`eventId` is the SHA-256 of the recorded event, the same each time one event is reported; the Pub/Sub attributes `projectId`, `eventId` and `type` repeat the message's. Two types are published:

- `kill`, with a `reason` of `out_of_memory`, `out_of_disk`, `evicted` or `killed`.
- `feedback`, what a developer sends with the MCP `send_feedback` tool: `{ "organizationId": "org-1", "atMs": 1700000000000, "message": "Deploys are slow" }`.

Subscribers must ignore the types and fields they do not know.

The pool manager reports kills, and only for Kubernetes sandboxes. When it restarts it publishes the kills it still sees again under their original `eventId`, so subscribers must skip the IDs they have handled. The control plane that serves the MCP request publishes its feedback. Publishing is best effort: a failed publication, a process that exits, or a kill whose actor takes over two seconds to read can lose an event.

## Modal data plane

Set `DURABLE_ACTORS_SANDBOX_PROVIDER=modal` to run hosts on Modal. The default is `gke`.
Both providers use the same PostgreSQL spare pool, deployment API, actor runtime, and GCS storage.

| Variable | Default | Description |
| --- | --- | --- |
| `DURABLE_ACTORS_SANDBOX_PROVIDER` | `gke` | `gke` or `modal`. |
| `DURABLE_ACTORS_MODAL_APP` | Required for Modal | Dedicated Modal App for this control-plane installation. |
| `DURABLE_ACTORS_MODAL_REGIONS` | Required for Modal | JSON mapping of canonical actor regions to Modal regions, e.g. `{"north-america-west":"us-west"}`. |
| `DURABLE_ACTORS_MODAL_TIMEOUT_SECONDS` | `3600` | Maximum sandbox lifetime, 120–86400 seconds, including time spent as a spare. |

Modal reads the active `~/.modal.toml` profile or `MODAL_TOKEN_ID` / `MODAL_TOKEN_SECRET`.
Its control-plane URL must be an HTTPS origin that carries gRPC. Proxies must preserve HTTP/2
and gRPC response trailers on `/durable_actors.v1.ActorControlPlaneService/*`. GKE zone settings
are unnecessary in Modal mode. CPU requests and limits are passed as Modal physical cores
(`cpu_millis / 1000`); memory requests and limits retain their MiB values.

Sandboxes run on GCP. Use a dedicated Modal App and PostgreSQL spare registry for each
installation; sharing a registry between providers is unsupported. Runtime images must be
accessible without private registry credentials.

Deployment `secretRefs` use the same Kubernetes secrets as GKE. The control plane reads them
from `DURABLE_ACTORS_GKE_NAMESPACE` (default `terse-sandboxes`) at assignment and requires
Kubernetes credentials with permission to get those secrets. Secrets must carry the
`terse.ai/customer-secret=true` label; duplicate environment keys and non-UTF-8 values are rejected.
Deployments without secret references do not require Kubernetes access in Modal mode.
