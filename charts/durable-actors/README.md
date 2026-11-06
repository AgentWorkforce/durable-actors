# Durable Actors on GCP

Run Durable Actors on GKE Sandbox with one Standard GCS bucket and PostgreSQL. Standard GCS is the default persistence mode. Enable GCS Rapid when you need faster state writes.

Follow the [self-hosting guide](../../docs/self-hosting.md) for cluster prerequisites, IAM, credentials, installation, and Rapid configuration.

```yaml
publicUrl: https://actors.example.com
placement:
  zone: us-west4-a
storage:
  bucket: my-actor-state
serviceAccount:
  googleServiceAccount: actors@my-project.iam.gserviceaccount.com
ingress:
  enabled: true
  className: my-ingress
  tlsSecret: actors-tls
```

Use your installed ingress controller's class. The controller must support WebSockets; configure its connection timeouts through `ingress.annotations`. With ingress disabled, route your HTTPS endpoint to the chart's ClusterIP Service on port 7100.

Published chart packages pin three matching image digests: `images.controlPlane.digest`, `images.typescript.digest`, and `images.python.digest`. For source builds, supply all three from the same source revision, without the `sha256:` prefix. Each image also has its own `repository` setting.

The Dockerfile has `control-plane`, `typescript`, and `python` targets. The default target contains the Rust control plane. The TypeScript image adds Bun and the execution SDK; the Python image adds Python and its SDK. Releases publish `<version>-control-plane`, `<version>-typescript`, and `<version>-python` tags, each supporting amd64 and arm64. The controller selects the actor image from the compiled artifact and keeps warm pools separate by image.

When upgrading from the shared image configuration, replace `image.repository` and `image.digest` with the three `images` entries. Direct control-plane deployments must replace `DURABLE_ACTORS_RUNTIME_IMAGE` with `DURABLE_ACTORS_TYPESCRIPT_IMAGE` and `DURABLE_ACTORS_PYTHON_IMAGE`. Redeploy existing actor bundles to replace their stored image references.

## Defaults

- One controller handles API requests and WebSockets.
- Actor pods start on demand: `actors.warm: 0`.
- Warm pools refill every second (`actors.refillIntervalMs`), starting up to 32 spares at once across controllers (`actors.maxStarting`). Each controller deletes finished pods every second (`actors.podReapIntervalMs`, `actors.reapConcurrency`) and retired spares 64 at a time (`actors.retireConcurrency`).
- The controller checks for due alarms every 100 milliseconds; configure `alarms.pollIntervalMs` to change the cadence.
- Organization activation limits default to 500 starting/running hosts, 50 starts per second, and a burst of 100, shared by all of an organization's projects. Operators configure `organizationLimits.enabled`, `organizationLimits.maxActiveActors`, `organizationLimits.actorStartsPerSecond`, and `organizationLimits.actorStartBurst`. Configured Redis shares enforcement across replicas; a single controller without Redis uses memory. See [organization overrides](../../docs/reference/configuration.md#organization-actor-limits) for manual PostgreSQL overrides and missing-stop handling.
- Each actor requests and is limited to 500 millicores and 256 MiB unless its deployment overrides resources.
- The controller requests 250 millicores and 512 MiB, with a 1 GiB memory limit. Measure and adjust `resources` for your workload.
- The sandbox namespace defaults to `<release-namespace>-<release-name>`; use one unique sandbox namespace per installation.
- gVisor, separate sandbox credentials, RBAC, and sandbox NetworkPolicy are included.

A single controller can interrupt service during replacement; clients must reconnect WebSockets. Increase `replicaCount` for controller redundancy; more than one controller requires Redis (`redis.urlSecretKey`). Increase `actors.warm` to reduce cold starts. The warm count is per image/region and supports 0–256. It does not cap active actors. Node autoscaling and quotas must accommodate your active workload.

## Rapid writes

Add one or two precreated Rapid buckets in distinct supported zones. The Standard bucket continues to store ownership, code, and permanent archives.

```yaml
storage:
  bucket: my-actor-state
  mode: rapid
  rapid:
    buckets:
      - {bucket: my-actor-rapid-a, zone: us-west4-a}
      - {bucket: my-actor-rapid-b, zone: us-west4-b}
```

Set `storage.rapid.placement: nearest` with one bucket in each of two regions to write each actor's log only in the zone that opens first from its host.

Every configured copy must durably flush before a Rapid write is acknowledged. With one zone, there is no second Rapid copy if that zone is lost before archival. Mode and bucket identities are fixed for existing actor ownership records; choose Rapid before creating actors in that installation. An existing installation needs a planned data migration, not a values-only mode change.

## Advanced settings

`credentialsSecret` defaults to `actors-credentials`; it must contain `postgres-url`, `api-key`, and `jwt-signing-key`. For Redis, add the keys named by `redis.urlSecretKey` and, optionally, `redis.caSecretKey`. `nodeSelector` places controllers on ordinary nodes. `networkPolicy.dnsCidrs` permits specific host-networked or link-local DNS endpoints, for example a NodeLocal DNS `/32`. Add only the address your cluster uses.

See [values.yaml](values.yaml) for the full values reference and [values.schema.json](values.schema.json) for validation. No database, bucket, ingress controller, DNS record, or certificate is provisioned by this chart.

## ClickHouse analytics

To serve actor inventory and request analytics from ClickHouse, add these values with your database connection details:

```yaml
analytics:
  backend: clickhouse
  clickhouse:
    url: https://clickhouse.example.com:8443
    database: actors
    user: actors
    passwordSecretKey: clickhouse-password
```

Store `clickhouse-password` in `credentialsSecret`. Precreate the database and grant the application user SELECT, INSERT, CREATE TABLE, CREATE VIEW, and ALTER TABLE privileges. The control plane creates the lifecycle tables and materialized view. Actor hosts only report to the control plane and need no ClickHouse credentials.

For an existing installation, first deploy lifecycle-capable control planes, then upgrade the TypeScript/Python images and redeploy actor bundles so new hosts report. Enable `analytics.backend: clickhouse` after that rollout. Installations already using ClickHouse switch inventory readers on upgrade; old hosts do not appear until an upgraded activation reports. Historical inventory is not backfilled.

PostgreSQL remains required for control-plane bookkeeping and deployment metadata. The default `analytics.backend: none` reads inventory from an in-memory lifecycle cache on each control-plane replica; it resets on restart and is not shared across replicas. All modes use lifecycle reports for inventory without GCS scans. See [host lifecycle reports](../../docs/reference/configuration.md#host-lifecycle-reports) for freshness and retention behavior.

## Request spans and actor logs

Hosts export request spans and actor console output to an OpenTelemetry Collector, which writes them to ClickHouse and publishes them to Redis for live views. Enable it with ClickHouse analytics and Redis:

```yaml
telemetry:
  collector:
    enabled: true
```

The collector accepts only host tokens and replaces any tenant attributes with the token's verified project, actor, host, and session. It fetches the verifying keys from `/.well-known/openid-configuration` at `publicUrl` and checks each token's signature and audience but not its issuer, so the control plane keeps its token issuer and tokens held by running sandboxes stay valid. Sandboxes reach it on port 4317, and sandboxes outside the cluster use the ingress. The control plane creates `otel_traces` and `otel_logs`, and the collector inserts into them with the same ClickHouse user. The old `actor_traces` and `actor_trace_status` tables are no longer written and empty out through their TTL; drop them by hand if you want the space back sooner.

## Usage publishing

To publish actor activation usage, set an existing Pub/Sub topic:

```yaml
usage:
  topic: projects/my-project/topics/actor-usage
```

Grant the control plane's Google service account `roles/pubsub.publisher` on that topic. Hosts use the existing lifecycle reporting path and need no Pub/Sub credentials. Publishing works with either analytics backend and is disabled when `usage.topic` is empty. Consumers must handle duplicates and out-of-order delivery; see the [usage event contract](../../docs/reference/configuration.md#usage-events) for aggregation and delivery limitations.

Missing-stop reconciliation uses the configured Redis to coordinate estimated usage ends and later corrections across controllers. Configure Redis persistence for restart recovery. Consumers must accept higher revisions that correct or reopen an estimated interval; estimates do not change ownership or dashboard status.
