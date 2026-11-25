# S3 Express state with a regional checkpoint

An S3 Express One Zone directory bucket can hold actor state (`DURABLE_ACTORS_BUCKET=<name>--<az-id>--x-s3`). It has lower request latency than S3 Standard, but it stores data in one Availability Zone. If that zone is lost, its state is lost too.

The checkpoint copies every write to a regional S3 Standard bucket in the background:

```
DURABLE_ACTORS_STORAGE_PROVIDER=s3
DURABLE_ACTORS_BUCKET=actors-state--usw2-az1--x-s3      # S3 Express, hot path
DURABLE_ACTORS_ARTIFACT_BUCKET=actors-regional           # compiled code stays on S3 Standard
DURABLE_ACTORS_S3_CHECKPOINT_BUCKET=actors-regional      # S3 Standard, async copy
DURABLE_ACTORS_S3_CHECKPOINT_MAX_LAG_MS=5000             # alert threshold (default 5000)
```

## How the copy works

Actor hosts and the control plane wrap the Express bucket in a `CheckpointedBucket`:

- **Reads** go to Express only.
- **Writes** go to Express. After a write succeeds, its key, bytes and generation are put on an in-memory queue, and the write returns without waiting for the copy.
- **A background task** drains the queue to the Standard bucket, one lane per actor.
  - **In write order:** each actor's writes are copied strictly in the order they were written, one at a time. Different actors copy in parallel (16 at once by default). A SQLite snapshot never reaches the copy before its parent, and an owner record never before the snapshot it names.
  - **Exact generations:** each copy is written at its exact `da-generation`, and a delayed copy never overwrites a newer one (`If-Match`/`If-None-Match` on the copy's current ETag).
  - **Collapsing:** several queued writes to the same key collapse into one copy of the newest, which moves to the newest write's place in the lane. A late completion of an older write never replaces a newer queued one.
  - **Unknown outcomes:** a write whose outcome is unknown (a lost reply) is resolved with one read of Express and queued in its place.
  - **Retries:** a failed copy is retried with backoff, from 100 ms up to 5 s. The actor's later writes wait for it; other actors continue.
- **Memory is bounded.** Once queued payloads reach 64 MiB, new entries keep only their key and generation. The task reads the object back from Express. If Express already holds a newer generation, the entry moves to the back of its lane, behind whatever that newer write depends on.
- **Crash catch-up.** A host's lane is held from startup until the actor's snapshots that Express has and the copy lacks are queued ahead of everything else. Those snapshots are left behind by a host that died before draining its queue. The catch-up is one LIST on each bucket, and it is retried until it succeeds. Activation does not wait for it.
- **Control-plane owner records.** Before the control plane copies an owner record (takeover, recovery), it copies any of that actor's snapshots the copy lacks. Those were written by another process.
- **Clean shutdown.** A host flushes its queue after releasing ownership, within the shutdown deadline. It logs `checkpointed=true|false` on the `host_storage_shutdown` event.

The copy keeps the bucket layout and generation metadata, so the Standard bucket is itself a complete durable-actors bucket.

## Metrics

Every 10 s each process logs `event="storage_checkpoint"` with these fields:

| field | meaning |
| --- | --- |
| `lag_ms` | age of the oldest write not yet copied (0 when current) |
| `pending` / `pending_bytes` | writes queued or in flight; payload bytes held in memory |
| `copied_total` / `failures_total` | cumulative copies and failed copy attempts |
| `lag_exceeded` | `lag_ms > DURABLE_ACTORS_S3_CHECKPOINT_MAX_LAG_MS` |

When the lag bound is exceeded it also logs `event="storage_checkpoint_lag_exceeded"` at WARN. Alert on that event, or on `lag_ms`, through a log-based metric (CloudWatch metric filter). The bound is an alert threshold, not backpressure: the write path never slows down because the copy is behind.

## RPO

| failure | data lost |
| --- | --- |
| Express zone lost, hosts healthy until then | writes newer than `lag_ms` at the moment of loss. Normally one Standard PUT (tens of ms). Alerting keeps it within `MAX_LAG_MS` (5 s default) unless regional S3 itself is degraded. |
| Express zone lost while a host is also lost without a clean shutdown | the same: that host's queue dies with it, and its writes exist only on Express |
| A host crashes, Express healthy | nothing. Express holds the writes; the next activation's catch-up copies them before anything newer for that actor |
| Regional S3 Standard degraded | nothing while Express is healthy. Lag grows and alerts; copies resume when S3 recovers |

The recovery point is per object and the copy is causally ordered. A restored actor resumes from an owner record and snapshot set that existed together on Express, possibly older than the latest one. Writes acknowledged to a caller after that point are lost, which is what an RPO of `lag_ms` means.

## Proving the copy is restorable

`durable-actors checkpoint-verify --bucket actors-regional --region us-west-2` checks every actor that has an owner record in the bucket. For each one it:
- loads the owner record;
- loads the snapshot activation would load;
- restores that snapshot's whole SQLite dependency chain, exactly as activation does.

It logs `event="checkpoint_verify"` with `owners`, `restorable` and `unrestorable_total`, and one `checkpoint_unrestorable` event per failing actor, with the reason. It exits 1 when any actor cannot be activated from the bucket alone.

Run it against the regional copy on a schedule, for example a CronJob every 15 minutes, and alert on `unrestorable_total > 0`. A brief non-zero count right after a host crash is expected until the next activation's catch-up runs; a count that persists means a zone loss now would lose those actors. Running it after a `checkpoint-restore` checks the restored bucket the same way.

## Recovery

1. Scale actor capacity to zero so no host writes.
2. Either:
   - **Fast:** set `DURABLE_ACTORS_BUCKET` to the Standard checkpoint bucket and unset `DURABLE_ACTORS_S3_CHECKPOINT_BUCKET`. Actors run on S3 Standard at higher latency.
   - **Back to Express:** create a directory bucket in a healthy zone, then run
     `durable-actors checkpoint-restore --from actors-regional --to actors-state--usw2-az3--x-s3 --region us-west-2`.
     Then point `DURABLE_ACTORS_BUCKET` at it, with the checkpoint still enabled.
     The restore keeps generations and skips objects already present at the same or a later generation, so an interrupted restore can be re-run. `--prefix` restores one project or actor.
     Run `checkpoint-verify` on the restored bucket before scaling back up.
3. Scale actor capacity back up.

## Credentials

Directory buckets authorize object operations through a session from `s3express:CreateSession`, not through `s3:*` object actions. Each actor's STS session policy grants `s3express:CreateSession` on the Express bucket: `ReadOnly` mode when the actor only reads it, `ReadWrite` otherwise. The policy also keeps prefix-scoped `s3:*` grants for the actor's own objects in the checkpoint and artifact buckets.

IAM cannot scope `CreateSession` to a key prefix, so on the Express bucket per-actor isolation is bucket-level. A host can reach other actors' objects in that bucket, but not their copies on S3 Standard. Prefix isolation on Express needs hosts without Express credentials. Upstream TerseAI/durable-actors#273 does this with control-plane-signed per-object URLs. Moving to that broker is the follow-up.

The actor role and the control-plane role each need `s3express:CreateSession` on the directory bucket ARN (`arn:aws:s3express:<region>:<account>:bucket/<name>`). Grant the actor role nothing else on Express. AgentWorkforce/infra defines both roles.
