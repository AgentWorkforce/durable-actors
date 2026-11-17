# Durable Actors on AWS

Run Durable Actors on EKS with one Standard S3 bucket and PostgreSQL.

```yaml
publicUrl: https://actors.example.com
placement:
  zone: us-west-2a
storage:
  bucket: my-actor-state
  region: us-west-2
  actorRoleArn: arn:aws:iam::123456789012:role/durable-actors-actor
```

The cluster must provide:

- A node group for actors running gVisor, with nodes labelled and tainted `terse.ai/sandbox-runtime=gvisor`, and a `gvisor` RuntimeClass that schedules onto them:

  ```yaml
  apiVersion: node.k8s.io/v1
  kind: RuntimeClass
  metadata: {name: gvisor}
  handler: runsc
  scheduling:
    nodeSelector: {terse.ai/sandbox-runtime: gvisor}
    tolerations: [{key: terse.ai/sandbox-runtime, operator: Equal, value: gvisor, effect: NoSchedule}]
  ```
- IMDSv2 with a hop limit of 1 on actor nodes, and NetworkPolicy enforcement (for example, the VPC CNI's `enableNetworkPolicy`).
- An EKS Pod Identity association for the controller's service account (`<release>-durable-actors`) to a role that can read and write the bucket and assume `storage.actorRoleArn`. Don't associate a role with the sandbox namespace's `sandbox` service account.

Set `VERSION` to a [Durable Actors release](https://github.com/TerseAI/durable-actors/releases). Published charts pin the control-plane, TypeScript and Python image digests.

```sh
helm upgrade --install actors oci://ghcr.io/terseai/charts/durable-actors-eks --version "$VERSION" \
  --namespace actors --values actors-values.yaml --wait --timeout 10m
```

## Images from your own ECR

By default nodes pull the published images from Google Artifact Registry. To serve them from ECR instead, copy them by digest, so the chart's pinned digests still match:

```sh
SOURCE=us-central1-docker.pkg.dev/fluid-analogy-473415-c2/public/durable-actors
TARGET=123456789012.dkr.ecr.us-west-2.amazonaws.com/durable-actors
aws ecr create-repository --repository-name durable-actors
aws ecr get-login-password | crane auth login "${TARGET%%/*}" --username AWS --password-stdin
helm show values oci://ghcr.io/terseai/charts/durable-actors-eks --version "$VERSION" |
  grep -oE '[a-f0-9]{64}' | while read -r digest; do
    crane copy "$SOURCE@sha256:$digest" "$TARGET:sha256-$digest"
  done
```

Then point the chart at the copies:

```yaml
images:
  controlPlane: {repository: 123456789012.dkr.ecr.us-west-2.amazonaws.com/durable-actors}
  typescript: {repository: 123456789012.dkr.ecr.us-west-2.amazonaws.com/durable-actors}
  python: {repository: 123456789012.dkr.ecr.us-west-2.amazonaws.com/durable-actors}
```

Controller and actor nodes pull with their node role, which needs `AmazonEC2ContainerRegistryReadOnly` (or a repository policy for another account). `docker pull`, `tag` and `push` can change digests, so use `crane copy` or `skopeo copy --all`.

`storage.actorRoleArn` must trust the controller role. Its permissions are the most any actor session gets; each session is narrowed to one actor's objects. Add a bucket lifecycle rule that aborts incomplete multipart uploads.

## S3 Express logs

For faster writes, add one or two S3 Express One Zone directory buckets in distinct zones. Each write is acknowledged once it is stored in every listed bucket; `storage.bucket` keeps ownership, code and the permanent archive.

```yaml
storage:
  express:
    buckets:
      - {bucket: actor-logs--use2-az1--x-s3, zone: use2-az1}
      - {bucket: actor-logs--use2-az2--x-s3, zone: use2-az2}
```

The controller role also needs `s3express:CreateSession` on these buckets. It opens one session on each at startup and fails with the missing permission rather than on the first write. Actor hosts never receive Express credentials: they write through short-lived URLs, each signed by the controller for one log object.

Each Express bucket lives in one Availability Zone. With two or more zones and the default `all` placement, an acknowledged write is in every zone its log segment opened in, so losing one zone loses nothing. A zone that is unavailable when a segment opens is skipped for that segment. With a single bucket, or `nearest` placement, the Standard archive bucket is the only other copy. Losing that zone loses the writes not yet archived. That is at most `rapid_archive_lag` (`lag_ms`), normally under one batch interval. The controller logs `rapid_single_zone` at startup, and hosts log `rapid_archive_lag_exceeded` when the lag passes `DURABLE_ACTORS_ARCHIVE_MAX_LAG_MS`.

Spares (`actors.warm`), image, ingress, Redis and `credentialsSecret` settings match the [GKE chart](../durable-actors/README.md). S3 spares open their storage connections at assignment.
