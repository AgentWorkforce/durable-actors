import assert from "node:assert/strict"
import { spawnSync } from "node:child_process"
import { mkdtempSync, rmSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { fileURLToPath } from "node:url"
import test from "node:test"

const chart = fileURLToPath(new URL("../", import.meta.url))
const helm = process.env.HELM ?? "helm"
function render(overrides = {}) {
    const directory = mkdtempSync(join(tmpdir(), "actors-chart-"))
    try {
        const values = join(directory, "values.json")
        writeFileSync(values, JSON.stringify(overrides))
        return spawnSync(helm, ["template", "actors", chart, "--namespace", "actors", "-f", `${chart}tests/values.yaml`, "-f", values], { encoding: "utf8" })
    } finally {
        rmSync(directory, { recursive: true })
    }
}

function manifests(overrides) {
    const result = render(overrides)
    assert.equal(result.status, 0, result.stderr)
    return result.stdout
}

test("enables usage publishing only when a topic is configured", () => {
    assert.doesNotMatch(manifests(), /DURABLE_ACTORS_USAGE_TOPIC/)
    const output = manifests({ usage: { topic: "projects/example/topics/actor-usage" } })
    assert.match(output, /DURABLE_ACTORS_USAGE_TOPIC, value: "projects\/example\/topics\/actor-usage"/)
})

test("configures ClickHouse analytics with a password from the credentials secret", () => {
    const output = manifests({ analytics: { backend: "clickhouse", clickhouse: {
        url: "https://clickhouse.example.com:8443", database: "actors", user: "writer",
        passwordSecretKey: "clickhouse-password"
    } } })
    assert.match(output, /DURABLE_ACTORS_ANALYTICS_BACKEND, value: "clickhouse"/)
    assert.match(output, /DURABLE_ACTORS_CLICKHOUSE_URL, value: "https:\/\/clickhouse.example.com:8443"/)
    assert.match(output, /DURABLE_ACTORS_CLICKHOUSE_DATABASE, value: "actors"/)
    assert.match(output, /DURABLE_ACTORS_CLICKHOUSE_USER, value: "writer"/)
    assert.match(output, /name: DURABLE_ACTORS_CLICKHOUSE_PASSWORD\n\s+valueFrom:\n\s+secretKeyRef: \{name: "actors-credentials", key: "clickhouse-password"\}/)
})

test("reads the invoke API key from the credentials secret only when that key exists", () => {
    const output = manifests()
    assert.match(output, /name: DURABLE_ACTORS_SECRET\n\s+valueFrom:\n\s+secretKeyRef: \{name: "actors-credentials", key: api-key\}/)
    assert.match(output, /name: DURABLE_ACTORS_INVOKE_API_KEY\n\s+valueFrom:\n\s+secretKeyRef: \{name: "actors-credentials", key: invoke-api-key, optional: true\}/)
})

test("starts one controller and one pool manager using one Standard bucket", () => {
    const output = manifests()
    assert.match(output, /replicas: 1\b/)
    assert.match(output, /DURABLE_ACTORS_PERSISTENCE, value: "standard"/)
    for (const name of ["BUCKET", "ARTIFACT_BUCKET"]) assert.match(output, new RegExp(`DURABLE_ACTORS_${name}, value: "test-state"`))
    assert.match(output, /DURABLE_ACTORS_SPARE_IDLE, value: "0"/)
    assert.match(output, /kind: Service\b/)
    assert.equal(output.match(/kind: Deployment\n/g).length, 2)
})

test("configures organization actor capacity and start-rate limits", () => {
    const output = manifests({ organizationLimits: { enabled: false, maxActiveActors: 20000, actorStartsPerSecond: 100, actorStartBurst: 200 } })
    assert.match(output, /DURABLE_ACTORS_ORGANIZATION_LIMITS_ENABLED, value: "false"/)
    assert.match(output, /DURABLE_ACTORS_ORGANIZATION_MAX_ACTIVE_ACTORS, value: "20000"/)
    assert.match(output, /DURABLE_ACTORS_ORGANIZATION_ACTOR_STARTS_PER_SECOND, value: "100"/)
    assert.match(output, /DURABLE_ACTORS_ORGANIZATION_ACTOR_START_BURST, value: "200"/)
})

test("isolates sandboxes in a release-specific namespace with constrained credentials and network access", () => {
    const output = manifests()
    assert.match(output, /namespace: actors-actors/)
    assert.match(output, /automountServiceAccountToken: false/)
    assert.match(output, /kind: RoleBinding/)
    assert.match(output, /kind: NetworkPolicy/)
    assert.match(output, /169\.254\.0\.0\/16/)
    assert.match(output, /runAsNonRoot: true/)
})

test("enables Rapid with two distinct zones and the same Standard bucket for archives", () => {
    const output = manifests({ storage: { mode: "rapid", rapid: { buckets: [
        { bucket: "rapid-a", zone: "us-west4-a" }, { bucket: "rapid-b", zone: "us-west4-b" }
    ] } } })
    assert.match(output, /DURABLE_ACTORS_PERSISTENCE, value: "rapid"/)
    assert.match(output, /DURABLE_ACTORS_ARCHIVE_BUCKET, value: "test-state"/)
    for (const value of ["rapid-a", "rapid-b", "us-west4-a", "us-west4-b"]) assert.ok(output.includes(value))
})

test("places each Rapid log in whichever configured zone opens first", () => {
    const output = manifests({ storage: { mode: "rapid", rapid: { placement: "nearest", buckets: [
        { bucket: "rapid-a", zone: "us-west1-a" }, { bucket: "rapid-b", zone: "us-west4-a" }
    ] } } })
    assert.match(output, /DURABLE_ACTORS_RAPID_PLACEMENT, value: "nearest"/)
})

test("enables Rapid with one zone", () => {
    const output = manifests({ storage: { mode: "rapid", rapid: { buckets: [
        { bucket: "rapid-a", zone: "us-west4-a" }
    ] } } })
    assert.match(output, /DURABLE_ACTORS_RAPID_BUCKETS/)
    assert.ok(output.includes("rapid-a"))
})

test("routes HTTPS and WebSockets to the same service through the selected ingress controller", () => {
    const output = manifests({ ingress: { enabled: true, className: "example", tlsSecret: "actors-tls", annotations: { "example.com/websocket-timeout": "3600" } } })
    assert.match(output, /apiVersion: networking.k8s.io\/v1\nkind: Ingress/)
    assert.match(output, /ingressClassName: "example"/)
    assert.match(output, /secretName: "actors-tls"/)
    assert.match(output, /host: "actors.example.com"/)
    assert.match(output, /example.com\/websocket-timeout: "3600"/)
})

test("allows extra controllers and a small warm pool", () => {
    const output = manifests({ replicaCount: 2, actors: { warm: 2 }, redis: { urlSecretKey: "redis-url" } })
    assert.match(output, /replicas: 2\b/)
    assert.match(output, /DURABLE_ACTORS_SPARE_IDLE, value: "2"/)
    assert.match(output, /kind: PodDisruptionBudget/)
})

test("runs exactly one pool manager beside the serving control plane", () => {
    const deployments = manifests({ replicaCount: 3, redis: { urlSecretKey: "redis-url" } }).split("---").filter(document => document.includes("kind: Deployment"))
    const manager = deployments.find(document => document.includes("-pool-manager\n"))
    const serving = deployments.find(document => document.includes("value: control_plane"))
    assert.ok(manager && serving)
    assert.match(manager, /replicas: 1\b/)
    assert.match(manager, /strategy: \{type: Recreate\}/)
    assert.match(manager, /DURABLE_ACTORS_PROCESS_ROLE, value: pool_manager/)
    assert.doesNotMatch(manager, /readinessProbe/)
    assert.match(manager, /livenessProbe:\n\s+httpGet: \{path: \/healthz/)
    assert.match(serving, /replicas: 3\b/)
})

test("configures how fast warm pools refill and drain", () => {
    const output = manifests({ actors: { maxStarting: 128, refillIntervalMs: 250, retireConcurrency: 128, podReapIntervalMs: 500, reapConcurrency: 32, registryReapIntervalMs: 5000 } })
    assert.match(output, /DURABLE_ACTORS_SPARE_MAX_STARTING, value: "128"/)
    assert.match(output, /DURABLE_ACTORS_SPARE_REFILL_INTERVAL_MS, value: "250"/)
    assert.match(output, /DURABLE_ACTORS_SPARE_RETIRE_CONCURRENCY, value: "128"/)
    assert.match(output, /DURABLE_ACTORS_SPARE_POD_REAP_INTERVAL_MS, value: "500"/)
    assert.match(output, /DURABLE_ACTORS_GKE_REAP_CONCURRENCY, value: "32"/)
    assert.match(output, /DURABLE_ACTORS_SPARE_REAP_INTERVAL_MS, value: "5000"/)
})

for (const [name, overrides] of [
    ["unknown storage mode", { storage: { mode: "standrad" } }],
    ["missing Rapid placements", { storage: { mode: "rapid" } }],
    ["duplicate Rapid zones", { storage: { mode: "rapid", rapid: { buckets: [{ bucket: "rapid-a", zone: "us-west4-a" }, { bucket: "rapid-b", zone: "us-west4-a" }] } } }],
    ["shared Standard and Rapid bucket", { storage: { mode: "rapid", rapid: { buckets: [{ bucket: "test-state", zone: "us-west4-a" }, { bucket: "rapid-b", zone: "us-west4-b" }] } } }],
    ["Rapid settings in Standard mode", { storage: { rapid: { buckets: [{ bucket: "rapid-a", zone: "us-west4-a" }] } } }],
    ["nearest Rapid placement in Standard mode", { storage: { rapid: { placement: "nearest" } } }],
    ["nearest Rapid placement with one zone", { storage: { mode: "rapid", rapid: { placement: "nearest", buckets: [{ bucket: "rapid-a", zone: "us-west4-a" }] } } }],
    ["unknown Rapid placement", { storage: { mode: "rapid", rapid: { placement: "closest", buckets: [{ bucket: "rapid-a", zone: "us-west4-a" }] } } }],
    ["mutable control-plane image", { images: { controlPlane: { digest: "latest" } } }],
    ["mutable TypeScript image", { images: { typescript: { digest: "latest" } } }],
    ["mutable Python image", { images: { python: { digest: "latest" } } }],
    ["insecure public URL", { publicUrl: "http://actors.example.com" }],
    ["shared trust namespace", { sandboxNamespace: "actors" }],
    ["missing ingress certificate", { ingress: { enabled: true } }],
    ["warm pool above fleet limit", { actors: { warm: 300 } }],
    ["refill interval below 50 ms", { actors: { refillIntervalMs: 10 } }]
]) test(`rejects ${name}`, () => assert.notEqual(render(overrides).status, 0))

test("deploys the control plane separately and configures language-specific actor images", () => {
    const output = manifests()
    assert.match(output, new RegExp(`image: "[^"\\n]+@sha256:${"a".repeat(64)}"`))
    assert.match(output, new RegExp(`DURABLE_ACTORS_TYPESCRIPT_IMAGE, value: "[^"\\n]+@sha256:${"b".repeat(64)}"`))
    assert.match(output, new RegExp(`DURABLE_ACTORS_PYTHON_IMAGE, value: "[^"\\n]+@sha256:${"c".repeat(64)}"`))
})

test("replicas reach each other's sockets through Redis, which several replicas require", () => {
    const single = render()
    assert.equal(single.status, 0, single.stderr)
    assert.doesNotMatch(single.stdout, /DURABLE_ACTORS_REDIS_|DURABLE_ACTORS_GATEWAY_ROUTE/)
    const missing = render({ replicaCount: 2 })
    assert.notEqual(missing.status, 0)
    assert.match(missing.stderr, /redis.urlSecretKey/)

    const shared = render({ replicaCount: 2, redis: { urlSecretKey: "redis-url", caSecretKey: "redis-ca" } })
    assert.equal(shared.status, 0, shared.stderr)
    assert.match(shared.stdout, /name: DURABLE_ACTORS_REDIS_URL\n\s+valueFrom:\n\s+secretKeyRef: \{name: "actors-credentials", key: "redis-url"\}/)
    assert.match(shared.stdout, /name: DURABLE_ACTORS_REDIS_CA_CERT\n\s+valueFrom:\n\s+secretKeyRef: \{name: "actors-credentials", key: "redis-ca"\}/)
    assert.notEqual(render({ redis: { url: "redis://inline" } }).status, 0)
})
