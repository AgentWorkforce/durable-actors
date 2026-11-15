import assert from "node:assert/strict"
import { createServer } from "node:http"
import type { Server, ServerResponse } from "node:http"
import { test } from "node:test"

import type { ActorConnection } from "../../src/actor/socket.js"
import { RemoteActorClient } from "../../src/client/remoteClient.js"
import { ActorInvocationError } from "../../src/errors.js"

const target = {
    route: "https://host.example.com",
    token: "invocation-ticket",
    ownerEpoch: 1,
    expiresAtMs: 4_000_000_000_000
}

test("does not retry failed or ambiguous invocations", async () => {
    for (const [response, code] of [
        [null, "outcome_unknown"],
        [
            Response.json({
                target,
                outcome: { type: "failed", code: "unauthenticated", message: "actor method failed" }
            }),
            "unauthenticated"
        ],
        [Response.json({ target, outcome: { type: "failed", code: "actor_error", message: "boom" } }), "actor_error"],
        [Response.json({ error: { code: "outcome_unknown", message: "lost" } }, { status: 502 }), "outcome_unknown"],
        [Response.json({}, { status: 401 }), "unauthenticated"]
    ] as const) {
        const requests: string[] = []
        const client = new RemoteActorClient(
            { projectId: "default", apiKey: "backend-key", controlPlaneUrl: "https://control.example.com" },
            {
                telemetry: () => {},
                fetch: async url => {
                    requests.push(String(url))
                    if (response === null) throw new Error("connection lost")
                    return response.clone()
                }
            }
        )
        await assert.rejects(
            client.invoke("Counter", "one", "increment", [1]),
            error => error instanceof ActorInvocationError && error.code === code
        )
        assert.deepEqual(requests, ["https://control.example.com/v1/projects/default/actors/Counter/one/invoke"])
    }
})

test("cold and warm HTTP invocations report their respective timings", async () => {
    const authorizations: (string | undefined)[] = []
    const telemetry: unknown[] = []
    const server = createServer(async (request, response) => {
        assert.equal(request.method, "POST")
        assert.equal(request.url, "/v1/projects/default/actors/Counter/counter-1/invoke")
        authorizations.push(request.headers.authorization)
        const body = (await requestBody(request)) as Record<string, unknown>
        const outcome = { type: "completed", result: 7 }
        assert.equal(body.ownerEpoch, undefined)
        json(response, 200, { outcome })
    })
    const port = await listen(server)
    const client = new RemoteActorClient(
        { projectId: "default", apiKey: "backend-key", controlPlaneUrl: `http://127.0.0.1:${port}` },
        {
            requestId: () => "request-1",
            monotonicNow: tickingClock(),
            telemetry: event => telemetry.push(event)
        }
    )
    try {
        for (const amount of [2, 3]) assert.equal(await client.invoke("Counter", "counter-1", "increment", [amount]), 7)
        assert.deepEqual(authorizations, ["Bearer backend-key", "Bearer backend-key"])
        assert.deepEqual(
            telemetry,
            ["control_plane_invocation_completed_at_ms", "control_plane_invocation_completed_at_ms"].map(completed => ({
                event: "actor_client_invocation",
                request_id: "request-1",
                actor_name: "Counter",
                actor_id: "counter-1",
                method: "increment",
                started_at_ms: 0,
                invocation_built_at_ms: 1,
                [completed]: 2,
                completed_at_ms: 3,
                outcome: "completed"
            }))
        )
    } finally {
        await close(server)
    }
})

test("resolves a fresh host socket grant before connecting", async () => {
    const requests: unknown[] = []
    const connection = fakeConnection()
    const client = new RemoteActorClient(
        {
            projectId: "default",
            apiKey: "backend-key",
            controlPlaneUrl: "https://control.example.com"
        },
        {
            requestId: () => "connection-request",
            fetch: async (url, init) => {
                assert.equal(
                    String(url),
                    "https://control.example.com/v1/projects/default/actors/ChatRoom/room-1/find-websocket"
                )
                assert.equal(JSON.parse(init!.body as string).backend, undefined)
                assert.deepEqual(JSON.parse(init!.body as string).metadata, { userId: "user-1" })
                return Response.json({
                    websocketUrl: "wss://host.modal.test/v1/socket?key=host-ticket",
                    key: "host-ticket"
                })
            },
            connectWebSocket: async url => {
                requests.push({ url })
                return connection
            }
        }
    )
    assert.equal(await client.connect("ChatRoom", "room-1", { userId: "user-1" }), connection)
    assert.deepEqual(requests, [
        {
            url: "wss://host.modal.test/v1/socket?key=host-ticket"
        }
    ])
})

test("a lost broadcast response reports an unknown outcome", async () => {
    let calls = 0
    const server = createServer(request => {
        calls += 1
        request.socket.destroy()
    })
    const port = await listen(server)
    const client = new RemoteActorClient({
        projectId: "default",
        apiKey: "backend-key",
        controlPlaneUrl: `http://127.0.0.1:${port}`
    })
    try {
        await assert.rejects(
            client.broadcast("Counter", "counter-1", "updated"),
            error => error instanceof ActorInvocationError && error.code === "outcome_unknown"
        )
        assert.equal(calls, 1)
    } finally {
        await close(server)
    }
})

test("preserves a structured actor failure from HTTP", async () => {
    const server = createServer((_request, response) => {
        json(response, 422, {
            error: {
                code: "actor_error",
                message: "actor method failed",
                requestId: "server-request"
            }
        })
    })
    const port = await listen(server)
    const client = new RemoteActorClient({
        projectId: "default",
        apiKey: "backend-key",
        controlPlaneUrl: `http://127.0.0.1:${port}`
    })
    try {
        await assert.rejects(
            client.invoke("Counter", "counter-1", "increment", [2]),
            error =>
                error instanceof ActorInvocationError &&
                error.code === "actor_error" &&
                error.requestId === "server-request"
        )
    } finally {
        await close(server)
    }
})

function json(response: ServerResponse, status: number, body: unknown): void {
    response.writeHead(status, { "content-type": "application/json" })
    response.end(JSON.stringify(body))
}

function listen(server: Server): Promise<number> {
    return new Promise((resolve, reject) => {
        server.once("error", reject)
        server.listen(0, "127.0.0.1", () => {
            server.off("error", reject)
            const address = server.address()
            if (address === null || typeof address === "string")
                reject(new Error("test HTTP server has no TCP address"))
            else resolve(address.port)
        })
    })
}

function close(server: Server): Promise<void> {
    return new Promise((resolve, reject) => server.close(error => (error ? reject(error) : resolve())))
}

async function requestBody(request: import("node:http").IncomingMessage): Promise<unknown> {
    const chunks: Buffer[] = []
    for await (const chunk of request) chunks.push(Buffer.from(chunk))
    return chunks.length === 0 ? undefined : (JSON.parse(Buffer.concat(chunks).toString("utf8")) as unknown)
}

function fakeConnection(): ActorConnection {
    return {
        readyState: 1,
        send: () => undefined,
        close: () => undefined,
        addEventListener: () => undefined,
        removeEventListener: () => undefined
    }
}

function tickingClock(): () => number {
    let current = 0
    return () => current++
}

test("project clients send RPCs and broadcasts to their configured gateway", async () => {
    const requests: string[] = []
    for (const projectId of ["team-a", "team-b"]) {
        const client = new RemoteActorClient(
            { projectId, apiKey: "key", controlPlaneUrl: "https://gateway.example" },
            {
                fetch: async (url, init) => {
                    requests.push(String(url))
                    assert.equal(new Headers(init?.headers).get("authorization"), "Bearer key")
                    if (String(url).endsWith("/invoke"))
                        return Response.json({ outcome: { type: "completed", result: projectId } })
                    assert.deepEqual(JSON.parse(String(init?.body)), {
                        effects: [
                            {
                                type: "broadcast",
                                message: { type: "text", data: '\"updated\"' },
                                except_connection_ids: [],
                                tags: []
                            }
                        ]
                    })
                    return new Response(null, { status: 204 })
                }
            }
        )
        assert.equal(await client.invoke("Counter", "same", "increment", []), projectId)
        await client.broadcast("Counter", "same", "updated")
    }
    assert.deepEqual(
        requests,
        ["team-a", "team-b"].flatMap(project =>
            ["invoke", "socket-effects"].map(
                endpoint => `https://gateway.example/v1/projects/${project}/actors/Counter/same/${endpoint}`
            )
        )
    )
})
