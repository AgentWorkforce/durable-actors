import assert from "node:assert/strict"
import { cp, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises"
import { get } from "node:http"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { test } from "node:test"
import { pathToFileURL } from "node:url"

import { Observer } from "../../src/cli/observe.js"

const metricsClient = {
    getMetrics: async () => ({
        total: { actorName: "", count: 0, success: null, p95: null, queueP95: null },
        classes: []
    }),
    listQueueWaits: async () => [],
    getState: async () => ({ snapshot: null, schema: null }),
    listStateHistory: async () => ({ records: [], retention: null, nextBefore: null }),
    listWebSockets: async () => []
}

const assets = new URL("./", import.meta.resolve("durable-actors-observer/standalone/index.html"))

test("observer serves the installed UI package by default", async t => {
    const observer = new Observer(
        { ...metricsClient, checkConnection: async () => {}, listActors: async () => ({ actors: [] }) },
        async () => {}
    )
    t.after(() => observer.close())
    const { url } = await observer.start(false)
    const response = await fetch(url)
    assert.equal(response.status, 200)
    assert.match(await response.text(), /src="\.\/app.js"/u)
    assert.equal((await fetch(`${url}/app.js`)).status, 200)
    assert.equal((await fetch(`${url}/app.css`)).status, 200)
})

test("observer serves generated assets without a hardcoded filename list", async t => {
    const directory = await mkdtemp(join(tmpdir(), "observer-assets-"))
    await cp(assets, directory, { recursive: true })
    await mkdir(join(directory, "assets"), { recursive: true })
    await writeFile(join(directory, "assets", "details-abc123.js"), "export const details = true")
    const observer = new Observer(
        { ...metricsClient, checkConnection: async () => {}, listActors: async () => ({ actors: [] }) },
        async () => {},
        pathToFileURL(`${directory}/`)
    )
    t.after(async () => {
        await observer.close()
        await rm(directory, { recursive: true, force: true })
    })
    const { url } = await observer.start(false)
    const response = await fetch(`${url}/assets/details-abc123.js`)
    assert.equal(response.status, 200)
    assert.match(response.headers.get("content-type")!, /javascript/u)
    assert.equal(await response.text(), "export const details = true")
    const head = await fetch(`${url}/assets/details-abc123.js`, { method: "HEAD" })
    assert.equal(head.status, 200)
    assert.equal(await head.text(), "")
})

test("observer proxies connection checks and reports a later outage without exposing upstream errors", async t => {
    let available = true
    const observer = new Observer(
        {
            ...metricsClient,
            listActors: async () => ({ actors: [] }),
            checkConnection: async () => {
                if (!available) throw new Error("secret-admin-key")
            }
        },
        async () => {},
        assets
    )
    t.after(() => observer.close())
    const { url } = await observer.start(false)
    const connected = await fetch(`${url}/api/observe/connection`)
    assert.equal(connected.status, 200)
    assert.deepEqual(await connected.json(), { connected: true })
    available = false
    const disconnected = await fetch(`${url}/api/observe/connection`)
    assert.equal(disconnected.status, 503)
    assert.deepEqual(await disconnected.json(), { error: "Control plane connection failed" })
    assert.equal((await fetch(`${url}/api/observe/connection`, { method: "POST" })).status, 405)
    assert.equal(
        (await fetch(`${url}/api/observe/connection`, { headers: { origin: "https://untrusted.example" } })).status,
        403
    )
    const rejectedHost = await new Promise<number | undefined>((resolve, reject) => {
        get(`${url}/api/observe/connection`, { headers: { host: "untrusted.example" } }, response => {
            response.resume()
            resolve(response.statusCode)
        }).on("error", reject)
    })
    assert.equal(rejectedHost, 403)
})

test("observer opens the browser only after authentication and local serving succeed", async t => {
    let connected = false
    let opened: string | undefined
    const observer = new Observer(
        {
            ...metricsClient,
            listActors: async () => ({ actors: [] }),
            checkConnection: async () => {
                connected = true
            }
        },
        async url => {
            assert.equal(connected, true)
            const response = await fetch(url)
            assert.match(response.headers.get("content-type")!, /^text\/html(?:;|$)/u)
            assert.match(await response.text(), /src="\.\/app.js"/u)
            assert.match(await (await fetch(`${url}/app.js`)).text(), /No actor types deployed yet/u)
            assert.match((await fetch(`${url}/app.css`)).headers.get("content-type")!, /text\/css/u)
            opened = url
        },
        assets
    )
    t.after(() => observer.close())
    const result = await observer.start()
    assert.equal(opened, result.url)
    assert.equal(result.browserOpened, true)
    assert.equal((await fetch(`${result.url}/missing`)).status, 404)
    assert.equal((await fetch(`${result.url}/package.json`)).status, 404)
})

test("observer does not open a browser when the control plane rejects the connection", async () => {
    let opened = false
    const observer = new Observer(
        {
            ...metricsClient,
            listActors: async () => ({ actors: [] }),
            checkConnection: async () => {
                throw new Error("Unauthorized")
            }
        },
        async () => {
            opened = true
        }
    )
    await assert.rejects(observer.start(), /Unauthorized/u)
    assert.equal(opened, false)
})

test("observer keeps the local UI available if browser launching fails", async t => {
    const observer = new Observer(
        { ...metricsClient, checkConnection: async () => {}, listActors: async () => ({ actors: [] }) },
        async () => {
            throw new Error("No browser")
        },
        assets
    )
    t.after(() => observer.close())
    const result = await observer.start()
    assert.equal(result.browserOpened, false)
    assert.equal((await fetch(result.url)).status, 200)
})

test("observer proxies actor inventory and hides upstream failures", async t => {
    let fail = false
    const inventory = { actors: [{ actorName: "Room", live: 1, dormant: 2, unknown: 0 }] }
    const observer = new Observer(
        {
            ...metricsClient,
            checkConnection: async () => {},
            listActors: async () => {
                if (fail) throw new Error("private-admin-key")
                return inventory
            }
        },
        async () => {},
        assets
    )
    t.after(() => observer.close())
    const { url } = await observer.start(false)
    assert.deepEqual(await (await fetch(`${url}/api/observe/actors`)).json(), inventory)
    fail = true
    const response = await fetch(`${url}/api/observe/actors`)
    assert.equal(response.status, 503)
    assert.deepEqual(await response.json(), { error: "Actor inventory unavailable" })
})

for (const [method, path, event] of [
    ["openActorStream", "/api/observe/events", "inventory"],
    ["openRequestStream", "/api/observe/requests/events", "requests"]
] as const) {
    test(`observer forwards ${event} events and aborts upstream when the viewer disconnects`, async t => {
        let signal: AbortSignal | undefined
        const observer = new Observer(
            {
                ...metricsClient,
                checkConnection: async () => {},
                listActors: async () => ({}),
                [method]: async (incoming: AbortSignal) => {
                    signal = incoming
                    return new Response(
                        new ReadableStream({
                            start(controller) {
                                controller.enqueue(new TextEncoder().encode(`event: ${event}\ndata: {}\n\n`))
                            }
                        }),
                        { headers: { "content-type": "text/event-stream" } }
                    )
                }
            },
            async () => {},
            assets
        )
        t.after(() => observer.close())
        const { url } = await observer.start(false)
        const controller = new AbortController()
        const response = await fetch(`${url}${path}`, { signal: controller.signal })
        assert.equal(response.status, 200)
        assert.match(response.headers.get("content-type")!, /text\/event-stream/u)
        const reader = response.body!.getReader()
        assert.match(new TextDecoder().decode((await reader.read()).value), new RegExp(`event: ${event}`, "u"))
        const stopped = new Promise<void>(resolve => signal!.addEventListener("abort", () => resolve(), { once: true }))
        controller.abort()
        await stopped
        assert.equal(signal!.aborted, true)
    })
}

test("observer forwards history filters and opaque replay cursors", async t => {
    const observer = new Observer(
        {
            ...metricsClient,
            checkConnection: async () => {},
            listActors: async () => ({}),
            listRequests: async (query, signal) => {
                assert.deepEqual(Object.fromEntries(query), {
                    actorId: "one",
                    outcome: "failed",
                    limit: "100",
                    cursor: "page+token"
                })
                assert.ok(signal)
                return { records: [], nextCursor: null }
            },
            openRequestStream: async (_signal, after) => {
                assert.equal(after, "resume+token")
                return new Response("event: requests\ndata: {}\n\n", {
                    headers: { "content-type": "text/event-stream" }
                })
            }
        },
        async () => {},
        assets
    )
    t.after(() => observer.close())
    const { url } = await observer.start(false)
    assert.deepEqual(
        await (
            await fetch(`${url}/api/observe/requests?actorId=one&outcome=failed&limit=100&cursor=page%2Btoken`)
        ).json(),
        { records: [], nextCursor: null }
    )
    assert.match(
        await (await fetch(`${url}/api/observe/requests/events?after=resume%2Btoken`)).text(),
        /event: requests/u
    )
})

test("history proxy rejects writes and cross-origin requests and hides upstream errors", async t => {
    let calls = 0
    const observer = new Observer(
        {
            ...metricsClient,
            checkConnection: async () => {},
            listActors: async () => ({}),
            listRequests: async () => {
                calls++
                throw new Error("private-admin-key")
            }
        },
        async () => {},
        assets
    )
    t.after(() => observer.close())
    const { url } = await observer.start(false)
    const endpoint = `${url}/api/observe/requests`
    assert.equal((await fetch(endpoint, { method: "POST" })).status, 405)
    assert.equal((await fetch(endpoint, { headers: { origin: "https://untrusted.example" } })).status, 403)
    assert.equal(calls, 0)
    const rejected = await fetch(endpoint)
    assert.equal(rejected.status, 503)
    assert.deepEqual(await rejected.json(), { error: "Request history unavailable" })
    assert.equal(calls, 1)
})

test("history proxy cancels the upstream request when the viewer disconnects", async t => {
    let signal: AbortSignal | undefined
    let started!: () => void
    let cancelled!: () => void
    const ready = new Promise<void>(resolve => {
        started = resolve
    })
    const stopped = new Promise<void>(resolve => {
        cancelled = resolve
    })
    const observer = new Observer(
        {
            ...metricsClient,
            checkConnection: async () => {},
            listActors: async () => ({}),
            listRequests: async (_query, upstream) => {
                signal = upstream
                started()
                await new Promise<void>(resolve =>
                    upstream!.addEventListener(
                        "abort",
                        () => {
                            cancelled()
                            resolve()
                        },
                        { once: true }
                    )
                )
                return { records: [] }
            }
        },
        async () => {},
        assets
    )
    t.after(() => observer.close())
    const { url } = await observer.start(false)
    const controller = new AbortController()
    const response = fetch(`${url}/api/observe/requests`, { signal: controller.signal })
    const rejected = assert.rejects(response, { name: "AbortError" })
    await ready
    controller.abort()
    await rejected
    await stopped
    assert.equal(signal?.aborted, true)
})

for (const [method, path] of [
    ["getMetrics", "metrics"],
    ["listQueueWaits", "queue-waits"],
    ["listWebSockets", "websockets"],
    ["listResidency", "residency"],
    ["getState", "state"],
    ["listStateHistory", "state/history"]
] as const) {
    test(`observer proxies ${path} filters with origin and method checks`, async t => {
        let signal: AbortSignal | undefined
        const observer = new Observer(
            {
                ...metricsClient,
                checkConnection: async () => {},
                listActors: async () => ({}),
                [method]: async (query: URLSearchParams, incoming: AbortSignal) => {
                    assert.deepEqual(Object.fromEntries(query), { fromMs: "10", toMs: "20" })
                    signal = incoming
                    return { saved: true }
                }
            },
            async () => {},
            assets
        )
        t.after(() => observer.close())
        const { url } = await observer.start(false)
        const endpoint = `${url}/api/observe/${path}?fromMs=10&toMs=20`
        const response = await fetch(endpoint)
        assert.equal(response.status, 200)
        assert.deepEqual(await response.json(), { saved: true })
        assert.ok(signal)
        assert.equal((await fetch(endpoint, { method: "POST" })).status, 405)
        assert.equal((await fetch(endpoint, { headers: { origin: "https://untrusted.example" } })).status, 403)
    })
}
