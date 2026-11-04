import assert from "node:assert/strict"
import { once } from "node:events"
import { mkdtemp, rm, unlink, writeFile } from "node:fs/promises"
import type { Socket } from "node:net"
import { createServer } from "node:net"
import { createInterface } from "node:readline"
import { before, test } from "node:test"
import { fileURLToPath } from "node:url"

import { buildActor } from "../../src/compiler/actor-build.js"
import { ActorSession, parseHostSettings } from "../../src/host/actor-host.js"
import type { DatabaseCall } from "../../src/host/database.js"
import type { ActorExecutorReply } from "../../src/host/protocol.js"
import type { DatabaseRequester } from "../../src/host/types.js"
import { ActorWorkerSupervisor } from "../../src/host/worker-supervisor.js"
import { HostDatabase } from "../fixtures/host-database.js"
import { assertReply } from "../fixtures/reply.js"

before(
    async () => {
        await actorBundle()
    },
    { timeout: 30_000 }
)

test("loads a prepared JavaScript artifact only inside the first execution Worker", { timeout: 5_000 }, async () => {
    const root = await mkdtemp("/tmp/actor-discovery-")
    const entrypoint = `${root}/actors.js`
    const fixture = await actorBundle()
    await writeFile(`${root}/package.json`, JSON.stringify({ type: "module" }))
    await writeFile(
        entrypoint,
        `import { isMainThread } from "node:worker_threads"; if (isMainThread) throw new Error("customer code loaded in supervisor"); export * from ${JSON.stringify(fixture)};`
    )
    const server = createServer(socket => {
        const lines = createInterface({ input: socket })
        lines.once("line", line => {
            assert.deepEqual(JSON.parse(line).actor_names, ["SessionCounter"])
            socket.write(`${JSON.stringify({ type: "attached", protocol: 31, actor: actorIdentity() })}\n`)
            socket.end()
        })
    })
    server.listen(`${root}/executor.sock`)
    await once(server, "listening")
    const session = new ActorSession(
        parseHostSettings({
            DURABLE_ACTORS_EXECUTOR_SOCKET: `${root}/executor.sock`,
            DURABLE_ACTORS_ENTRYPOINT: entrypoint
        })
    )
    try {
        await session.start()
        await session.waitUntilDisconnected()
    } finally {
        server.close()
        await rm(root, { recursive: true, force: true })
    }
})

test("a stalled actor import times out and closes the Worker", { timeout: 5_000 }, async () => {
    let closed = 0
    const session = new ActorSession(
        parseHostSettings({
            DURABLE_ACTORS_EXECUTOR_SOCKET: `/tmp/ta-unused-${process.pid}.sock`,
            DURABLE_ACTORS_ENTRYPOINT: await actorBundle(),
            DURABLE_ACTORS_HOST_STARTUP_MS: "20"
        }),
        () => ({
            ready: () => new Promise(() => {}),
            async handle() {
                return { type: "evicted" }
            },
            close() {
                closed += 1
            },
            assign() {},
            isResident: () => false,
            onResidencyChange: () => () => {}
        })
    )
    await assert.rejects(session.start(), /actor module loading timed out/)
    assert.equal(closed, 1)
})

test("database calls outside a command carry only their call ID", { timeout: 5_000 }, async () => {
    const socketPath = `/tmp/ta-activation-${process.pid}.sock`
    await removeSocket(socketPath)
    const server = createServer()
    const customerSocketPromise = new Promise<Socket>(resolve => {
        server.once("connection", resolve)
    })
    server.listen(socketPath)
    await once(server, "listening")
    let activationDatabase!: DatabaseRequester
    const session = new ActorSession(
        parseHostSettings({
            DURABLE_ACTORS_EXECUTOR_SOCKET: socketPath,
            DURABLE_ACTORS_ENTRYPOINT: await actorBundle()
        }),
        () => ({
            ready: async () => ["SessionCounter"],
            async handle() {
                return { type: "evicted" }
            },
            close() {},
            assign(_actor, database) {
                activationDatabase = database
            },
            isResident: () => false,
            onResidencyChange: () => () => {}
        })
    )
    const startup = session.start()
    try {
        const customerSocket = await customerSocketPromise
        const iterator = createInterface({ input: customerSocket, crlfDelay: Infinity })[Symbol.asyncIterator]()
        await readMessage(iterator)
        customerSocket.write(`${JSON.stringify({ type: "attached", protocol: 31, actor: actorIdentity() })}\n`)
        await startup
        const result = activationDatabase({ op: "fields" })
        let message: unknown
        do message = await readMessage(iterator)
        while ((message as { type: string }).type === "residency")
        assert.deepEqual(message, { type: "database_call", call_id: 1, call: { op: "fields" } })
        customerSocket.write(
            `${JSON.stringify({ type: "database_result", call_id: 1, result: { type: "fields", fields: {} } })}\n`
        )
        assert.deepEqual(await result, { type: "fields", fields: {} })
        customerSocket.destroy()
        await session.waitUntilDisconnected()
    } finally {
        server.close()
        await removeSocket(socketPath)
    }
})

test("the actor session carries only owned execution commands", async t => {
    const socketPath = `/tmp/ta-session-${process.pid}.sock`
    await removeSocket(socketPath)
    const server = createServer()
    const customerSocketPromise = new Promise<Socket>(resolve => {
        server.once("connection", resolve)
    })
    server.listen(socketPath)
    await once(server, "listening")
    let closed = 0
    const session = new ActorSession(
        parseHostSettings({
            DURABLE_ACTORS_EXECUTOR_SOCKET: socketPath,
            DURABLE_ACTORS_ENTRYPOINT: await actorBundle()
        }),
        options => {
            const supervisor = new ActorWorkerSupervisor(options)
            const close = supervisor.close.bind(supervisor)
            t.mock.method(supervisor, "close", () => {
                closed += 1
                close()
            })
            return supervisor
        }
    )
    const startup = session.start()

    try {
        const customerSocket = await Promise.race([customerSocketPromise, startup.then(() => customerSocketPromise)])
        const lines = createInterface({ input: customerSocket, crlfDelay: Infinity })
        const iterator = lines[Symbol.asyncIterator]()

        assert.deepEqual(await readMessage(iterator), {
            type: "attach",
            protocol: 31,
            actor_names: ["SessionCounter"]
        })
        customerSocket.write(`${JSON.stringify({ type: "attached", protocol: 31, actor: actorIdentity() })}\n`)
        await startup

        const database = new HostDatabase()
        const serve = databaseServer(database, message => customerSocket.write(`${JSON.stringify(message)}\n`))
        const next = () => readHostMessage(iterator, serve)
        const invoke = (messageId: number, method: string, args: number[]) =>
            customerSocket.write(
                `${JSON.stringify({
                    type: "command",
                    message_id: messageId,
                    command: {
                        type: "invoke",
                        request_id: `request-${messageId}`,
                        method,
                        args
                    }
                })}\n`
            )
        invoke(1, "increment", [4])
        assertSessionReply(await next(), 1, { type: "invoked", result: 4 })
        assert.deepEqual(database.fields(), { count: 4 })

        invoke(100, "stream", [])
        for (const data of ["first", "last"]) {
            assert.deepEqual(await next(), {
                type: "socket_effects",
                message_id: 100,
                effects: [
                    {
                        type: "broadcast",
                        message: { type: "text", data: JSON.stringify({ delta: data }) },
                        except_connection_ids: [],
                        tags: []
                    }
                ]
            })
        }
        assertSessionReply(await next(), 100, { type: "invoked", result: 4 })

        invoke(3, "increment", [1])
        assertSessionReply(await next(), 3, { type: "invoked", result: 5 })
        assert.deepEqual(database.fields(), { count: 5 })

        customerSocket.write(
            `${JSON.stringify({
                type: "command",
                message_id: 4,
                command: { type: "evict", graceful: false }
            })}\n`
        )
        assert.deepEqual(await next(), {
            type: "reply",
            message_id: 4,
            reply: { type: "evicted" }
        })

        customerSocket.end()
        await session.waitUntilDisconnected()
        assert.equal(closed, 1)
        lines.close()
        server.close()
        await once(server, "close")
    } finally {
        if (server.listening) server.close()
        await removeSocket(socketPath)
    }
})

test("a failed session connection cleans up the speculative Worker", async () => {
    let closed = 0
    const session = new ActorSession(
        parseHostSettings({
            DURABLE_ACTORS_EXECUTOR_SOCKET: `/tmp/ta-missing-${process.pid}.sock`,
            DURABLE_ACTORS_ENTRYPOINT: await actorBundle()
        }),
        () => ({
            async ready() {
                return ["SessionCounter"]
            },
            async handle() {
                return { type: "evicted" }
            },
            close() {
                closed += 1
            },
            assign() {},
            isResident: () => false,
            onResidencyChange: () => () => {}
        })
    )
    await assert.rejects(session.start(), /could not attach to Rust host/)
    assert.equal(closed, 1)
})

function actorIdentity(): Record<string, string> {
    return {
        project_id: "default",
        actor_name: "SessionCounter",
        actor_id: "counter-1"
    }
}

function assertSessionReply(message: unknown, id: number, expected: object): void {
    const { reply, ...envelope } = message as { reply: ActorExecutorReply; type: string; message_id: number }
    assert.deepEqual(envelope, { type: "reply", message_id: id })
    assertReply(reply, expected)
}

function databaseServer(database: HostDatabase, send: (message: unknown) => void) {
    return (message: { call_id: number; call: DatabaseCall }) => {
        try {
            send({ type: "database_result", call_id: message.call_id, result: database.call(message.call) })
        } catch (error) {
            send({ type: "database_result", call_id: message.call_id, error: (error as Error).message })
        }
    }
}

async function readHostMessage(
    iterator: AsyncIterator<string>,
    serve: ReturnType<typeof databaseServer>
): Promise<unknown> {
    for (;;) {
        const message = (await readMessage(iterator)) as {
            type: string
            call_id: number
            message_id: number
            call: DatabaseCall
        }
        if (message.type === "database_call") serve(message)
        else if (message.type !== "residency") return message
    }
}

async function readMessage(iterator: AsyncIterator<string>): Promise<unknown> {
    const next = await iterator.next()
    assert.equal(next.done, false)
    const message: unknown = JSON.parse(next.value ?? "")
    return message
}

async function removeSocket(socketPath: string): Promise<void> {
    try {
        await unlink(socketPath)
    } catch (error) {
        if (!(error instanceof Error) || !("code" in error) || error.code !== "ENOENT") throw error
    }
}

test("assigns the attached actor and reports whether it is resident", { timeout: 5_000 }, async () => {
    const root = await mkdtemp("/tmp/actor-residency-")
    const actor = { project_id: "default", actor_name: "SessionCounter", actor_id: "one" }
    let assigned: unknown
    let received: unknown
    const server = createServer(socket => {
        const lines = createInterface({ input: socket })
        lines.on("line", line => {
            const message = JSON.parse(line)
            if (message.type === "attach")
                socket.write(`${JSON.stringify({ type: "attached", protocol: 31, actor })}\n`)
            else if (message.type === "residency") {
                received = message
                socket.end()
            }
        })
    })
    server.listen(`${root}/executor.sock`)
    await once(server, "listening")
    const session = new ActorSession(
        parseHostSettings({
            DURABLE_ACTORS_EXECUTOR_SOCKET: `${root}/executor.sock`,
            DURABLE_ACTORS_ENTRYPOINT: await actorBundle()
        }),
        () => ({
            ready: async () => ["SessionCounter"],
            handle: async () => ({ type: "evicted" }),
            close() {},
            assign(identity) {
                assigned = identity
            },
            isResident: () => true,
            onResidencyChange: () => () => {}
        })
    )
    try {
        await session.start()
        await session.waitUntilDisconnected()
        assert.deepEqual(assigned, actor)
        assert.deepEqual(received, { type: "residency", resident: true })
    } finally {
        server.close()
        await rm(root, { recursive: true, force: true })
    }
})

let preparedFixture: Promise<string> | undefined

function actorBundle(): Promise<string> {
    return (preparedFixture ??= (async () => {
        const source = fileURLToPath(new URL("../fixtures/actorSession.ts", import.meta.url))
        const output = fileURLToPath(new URL("../fixtures/actorSession.mjs", import.meta.url))
        await buildActor(source, output, { local: true })
        return output
    })())
}

for (const outcome of ["success", "rejection", "disconnect", "atomic", "method_failure", "atomic_failure"] as const) {
    test(`database calls cross the Worker/session boundary: ${outcome}`, { timeout: 10_000 }, async t => {
        const root = await mkdtemp("/tmp/immediate-session-")
        const database = new HostDatabase()
        const writes: boolean[] = []
        let peer: Socket | undefined
        let completed = false
        let resolve!: () => void
        let reject!: (error: unknown) => void
        const done = new Promise<void>((yes, no) => {
            resolve = yes
            reject = no
        })
        const server = createServer(socket => {
            peer = socket
            const send = (message: unknown) => socket.write(`${JSON.stringify(message)}\n`)
            const serve = databaseServer(database, send)
            const lines = createInterface({ input: socket })
            lines.on("line", line => {
                void (async () => {
                    const message = JSON.parse(line)
                    if (message.type === "attach") {
                        send({ type: "attached", protocol: 31, actor: actorIdentity() })
                        send({
                            type: "command",
                            message_id: 17,
                            command: {
                                type: "invoke",
                                request_id: "immediate",
                                method:
                                    outcome === "atomic_failure"
                                        ? "failAtomic"
                                        : outcome === "atomic"
                                          ? "incrementAtomic"
                                          : outcome === "method_failure"
                                            ? "failAfterWrite"
                                            : "incrementTwice",
                                args: []
                            }
                        })
                    } else if (message.type === "database_call") {
                        if (message.call.op !== "persist_field") return serve(message)
                        writes.push(message.call.immediate)
                        await new Promise(ready => setTimeout(ready, 20))
                        assert.equal(completed, false)
                        if (outcome === "disconnect") {
                            socket.destroy()
                            resolve()
                        } else if (outcome === "rejection")
                            send({
                                type: "database_result",
                                call_id: message.call_id,
                                error: "durable storage rejected write"
                            })
                        else serve(message)
                    } else if (message.type === "reply") {
                        completed = true
                        if (
                            (outcome === "method_failure" || outcome === "atomic_failure") &&
                            message.message_id === 17
                        ) {
                            assert.equal(message.reply.code, "actor_method_failed")
                            send({
                                type: "command",
                                message_id: 18,
                                command: {
                                    type: "invoke",
                                    request_id: "read",
                                    method: "read",
                                    args: []
                                }
                            })
                            return
                        }
                        if (outcome === "method_failure" || outcome === "atomic_failure") {
                            assert.equal(message.reply.type, "invoked")
                            assert.equal(message.reply.result, outcome === "atomic_failure" ? 0 : 1)
                            assert.deepEqual(writes, [true])
                            assert.equal(database.commits, outcome === "method_failure" ? 2 : 1)
                        } else if (outcome === "success" || outcome === "atomic") {
                            assert.equal(message.reply.type, "invoked")
                            assert.equal(message.reply.result, 2)
                            assert.deepEqual(writes, [true, true])
                            assert.equal(database.commits, outcome === "atomic" ? 2 : 3)
                        } else {
                            assert.equal(message.reply.code, "actor_method_failed")
                            assert.match(message.reply.message, /durable storage rejected write/)
                            assert.deepEqual(writes, [true])
                        }
                        socket.end()
                        resolve()
                    }
                })().catch(reject)
            })
        })
        t.after(async () => {
            peer?.destroy()
            server.close()
            database.close()
            await rm(root, { recursive: true, force: true })
        })
        server.listen(`${root}/executor.sock`)
        await once(server, "listening")
        const session = new ActorSession(
            parseHostSettings({
                DURABLE_ACTORS_EXECUTOR_SOCKET: `${root}/executor.sock`,
                DURABLE_ACTORS_ENTRYPOINT: await actorBundle()
            })
        )
        await session.start()
        await done
        await session.waitUntilDisconnected()
    })
}
