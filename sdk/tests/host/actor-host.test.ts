import assert from "node:assert/strict"
import { once } from "node:events"
import { closeSync, openSync } from "node:fs"
import { mkdtemp, rm, unlink, writeFile } from "node:fs/promises"
import type { Socket } from "node:net"
import { createServer } from "node:net"
import { createInterface } from "node:readline"
import { before, test } from "node:test"
import { fileURLToPath } from "node:url"

import { buildActor } from "../../src/compiler/actor-build.js"
import { ActorSession, parseHostSettings } from "../../src/host/actor-host.js"
import { ResidentActor } from "../../src/host/resident-actor.js"
import type { InvocationDatabase, SessionActorFactory } from "../../src/host/types.js"
import { DatabaseChannel } from "../../src/payload/database-channel.js"
import { GarbageCollection } from "../../src/payload/garbage-collection.js"
import { DatabasePeer } from "../fixtures/database-peer.js"
import type { WireReply } from "../fixtures/detached.js"
import { HostDatabase } from "../fixtures/host-database.js"
import { readSession, sessionReader, writeSession } from "../fixtures/session-peer.js"

before(
    async () => {
        await actorBundle()
    },
    { timeout: 30_000 }
)

test("attaches with the actors that its artifact exports", { timeout: 5_000 }, async () => {
    const root = await mkdtemp("/tmp/actor-discovery-")
    const server = createServer(socket => {
        const lines = createInterface({ input: socket })
        lines.once("line", line => {
            assert.deepEqual(JSON.parse(line).actor_names, ["SessionCounter"])
            socket.write(`${JSON.stringify({ type: "attached", protocol: 34, actor: actorIdentity() })}\n`)
            socket.end()
        })
    })
    server.listen(`${root}/executor.sock`)
    await once(server, "listening")
    const session = new ActorSession(
        parseHostSettings({
            DURABLE_ACTORS_EXECUTOR_SOCKET: `${root}/executor.sock`,
            DURABLE_ACTORS_ENTRYPOINT: await actorBundle()
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

test("a stalled actor import times out and closes the actor", { timeout: 5_000 }, async () => {
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
    const database = new HostDatabase()
    const session = new ActorSession(
        parseHostSettings({
            DURABLE_ACTORS_EXECUTOR_SOCKET: socketPath,
            DURABLE_ACTORS_ENTRYPOINT: await actorBundle()
        }),
        options => {
            const actor = new ResidentActor({ ...options, database: database.call })
            const close = actor.close.bind(actor)
            t.mock.method(actor, "close", () => {
                closed += 1
                close()
            })
            return actor
        }
    )
    const startup = session.start()

    try {
        const customerSocket = await Promise.race([customerSocketPromise, startup.then(() => customerSocketPromise)])
        const read = sessionReader(customerSocket)
        const next = () => readHostMessage(read)

        assert.deepEqual(await read(), {
            type: "attach",
            protocol: 34,
            actor_names: ["SessionCounter"]
        })
        customerSocket.write(`${JSON.stringify({ type: "attached", protocol: 34, actor: actorIdentity() })}\n`)
        await startup

        const invoke = (messageId: number, method: string, args: number[]) =>
            writeSession(customerSocket, {
                type: "command",
                message_id: messageId,
                command: { type: "invoke", request_id: `request-${messageId}`, method, args }
            })
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
        server.close()
        await once(server, "close")
    } finally {
        if (server.listening) server.close()
        database.close()
        await removeSocket(socketPath)
    }
})

test("a large database result and the socket message built from it cross the session intact", async t => {
    const root = await mkdtemp("/tmp/large-session-")
    const database = new HostDatabase()
    const html = '<p class="note">caf\u00e9 \\ \u2028 \u{1f600}</p>\n'
    const frames = [20_000, 1, 1_000, 0, 2_500, 3_000].map(repeats => ({ html: html.repeat(repeats) }))
    const channel = new DatabaseChannel(
        new DatabasePeer(call =>
            call.op === "exec"
                ? { type: "rows", columns: ["html"], rows: frames.map(frame => [frame.html]) }
                : database.call(call)
        ),
        new GarbageCollection()
    )
    const replied = new Promise<{ message_id: number; reply: WireReply }>((resolve, reject) => {
        const server = createServer(socket => {
            const send = (message: object) => writeSession(socket, message)
            const handle = (message: { type: string }) => {
                if (message.type === "attach") {
                    send({ type: "attached", protocol: 34, actor: actorIdentity() })
                    send({
                        type: "command",
                        message_id: 1,
                        command: {
                            type: "websocket_event",
                            request_id: "connect",
                            event: { type: "connect", connection: { id: "socket-1", metadata: null, tags: [] } },
                            connections: []
                        }
                    })
                } else if (message.type === "reply") {
                    resolve(message as never)
                    socket.end()
                }
            }
            readSession(socket, message => {
                try {
                    handle(message)
                } catch (error) {
                    reject(error)
                }
            })
            socket.on("error", reject)
        })
        t.after(async () => {
            server.close()
            database.close()
            await rm(root, { recursive: true, force: true })
        })
        server.listen(`${root}/executor.sock`)
    })
    const session = new ActorSession(
        parseHostSettings({
            DURABLE_ACTORS_EXECUTOR_SOCKET: `${root}/executor.sock`,
            DURABLE_ACTORS_ENTRYPOINT: await actorBundle()
        }),
        withDatabase((call, invocation) => channel.call(call, invocation))
    )
    await session.start()
    const { message_id, reply } = await replied
    await session.waitUntilDisconnected()

    assert.equal(message_id, 1)
    assert.ok(reply.type === "websocket_handled", JSON.stringify(reply).slice(0, 400))
    const [effect, ...others] = reply.effects
    assert.deepEqual(others, [])
    assert.ok(effect?.type === "send" && effect.connection_id === "socket-1" && effect.message.type === "text")
    assert.deepEqual(JSON.parse(effect.message.data), { type: "frames", frames })
})

test("a database channel that stops answering ends the session", { timeout: 5_000 }, async t => {
    const root = await mkdtemp("/tmp/broken-channel-")
    await writeFile(`${root}/closed-channel`, "")
    const descriptor = openSync(`${root}/closed-channel`, "r+")
    const server = createServer(socket => {
        readSession(socket, message => {
            if (message.type !== "attach") return
            writeSession(socket, { type: "attached", protocol: 34, actor: actorIdentity() })
            writeSession(socket, {
                type: "command",
                message_id: 1,
                command: { type: "invoke", request_id: "read", method: "read", args: [] }
            })
        })
    })
    t.after(async () => {
        server.close()
        closeSync(descriptor)
        await rm(root, { recursive: true, force: true })
    })
    server.listen(`${root}/executor.sock`)
    await once(server, "listening")
    const session = new ActorSession(
        parseHostSettings({
            DURABLE_ACTORS_EXECUTOR_SOCKET: `${root}/executor.sock`,
            DURABLE_ACTORS_ENTRYPOINT: await actorBundle(),
            DURABLE_ACTORS_DATABASE_FD: String(descriptor)
        })
    )

    await session.start()
    await session.waitUntilDisconnected()
})

test("a failed session connection closes the actor", async () => {
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
                socket.write(`${JSON.stringify({ type: "attached", protocol: 34, actor })}\n`)
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

for (const outcome of ["success", "rejection", "atomic", "method_failure", "atomic_failure"] as const) {
    test(`a write is in the database before its command replies: ${outcome}`, { timeout: 10_000 }, async t => {
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
        const call: InvocationDatabase = request => {
            if (request.op !== "persist_field") return database.call(request)
            writes.push(request.immediate)
            assert.equal(completed, false)
            if (outcome === "rejection") throw new Error("durable storage rejected write")
            return database.call(request)
        }
        const server = createServer(socket => {
            peer = socket
            const send = (message: object) => writeSession(socket, message)
            const invoke = (messageId: number, method: string) =>
                send({
                    type: "command",
                    message_id: messageId,
                    command: { type: "invoke", request_id: method, method, args: [] }
                })
            readSession(socket, message => {
                void (async () => {
                    if (message.type === "attach") {
                        send({ type: "attached", protocol: 34, actor: actorIdentity() })
                        invoke(
                            17,
                            outcome === "atomic_failure"
                                ? "failAtomic"
                                : outcome === "atomic"
                                  ? "incrementAtomic"
                                  : outcome === "method_failure"
                                    ? "failAfterWrite"
                                    : "incrementTwice"
                        )
                    } else if (message.type === "reply") {
                        completed = true
                        if (
                            (outcome === "method_failure" || outcome === "atomic_failure") &&
                            message.message_id === 17
                        ) {
                            assert.equal(message.reply.code, "actor_method_failed")
                            invoke(18, "read")
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
            }),
            withDatabase(call)
        )
        await session.start()
        await done
        await session.waitUntilDisconnected()
    })
}

function withDatabase(database: InvocationDatabase): SessionActorFactory {
    return options => new ResidentActor({ ...options, database })
}

function actorIdentity(): Record<string, string> {
    return {
        project_id: "default",
        actor_name: "SessionCounter",
        actor_id: "counter-1"
    }
}

function assertSessionReply(message: unknown, id: number, expected: object): void {
    const { reply, ...envelope } = message as { reply: object; type: string; message_id: number }
    assert.deepEqual(envelope, { type: "reply", message_id: id })
    assert.deepEqual(reply, expected)
}

async function readHostMessage(read: () => Promise<any>): Promise<unknown> {
    for (;;) {
        const message = await read()
        if (message.type !== "residency" && message.type !== "log") return message
    }
}

async function removeSocket(socketPath: string): Promise<void> {
    try {
        await unlink(socketPath)
    } catch (error) {
        if (!(error instanceof Error) || !("code" in error) || error.code !== "ENOENT") throw error
    }
}

let preparedFixture: Promise<string> | undefined

function actorBundle(): Promise<string> {
    return (preparedFixture ??= (async () => {
        const source = fileURLToPath(new URL("../fixtures/actorSession.ts", import.meta.url))
        const output = fileURLToPath(new URL("../fixtures/actorSession.mjs", import.meta.url))
        await buildActor(source, output, { local: true })
        return output
    })())
}
