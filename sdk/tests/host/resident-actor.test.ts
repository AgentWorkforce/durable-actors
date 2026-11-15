import assert from "node:assert/strict"
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { test } from "node:test"
import { fileURLToPath, pathToFileURL } from "node:url"

import type { HostSocketEffect } from "../../src/actor/socketProtocol.js"
import { buildActor } from "../../src/compiler/actor-build.js"
import type { DatabaseCall } from "../../src/host/database.js"
import type { ActorExecutorCommand, ActorExecutorReply, ActorLog } from "../../src/host/protocol.js"
import { ResidentActor } from "../../src/host/resident-actor.js"
import type { InvocationDatabase } from "../../src/host/types.js"
import { wireEffects } from "../fixtures/detached.js"
import { HostDatabase } from "../fixtures/host-database.js"
import { assertReply, resultOf } from "../fixtures/reply.js"
import { command } from "../fixtures/resident-actor.js"

const sdk = JSON.stringify(fileURLToPath(new URL("../../src/index.js", import.meta.url)))
const evict = { type: "evict", graceful: false } as const

test("eviction leaves fields and SQL in the database for the actor's next stay", { timeout: 30_000 }, async t => {
    const { actor, database } = await resident(
        t,
        "SqliteResident",
        `export class SqliteResident extends Actor {
            @Persisted count = 0
            async increment(): Promise<number> {
                this.db.exec("CREATE TABLE IF NOT EXISTS entries (count INTEGER, note TEXT)")
                this.db.exec("INSERT INTO entries VALUES (?, ?)", ++this.count, "caf\u00e9")
                return this.count
            }
            async read(): Promise<{ count: number; note: string }[]> {
                return this.db.exec("SELECT count, note FROM entries ORDER BY count")
            }
        }`
    )

    assert.equal((await run(actor, invoke("increment"))).type, "invoked")
    assert.deepEqual(database.fields(), { count: 1 })
    assert.deepEqual(await run(actor, evict), { type: "evicted" })

    assert.deepEqual(resultOf(await run(actor, invoke("read"))), [{ count: 1, note: "caf\u00e9" }])
})

test("console output reaches the host tagged with the command that wrote it", { timeout: 30_000 }, async t => {
    const logs: ActorLog[] = []
    const { actor } = await resident(
        t,
        "LoggingResident",
        `export class LoggingResident extends Actor {
            async speak(): Promise<void> {
                console.warn("inside %s", "speak")
                setTimeout(() => console.info("after", { replied: true }))
            }
        }`,
        { logs: log => logs.push(log) }
    )

    await actor.handle(invoke("speak"), command(7))
    await new Promise(resolve => setTimeout(resolve, 10))
    console.debug("outside any command")

    assert.deepEqual(logs, [
        { messageId: 7, level: "warn", message: "inside speak" },
        { messageId: 7, level: "info", message: "after { replied: true }" },
        { messageId: undefined, level: "debug", message: "outside any command" }
    ])
})

test("a database call names its command only while that command runs", { timeout: 30_000 }, async t => {
    const named: (number | undefined)[] = []
    const { actor, database } = await resident(
        t,
        "LingeringResident",
        `export class LingeringResident extends Actor {
            async write(): Promise<void> {
                this.db.exec("CREATE TABLE notes (id INTEGER)")
                setTimeout(() => this.db.exec("INSERT INTO notes VALUES (1)"))
            }
        }`,
        {
            database: host => (call, invocation) => {
                if (call.op === "exec" && call.sql.includes("notes")) named.push(invocation)
                return host.call(call)
            }
        }
    )

    await actor.handle(invoke("write"), command(41))
    await new Promise(resolve => setTimeout(resolve, 10))

    assert.deepEqual(named, [41, undefined])
    assert.equal(database.query("SELECT id FROM notes").length, 1)
})

test("the actor stays in memory between commands until it is evicted", { timeout: 30_000 }, async t => {
    const { actor } = await resident(
        t,
        "RememberingResident",
        `export class RememberingResident extends Actor {
            @Ephemeral calls = 0
            async call(): Promise<number> { return ++this.calls }
        }`
    )
    const seen: boolean[] = []
    const unsubscribe = actor.onResidencyChange(() => seen.push(actor.isResident()))

    assert.equal(actor.isResident(), false)
    assertReply(await run(actor, invoke("call")), { type: "invoked", result: 1 })
    assertReply(await run(actor, invoke("call")), { type: "invoked", result: 2 })
    assert.equal(actor.isResident(), true)
    await run(actor, evict)
    assert.equal(actor.isResident(), false)
    assert.deepEqual(seen, [true, false])

    unsubscribe()
    assertReply(await run(actor, invoke("call")), { type: "invoked", result: 1 })
    assert.deepEqual(seen, [true, false])
})

test("overlapping commands keep their own replies, socket output and lookups", { timeout: 30_000 }, async t => {
    const { actor } = await resident(
        t,
        "InterleavedResident",
        `export class InterleavedResident extends Actor {
            @Persisted count = 0
            @Ephemeral private release: (() => void) | undefined = undefined
            @Interleave async hold() {
                const sockets = await this.getConnections()
                await new Promise<void>(resolve => {
                    this.release = resolve
                    this.broadcast("started:" + sockets[0]?.id)
                })
                this.broadcast("ended:" + sockets[0]?.id)
                return ++this.count
            }
            async increment() {
                this.broadcast("increment")
                const count = ++this.count
                this.release?.()
                return count
            }
        }`
    )
    const held: string[] = []
    const admitted: number[] = []
    let started!: () => void
    const start = new Promise<void>(resolve => (started = resolve))

    const holding = actor.handle(
        invoke("hold"),
        command(1, {
            allowNext: () => admitted.push(1),
            publish(effects) {
                held.push(...texts(effects))
                started()
            },
            connections: async () => [{ id: "one", metadata: {}, tags: [] }]
        })
    )
    await start
    const second: string[] = []
    const incremented = await actor.handle(
        invoke("increment"),
        command(2, { publish: effects => second.push(...texts(effects)) })
    )

    assert.deepEqual(admitted, [1])
    assert.equal(resultOf(incremented), 1)
    assert.deepEqual(second, ['"increment"'])
    assert.equal(resultOf(await holding), 2)
    assert.deepEqual(held, ['"started:one"', '"ended:one"'])

    await run(actor, evict)
    const resumed = await run(actor, invoke("increment"))
    assert.ok(resumed.type === "invoked")
    assert.equal(resultOf(resumed), 3)
    assert.equal(resumed.sequence, 3, "an actor's replies stay in order across its stays in memory")
})

test("socket handlers run for the connections of their command", { timeout: 30_000 }, async t => {
    const { actor } = await resident(t, "SocketResident", counter("SocketResident"))
    const connection = { id: "socket-1", metadata: { userId: "user-1" }, tags: [] }

    assertReply(
        await run(actor, {
            type: "websocket_event",
            request_id: "connect",
            event: { type: "connect", connection },
            connections: [connection]
        }),
        { type: "websocket_handled", effects: [] }
    )
    await run(actor, evict)
    const published: HostSocketEffect[] = []
    assertReply(
        await actor.handle(
            message("hello", connection),
            command(2, { publish: effects => published.push(...effects) })
        ),
        { type: "websocket_handled", effects: [] }
    )

    assert.deepEqual(wireEffects(published), [
        {
            type: "send",
            connection_id: "socket-1",
            message: { type: "text", data: JSON.stringify({ text: "user-1:hello" }) }
        }
    ])
})

test("a failure the actor caused rolls its state back and leaves it in memory", { timeout: 30_000 }, async t => {
    const { actor } = await resident(t, "FailingResident", counter("FailingResident"))
    const seen: boolean[] = []
    actor.onResidencyChange(() => seen.push(actor.isResident()))
    await run(actor, invoke("increment"))

    for (const failure of [
        invoke("explode"),
        invoke("missing"),
        message("fail", { id: "socket-1", metadata: { userId: "user-1" }, tags: [] })
    ]) {
        assert.equal((await run(actor, failure)).type, "failed")
        assert.equal(actor.isResident(), true)
        assertReply(await run(actor, invoke("getCount")), { type: "invoked", result: 1 })
    }
    assert.deepEqual(seen, [true])
})

test("eviction answers a command that is still running and closes the database to its code", async t => {
    const { actor, database } = await resident(
        t,
        "HeldResident",
        `let release: () => void = () => {}
        export class HeldResident extends Actor {
            @Persisted count = 0
            @Interleave async hold() {
                await new Promise<void>(resolve => (release = resolve))
                return ++this.count
            }
            async release() { release() }
            async increment() { return ++this.count }
        }`
    )
    let admitted!: () => void
    const admission = new Promise<void>(resolve => (admitted = resolve))
    const holding = actor.handle(invoke("hold"), command(1, { allowNext: admitted }))
    await admission

    assert.deepEqual(await run(actor, evict), { type: "evicted" })
    assertFailed(await holding, "actor_worker_terminated", /evicted/)
    assert.equal(actor.isResident(), false)

    await run(actor, invoke("release"))
    await new Promise(resolve => setImmediate(resolve))
    assert.deepEqual(database.fields(), { count: 0 }, "the stopped code wrote nothing")
    assert.equal(resultOf(await run(actor, invoke("increment"))), 1)
})

test("a failed commit stops the actor, which starts again from its database", { timeout: 30_000 }, async t => {
    let failing = false
    const { actor } = await resident(t, "UncommittedResident", counter("UncommittedResident"), {
        database: host => call => {
            if (failing && call.op === "commit") throw new Error("replication failed")
            return host.call(call)
        }
    })
    const seen: boolean[] = []
    actor.onResidencyChange(() => seen.push(actor.isResident()))
    await run(actor, invoke("increment"))

    failing = true
    assertFailed(await run(actor, invoke("increment")), "actor_database_failed", /replication failed/)
    assert.equal(actor.isResident(), false)

    failing = false
    assertReply(await run(actor, invoke("getCount")), { type: "invoked", result: 2 })
    assert.deepEqual(seen, [true, false, true])
})

test("an error nothing caught stops the actor and fails what is running", { timeout: 30_000 }, async t => {
    const logs: ActorLog[] = []
    const { actor } = await resident(
        t,
        "CrashingResident",
        `export class CrashingResident extends Actor {
            @Persisted count = 0
            @Interleave async hold() { await new Promise(() => {}) }
            async increment() { return ++this.count }
        }`,
        { logs: log => logs.push(log) }
    )
    let admitted!: () => void
    const admission = new Promise<void>(resolve => (admitted = resolve))
    const holding = actor.handle(invoke("hold"), command(1, { allowNext: admitted }))
    await admission

    process.emit("uncaughtException", new Error("timer crashed"))

    assertFailed(await holding, "actor_worker_failed", /timer crashed/)
    assert.equal(actor.isResident(), false)
    assert.match(logs.at(-1)!.message, /actor code failed outside a call: Error: timer crashed/)
    assert.equal(resultOf(await run(actor, invoke("increment"))), 1)
})

test("commands fail until an actor the entrypoint exports is assigned", { timeout: 30_000 }, async t => {
    const { actor } = await resident(t, "AssignedResident", counter("AssignedResident"), { assign: false })

    assert.deepEqual(await run(actor, evict), { type: "evicted" })
    assertFailed(await run(actor, invoke("increment")), "actor_worker_failed", /no assigned actor/)
    assert.throws(() => actor.assign(identity("OtherResident"), noActorCalls), /OtherResident is not loaded/)

    actor.assign(identity("AssignedResident"), noActorCalls)
    assertReply(await run(actor, invoke("increment")), { type: "invoked", result: 1 })
})

test("a closed actor answers commands as terminated and gives the console back", { timeout: 30_000 }, async t => {
    const log = console.log
    const { actor } = await resident(t, "ClosedResident", counter("ClosedResident"))
    await run(actor, invoke("increment"))
    assert.notEqual(console.log, log)

    actor.close()
    actor.close()

    assert.equal(console.log, log)
    assert.equal(actor.isResident(), false)
    assertFailed(await run(actor, invoke("increment")), "actor_worker_terminated", /closed/)
    await assert.rejects(actor.ready(), /closed/)
})

test("an actor module that throws while loading fails readiness", { timeout: 30_000 }, async t => {
    const entrypoint = await build(
        t,
        "UnloadableResident",
        `if (process.env.UNLOADABLE_RESIDENT !== "loads") throw new Error("import failed")
        ${counter("UnloadableResident")}`
    )
    const actor = new ResidentActor({ actorEntrypointUrl: entrypoint, database: unusedDatabase })
    t.after(() => actor.close())

    await assert.rejects(actor.ready(), /import failed/)
})

interface ResidentOptions {
    readonly assign?: boolean
    readonly logs?: (log: ActorLog) => void
    readonly database?: (host: HostDatabase) => InvocationDatabase
}

async function resident(
    t: { after(cleanup: () => unknown): void },
    actorName: string,
    source: string,
    options: ResidentOptions = {}
): Promise<{ actor: ResidentActor; database: HostDatabase }> {
    const database = new HostDatabase()
    const actor = new ResidentActor({
        actorEntrypointUrl: await build(t, actorName, source),
        database: options.database?.(database) ?? database.call
    })
    t.after(() => {
        actor.close()
        database.close()
    })
    await actor.ready()
    if (options.assign !== false) actor.assign(identity(actorName), noActorCalls, options.logs)
    return { actor, database }
}

async function build(t: { after(cleanup: () => unknown): void }, actorName: string, source: string): Promise<string> {
    const root = await mkdtemp(path.join(os.tmpdir(), "durable-actors-resident-"))
    t.after(() => rm(root, { recursive: true, force: true }))
    await mkdir(path.join(root, "src"))
    await writeFile(path.join(root, "package.json"), JSON.stringify({ type: "module" }))
    await writeFile(
        path.join(root, "src/actors.ts"),
        `import { Actor, Ephemeral, Interleave, Persisted } from ${sdk}\n${source}\n`
    )
    const output = path.join(root, `src/${actorName}.mjs`)
    await buildActor(path.join(root, "src/actors.ts"), output, { local: true })
    return pathToFileURL(output).href
}

function counter(actorName: string): string {
    return `export class ${actorName} extends Actor<{ userId: string }, { text: string }> {
        @Persisted count = 0

        async increment(amount = 1): Promise<number> {
            this.count += amount
            return this.count
        }

        async getCount(): Promise<number> {
            return this.count
        }

        async explode(): Promise<void> {
            return this.db.transaction(async () => {
                this.count = 999
                throw new Error("failed")
            })
        }

        async onConnect(): Promise<void> {}

        async onMessage(
            socket: { metadata: { userId: string }; send(message: { text: string }): void },
            message: { text: string }
        ): Promise<void> {
            return this.db.transaction(async () => {
                this.count += 1
                if (message.text === "fail") throw new Error("failed")
                socket.send({ text: socket.metadata.userId + ":" + message.text })
            })
        }
    }`
}

let nextCommand = 100

function run(actor: ResidentActor, executed: ActorExecutorCommand): Promise<ActorExecutorReply> {
    return actor.handle(executed, command(nextCommand++))
}

function invoke(method: string, ...args: number[]): ActorExecutorCommand {
    return { type: "invoke", request_id: `request-${method}`, method, args }
}

function message(text: string, connection: { id: string; metadata: { userId: string }; tags: string[] }) {
    return {
        type: "websocket_event" as const,
        request_id: `socket-${text}`,
        event: {
            type: "message" as const,
            connection_id: connection.id,
            message: { type: "text" as const, data: JSON.stringify({ text }) }
        },
        connections: [connection]
    }
}

function texts(effects: readonly HostSocketEffect[]): string[] {
    return wireEffects(effects).flatMap(effect => (effect.type === "broadcast" ? [effect.message.data] : []))
}

function assertFailed(reply: ActorExecutorReply, code: string, message: RegExp): void {
    assert.ok(reply.type === "failed", JSON.stringify(reply))
    assert.equal(reply.code, code)
    assert.match(reply.message, message)
}

function identity(actorName: string) {
    return { project_id: "default", actor_name: actorName, actor_id: "resident-1" }
}

async function noActorCalls(): Promise<never> {
    throw new Error("unexpected actor call")
}

function unusedDatabase(call: DatabaseCall): never {
    throw new Error(`unexpected database call ${call.op}`)
}
