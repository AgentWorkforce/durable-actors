import assert from "node:assert/strict"
import { afterEach, test } from "node:test"

import { Actor, registerActorClass } from "../../src/actor/actor.js"
import type { ActorDefinition } from "../../src/actor/actor.js"
import { Ephemeral } from "../../src/actor/decorators.js"
import { Persistence } from "../../src/actor/schema.js"
import type { InvokeCommand } from "../../src/host/protocol.js"
import { ActorRuntime, DatabaseProxy } from "../fixtures/actor-runtime.js"
import { HostDatabase } from "../fixtures/host-database.js"
import { resultOf } from "../fixtures/reply.js"

class SqliteCounter extends Actor {
    count = 0

    async initialize() {
        this.db.exec("CREATE TABLE IF NOT EXISTS entries (value TEXT NOT NULL)")
    }

    async insert(value: string) {
        this.db.exec("INSERT INTO entries VALUES (?)", value)
    }

    async increment() {
        return ++this.count
    }

    async read() {
        return { count: this.count, rows: this.db.exec("SELECT value FROM entries ORDER BY rowid") }
    }

    async migrate() {
        this.db.exec("ALTER TABLE entries ADD COLUMN enabled INTEGER NOT NULL DEFAULT 1")
    }

    async fail() {
        return this.db.transaction(async () => {
            ++this.count
            this.db.exec("INSERT INTO entries (value) VALUES ('discarded')")
            this.db.exec("CREATE TABLE discarded (id INTEGER)")
            throw new Error("rollback both stores")
        })
    }

    async tables() {
        return this.db.exec(
            "SELECT name FROM sqlite_schema WHERE type = 'table' AND name IN ('entries', 'discarded') ORDER BY name"
        )
    }
}

const definition = registerActorClass(SqliteCounter, {
    actorName: "SqliteCounter",

    fields: [{ name: "count", persistence: Persistence.Persisted }]
})

const runtimes: ActorRuntime[] = []
const hosts: HostDatabase[] = []
afterEach(() => {
    runtimes.splice(0).forEach(runtime => void runtime.close(true))
    hosts.splice(0).forEach(host => host.close())
})

function createRuntime(actor: ActorDefinition = definition, host = new HostDatabase()) {
    const runtime = new ActorRuntime(
        actor,
        () => {},
        undefined,
        async () => [],
        new DatabaseProxy(host.call)
    )
    runtimes.push(runtime)
    hosts.push(host)
    return { runtime, host }
}

function restore(host: HostDatabase, actor: ActorDefinition = definition) {
    hosts.splice(hosts.indexOf(host), 1)
    return createRuntime(actor, host.reopen())
}

function invoke(runtime: ActorRuntime, method: string, args: string[] = [], actorName = "SqliteCounter") {
    return runtime.handle({
        type: "invoke",
        request_id: method,
        actor: { project_id: "test", actor_name: actorName, actor_id: "one" },
        method,
        args
    } satisfies InvokeCommand)
}

test("SQL data and schema changes commit alongside unchanged JSON fields", async () => {
    const { runtime, host } = createRuntime()
    assert.equal((await invoke(runtime, "initialize")).type, "invoked")
    const afterSchema = host.commits
    assert.equal((await invoke(runtime, "insert", ["hello ' SQL"])).type, "invoked")
    assert.deepEqual(host.fields(), { count: 0 })
    assert.equal(host.commits, afterSchema + 1)
    await invoke(runtime, "migrate")
    assert.equal(host.commits, afterSchema + 2)

    const read = await invoke(restore(host).runtime, "read")
    assert.deepEqual(resultOf(read), { count: 0, rows: [{ value: "hello ' SQL" }] })
})

test("field-only writes commit and reads leave the database unchanged", async () => {
    const { runtime, host } = createRuntime()
    await invoke(runtime, "initialize")
    const initialized = host.commits
    await invoke(runtime, "increment")
    assert.deepEqual(host.fields(), { count: 1 })
    assert.equal(host.commits, initialized + 1)
    await invoke(runtime, "read")
    assert.equal(host.commits, initialized + 1)
    assert.deepEqual(restore(host).host.fields(), { count: 1 })
})

test("large JSON fields and SQLite databases restore together", async () => {
    const fieldBytes = 17 * 1024 * 1024
    const databaseBytes = 25 * 1024 * 1024
    class LargeState extends Actor {
        value = ""

        async initialize() {
            this.value = "x".repeat(fieldBytes)
            this.db.exec("CREATE TABLE payload (data BLOB)")
            this.db.exec("INSERT INTO payload VALUES (zeroblob(?))", databaseBytes)
        }

        async read() {
            return {
                fieldBytes: this.value.length,
                databaseBytes: this.db.exec<{ bytes: number }>("SELECT length(data) AS bytes FROM payload")[0]!.bytes
            }
        }
    }
    const actor = registerActorClass(LargeState, {
        actorName: "LargeState",
        fields: [{ name: "value", persistence: Persistence.Persisted }]
    })
    const { runtime, host } = createRuntime(actor)
    assert.equal((await invoke(runtime, "initialize", [], "LargeState")).type, "invoked")
    const read = await invoke(restore(host, actor).runtime, "read", [], "LargeState")
    assert.deepEqual(resultOf(read), { fieldBytes, databaseBytes })
})

test("failed atomic calls roll back object fields, SQL writes, and SQL schema changes", async () => {
    const { runtime, host } = createRuntime()
    await invoke(runtime, "initialize")
    await invoke(runtime, "insert", ["retained"])
    const committed = host.commits
    assert.deepEqual(await invoke(runtime, "fail"), {
        type: "failed",
        code: "actor_method_failed",
        message: "rollback both stores"
    })
    const read = await invoke(runtime, "read")
    assert.deepEqual(resultOf(read), { count: 0, rows: [{ value: "retained" }] })
    assert.equal(host.commits, committed)
    assert.deepEqual(resultOf(await invoke(runtime, "tables")), [{ name: "entries" }])
})

test("socket hooks persist SQL and fields in the same completion", async () => {
    class SqliteRoom extends Actor {
        count = 0
        async onConnect() {
            this.db.exec("CREATE TABLE visits (count INTEGER)")
            this.db.exec("INSERT INTO visits VALUES (?)", ++this.count)
        }
        async read() {
            return this.db.exec("SELECT count FROM visits")
        }
    }
    const actor = registerActorClass(SqliteRoom, {
        actorName: "SqliteRoom",
        fields: [{ name: "count", persistence: Persistence.Persisted }]
    })
    const { runtime, host } = createRuntime(actor)
    const connection = { id: "socket-1", metadata: {}, tags: [] }
    const reply = await runtime.handle({
        type: "websocket_event",
        request_id: "connect",
        actor: { project_id: "test", actor_name: "SqliteRoom", actor_id: "one" },
        event: { type: "connect", connection },
        connections: [connection]
    })
    assert.equal(reply.type, "websocket_handled")
    if (reply.type !== "websocket_handled") return
    assert.deepEqual(host.fields(), { count: 1 })
    assert.deepEqual(reply.effects, [])
    const read = await invoke(restore(host, actor).runtime, "read", [], "SqliteRoom")
    assert.deepEqual(resultOf(read), [{ count: 1 }])
})

test("reentrant failures preserve SQL already committed by an overlapping success", async () => {
    let release!: () => void
    const gate = new Promise<void>(resolve => {
        release = resolve
    })
    class ReentrantSqlite extends Actor {
        count = 0
        async hold() {
            this.db.exec("CREATE TABLE entries (count INTEGER)")
            this.db.exec("INSERT INTO entries VALUES (?)", ++this.count)
            await gate
            throw new Error("failed")
        }
        async add() {
            this.db.exec("INSERT INTO entries VALUES (?)", ++this.count)
        }
        async read() {
            return this.db.exec("SELECT count FROM entries ORDER BY count")
        }
    }
    const { runtime, host } = createRuntime(
        registerActorClass(ReentrantSqlite, {
            actorName: "ReentrantSqlite",
            fields: [{ name: "count", persistence: Persistence.Persisted }],
            interleaveMethods: ["hold"]
        })
    )
    await runtime.handle({
        type: "hydrate",
        actor: { project_id: "test", actor_name: "ReentrantSqlite", actor_id: "one" }
    })
    const holding = invoke(runtime, "hold", [], "ReentrantSqlite")
    await invoke(runtime, "add", [], "ReentrantSqlite")
    release()
    assert.equal((await holding).type, "failed")
    const read = await invoke(runtime, "read", [], "ReentrantSqlite")
    assert.deepEqual(resultOf(read), [{ count: 1 }, { count: 2 }])
    assert.deepEqual(host.fields(), { count: 2 })
})

test("compiled object schema changes preserve SQLite while adding and removing fields", async () => {
    const { runtime, host } = createRuntime()
    await invoke(runtime, "initialize")
    await invoke(runtime, "insert", ["retained"])
    const updated = restore(host, {
        ...definition,
        state: {
            ...definition.state,
            fields: [
                { name: "count", persistence: Persistence.Ephemeral, private: false, emittable: false, resource: false }
            ]
        }
    })
    const read = await invoke(updated.runtime, "read")
    assert.deepEqual(resultOf(read), { count: 0, rows: [{ value: "retained" }] })
})

test("database handles keep writing after their invocation completes", async () => {
    let release!: () => void
    let pending!: Promise<void>
    const gate = new Promise<void>(resolve => {
        release = resolve
    })
    class DetachedSqlite extends Actor {
        async start() {
            const database = this.db
            pending = gate.then(() => {
                database.exec("CREATE TABLE escaped (id INTEGER)")
                database.exec("INSERT INTO escaped VALUES (1)")
            })
        }

        async read() {
            return this.db.exec("SELECT id FROM escaped")
        }
    }
    const detached = registerActorClass(DetachedSqlite, { actorName: "DetachedSqlite", fields: [] })
    const { runtime, host } = createRuntime(detached)
    assert.equal((await invoke(runtime, "start", [], "DetachedSqlite")).type, "invoked")
    release()
    await pending
    const restored = restore(host, detached)
    assert.deepEqual(resultOf(await invoke(restored.runtime, "read", [], "DetachedSqlite")), [{ id: 1 }])
})

test("background SQL is rejected while an explicit transaction is open", async () => {
    let releaseBackground!: () => void
    let releaseAtomic!: () => void
    let entered!: () => void
    let rejected!: Promise<unknown>
    const backgroundGate = new Promise<void>(resolve => {
        releaseBackground = resolve
    })
    const atomicGate = new Promise<void>(resolve => {
        releaseAtomic = resolve
    })
    const holding = new Promise<void>(resolve => {
        entered = resolve
    })
    class AtomicSqlite extends Actor {
        async start() {
            const database = this.db
            rejected = backgroundGate.then(() => database.exec("CREATE TABLE escaped (id INTEGER)"))
        }

        async hold() {
            return this.db.transaction(async () => {
                this.db.exec("CREATE TABLE held (id INTEGER)")
                entered()
                await atomicGate
            })
        }
    }
    const atomic = registerActorClass(AtomicSqlite, {
        actorName: "AtomicSqlite",

        fields: []
    })
    const { runtime } = createRuntime(atomic)
    assert.equal((await invoke(runtime, "start", [], "AtomicSqlite")).type, "invoked")
    const held = invoke(runtime, "hold", [], "AtomicSqlite")
    await holding
    releaseBackground()
    await assert.rejects(rejected, /in use by a db.transaction/)
    releaseAtomic()
    assert.equal((await held).type, "invoked")
})

test("field initializers read the actor identity and SQLite during construction", async () => {
    class InitializedSqlite extends Actor {
        @Ephemeral identity = this.id
        @Ephemeral created = this.db.exec("CREATE TABLE IF NOT EXISTS initialized (id INTEGER)")

        async read() {
            return {
                identity: this.identity,
                tables: this.db.exec("SELECT name FROM sqlite_schema WHERE name = 'initialized'")
            }
        }
    }
    const { runtime } = createRuntime(
        registerActorClass(InitializedSqlite, {
            actorName: "InitializedSqlite",
            fields: [
                { name: "identity", persistence: Persistence.Ephemeral },
                { name: "created", persistence: Persistence.Ephemeral }
            ]
        })
    )
    const reply = await invoke(runtime, "read", [], "InitializedSqlite")
    assert.deepEqual(resultOf(reply), { identity: "one", tables: [{ name: "initialized" }] })
})

test("a commit failure prevents further execution until the actor is reloaded", async () => {
    const host = new HostDatabase()
    hosts.push(host)
    const runtime = new ActorRuntime(
        definition,
        () => {},
        undefined,
        async () => [],
        new DatabaseProxy(call => {
            if (call.op === "commit") throw new Error("replication failed")
            return host.call(call)
        })
    )
    runtimes.push(runtime)
    const failed = await invoke(runtime, "initialize")
    assert.equal(failed.type, "failed")
    assert.equal(Reflect.get(failed, "code"), "actor_database_failed")
    assert.match(Reflect.get(failed, "message"), /replication failed/)
    assert.deepEqual(await invoke(runtime, "increment"), failed)
})
