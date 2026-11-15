import assert from "node:assert/strict"
import { test } from "node:test"

import { runWithActorDatabase } from "../../src/actor/database.js"
import { DatabaseCommitError, DatabaseProxy } from "../../src/host/database.js"
import type { DatabaseCall, DatabaseResult } from "../../src/host/database.js"
import { HostDatabase } from "../fixtures/host-database.js"

test("values keep their SQLite storage classes across the host boundary", () => {
    const host = new HostDatabase()
    const database = new DatabaseProxy(host.call)
    database.exec("CREATE TABLE value_types (value)")
    const values = [null, 9007199254740993n, 7, 1.5, "text", new Uint8Array([0, 1, 255])]
    for (const value of values) database.exec("INSERT INTO value_types VALUES (?)", value)
    assert.deepEqual(database.exec("SELECT value, typeof(value) AS type FROM value_types ORDER BY rowid"), [
        { value: null, type: "null" },
        { value: 9007199254740993n, type: "integer" },
        { value: 7, type: "integer" },
        { value: 1.5, type: "real" },
        { value: "text", type: "text" },
        { value: new Uint8Array([0, 1, 255]), type: "blob" }
    ])
    assert.deepEqual((host.calls[1] as Extract<DatabaseCall, { op: "exec" }>).bindings, [null])
    assert.deepEqual((host.calls[2] as Extract<DatabaseCall, { op: "exec" }>).bindings, [9007199254740993n])
    host.close()
})

test("query returns columns and rows in column order, keeping duplicate names", () => {
    const host = new HostDatabase()
    const database = new DatabaseProxy(host.call)
    database.exec("CREATE TABLE agents (id INTEGER, name TEXT)")
    database.exec("CREATE TABLE channels (id INTEGER, agent_id INTEGER)")
    database.exec("INSERT INTO agents VALUES (1, 'ada')")
    database.exec("INSERT INTO channels VALUES (10, 1)")
    assert.deepEqual(
        database.query(
            "SELECT agents.id, channels.id, agents.name FROM agents JOIN channels ON channels.agent_id = agents.id WHERE agents.id = ?",
            1
        ),
        { columns: ["id", "id", "name"], rows: [[1, 10, "ada"]] }
    )
    assert.deepEqual(database.query("INSERT INTO agents VALUES (2, 'grace')"), { columns: [], rows: [] })
    host.close()
})

test("background work cannot read or write while an explicit transaction runs", async () => {
    const host = new HostDatabase()
    const database = new DatabaseProxy(host.call)
    let release!: () => void
    const gate = new Promise<void>(resolve => (release = resolve))
    let exec!: Promise<unknown>
    let query!: Promise<unknown>
    await runWithActorDatabase({}, async () => {
        exec = gate.then(() => database.exec("SELECT 1"))
        query = gate.then(() => database.query("SELECT 1"))
    })
    await database.transaction(async () => {
        release()
        await assert.rejects(exec, /in use by a db.transaction/)
        await assert.rejects(query, /in use by a db.transaction/)
    })
    host.close()
})

test("ordinary SQL autocommits and explicit scopes group persisted writes", async () => {
    const host = new HostDatabase()
    const database = new DatabaseProxy(host.call)
    database.exec("CREATE TABLE entries (value INTEGER)")
    database.persistField("count", 1)
    const committed = host.commits
    await assert.rejects(
        database.transaction(async () => {
            database.persistField("count", 2)
            database.persistField("count", undefined)
            assert.equal(host.commits, committed)
            throw new Error("abort")
        }),
        /abort/
    )
    assert.equal(host.commits, committed)
    assert.deepEqual(database.fields(), { count: 1 })
    host.close()
})

test("host errors surface to actor code and commit failures are distinguishable", () => {
    const database = new DatabaseProxy((call: DatabaseCall): DatabaseResult => {
        throw new Error(call.op === "commit" ? "replication failed" : "SQLite runtime table names are reserved")
    })
    assert.throws(() => database.exec("SELECT * FROM __terse_fields"), /SQLite runtime table names are reserved/)
    assert.throws(() => database.commit(), DatabaseCommitError)
})

test("unexpected host results are rejected", () => {
    const database = new DatabaseProxy(() => ({ type: "done" }))
    assert.throws(() => database.exec("SELECT 1"), /unexpected actor database result/)
})
