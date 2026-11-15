import { mkdtempSync, rmSync } from "node:fs"
import { createRequire } from "node:module"
import { join } from "node:path"

import type { DatabaseCall, DatabaseResult } from "../../src/host/database.js"
import type { JsonObject, JsonValue } from "../../src/json.js"
import type { JsonText } from "../../src/payload/json-text.js"

type Binding = null | bigint | number | string | Uint8Array

interface Statement {
    all(...bindings: Binding[]): Record<string, Binding>[]
    run(...bindings: Binding[]): unknown
    get(...bindings: Binding[]): Record<string, Binding> | null | undefined
}

interface BunStatement extends Statement {
    readonly columnNames: string[]
    values(...bindings: Binding[]): Binding[][]
}

interface NodeStatement extends Statement {
    columns(): { name: string }[]
    setReturnArrays(enabled: boolean): void
}

interface Connection {
    readonly inTransaction?: boolean
    readonly isTransaction?: boolean
    exec(sql: string): void
    prepare(sql: string): Statement
    close(): void
}

const directories: string[] = []
process.once("exit", () => directories.forEach(directory => rmSync(directory, { recursive: true, force: true })))

export class HostDatabase {
    commits = 0
    readonly calls: DatabaseCall[] = []
    private readonly connection: Connection
    private synced: string

    constructor(readonly path = freshPath()) {
        this.connection = connect(path)
        this.connection.exec(
            "PRAGMA journal_mode=WAL; CREATE TABLE IF NOT EXISTS __terse_fields(name TEXT PRIMARY KEY, value TEXT NOT NULL CHECK(json_valid(value)))"
        )
        this.synced = this.changeToken()
    }

    static with(fields: JsonObject): HostDatabase {
        const database = new HostDatabase()
        for (const [name, value] of Object.entries(fields)) database.persist(name, JSON.stringify(value))
        database.synced = database.changeToken()
        return database
    }

    reopen(): HostDatabase {
        this.close()
        return new HostDatabase(this.path)
    }

    call = (call: DatabaseCall): DatabaseResult => {
        this.calls.push(call)
        switch (call.op) {
            case "exec":
                return this.write(
                    () => this.exec(call.sql, call.bindings.map(bound)),
                    /^\s*rollback(?:\s*;)?\s*$/i.test(call.sql)
                )
            case "fields":
                return { type: "fields", fields: this.fields() }
            case "initialize_fields":
                return this.write(() => {
                    for (const [name, value] of call.fields)
                        this.connection
                            .prepare("INSERT INTO __terse_fields VALUES (?, ?) ON CONFLICT(name) DO NOTHING")
                            .run(name, jsonString(value))
                })
            case "persist_field":
                return this.write(() => this.persist(call.name, jsonString(call.value)))
            case "delete_field":
                return this.write(() => {
                    this.connection.prepare("DELETE FROM __terse_fields WHERE name = ?").run(call.name)
                })
            case "alarm":
                return this.write(() => this.alarm())
            case "set_alarm":
                return this.write(() => this.updateAlarm(call.timestamp_ms))
            case "delete_alarm":
                return this.write(() => this.updateAlarm(null))
            case "commit":
                this.commit()
                return { type: "done" }
            case "rollback":
                if (this.inTransaction) this.connection.exec("ROLLBACK")
                this.synced = this.changeToken()
                return { type: "done" }
        }
    }

    fields(): JsonObject {
        return Object.fromEntries(
            this.connection
                .prepare("SELECT name, value FROM __terse_fields")
                .all()
                .map(row => [String(row.name), JSON.parse(String(row.value)) as JsonValue])
        )
    }

    query(sql: string): Record<string, Binding>[] {
        return this.connection.prepare(sql).all()
    }

    close(): void {
        if (this.inTransaction) this.connection.exec("ROLLBACK")
        this.connection.close()
    }

    private get inTransaction(): boolean {
        return this.connection.inTransaction ?? this.connection.isTransaction ?? false
    }

    private write(operation: () => DatabaseResult | void, rollback = false): DatabaseResult {
        try {
            return operation() ?? { type: "done" }
        } finally {
            if (!this.inTransaction) {
                const token = this.changeToken()
                if (!rollback && token !== this.synced) this.commits++
                this.synced = token
            }
        }
    }

    private exec(sql: string, bindings: Binding[]): DatabaseResult {
        const { columns, rows } = positionalRows(this.connection.prepare(sql), bindings)
        return { type: "rows", columns, rows: rows.map(row => row.map(read)) }
    }

    private persist(name: string, json: string): void {
        this.connection
            .prepare(
                "INSERT INTO __terse_fields VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET value = excluded.value WHERE value <> excluded.value"
            )
            .run(name, json)
    }

    private alarm(): DatabaseResult {
        this.ensureAlarmTable()
        const row = this.connection.prepare("SELECT timestamp FROM __terse_alarm WHERE singleton = 1").get()
        return {
            type: "alarm",
            timestamp_ms: row == null || row.timestamp === null ? null : Number(row.timestamp)
        }
    }

    private updateAlarm(timestampMs: number | null): void {
        this.ensureAlarmTable()
        this.connection
            .prepare(
                "INSERT INTO __terse_alarm (singleton, timestamp) VALUES (1, ?) ON CONFLICT(singleton) DO UPDATE SET timestamp = excluded.timestamp"
            )
            .run(timestampMs)
    }

    private ensureAlarmTable(): void {
        this.connection.exec(
            "CREATE TABLE IF NOT EXISTS __terse_alarm (singleton INTEGER PRIMARY KEY CHECK(singleton = 1), timestamp INTEGER)"
        )
    }

    private commit(): void {
        this.write(() => {
            if (this.inTransaction) this.connection.exec("COMMIT")
        })
    }

    private changeToken(): string {
        return JSON.stringify(
            [
                this.connection.prepare("SELECT total_changes() AS changes").get(),
                this.connection.prepare("PRAGMA schema_version").get(),
                this.connection.prepare("PRAGMA user_version").get()
            ],
            (_, value: unknown) => (typeof value === "bigint" ? value.toString() : value)
        )
    }
}

/** The JSON a field was sent as, which the host stores as it is. */
export function jsonString(text: JsonText): string {
    return Buffer.concat(text.chunks).toString()
}

// The connection reads every integer as a bigint, and binds a number as a real. A whole number is an integer
// to the Rust host when 64 bits hold it.
function bound(value: Binding): Binding {
    const integer = typeof value === "number" && Number.isInteger(value) && value >= -(2 ** 63) && value < 2 ** 63
    return integer ? BigInt(value) : value
}

// An integer reaches the actor as the Rust host sends it: a number when a number holds it exactly.
function read(value: Binding): Binding {
    return typeof value === "bigint" && Number.isSafeInteger(Number(value)) ? Number(value) : value
}

function positionalRows(statement: Statement, bindings: Binding[]): { columns: string[]; rows: Binding[][] } {
    if ("values" in statement) {
        const bun = statement as BunStatement
        return { rows: bun.values(...bindings) ?? [], columns: bun.columnNames }
    }
    const node = statement as NodeStatement
    node.setReturnArrays(true)
    return { rows: node.all(...bindings) as unknown as Binding[][], columns: node.columns().map(column => column.name) }
}

function freshPath(): string {
    const directory = mkdtempSync("/tmp/sdk-host-database-")
    directories.push(directory)
    return join(directory, "actor.sqlite")
}

function connect(path: string): Connection {
    const require = createRequire(import.meta.url)
    if (process.versions.bun) {
        const { Database } = require("bun:sqlite") as {
            Database: new (path: string, options: { safeIntegers: boolean }) => Connection
        }
        return new Database(path, { safeIntegers: true })
    }
    const { DatabaseSync } = require("node:sqlite") as {
        DatabaseSync: new (path: string, options: { readBigInts: boolean }) => Connection
    }
    return new DatabaseSync(path, { readBigInts: true })
}
