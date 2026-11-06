import { AsyncLocalStorage } from "node:async_hooks"
import { isDeepStrictEqual } from "node:util"

import type { ActorAlarmState, ActorAlarmStorage } from "../actor/alarm.js"
import type { ActorQueryResult, RuntimeActorDatabase, SqliteValue } from "../actor/database.js"
import { errorMessage } from "../errors.js"
import type { JsonObject, JsonValue } from "../json.js"

type DatabaseValue =
    | { readonly type: "null" }
    | { readonly type: "integer"; readonly value: string }
    | { readonly type: "real"; readonly value: number }
    | { readonly type: "text"; readonly value: string }
    | { readonly type: "blob"; readonly value: string }

type DatabaseCall =
    | { readonly op: "exec"; readonly sql: string; readonly bindings: DatabaseValue[]; readonly immediate: boolean }
    | { readonly op: "fields"; readonly names?: readonly string[] }
    | { readonly op: "initialize_fields"; readonly fields: JsonObject }
    | { readonly op: "persist_field"; readonly name: string; readonly value: JsonValue; readonly immediate: boolean }
    | { readonly op: "delete_field"; readonly name: string; readonly immediate: boolean }
    | { readonly op: "replace_fields"; readonly fields: JsonObject }
    | { readonly op: "alarm" }
    | { readonly op: "set_alarm"; readonly timestamp_ms: number; readonly immediate: boolean }
    | { readonly op: "delete_alarm"; readonly immediate: boolean }
    | { readonly op: "commit" }
    | { readonly op: "rollback" }

type DatabaseResult =
    | { readonly type: "rows"; readonly columns: string[]; readonly rows: DatabaseValue[][] }
    | { readonly type: "fields"; readonly fields: JsonObject }
    | { readonly type: "alarm"; readonly timestamp_ms: number | null }
    | { readonly type: "done" }

type DatabaseTransport = (call: DatabaseCall) => DatabaseResult

class DatabaseCommitError extends Error {}

type FieldRestore = (value: JsonValue | undefined) => void

class DatabaseProxy implements RuntimeActorDatabase, ActorAlarmStorage {
    private readonly transactionDepth = new AsyncLocalStorage<number>()
    private depth = 0
    private transactionsUnavailable: string | undefined
    private readonly fieldRestores = new Map<string, FieldRestore>()

    constructor(private readonly transport: DatabaseTransport) {}

    exec<Row extends object>(sql: string, ...bindings: SqliteValue[]): Row[] {
        const { columns, rows } = this.query(sql, ...bindings)
        return rows.map(row => Object.fromEntries(row.map((value, index) => [columns[index], value])) as Row)
    }

    query(sql: string, ...bindings: SqliteValue[]): ActorQueryResult {
        this.assertAccess()
        const result = this.transport({ op: "exec", sql, bindings: bindings.map(encode), immediate: true })
        if (result.type !== "rows") throw unexpected(result)
        return { columns: result.columns, rows: result.rows.map(row => row.map(decode)) }
    }

    async transaction<T>(operation: () => Promise<T>): Promise<T> {
        if (this.transactionsUnavailable !== undefined) throw new Error(this.transactionsUnavailable)
        this.assertAccess()
        const depth = this.depth + 1
        this.control(depth === 1 ? "BEGIN" : `SAVEPOINT ${savepoint(depth)}`)
        this.depth = depth
        try {
            const result = await this.transactionDepth.run(depth, operation)
            if (this.depth !== depth) throw new Error("await nested db.transaction calls before their parent returns")
            this.control(depth === 1 ? "COMMIT" : `RELEASE ${savepoint(depth)}`)
            return result
        } catch (error) {
            if (this.depth >= depth) this.rollbackTo(depth)
            throw error
        } finally {
            this.depth = Math.min(this.depth, depth - 1)
        }
    }

    rejectTransactions(reason: string): void {
        this.transactionsUnavailable = reason
    }

    onFieldRestore(name: string, restore: FieldRestore): void {
        this.fieldRestores.set(name, restore)
    }

    // With names, only those fields cross from the host: a field read moves its whole value.
    fields(names?: readonly string[]): JsonObject {
        const result = this.transport(names === undefined ? { op: "fields" } : { op: "fields", names })
        if (result.type !== "fields") throw unexpected(result)
        return result.fields
    }

    getAlarmState(): ActorAlarmState {
        const result = this.transport({ op: "alarm" })
        if (result.type !== "alarm") throw unexpected(result)
        return { timestampMs: result.timestamp_ms }
    }

    setAlarm(timestampMs: number): void {
        this.transport({ op: "set_alarm", timestamp_ms: timestampMs, immediate: true })
    }

    deleteAlarm(): void {
        this.transport({ op: "delete_alarm", immediate: true })
    }

    initializeFields(fields: JsonObject): void {
        this.transport({ op: "initialize_fields", fields })
    }

    persistField(name: string, value: JsonValue | undefined): void {
        this.assertAccess()
        this.transport(
            value === undefined
                ? { op: "delete_field", name, immediate: true }
                : { op: "persist_field", name, value, immediate: true }
        )
    }

    commit(): void {
        if (this.depth > 0) throw new Error("await every db.transaction before the actor method returns")
        try {
            this.transport({ op: "commit" })
        } catch (cause) {
            throw new DatabaseCommitError(`failed to replicate actor SQLite commit: ${errorMessage(cause)}`, { cause })
        }
    }

    rollback(): void {
        this.transport({ op: "rollback" })
        this.depth = 0
    }

    // Work outside the open transaction, such as un-awaited or background calls, would otherwise join it.
    private assertAccess(): void {
        if ((this.transactionDepth.getStore() ?? 0) !== this.depth)
            throw new Error(
                "actor database is in use by a db.transaction; await it before using the database elsewhere"
            )
    }

    private rollbackTo(depth: number): void {
        const written = this.fields()
        try {
            if (depth === 1) this.transport({ op: "rollback" })
            else {
                this.control(`ROLLBACK TO ${savepoint(depth)}`)
                this.control(`RELEASE ${savepoint(depth)}`)
            }
        } catch {
            // SQLite already rolled back the whole transaction, for example after INSERT OR ROLLBACK.
            this.rollback()
            return this.restoreFields()
        }
        this.restoreFields(written)
    }

    private control(sql: string): void {
        this.transport({ op: "exec", sql, bindings: [], immediate: true })
    }

    // Only fields the rollback changed are restored, so unsaved in-memory edits to other fields survive.
    private restoreFields(written?: JsonObject): void {
        const fields = this.fields()
        for (const [name, restore] of this.fieldRestores)
            if (written === undefined || !isDeepStrictEqual(written[name], fields[name])) restore(fields[name])
    }
}

function savepoint(depth: number): string {
    return `actor_transaction_${depth}`
}

function encode(value: SqliteValue): DatabaseValue {
    if (value === null) return { type: "null" }
    if (typeof value === "bigint") return { type: "integer", value: value.toString() }
    if (typeof value === "number")
        return Number.isInteger(value) ? { type: "integer", value: value.toString() } : { type: "real", value }
    if (typeof value === "string") return { type: "text", value }
    return { type: "blob", value: Buffer.from(value.buffer, value.byteOffset, value.byteLength).toString("base64") }
}

function decode(value: DatabaseValue): SqliteValue {
    switch (value.type) {
        case "null":
            return null
        case "integer": {
            const integer = BigInt(value.value)
            return Number.isSafeInteger(Number(integer)) ? Number(integer) : integer
        }
        case "real":
        case "text":
            return value.value
        case "blob":
            return new Uint8Array(Buffer.from(value.value, "base64"))
    }
}

function unexpected(result: DatabaseResult): Error {
    return new Error(`unexpected actor database result ${result.type}`)
}

export { DatabaseCommitError, DatabaseProxy }
export type { DatabaseCall, DatabaseResult, DatabaseTransport, DatabaseValue }
