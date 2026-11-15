import type { SqliteValue } from "../actor/database.js"
import { ActorProtocolError, errorMessage } from "../errors.js"
import type { DatabaseCall, DatabaseResult } from "../host/database.js"
import type { JsonValue } from "../json.js"

import type { BlockingStream } from "./blocking-stream.js"
import { CountedStream } from "./blocking-stream.js"
import type { GarbageCollection } from "./garbage-collection.js"
import type { JsonText } from "./json-text.js"
import { StreamReader } from "./stream-reader.js"
import { StreamWriter } from "./stream-writer.js"

const NULL = 0
const INTEGER = 1
const REAL = 2
const TEXT = 3
const BLOB = 4

const COLUMNS = 0x10
const ROW = 0x11
const RESULT = 0x12
const FAILED = 0x13

// SQLite's own limit on the values one statement binds. The Rust host ends the channel beyond it.
const MAX_BINDINGS = 32_766
const MAX_SIZE = 0xffff_ffff

/** The channel can no longer be used: an exchange stopped part way, so the next would be misread. */
class DatabaseChannelError extends ActorProtocolError {}

type Answer = DatabaseResult | { readonly type: "failed"; readonly message: string }

/** What follows a call: a value a statement binds, or the JSON of a field the call writes. */
type CarriedValue = SqliteValue | JsonText

/**
 * The actor's database in the Rust host. A call holds the thread until it is answered, which is what lets
 * `this.db` be synchronous. Bound values, rows and the JSON of fields cross as bytes beside the call, and are never
 * held as JSON inside it.
 */
class DatabaseChannel {
    private readonly stream: CountedStream
    private readonly reader: StreamReader
    private readonly writer: StreamWriter
    private failure: DatabaseChannelError | undefined

    constructor(
        stream: BlockingStream,
        private readonly collection: GarbageCollection
    ) {
        this.stream = new CountedStream(stream)
        this.reader = new StreamReader(this.stream)
        this.writer = new StreamWriter(this.stream)
    }

    /** `invocation` names the command whose database the call reaches. Without it, the activation's. */
    call(call: DatabaseCall, invocation?: number): DatabaseResult {
        const answer = this.exchange(call, invocation)
        if (answer.type === "failed") throw new Error(answer.message)
        return answer
    }

    private exchange(call: DatabaseCall, invocation: number | undefined): Answer {
        if (this.failure !== undefined) throw this.failure
        const values = carried(call)
        try {
            this.request(call, invocation, values)
            return this.answer(call.op === "fields")
        } catch (error) {
            this.failure = new DatabaseChannelError(`actor database channel failed: ${errorMessage(error)}`, {
                cause: error
            })
            throw this.failure
        } finally {
            this.reader.release()
            this.collection.payloadPassed(this.stream.take())
        }
    }

    private request(call: DatabaseCall, invocation: number | undefined, values: readonly CarriedValue[]): void {
        this.writer.text(JSON.stringify({ message_id: invocation, call: head(call), bindings: values.length }))
        for (const value of values) this.value(value)
        this.writer.flush()
    }

    private value(value: CarriedValue): void {
        if (value === null) this.writer.u8(NULL)
        else if (typeof value === "string") {
            this.writer.u8(TEXT)
            this.writer.text(value)
        } else if (value instanceof Uint8Array) {
            this.writer.u8(BLOB)
            this.writer.bytes(value)
        } else if (typeof value === "object") {
            this.writer.u8(TEXT)
            this.writer.encoded(value)
        } else if (typeof value === "bigint" || fitsInteger(value)) {
            this.writer.u8(INTEGER)
            this.writer.i64(BigInt(value))
        } else {
            this.writer.u8(REAL)
            this.writer.f64(value)
        }
    }

    // Rows written before a call fails are dropped with it.
    private answer(fields: boolean): Answer {
        let columns: string[] = []
        const rows: SqliteValue[][] = []
        const stored: [string, JsonValue][] = []
        for (;;) {
            const record = this.reader.u8()
            switch (record) {
                case COLUMNS:
                    columns = Array.from({ length: this.reader.u32() }, () => this.reader.text())
                    break
                case ROW:
                    if (fields) stored.push(this.field())
                    else rows.push(Array.from(columns, () => this.cell()))
                    break
                case RESULT: {
                    const result = ending(this.reader.text())
                    if (result !== undefined) return result
                    return fields
                        ? { type: "fields", fields: Object.fromEntries(stored) }
                        : { type: "rows", columns, rows }
                }
                case FAILED:
                    return { type: "failed", message: this.reader.text() }
                default:
                    throw new Error(`unknown record ${record}`)
            }
        }
    }

    // A field is a row of its name and its JSON. Each is parsed as it arrives, so no more than one field's text is
    // held beside the values.
    private field(): [string, JsonValue] {
        const name = this.cell()
        const json = this.cell()
        if (typeof name !== "string" || typeof json !== "string") throw new Error("a field is its name and its JSON")
        return [name, JSON.parse(json) as JsonValue]
    }

    private cell(): SqliteValue {
        const type = this.reader.u8()
        switch (type) {
            case NULL:
                return null
            case INTEGER:
                return this.reader.integer()
            case REAL:
                return this.reader.f64()
            case TEXT:
                return this.reader.text()
            case BLOB:
                return this.reader.bytes()
            default:
                throw new Error(`unknown value type ${type}`)
        }
    }
}

function carried(call: DatabaseCall): readonly CarriedValue[] {
    switch (call.op) {
        case "exec":
            return bindable(call.bindings)
        case "persist_field":
            return [call.value]
        case "initialize_fields":
            return [...call.fields.values()]
        default:
            return []
    }
}

// What cannot be sent is refused before anything is written, so a refusal leaves the channel usable.
function bindable(bindings: readonly SqliteValue[]): readonly SqliteValue[] {
    if (bindings.length > MAX_BINDINGS)
        throw new RangeError(`a statement binds at most ${MAX_BINDINGS} values, not ${bindings.length}`)
    bindings.forEach(assertBindable)
    return bindings
}

function assertBindable(value: unknown, index: number): void {
    if (value === null || typeof value === "string" || typeof value === "number") return
    if (typeof value === "bigint") {
        if (BigInt.asIntN(64, value) !== value)
            throw new RangeError(`binding ${index + 1} does not fit a 64-bit SQLite integer`)
    } else if (value instanceof Uint8Array) {
        if (value.byteLength > MAX_SIZE) throw new RangeError(`binding ${index + 1} is larger than 4 GiB`)
    } else
        throw new TypeError(
            `binding ${index + 1} is ${value === undefined ? "undefined" : typeof value}; SQLite binds null, numbers, bigints, strings and Uint8Arrays`
        )
}

// The call without the values that follow it.
function head(call: DatabaseCall): object {
    switch (call.op) {
        case "exec": {
            const { bindings: _, ...statement } = call
            return statement
        }
        case "persist_field": {
            const { value: _, ...field } = call
            return field
        }
        case "initialize_fields":
            return { op: call.op, names: [...call.fields.keys()] }
        default:
            return call
    }
}

// A whole number is bound as a SQLite integer when 64 bits hold it, and as a real otherwise.
function fitsInteger(value: number): boolean {
    return Number.isInteger(value) && value >= -(2 ** 63) && value < 2 ** 63
}

// What a call ended with, or nothing when its result is the rows sent before the ending.
function ending(document: string): DatabaseResult | undefined {
    const parsed = JSON.parse(document) as DatabaseResult
    switch (parsed.type) {
        case "rows":
            return undefined
        case "alarm":
        case "done":
            return parsed
        default:
            throw new Error(`unknown result ${document.slice(0, 80)}`)
    }
}

export { DatabaseChannel, DatabaseChannelError }
