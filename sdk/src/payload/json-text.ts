import { types } from "node:util"

import type { JsonValue } from "../json.js"

const CHUNK_BYTES = 64 * 1024
// The most one UTF-16 code unit is written as: a `\u` escape.
const LONGEST_UNIT = 6

const QUOTE = 0x22
const BACKSLASH = 0x5c

/** The text of a JSON value as UTF-8, in chunks of at most 64 KB. */
interface JsonText {
    readonly byteLength: number
    readonly chunks: readonly Uint8Array[]
}

interface RawJson {
    readonly rawJSON: string
}

/**
 * Writes the text of a value straight into chunks, byte for byte what `JSON.stringify` gives, and for the same
 * values: it gives nothing where `JSON.stringify` does, and throws where it throws. The text is never a string: one
 * character outside Latin-1 makes that string two bytes a character, and building it holds as much again, so a
 * 12 MB message cost 57 MiB before any of it was sent.
 */
function encodeJson(value: JsonValue): JsonText
function encodeJson(value: unknown): JsonText | undefined
function encodeJson(value: unknown): JsonText | undefined {
    const form = jsonForm(value, "")
    if (hasNoJson(form)) return undefined
    const writer = new JsonWriter()
    writer.value(form)
    return writer.finish()
}

class JsonWriter {
    private readonly chunks: Uint8Array[] = []
    // The arrays and objects being written, each inside the one before it.
    private readonly open: object[] = []
    private chunk = takeSpare()
    private bytes = 0
    private used = 0

    /** Writes a value that `jsonForm` gave and JSON has a text for. */
    value(value: unknown): void {
        if (typeof value === "string") this.string(value)
        else if (typeof value === "number") this.ascii(Number.isFinite(value) ? String(value) : "null")
        else if (typeof value === "bigint") throw new TypeError("JSON cannot serialize a BigInt")
        else if (typeof value !== "object" || value === null) this.ascii(String(value))
        else if (Array.isArray(value)) this.array(value)
        else if (isRawJson(value)) this.raw(value.rawJSON)
        else this.object(value as Record<string, unknown>)
    }

    // The last chunk is copied out at its own size, so a short text holds no more than its length.
    finish(): JsonText {
        if (this.used > 0) this.keep(this.chunk.slice(0, this.used))
        spare = this.chunk
        return { byteLength: this.bytes, chunks: this.chunks }
    }

    private array(values: readonly unknown[]): void {
        this.enter(values)
        this.byte(0x5b)
        const length = values.length
        for (let index = 0; index < length; index++) {
            if (index > 0) this.byte(0x2c)
            const value = jsonForm(values[index], String(index))
            this.value(hasNoJson(value) ? null : value)
        }
        this.byte(0x5d)
        this.open.pop()
    }

    private object(members: Record<string, unknown>): void {
        this.enter(members)
        this.byte(0x7b)
        let written = 0
        for (const key of Object.keys(members)) {
            const value = jsonForm(members[key], key)
            if (hasNoJson(value)) continue
            if (written++ > 0) this.byte(0x2c)
            this.string(key)
            this.byte(0x3a)
            this.value(value)
        }
        this.byte(0x7d)
        this.open.pop()
    }

    private enter(container: object): void {
        if (this.open.includes(container)) throw new TypeError("JSON cannot serialize a value that contains itself")
        this.open.push(container)
    }

    private string(text: string): void {
        this.byte(QUOTE)
        for (let at = 0; at < text.length;) at = this.fill(text, at)
        this.byte(QUOTE)
    }

    // Writes `text` from `at` until it ends or the chunk is full, and returns where it stopped.
    private fill(text: string, at: number): number {
        const full = CHUNK_BYTES - LONGEST_UNIT
        if (this.used > full) this.next()
        const chunk = this.chunk
        let used = this.used
        for (; at < text.length && used <= full; at++) {
            const unit = text.charCodeAt(at)
            if (unit < 0x80) {
                if (unit >= 0x20 && unit !== QUOTE && unit !== BACKSLASH) chunk[used++] = unit
                else used = escaped(chunk, used, unit)
            } else if (unit < 0x800) {
                chunk[used++] = 0xc0 | (unit >> 6)
                chunk[used++] = 0x80 | (unit & 0x3f)
            } else if (unit < 0xd800 || unit > 0xdfff) {
                chunk[used++] = 0xe0 | (unit >> 12)
                chunk[used++] = 0x80 | ((unit >> 6) & 0x3f)
                chunk[used++] = 0x80 | (unit & 0x3f)
            } else if (isPair(unit, text.charCodeAt(at + 1))) {
                const point = 0x10000 + ((unit - 0xd800) << 10) + (text.charCodeAt(++at) - 0xdc00)
                chunk[used++] = 0xf0 | (point >> 18)
                chunk[used++] = 0x80 | ((point >> 12) & 0x3f)
                chunk[used++] = 0x80 | ((point >> 6) & 0x3f)
                chunk[used++] = 0x80 | (point & 0x3f)
            } else used = escaped(chunk, used, unit)
        }
        this.used = used
        return at
    }

    // Numbers, booleans and null.
    private ascii(text: string): void {
        if (this.used > CHUNK_BYTES - text.length) this.next()
        for (let at = 0; at < text.length; at++) this.chunk[this.used++] = text.charCodeAt(at)
    }

    private raw(text: string): void {
        for (const byte of encoder.encode(text)) this.byte(byte)
    }

    private byte(value: number): void {
        if (this.used === CHUNK_BYTES) this.next()
        this.chunk[this.used++] = value
    }

    private next(): void {
        this.keep(this.chunk.subarray(0, this.used))
        this.chunk = new Uint8Array(CHUNK_BYTES)
        this.used = 0
    }

    private keep(chunk: Uint8Array): void {
        this.chunks.push(chunk)
        this.bytes += chunk.byteLength
    }
}

// What `JSON.stringify` writes in place of `value`, the member `key` of what holds it: the answer of its `toJSON`,
// and the primitive inside a Number, String, Boolean or BigInt object.
function jsonForm(value: unknown, key: string): unknown {
    if ((typeof value === "object" && value !== null) || typeof value === "bigint") {
        const toJSON: unknown = (value as { toJSON?: unknown }).toJSON
        if (typeof toJSON === "function") value = toJSON.call(value, key)
    }
    return typeof value === "object" && value !== null && types.isBoxedPrimitive(value) ? unboxed(value) : value
}

function unboxed(value: object): unknown {
    if (types.isNumberObject(value)) return Number(value)
    if (types.isStringObject(value)) return String(value)
    if (types.isBooleanObject(value)) return Boolean.prototype.valueOf.call(value)
    if (types.isBigIntObject(value)) return BigInt.prototype.valueOf.call(value)
    return value
}

function hasNoJson(value: unknown): boolean {
    return value === undefined || typeof value === "function" || typeof value === "symbol"
}

// An engine without `JSON.rawJSON` has no such values.
const isRawJson = ((JSON as { isRawJSON?: unknown }).isRawJSON ?? (() => false)) as (value: object) => value is RawJson

function isPair(high: number, low: number): boolean {
    return high < 0xdc00 && low >= 0xdc00 && low <= 0xdfff
}

// Writes the escape for `unit` and returns where it ends. Half of a surrogate pair on its own has no UTF-8, so
// JSON escapes it the way it does a control character.
function escaped(chunk: Uint8Array, used: number, unit: number): number {
    chunk[used++] = BACKSLASH
    const letter = SHORT_ESCAPES[unit]
    if (letter !== undefined && letter !== 0) {
        chunk[used++] = letter
        return used
    }
    chunk[used++] = 0x75
    for (let shift = 12; shift >= 0; shift -= 4) chunk[used++] = HEX[(unit >> shift) & 0xf]!
    return used
}

// The letter that follows the backslash for each character JSON escapes with one.
const SHORT_ESCAPES = new Uint8Array(BACKSLASH + 1)
for (const [character, letter] of Object.entries({ "\b": "b", "\t": "t", "\n": "n", "\f": "f", "\r": "r" }))
    SHORT_ESCAPES[character.charCodeAt(0)] = letter.charCodeAt(0)
SHORT_ESCAPES[QUOTE] = QUOTE
SHORT_ESCAPES[BACKSLASH] = BACKSLASH

const HEX = Uint8Array.from("0123456789abcdef", digit => digit.charCodeAt(0))

const encoder = new TextEncoder()

// A text starts in the buffer the last one finished in, having copied its own bytes out, so a short text allocates
// only its length. A text that is being written keeps its buffer to itself: code the value runs, such as a
// `toJSON`, may encode another text meanwhile.
let spare: Uint8Array | undefined

function takeSpare(): Uint8Array {
    const chunk = spare ?? new Uint8Array(CHUNK_BYTES)
    spare = undefined
    return chunk
}

export { encodeJson }
export type { JsonText }
