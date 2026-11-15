import assert from "node:assert/strict"
import { test } from "node:test"

import type { SqliteValue } from "../../src/actor/database.js"
import type { DatabaseCall, DatabaseResult } from "../../src/host/database.js"
import type { BlockingStream } from "../../src/payload/blocking-stream.js"
import { DatabaseChannel, DatabaseChannelError } from "../../src/payload/database-channel.js"
import { GarbageCollection } from "../../src/payload/garbage-collection.js"
import { encodeJson } from "../../src/payload/json-text.js"
import { DatabasePeer, ending, sized, value } from "../fixtures/database-peer.js"

const emoji = "\u{1f600}"

test("bound values come back as rows of the same SQLite storage classes", () => {
    const blob = Uint8Array.from({ length: 150_000 }, (_, index) => index % 251)
    // An odd number of leading characters puts a surrogate pair across the boundary of each encoded slice.
    const text = `a${emoji.repeat(40_000)}caf\u00e9`
    const bindings = [null, 7, -1, 1.5, 1e300, "", "text", text, new Uint8Array([0, 1, 255]), blob, -(2n ** 63n)]
    const channel = channelTo(new DatabasePeer(echo, 1021))

    const result = channel.call(statement("SELECT ?", bindings))

    const columns = bindings.map((_, index) => `column ${index}`)
    assert.deepEqual(result, { type: "rows", columns, rows: [bindings, bindings] })
})

test("an integer is a number when a number holds it exactly, and a bigint otherwise", () => {
    const channel = channelTo(new DatabasePeer(echo))
    const sent = [2 ** 53 - 1, -(2 ** 53 - 1), 2 ** 53, -(2 ** 53), 2 ** 62, 9007199254740993n, 4_294_967_296, -1n]

    const result = channel.call(statement("SELECT ?", sent))

    assert.ok(result.type === "rows")
    assert.deepEqual(result.rows[0], [
        2 ** 53 - 1,
        -(2 ** 53 - 1),
        9007199254740992n,
        -9007199254740992n,
        4611686018427387904n,
        9007199254740993n,
        4_294_967_296,
        -1
    ])
})

test("half of a surrogate pair is sent as U+FFFD, as it is counted", () => {
    const channel = channelTo(new DatabasePeer(echo))

    const result = channel.call(statement("SELECT ?", ["\ud83d", `${"a".repeat(21_844)}\ud83dz`]))

    assert.ok(result.type === "rows")
    assert.deepEqual(result.rows[0], ["\ufffd", `${"a".repeat(21_844)}\ufffdz`])
})

test("a call names the command it belongs to, and nothing outside one", () => {
    const peer = new DatabasePeer(() => ({ type: "done" }))
    const channel = channelTo(peer)

    channel.call({ op: "commit" }, 17)
    channel.call({ op: "rollback" })
    channel.call(statement("DELETE FROM notes WHERE id = ?", [3]), 18)

    assert.deepEqual(peer.requests, [
        { invocation: 17, call: { op: "commit" } },
        { invocation: undefined, call: { op: "rollback" } },
        { invocation: 18, call: { op: "exec", sql: "DELETE FROM notes WHERE id = ?", immediate: true, bindings: [3n] } }
    ])
})

test("results that are not rows are read as the host wrote them", () => {
    const results: DatabaseResult[] = [
        { type: "alarm", timestamp_ms: 1_000 },
        { type: "alarm", timestamp_ms: null },
        { type: "done" }
    ]
    let next = 0
    const channel = channelTo(new DatabasePeer(() => results[next]!))

    for (; next < results.length; next++) assert.deepEqual(channel.call({ op: "alarm" }), results[next])
})

test("a field's JSON follows its call instead of being inside it, and fields are read from rows", () => {
    const note = { z: `caf\u00e9 ${emoji} "quoted"\n`.repeat(20_000), a: [1, null] }
    const done = ending(0x12, '{"type":"done"}')
    const stored = [row("note", JSON.stringify(note)), row("count", "1")]
    const stream = new Scripted([
        done,
        done,
        Buffer.concat([columns("name", "value"), ...stored, ending(0x12, '{"type":"rows"}')])
    ])
    const channel = channelTo(stream)

    channel.call({ op: "persist_field", name: "note", value: encodeJson(note), immediate: true }, 7)
    channel.call({
        op: "initialize_fields",
        fields: new Map([
            ["count", encodeJson(1)],
            ["ready", encodeJson(true)]
        ])
    })

    assert.deepEqual(stream.sent, [
        Buffer.concat([
            sized('{"message_id":7,"call":{"op":"persist_field","name":"note","immediate":true},"bindings":1}'),
            value(JSON.stringify(note))
        ]),
        Buffer.concat([
            sized('{"call":{"op":"initialize_fields","names":["count","ready"]},"bindings":2}'),
            value("1"),
            value("true")
        ])
    ])
    assert.deepEqual(channel.call({ op: "fields" }), { type: "fields", fields: { note, count: 1 } })
})

test("what crosses in either direction counts towards the next collection", () => {
    let collections = 0
    const text = "x".repeat(5 * 1024 * 1024)
    const peer = new DatabasePeer(
        call => (call.op === "fields" ? { type: "fields", fields: { text } } : { type: "done" }),
        64 * 1024
    )
    const channel = new DatabaseChannel(peer, new GarbageCollection(() => collections++))

    channel.call({ op: "persist_field", name: "text", value: encodeJson(text), immediate: true })
    assert.equal(collections, 0)
    channel.call({ op: "fields" })
    assert.equal(collections, 1)
})

test("a failed statement throws the host's message and leaves the channel usable", () => {
    const channel = channelTo(
        new Scripted([
            Buffer.concat([columns("id"), row(1), row(2), ending(0x13, "UNIQUE constraint failed: notes.id")]),
            Buffer.concat([columns("id"), row(3), ending(0x12, '{"type":"rows"}')])
        ])
    )

    assert.throws(() => channel.call(statement("INSERT")), /^Error: UNIQUE constraint failed: notes.id$/)
    assert.deepEqual(channel.call(statement("SELECT")), { type: "rows", columns: ["id"], rows: [[3]] })
})

test("a value that SQLite cannot bind is refused before anything is sent", () => {
    const peer = new DatabasePeer(echo)
    const channel = channelTo(peer)

    assert.throws(() => channel.call(statement("SELECT ?", [1, true as never])), /binding 2 is boolean/)
    assert.throws(() => channel.call(statement("SELECT ?", [undefined as never])), /binding 1 is undefined/)
    assert.throws(() => channel.call(statement("SELECT ?", [2n ** 63n])), /does not fit a 64-bit SQLite integer/)
    assert.throws(() => channel.call(statement("SELECT ?", Array<null>(32_767).fill(null))), /at most 32766 values/)
    assert.deepEqual(peer.requests, [])
    assert.deepEqual(channel.call(statement("SELECT ?", ["still usable"])).type, "rows")
})

test("an answer cut short ends the channel for every later call", () => {
    const stream = new Scripted([Buffer.concat([columns("id"), Buffer.from([0x11, 3, 200, 0, 0, 0, 1, 2])])])
    const channel = channelTo(stream)

    assert.throws(() => channel.call({ op: "commit" }), DatabaseChannelError)
    assert.throws(() => channel.call({ op: "commit" }), DatabaseChannelError)
    assert.equal(stream.sent.length, 1)
})

function echo(call: DatabaseCall): DatabaseResult {
    if (call.op !== "exec") throw new Error(`unexpected ${call.op}`)
    const bindings = call.bindings.map(numberWhenExact)
    return { type: "rows", columns: bindings.map((_, index) => `column ${index}`), rows: [bindings, bindings] }
}

function numberWhenExact(value: SqliteValue): SqliteValue {
    return typeof value === "bigint" && Number.isSafeInteger(Number(value)) ? Number(value) : value
}

function statement(sql: string, bindings: readonly SqliteValue[] = []): DatabaseCall {
    return { op: "exec", sql, bindings, immediate: true }
}

function columns(...names: string[]): Buffer {
    const count = Buffer.alloc(5, 0x10)
    count.writeUInt32LE(names.length, 1)
    return Buffer.concat([count, ...names.map(sized)])
}

function row(...cells: SqliteValue[]): Buffer {
    return Buffer.concat([Buffer.from([0x11]), ...cells.map(value)])
}

function channelTo(stream: BlockingStream): DatabaseChannel {
    return new DatabaseChannel(stream, new GarbageCollection())
}

/** Answers each request with the next prepared answer, then closes. Keeps what each request wrote. */
class Scripted implements BlockingStream {
    readonly sent: Buffer[] = []
    private answer: Buffer | undefined = Buffer.alloc(0)

    constructor(private readonly answers: Buffer[]) {}

    write(bytes: Uint8Array): void {
        if (this.answer !== undefined) this.sent.push(Buffer.alloc(0))
        this.answer = undefined
        this.sent[this.sent.length - 1] = Buffer.concat([this.sent.at(-1)!, bytes])
    }

    read(into: Uint8Array, offset: number): number {
        this.answer ??= this.answers.shift() ?? Buffer.alloc(0)
        const taken = this.answer.subarray(0, into.byteLength - offset)
        into.set(taken, offset)
        this.answer = this.answer.subarray(taken.length)
        return taken.length
    }
}
