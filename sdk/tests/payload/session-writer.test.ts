import assert from "node:assert/strict"
import type { Socket } from "node:net"
import { test } from "node:test"

import { GarbageCollection } from "../../src/payload/garbage-collection.js"
import { SessionWriter } from "../../src/payload/session-writer.js"

const WRITE_BYTES = 64 * 1024

test("gives the socket one bounded buffer at a time, in order", () => {
    const { socket, written, flushed } = heldSocket()
    const writer = new SessionWriter(socket, new GarbageCollection())
    const large = "0123456789".repeat(15_000)

    writer.write(["a", "b", large, "c"])
    assert.equal(written.length, 1)
    writer.write(["", "d"])
    takeAll(written, flushed)

    assert.deepEqual(
        written.map(buffer => buffer.byteLength),
        [WRITE_BYTES, WRITE_BYTES, 150_004 - 2 * WRITE_BYTES]
    )
    assert.equal(Buffer.concat(written).toString(), `ab${large}cd`)
})

test("text of every width is encoded as the bytes its size announces, across buffers", () => {
    const texts = [
        "plain ".repeat(20_000),
        "caf\u00e9 ".repeat(20_000),
        "\u65e5\u672c\u8a9e".repeat(20_000),
        "\u{1f600}".repeat(40_000),
        `x${"\u{1f600}".repeat(40_000)}`,
        `\ud83d${"\u00e9".repeat(70_000)}\ude00`
    ]
    for (const text of texts) {
        const { socket, written, flushed } = heldSocket()
        new SessionWriter(socket, new GarbageCollection()).write([text])
        takeAll(written, flushed)

        assert.ok(written.every(buffer => buffer.byteLength <= WRITE_BYTES))
        assert.ok(Buffer.concat(written).equals(Buffer.from(text)))
        assert.equal(Buffer.concat(written).byteLength, Buffer.byteLength(text))
    }
})

test("bytes that are already encoded leave in order, and the short ones share a buffer", () => {
    const { socket, written, flushed } = heldSocket()
    const writer = new SessionWriter(socket, new GarbageCollection())
    const chunks = [Buffer.alloc(WRITE_BYTES, "a"), Buffer.alloc(WRITE_BYTES - 3, "b"), Buffer.from("tail")]

    writer.write(["head\n", ...chunks, "next\n", Buffer.from("short")])
    takeAll(written, flushed)

    assert.deepEqual(
        written.map(buffer => buffer.byteLength),
        [5, WRITE_BYTES, WRITE_BYTES - 3, 14]
    )
    assert.equal(written[1], chunks[0])
    assert.equal(written[2], chunks[1])
    assert.equal(Buffer.concat(written).toString(), `head\n${chunks.join("")}next\nshort`)
})

test("a large message is encoded only as the socket takes it", () => {
    const { socket, written, flushed } = heldSocket()
    const writer = new SessionWriter(socket, new GarbageCollection())

    writer.write(["\u00e9".repeat(1_000_000)])

    assert.equal(written.length, 1)
    flushed.shift()!()
    assert.equal(written.length, 2)
    assert.equal(written[0]!.byteLength + written[1]!.byteLength, 2 * WRITE_BYTES)
})

test("a buffer has passed once the socket has taken it", () => {
    const { socket, flushed } = heldSocket()
    let collections = 0
    const writer = new SessionWriter(socket, new GarbageCollection(() => collections++))

    writer.write(["x".repeat(8 * 1024 * 1024)])
    for (let taken = 1; taken < 128; taken++) flushed.shift()!()
    assert.equal(collections, 0)
    flushed.shift()!()
    assert.equal(collections, 1)
})

function takeAll(written: Uint8Array[], flushed: (() => void)[]): void {
    for (let writes = 1; flushed.length > 0; writes++) {
        assert.equal(written.length, writes)
        flushed.shift()!()
    }
}

/** A socket that takes a buffer only when the test says so. */
function heldSocket() {
    const written: Uint8Array[] = []
    const flushed: (() => void)[] = []
    const socket = {
        write(buffer: Uint8Array, flush: () => void) {
            written.push(buffer)
            flushed.push(flush)
            return true
        }
    } as unknown as Pick<Socket, "write">
    return { socket, written, flushed }
}
