import type { Socket } from "node:net"

import type { GarbageCollection } from "./garbage-collection.js"
import { sliceEnd } from "./text-slice.js"

const WRITE_BYTES = 64 * 1024
// A UTF-16 code unit encodes to at most three bytes of UTF-8.
const BYTES_PER_UNIT = 3

/** A string, or bytes that are already UTF-8. */
type SessionText = string | Uint8Array

// The socket is given one bounded buffer at a time. A string is encoded as the socket takes it, so a large one is
// never held encoded whole, and given 13 MB in one buffer the socket takes three times as long to send it.
class SessionWriter {
    private readonly texts: SessionText[] = []
    private start = 0
    // No fewer than the bytes still to write, which is what sizes the next buffer.
    private bound = 0
    private writing = false

    constructor(
        private readonly socket: Pick<Socket, "write">,
        private readonly collection: GarbageCollection
    ) {}

    write(texts: readonly SessionText[]): void {
        for (const text of texts) {
            if (text.length === 0) continue
            this.texts.push(text)
            this.bound += mostBytes(text)
        }
        this.drain()
    }

    private drain(): void {
        if (this.writing || this.texts.length === 0) return
        this.writing = true
        const buffer = this.nextWrite()
        this.socket.write(buffer, () => {
            this.writing = false
            this.collection.payloadPassed(buffer.byteLength)
            this.drain()
        })
    }

    // Short texts share a buffer, so a burst of short messages costs one system call. Bytes that fill most of a
    // buffer leave as they are, without a copy.
    private nextWrite(): Uint8Array {
        const first = this.texts[0]!
        if (typeof first !== "string" && first.byteLength > WRITE_BYTES / 2) return this.take(first)
        const buffer = Buffer.allocUnsafe(Math.min(WRITE_BYTES, this.bound))
        let used = 0
        while (this.texts.length > 0 && used < buffer.length) {
            const written = this.fill(buffer.subarray(used))
            if (written === 0) break
            used += written
        }
        return buffer.subarray(0, used)
    }

    // Writes the first text into `room`: as much of a string as fits, and bytes only whole.
    private fill(room: Uint8Array): number {
        const text = this.texts[0]!
        if (typeof text === "string") return this.encodeInto(text, room)
        if (text.byteLength > room.length) return 0
        room.set(this.take(text))
        return text.byteLength
    }

    private encodeInto(text: string, room: Uint8Array): number {
        const end = sliceEnd(text, this.start, room.length)
        const { read, written } = encoder.encodeInto(text.substring(this.start, end), room)
        this.start += read
        this.bound -= read * BYTES_PER_UNIT
        if (this.start === text.length) {
            this.texts.shift()
            this.start = 0
        }
        return written
    }

    private take(bytes: Uint8Array): Uint8Array {
        this.texts.shift()
        this.bound -= bytes.byteLength
        return bytes
    }
}

function mostBytes(text: SessionText): number {
    return typeof text === "string" ? text.length * BYTES_PER_UNIT : text.byteLength
}

const encoder = new TextEncoder()

export { SessionWriter }
export type { SessionText }
