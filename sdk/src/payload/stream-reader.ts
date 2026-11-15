import type { BlockingStream } from "./blocking-stream.js"

const BUFFER_BYTES = 64 * 1024

/** Reads little-endian values from a blocking stream. Nothing is held beyond the value being read. */
class StreamReader {
    private readonly buffer = Buffer.allocUnsafe(BUFFER_BYTES)
    private start = 0
    private end = 0
    private scratch: Buffer | undefined

    constructor(private readonly stream: BlockingStream) {}

    u8(): number {
        this.need(1)
        return this.buffer[this.start++]!
    }

    u32(): number {
        this.need(4)
        const value = this.buffer.readUInt32LE(this.start)
        this.start += 4
        return value
    }

    /** A 64-bit integer, as a number when a number holds it exactly. */
    integer(): number | bigint {
        this.need(8)
        const at = this.start
        this.start += 8
        const value = this.buffer.readInt32LE(at + 4) * 0x1_0000_0000 + this.buffer.readUInt32LE(at)
        return Number.isSafeInteger(value) ? value : this.buffer.readBigInt64LE(at)
    }

    f64(): number {
        this.need(8)
        const value = this.buffer.readDoubleLE(this.start)
        this.start += 8
        return value
    }

    /** Text after its `u32` size. Bytes that are not UTF-8 decode to U+FFFD. */
    text(): string {
        const size = this.u32()
        if (size > BUFFER_BYTES) return this.fillScratch(size).toString("utf8", 0, size)
        this.need(size)
        const at = this.start
        this.start += size
        return this.buffer.toString("utf8", at, at + size)
    }

    /** Bytes after their `u32` size, in a buffer of their own. */
    bytes(): Uint8Array {
        const bytes = new Uint8Array(this.u32())
        this.readInto(bytes)
        return bytes
    }

    /** Gives up the buffer that text larger than the read buffer was decoded from. */
    release(): void {
        this.scratch = undefined
    }

    // One buffer serves every large text of an answer, so they do not each leave one behind to be collected.
    private fillScratch(size: number): Buffer {
        if (this.scratch === undefined || this.scratch.byteLength < size) this.scratch = Buffer.allocUnsafe(size)
        this.readInto(this.scratch.subarray(0, size))
        return this.scratch
    }

    private readInto(target: Uint8Array): void {
        let filled = this.buffer.copy(target, 0, this.start, this.end)
        this.start += filled
        while (filled < target.byteLength) filled += this.read(target, filled)
    }

    private need(bytes: number): void {
        if (this.end - this.start >= bytes) return
        this.buffer.copyWithin(0, this.start, this.end)
        this.end -= this.start
        this.start = 0
        while (this.end < bytes) this.end += this.read(this.buffer, this.end)
    }

    private read(into: Uint8Array, offset: number): number {
        const read = this.stream.read(into, offset)
        if (read === 0) throw new Error("the stream closed inside a value")
        return read
    }
}

export { StreamReader }
