import type { BlockingStream } from "./blocking-stream.js"
import type { JsonText } from "./json-text.js"
import { sliceEnd } from "./text-slice.js"

const BUFFER_BYTES = 64 * 1024
// A UTF-16 code unit encodes to at most three bytes of UTF-8.
const UNITS_PER_BUFFER = Math.floor(BUFFER_BYTES / 3)

/** Writes little-endian values to a blocking stream through one fixed buffer, so text is never encoded whole. */
class StreamWriter {
    private readonly buffer = Buffer.allocUnsafe(BUFFER_BYTES)
    private used = 0

    constructor(private readonly stream: BlockingStream) {}

    u8(value: number): void {
        this.reserve(1)
        this.buffer[this.used++] = value
    }

    u32(value: number): void {
        this.reserve(4)
        this.used = this.buffer.writeUInt32LE(value, this.used)
    }

    i64(value: bigint): void {
        this.reserve(8)
        this.used = this.buffer.writeBigInt64LE(value, this.used)
    }

    f64(value: number): void {
        this.reserve(8)
        this.used = this.buffer.writeDoubleLE(value, this.used)
    }

    /** Text as UTF-8 after its `u32` size. */
    text(text: string): void {
        const size = Buffer.byteLength(text)
        this.u32(size)
        let written = 0
        for (let start = 0; start < text.length;) {
            const end = sliceEnd(text, start, UNITS_PER_BUFFER)
            written += this.encode(text.substring(start, end))
            start = end
        }
        if (written !== size) throw new Error(`text announced as ${size} bytes encoded to ${written}`)
    }

    /** Bytes after their `u32` size. */
    bytes(bytes: Uint8Array): void {
        this.u32(bytes.byteLength)
        this.raw(bytes)
    }

    /** Text that is already UTF-8, after its `u32` size. */
    encoded(text: JsonText): void {
        this.u32(text.byteLength)
        for (const chunk of text.chunks) this.raw(chunk)
    }

    flush(): void {
        if (this.used === 0) return
        const used = this.used
        this.used = 0
        this.stream.write(this.buffer.subarray(0, used))
    }

    private raw(bytes: Uint8Array): void {
        if (bytes.byteLength > BUFFER_BYTES - this.used) {
            this.flush()
            this.stream.write(bytes)
        } else {
            this.buffer.set(bytes, this.used)
            this.used += bytes.byteLength
        }
    }

    private encode(slice: string): number {
        this.reserve(slice.length * 3)
        const { written } = encoder.encodeInto(slice, this.buffer.subarray(this.used))
        this.used += written
        return written
    }

    private reserve(bytes: number): void {
        if (bytes > BUFFER_BYTES - this.used) this.flush()
    }
}

const encoder = new TextEncoder()

export { StreamWriter }
