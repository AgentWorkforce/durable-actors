import { readSync, writeSync } from "node:fs"

/** A connection whose reads and writes hold the calling thread until they are done. */
interface BlockingStream {
    /** Reads at least one byte into `into`, starting at `offset`. Returns 0 once the peer has closed. */
    read(into: Uint8Array, offset: number): number
    write(bytes: Uint8Array): void
}

class DescriptorStream implements BlockingStream {
    constructor(private readonly descriptor: number) {}

    read(into: Uint8Array, offset: number): number {
        return readSync(this.descriptor, into, offset, into.byteLength - offset, null)
    }

    write(bytes: Uint8Array): void {
        for (let written = 0; written < bytes.byteLength;)
            written += writeSync(this.descriptor, bytes, written, bytes.byteLength - written)
    }
}

/** Counts the bytes that cross a stream, in either direction. */
class CountedStream implements BlockingStream {
    private bytes = 0

    constructor(private readonly stream: BlockingStream) {}

    read(into: Uint8Array, offset: number): number {
        const read = this.stream.read(into, offset)
        this.bytes += read
        return read
    }

    write(bytes: Uint8Array): void {
        this.stream.write(bytes)
        this.bytes += bytes.byteLength
    }

    /** The bytes that crossed since this was last asked. */
    take(): number {
        const bytes = this.bytes
        this.bytes = 0
        return bytes
    }
}

export { CountedStream, DescriptorStream }
export type { BlockingStream }
