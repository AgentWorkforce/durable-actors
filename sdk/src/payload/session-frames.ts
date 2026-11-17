import { z } from "zod"

import { ActorProtocolError } from "../errors.js"

import type { GarbageCollection } from "./garbage-collection.js"

const NEWLINE = 0x0a

/** One session message: a line of JSON, then the text of each part whose size, in bytes, the line announces. */
interface SessionFrame {
    readonly head: unknown
    /** A part taken out of the list is held by nothing else. */
    readonly parts: string[]
}

// Each chunk is scanned once. A part is copied into a buffer of its own, so it is never searched, and the buffer is
// collected once the part is text, before anything parses it.
class SessionFrames {
    private readonly line: Buffer[] = []
    private filling: FillingFrame | undefined

    constructor(private readonly collection: GarbageCollection) {}

    accept(chunk: Buffer): SessionFrame[] {
        const frames: SessionFrame[] = []
        for (let rest = chunk; rest.length > 0;) {
            rest = this.filling === undefined ? this.readLine(rest) : this.fill(rest)
            if (!this.filling?.isComplete()) continue
            const bytes = this.filling.bytes
            frames.push(this.takeFrame())
            this.collection.payloadPassed(bytes)
        }
        return frames
    }

    private readLine(chunk: Buffer): Buffer {
        const end = chunk.indexOf(NEWLINE)
        if (end === -1) {
            this.line.push(chunk)
            return chunk.subarray(chunk.length)
        }
        const head = parseHead(this.completeLine(chunk.subarray(0, end)))
        this.filling = new FillingFrame(head, partSizes(head))
        return chunk.subarray(end + 1)
    }

    private fill(chunk: Buffer): Buffer {
        const rest = this.filling!.fill(chunk)
        this.collection.payloadPassed(chunk.length - rest.length)
        return rest
    }

    private completeLine(end: Buffer): Buffer {
        return this.line.length === 0 ? end : Buffer.concat([...this.line.splice(0), end])
    }

    private takeFrame(): SessionFrame {
        const frame = this.filling!.decode()
        this.filling = undefined
        return frame
    }
}

class FillingFrame {
    readonly bytes: number
    private readonly parts: Buffer[]
    private part = 0
    private filled = 0

    constructor(
        private readonly head: unknown,
        sizes: readonly number[]
    ) {
        this.bytes = sizes.reduce((total, size) => total + size, 0)
        this.parts = sizes.map(size => Buffer.allocUnsafe(size))
        this.skipFilledParts()
    }

    isComplete(): boolean {
        return this.part === this.parts.length
    }

    fill(chunk: Buffer): Buffer {
        const part = this.parts[this.part]!
        const taken = chunk.subarray(0, part.length - this.filled)
        part.set(taken, this.filled)
        this.filled += taken.length
        this.skipFilledParts()
        return chunk.subarray(taken.length)
    }

    decode(): SessionFrame {
        return { head: this.head, parts: this.parts.map(part => part.toString()) }
    }

    private skipFilledParts(): void {
        while (!this.isComplete() && this.filled === this.parts[this.part]!.length) {
            this.part++
            this.filled = 0
        }
    }
}

function parseHead(line: Buffer): unknown {
    try {
        return JSON.parse(line.toString())
    } catch (error) {
        throw new ActorProtocolError("actor session message is not valid JSON", { cause: error })
    }
}

function partSizes(head: unknown): readonly number[] {
    const result = partSizesSchema.safeParse(head)
    if (!result.success) throw new ActorProtocolError(`actor session message is invalid: ${result.error.message}`)
    return result.data.parts
}

const partSizesSchema = z.object({ parts: z.array(z.number().int().nonnegative()).default([]) })

export { SessionFrames }
export type { SessionFrame }
