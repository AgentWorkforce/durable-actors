import { ActorProtocolError } from "../errors.js"

const NEWLINE = 0x0a

// Each chunk is scanned once, and a line that arrives in several chunks is joined once, when it ends.
class SessionLines {
    private readonly unfinished: Buffer[] = []

    /** The message on each line that ends in `chunk`. */
    accept(chunk: Buffer): unknown[] {
        const messages: unknown[] = []
        let rest = chunk
        for (let end = rest.indexOf(NEWLINE); end !== -1; end = rest.indexOf(NEWLINE)) {
            messages.push(parseLine(this.finishLine(rest.subarray(0, end))))
            rest = rest.subarray(end + 1)
        }
        if (rest.length > 0) this.unfinished.push(rest)
        return messages
    }

    private finishLine(end: Buffer): Buffer {
        return this.unfinished.length === 0 ? end : Buffer.concat([...this.unfinished.splice(0), end])
    }
}

function parseLine(line: Buffer): unknown {
    try {
        return JSON.parse(line.toString())
    } catch (error) {
        throw new ActorProtocolError("actor session message is not valid JSON", { cause: error })
    }
}

export { SessionLines }
