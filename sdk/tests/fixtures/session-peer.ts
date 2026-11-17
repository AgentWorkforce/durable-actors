import type { Socket } from "node:net"

import { attachEffects, attachReply } from "./detached.js"
import { withResult } from "./reply.js"

interface Frame {
    readonly message: { type: string; reply?: object }
    readonly parts: string[]
    readonly length: number
}

/** The Rust host's end of an actor session: messages are read and written whole, with their parts in place. */
export function readSession(socket: Socket, handle: (message: any) => void): void {
    let pending: Buffer = Buffer.alloc(0)
    socket.on("data", (chunk: Buffer) => {
        pending = Buffer.concat([pending, chunk])
        for (let frame = takeFrame(pending); frame !== undefined; frame = takeFrame(pending)) {
            pending = pending.subarray(frame.length)
            handle(attach(frame))
        }
    })
}

export function sessionReader(socket: Socket): () => Promise<any> {
    const messages: unknown[] = []
    const waiting: ((message: unknown) => void)[] = []
    readSession(socket, message => {
        const next = waiting.shift()
        if (next === undefined) messages.push(message)
        else next(message)
    })
    return () =>
        new Promise(resolve => {
            if (messages.length > 0) resolve(messages.shift())
            else waiting.push(resolve)
        })
}

/** Writes a message as the Rust host does: an invocation's arguments follow the line as a part. */
export function writeSession(socket: Socket, message: object): void {
    const { command } = message as { command?: { type: string; args?: unknown } }
    if (command?.type !== "invoke") {
        socket.write(`${JSON.stringify(message)}\n`)
        return
    }
    const { args, ...line } = command
    const part = JSON.stringify(args)
    socket.write(`${JSON.stringify({ ...message, command: line, parts: [Buffer.byteLength(part)] })}\n${part}`)
}

// A message is a line of JSON, then the parts whose sizes the line announces.
function takeFrame(pending: Buffer): Frame | undefined {
    const end = pending.indexOf("\n")
    if (end === -1) return undefined
    const { parts: sizes = [], ...message } = JSON.parse(pending.subarray(0, end).toString())
    let start = end + 1
    const length = sizes.reduce((total: number, size: number) => total + size, start)
    if (length > pending.length) return undefined
    const parts = sizes.map((size: number) => pending.subarray(start, (start += size)).toString())
    return { message, parts, length }
}

function attach({ message, parts }: Frame): unknown {
    if (message.type === "socket_effects") return { ...message, effects: attachEffects(parts) }
    if (message.type !== "reply") return message
    return { ...message, reply: withResult(attachReply({ ...message.reply, parts } as never)) }
}
