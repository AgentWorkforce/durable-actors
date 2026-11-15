import assert from "node:assert/strict"

import { detachReply } from "../../src/host/protocol.js"
import type { ActorExecutorReply } from "../../src/host/protocol.js"

import { attachReply } from "./detached.js"
import type { WireReply } from "./detached.js"

export function assertReply(actual: ActorExecutorReply, expected: object): void {
    assert.deepEqual(readable(actual), expected)
}

/** A reply as the Rust host has it, with the result of an invocation as the value it encodes. */
function readable(reply: ActorExecutorReply): object {
    return withResult(attachReply(detachReply(reply)))
}

export function withResult(reply: WireReply): object {
    return reply.type === "invoked" ? { ...reply, result: JSON.parse(reply.result) } : reply
}

export function resultOf(reply: ActorExecutorReply): unknown {
    const wire = attachReply(detachReply(reply))
    if (wire.type !== "invoked") assert.fail(`expected an invoked reply, got ${JSON.stringify(wire)}`)
    return JSON.parse(wire.result)
}
