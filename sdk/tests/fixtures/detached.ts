import type { HostSocketEffect, SocketEffect } from "../../src/actor/socketProtocol.js"
import { detachEffects } from "../../src/host/protocol.js"
import type { ActorExecutorReply, DetachedReply, SessionPart } from "../../src/host/protocol.js"

/** A reply as the Rust host has it: a result and the text of each socket message are strings. */
export type WireReply = Wire<ActorExecutorReply>

type Wire<Reply> = {
    [Key in keyof Reply]: Key extends "effects" ? SocketEffect[] : Key extends "result" ? string : Reply[Key]
}

/** Puts the parts of a reply back the way the Rust host does: the result, then the effects and their texts. */
export function attachReply({ parts = [], ...head }: DetachedReply): WireReply {
    switch (head.type) {
        case "invoked": {
            const effects = attachEffects(parts.slice(1))
            return { ...head, type: "invoked", result: text(parts[0]!), ...(effects.length > 0 && { effects }) }
        }
        case "websocket_handled":
            return { ...head, type: "websocket_handled", effects: attachEffects(parts) }
        default:
            return head
    }
}

export function attachEffects(parts: readonly SessionPart[]): SocketEffect[] {
    const [effects, ...texts] = parts.map(text)
    return (JSON.parse(effects!) as SocketEffect[]).map(effect =>
        "message" in effect && effect.message.type === "text"
            ? { ...effect, message: { type: "text", data: texts.shift()! } }
            : effect
    )
}

/** Effects the way they reach the Rust host. */
export function wireEffects(effects: readonly HostSocketEffect[]): SocketEffect[] {
    return attachEffects(detachEffects(effects))
}

function text(part: SessionPart): string {
    return typeof part === "string" ? part : Buffer.concat(part.chunks).toString()
}
