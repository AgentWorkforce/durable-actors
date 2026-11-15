import { z } from "zod"

import type { ActorIdentity } from "../actor/identity.js"
import { actorComponentSchema, actorIdentitySchema } from "../actor/identity.js"
import type { HostSocketEffect, SocketQuery } from "../actor/socketProtocol.js"
import { socketConnectionSchema, socketEventSchema } from "../actor/socketProtocol.js"
import { ActorProtocolError } from "../errors.js"
import type { JsonValue } from "../json.js"
import { jsonValueSchema, serializeJson } from "../json.js"
import type { JsonText } from "../payload/json-text.js"

function parseActorSessionServerMessage(head: unknown): ActorSessionServerMessage {
    const result = actorSessionServerMessageSchema.safeParse(head)
    if (!result.success) throw new ActorProtocolError(`actor session message is invalid: ${result.error.message}`)
    return result.data
}

// What can be large follows a reply's line as parts, so it is never escaped into the line: the result, then the
// effects.
function detachReply(reply: ActorExecutorReply): DetachedReply {
    switch (reply.type) {
        case "invoked": {
            const { result, effects = [], ...head } = reply
            return { ...head, parts: [result, ...detachEffects(effects)] }
        }
        case "websocket_handled": {
            const { effects, ...head } = reply
            return { ...head, parts: detachEffects(effects) }
        }
        default:
            return reply
    }
}

// A text message leaves its text out of the effects. The texts follow as parts of their own, in the same order,
// so none is escaped into a JSON string.
function detachEffects(effects: readonly HostSocketEffect[]): SessionPart[] {
    const texts: JsonText[] = []
    return [
        serializeJson(
            effects.map(effect => detachText(effect, texts)),
            "socket effects"
        ),
        ...texts
    ]
}

function detachText(effect: HostSocketEffect, texts: JsonText[]): object {
    if ((effect.type !== "send" && effect.type !== "broadcast") || effect.message.type !== "text") return effect
    texts.push(effect.message.data)
    return { ...effect, message: { type: "text" } }
}

function failedReply(code: string, message: string): FailedReply {
    return { type: "failed", code, message }
}

const invokeCommandSchema = z.object({
    type: z.literal("invoke"),
    request_id: actorComponentSchema,
    method: actorComponentSchema,
    args: z.array(jsonValueSchema)
})

const websocketEventCommandSchema = z.object({
    type: z.literal("websocket_event"),
    request_id: actorComponentSchema,
    event: socketEventSchema,
    connections: z.array(socketConnectionSchema)
})

const evictCommandSchema = z.object({ type: z.literal("evict"), graceful: z.boolean() })

const hydrateCommandSchema = z.object({ type: z.literal("hydrate") })

const executorCommandSchema = z.discriminatedUnion("type", [
    invokeCommandSchema,
    websocketEventCommandSchema,
    evictCommandSchema,
    hydrateCommandSchema
])

const actorSessionServerMessageSchema = z.discriminatedUnion("type", [
    z.object({ type: z.literal("attached"), protocol: z.literal(33), actor: actorIdentitySchema }),
    z.object({
        type: z.literal("actor_result"),
        call_id: z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
        outcome: jsonValueSchema
    }),
    z.object({
        type: z.literal("socket_connections"),
        message_id: z.number().int().nonnegative(),
        connections: z.union([z.array(socketConnectionSchema), z.number().int().nonnegative()]),
        error: z.string().optional()
    }),
    z.object({
        type: z.literal("command"),
        message_id: z.number().int().nonnegative(),
        command: executorCommandSchema
    })
])

type InvokeCommand = z.infer<typeof invokeCommandSchema> & AssignedActor
type EvictCommand = z.infer<typeof evictCommandSchema>
type ActorExecutorCommand = z.infer<typeof executorCommandSchema>
type ActorExecutionCommand = Exclude<ActorExecutorCommand, EvictCommand>
/** A command with the actor this process was assigned. */
type AssignedCommand = ActorExecutorCommand & AssignedActor
type ActorSessionServerMessage = z.infer<typeof actorSessionServerMessageSchema>
type ActorExecutorReply =
    InvokedReply | WebSocketHandledReply | FailedReply | EvictedReply | { readonly type: "hydrated" }
type ReplyHead =
    | { readonly type: "invoked" | "websocket_handled"; readonly sequence?: number }
    | FailedReply
    | EvictedReply
    | { readonly type: "hydrated" }
/** What follows a line: a string, or the text of a result or a socket message, which is already UTF-8. */
type SessionPart = string | JsonText
type DetachedReply = ReplyHead & { readonly parts?: readonly SessionPart[] }
type ActorSessionClientMessage =
    | { readonly type: "actor_call"; readonly call_id: number; readonly call: ActorCallRequest }
    | { readonly type: "residency"; readonly resident: boolean }
    | AttachMessage
    | { readonly type: "ready_for_invocation"; readonly message_id: number }
    | { readonly type: "get_connections"; readonly message_id: number; readonly query?: SocketQuery }
    | { readonly type: "log"; readonly message_id?: number; readonly level: LogLevel; readonly message: string }
    | { readonly type: "socket_effects"; readonly message_id: number }
    | { readonly type: "reply"; readonly message_id: number; readonly reply: ReplyHead }

interface AttachMessage {
    readonly type: "attach"
    readonly protocol: 33
    readonly actor_names: readonly string[]
}

interface ActorCallRequest {
    readonly request_id: string
    readonly actor_name: string
    readonly actor_id: string
    readonly method: string
    readonly args: readonly JsonValue[]
}

interface AssignedActor {
    readonly actor: ActorIdentity
}

interface InvokedReply {
    readonly type: "invoked"
    readonly sequence?: number
    /** The result as JSON. It is encoded once, before the commit, so a result that has no JSON fails the call. */
    readonly result: JsonText
    readonly effects?: readonly HostSocketEffect[]
}

interface WebSocketHandledReply {
    readonly type: "websocket_handled"
    readonly sequence?: number
    readonly effects: readonly HostSocketEffect[]
}

interface FailedReply {
    readonly type: "failed"
    readonly code: string
    readonly message: string
}

interface EvictedReply {
    readonly type: "evicted"
}

type LogLevel = "debug" | "info" | "warn" | "error"

interface ActorLog {
    /** The command that wrote the line. Absent for output from outside any command. */
    readonly messageId?: number
    readonly level: LogLevel
    readonly message: string
}

type WebSocketEventCommand = z.infer<typeof websocketEventCommandSchema> & AssignedActor

export { detachEffects, detachReply, failedReply, parseActorSessionServerMessage }
export type {
    ActorCallRequest,
    ActorLog,
    LogLevel,
    ActorExecutionCommand,
    ActorExecutorCommand,
    ActorExecutorReply,
    ActorSessionClientMessage,
    ActorSessionServerMessage,
    AssignedCommand,
    DetachedReply,
    EvictCommand,
    FailedReply,
    InvokeCommand,
    SessionPart,
    WebSocketEventCommand
}
