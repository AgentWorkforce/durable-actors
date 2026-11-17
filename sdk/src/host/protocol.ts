import { z } from "zod"

import type { ActorIdentity } from "../actor/identity.js"
import { actorComponentSchema, actorIdentitySchema } from "../actor/identity.js"
import type { HostSocketEffect, SocketQuery } from "../actor/socketProtocol.js"
import { socketConnectionSchema, socketEventSchema } from "../actor/socketProtocol.js"
import { ActorProtocolError } from "../errors.js"
import type { JsonValue } from "../json.js"
import { jsonValueSchema, serializeJson } from "../json.js"
import type { JsonText } from "../payload/json-text.js"
import type { SessionFrame } from "../payload/session-frames.js"

function parseActorSessionServerMessage({ head, parts }: SessionFrame): ActorSessionServerMessage {
    const result = actorSessionServerMessageSchema.safeParse(head)
    if (!result.success) throw new ActorProtocolError(`actor session message is invalid: ${result.error.message}`)
    const message = attachArguments(result.data, parts)
    if (parts.length > 0) throw new ActorProtocolError("actor session message has a part left over")
    return message
}

// What can be large follows a command's line as a part, so it is never escaped into the line: an invocation's
// arguments. The part is taken out of the frame, so its text is garbage as soon as it is parsed.
function attachArguments(message: ActorSessionServerLine, parts: string[]): ActorSessionServerMessage {
    if (message.type !== "command") return message
    const { command } = message
    if (command.type !== "invoke") return { ...message, command }
    const text = parts.shift()
    if (text === undefined) throw new ActorProtocolError("an invocation's arguments are missing")
    return { ...message, command: { ...command, args: parseArguments(text) } }
}

function parseArguments(text: string): JsonValue[] {
    let args: unknown
    try {
        args = JSON.parse(text, withoutPrototypeMembers)
    } catch (error) {
        throw new ActorProtocolError("an invocation's arguments are not valid JSON", { cause: error })
    }
    if (!Array.isArray(args)) throw new ActorProtocolError("an invocation's arguments are not an array")
    return args
}

// Assigning a `__proto__` member to an object replaces the object's prototype rather than adding the member.
function withoutPrototypeMembers(key: string, value: unknown): unknown {
    return key === "__proto__" ? undefined : value
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
    method: actorComponentSchema
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
    z.object({ type: z.literal("attached"), protocol: z.literal(34), actor: actorIdentitySchema }),
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

type InvokeCommand = z.infer<typeof invokeCommandSchema> & InvocationArguments & AssignedActor
type EvictCommand = z.infer<typeof evictCommandSchema>
/** A command as its line has it, without the parts that follow. */
type CommandLine = z.infer<typeof executorCommandSchema>
type ActorExecutorCommand =
    Exclude<CommandLine, { type: "invoke" }> | (Extract<CommandLine, { type: "invoke" }> & InvocationArguments)
type ActorExecutionCommand = Exclude<ActorExecutorCommand, EvictCommand>
/** A command with the actor this process was assigned. */
type AssignedCommand = ActorExecutorCommand & AssignedActor
type ActorSessionServerLine = z.infer<typeof actorSessionServerMessageSchema>
type ActorSessionServerMessage =
    | Exclude<ActorSessionServerLine, { type: "command" }>
    | (Omit<Extract<ActorSessionServerLine, { type: "command" }>, "command"> & { command: ActorExecutorCommand })
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
    readonly protocol: 34
    readonly actor_names: readonly string[]
}

/** An invocation's arguments, which follow its command's line as a part. */
interface InvocationArguments {
    readonly args: JsonValue[]
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
