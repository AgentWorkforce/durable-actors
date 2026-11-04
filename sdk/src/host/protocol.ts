import { z } from "zod"

import type { ActorIdentity } from "../actor/identity.js"
import { actorComponentSchema, actorIdentitySchema } from "../actor/identity.js"
import type { SocketEffect, SocketLookup, SocketQuery } from "../actor/socketProtocol.js"
import { socketConnectionSchema, socketEventSchema } from "../actor/socketProtocol.js"
import { ActorProtocolError } from "../errors.js"
import type { JsonValue } from "../json.js"
import { jsonValueSchema } from "../json.js"

import type { DatabaseRequest } from "./database-bridge.js"
import type { DatabaseCall } from "./database.js"

function parseActorSessionServerMessage(document: string): ActorSessionServerMessage {
    let value: unknown
    try {
        value = JSON.parse(document)
    } catch (error) {
        throw new ActorProtocolError("actor session message is not valid JSON", { cause: error })
    }
    const result = actorSessionServerMessageSchema.safeParse(value)
    if (!result.success) throw new ActorProtocolError(`actor session message is invalid: ${result.error.message}`)
    return result.data
}

function failedReply(code: string, message: string): FailedReply {
    return { type: "failed", code, message }
}

const databaseValueSchema = z.discriminatedUnion("type", [
    z.object({ type: z.literal("null") }),
    z.object({ type: z.literal("integer"), value: z.string().regex(/^-?\d+$/) }),
    z.object({ type: z.literal("real"), value: z.number() }),
    z.object({ type: z.literal("text"), value: z.string() }),
    z.object({ type: z.literal("blob"), value: z.base64() })
])

const databaseResultSchema = z.discriminatedUnion("type", [
    z.object({ type: z.literal("rows"), columns: z.array(z.string()), rows: z.array(z.array(databaseValueSchema)) }),
    z.object({ type: z.literal("fields"), fields: z.record(z.string(), jsonValueSchema) }),
    z.object({
        type: z.literal("alarm"),
        timestamp_ms: z.number().int().safe().nullable()
    }),
    z.object({ type: z.literal("done") })
])

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
    z
        .object({
            type: z.literal("database_result"),
            call_id: z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
            result: databaseResultSchema.optional(),
            error: z.string().optional()
        })
        .refine(message => (message.result === undefined) !== (message.error === undefined)),
    z.object({ type: z.literal("attached"), protocol: z.literal(31), actor: actorIdentitySchema }),
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
type ActorWorkerCommand = ActorExecutorCommand & AssignedActor
type ActorSessionServerMessage = z.infer<typeof actorSessionServerMessageSchema>
type ActorExecutorReply =
    InvokedReply | WebSocketHandledReply | FailedReply | EvictedReply | { readonly type: "hydrated" }
type ActorSessionClientMessage =
    | {
          readonly type: "database_call"
          readonly call_id: number
          readonly message_id?: number
          readonly call: DatabaseCall
      }
    | { readonly type: "actor_call"; readonly call_id: number; readonly call: ActorCallRequest }
    | { readonly type: "residency"; readonly resident: boolean }
    | AttachMessage
    | { readonly type: "ready_for_invocation"; readonly message_id: number }
    | ReplyMessage
    | { readonly type: "get_connections"; readonly message_id: number; readonly query?: SocketQuery }
    | { readonly type: "socket_effects"; readonly message_id: number; readonly effects: readonly SocketEffect[] }

interface AttachMessage {
    readonly type: "attach"
    readonly protocol: 31
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

interface ReplyMessage {
    readonly type: "reply"
    readonly message_id: number
    readonly reply: ActorExecutorReply
}

interface InvokedReply {
    readonly type: "invoked"
    readonly sequence?: number
    readonly result: JsonValue
    readonly effects?: readonly SocketEffect[]
}

interface WebSocketHandledReply {
    readonly type: "websocket_handled"
    readonly sequence?: number
    readonly effects: readonly SocketEffect[]
}

interface FailedReply {
    readonly type: "failed"
    readonly code: string
    readonly message: string
}

interface EvictedReply {
    readonly type: "evicted"
}

interface ActorWorkerData {
    readonly moduleUrl: string
    readonly environment?: Readonly<Record<string, string>>
}

type ActorWorkerRequest =
    | { readonly type: "load"; readonly data: ActorWorkerData }
    | {
          readonly type: "execute"
          readonly messageId: number
          readonly command: ActorWorkerCommand
      }
    | {
          readonly type: "socket_connections"
          readonly messageId: number
          readonly connections: SocketLookup
          readonly error?: string
      }
    | { readonly type: "actor_result"; readonly callId: number; readonly outcome: JsonValue }
type ActorWorkerMessage =
    | {
          readonly type: "database_call"
          readonly messageId?: number
          readonly call: DatabaseCall
          readonly request: DatabaseRequest
      }
    | { readonly type: "actor_call"; readonly callId: number; readonly call: ActorCallRequest }
    | { readonly type: "warm" }
    | {
          readonly type: "ready"
          readonly actorNames: readonly string[]
      }
    | { readonly type: "reply"; readonly messageId: number; readonly reply: ActorExecutorReply }
    | FailedReply
    | { readonly type: "ready_for_invocation"; readonly messageId: number }
    | { readonly type: "socket_effects"; readonly messageId: number; readonly effects: readonly SocketEffect[] }
    | { readonly type: "get_connections"; readonly messageId: number; readonly query?: SocketQuery }

type WebSocketEventCommand = z.infer<typeof websocketEventCommandSchema> & AssignedActor
export { failedReply, parseActorSessionServerMessage }
export type {
    ActorCallRequest,
    ActorExecutionCommand,
    ActorExecutorCommand,
    ActorExecutorReply,
    ActorSessionClientMessage,
    ActorSessionServerMessage,
    ActorWorkerData,
    ActorWorkerCommand,
    ActorWorkerMessage,
    ActorWorkerRequest,
    EvictCommand,
    InvokeCommand,
    WebSocketEventCommand
}
