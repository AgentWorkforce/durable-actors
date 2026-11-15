import { z } from "zod"

import { jsonValueSchema } from "../json.js"
import type { JsonObject, JsonValue } from "../json.js"
import type { JsonText } from "../payload/json-text.js"

import { actorComponentSchema } from "./identity.js"

const socketConnectionIdSchema = actorComponentSchema.max(128)
const socketMetadataSchema = jsonValueSchema.refine(
    value => Buffer.byteLength(JSON.stringify(value)) <= 16 * 1024,
    "socket metadata must not exceed 16 KiB"
)
const socketTagSchema = z
    .string()
    .min(1)
    .refine(tag => [...tag].length <= 256, "socket tag must not exceed 256 characters")
const socketTagsSchema = z.array(socketTagSchema).max(10)
const socketTextSchema = z.string()
const socketBinarySchema = z
    .string()
    .regex(/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/u, "socket binary message must be base64")

const socketConnectionSchema = z.object({
    id: socketConnectionIdSchema,
    metadata: socketMetadataSchema,
    tags: socketTagsSchema
})

const socketMessageSchema = z.discriminatedUnion("type", [
    z.object({ type: z.literal("text"), data: socketTextSchema }),
    z.object({ type: z.literal("binary"), data: socketBinarySchema })
])

const socketEventSchema = z.discriminatedUnion("type", [
    z.object({ type: z.literal("connect"), connection: socketConnectionSchema }),
    z.object({ type: z.literal("message"), connection_id: actorComponentSchema, message: socketMessageSchema }),
    z.object({
        type: z.literal("disconnect"),
        connection: socketConnectionSchema,
        code: z.number().int().min(0).max(65535),
        reason: z.string(),
        was_clean: z.boolean()
    })
])

type SocketConnection = z.infer<typeof socketConnectionSchema>
/** `Text` is how the text of a message is held: a string, or in an actor host the UTF-8 it was encoded to. */
type SocketMessage<Text = string> =
    { readonly type: "text"; readonly data: Text } | { readonly type: "binary"; readonly data: string }
type SocketQuery = { readonly tag?: string; readonly countOnly?: boolean }
type SocketLookup = readonly SocketConnection[] | number
type SocketSource = (query?: SocketQuery) => Promise<SocketLookup>

type SocketEffect<Text = string> =
    | { readonly type: "set_auto_response"; readonly request: string | null; readonly response: string | null }
    | {
          readonly type: "state_snapshot"
          readonly connection_id: string
          readonly state: JsonObject
          readonly version?: number
      }
    | {
          readonly type: "state_update"
          readonly changes: JsonObject
          readonly removed: readonly string[]
          readonly except_connection_ids?: readonly string[]
          readonly version?: number
      }
    | { readonly type: "send"; readonly connection_id: string; readonly message: SocketMessage<Text> }
    | {
          readonly type: "broadcast"
          readonly message: SocketMessage<Text>
          readonly except_connection_ids: readonly string[]
          readonly tags: readonly string[]
          readonly tag_match?: "all" | "any"
      }
    | {
          readonly type: "close" | "reject"
          readonly connection_id: string
          readonly code: number
          readonly reason: string
      }
    | { readonly type: "set_metadata"; readonly connection_id: string; readonly metadata: JsonValue }
    | { readonly type: "set_tags"; readonly connection_id: string; readonly tags: readonly string[] }

/** An effect as an actor host holds it, from the actor's call until the Rust host has it. */
type HostSocketEffect = SocketEffect<JsonText>

export { socketConnectionSchema, socketEventSchema, socketTagsSchema }
export type { HostSocketEffect, SocketConnection, SocketEffect, SocketMessage }

export type { SocketQuery, SocketLookup, SocketSource }
