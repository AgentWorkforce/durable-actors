import { ActorSerializationError } from "./errors.js"

type JsonPrimitive = string | number | boolean | null
type JsonObject = { readonly [key: string]: JsonValue }
type JsonValue = JsonPrimitive | JsonObject | readonly JsonValue[]

function cloneJson(value: unknown, label: string): JsonValue {
    return JSON.parse(serializeJson(value, label)) as JsonValue
}

function serializeJson(value: unknown, label: string): string {
    try {
        const encoded = JSON.stringify(value)
        if (encoded === undefined) throw new ActorSerializationError(`${label} must be JSON serializable`)
        return encoded
    } catch (error) {
        if (error instanceof ActorSerializationError) throw error
        throw new ActorSerializationError(`${label} must be JSON serializable`, { cause: error })
    }
}

export { cloneJson, serializeJson }
export type { JsonValue }
