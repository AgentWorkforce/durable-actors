import { z } from "zod"

import { cloneJson, serializeJson } from "./client-runtime/json.js"
import { ActorSerializationError } from "./errors.js"

type JsonPrimitive = string | number | boolean | null
type JsonObject = { readonly [key: string]: JsonValue }
type JsonValue = JsonPrimitive | JsonObject | readonly JsonValue[]

const NOT_JSON = "a JSON value is null, a boolean, a string, a finite number, or an array or plain object of these"

function cloneJsonObject(value: unknown, label: string): JsonObject {
    const cloned = cloneJson(value, label)
    if (!isJsonObject(cloned)) throw new ActorSerializationError(`${label} must be a JSON object`)
    return cloned
}

function isJsonObject(value: JsonValue): value is JsonObject {
    return typeof value === "object" && value !== null && !Array.isArray(value)
}

// Not a schema that refers to itself: zod keeps the last value such a schema parsed until it parses another, so a
// 12 MB message stayed in memory long after it was sent.
const jsonValueSchema: z.ZodType<JsonValue> = z.transform((value, context) => {
    try {
        return copyJsonValue(value)
    } catch (error) {
        context.issues.push({ code: "custom", input: value, message: (error as Error).message })
        return z.NEVER
    }
})

/**
 * A copy of a value that is JSON as it stands: null, a boolean, a string, a finite number, or an array or plain
 * object of these. Unlike `cloneJson`, nothing is converted or left out: any other value throws.
 */
function copyJsonValue(value: unknown, containers = new Set<object>()): JsonValue {
    if (value === null || typeof value === "string" || typeof value === "boolean") return value
    if (typeof value === "number" && Number.isFinite(value)) return value
    if (!Array.isArray(value) && !z.util.isPlainObject(value)) throw new TypeError(NOT_JSON)
    if (containers.has(value)) throw new TypeError("a JSON value cannot contain itself")
    containers.add(value)
    const copy = Array.isArray(value)
        ? Array.from(value, member => copyJsonValue(member, containers))
        : copyMembers(value, containers)
    containers.delete(value)
    return copy
}

function copyMembers(value: Record<PropertyKey, unknown>, containers: Set<object>): JsonObject {
    const copy: Record<string, JsonValue> = {}
    for (const key of Reflect.ownKeys(value)) {
        // Assigning `__proto__` would replace the copy's prototype rather than add a member.
        if (key === "__proto__" || !Object.prototype.propertyIsEnumerable.call(value, key)) continue
        if (typeof key === "symbol") throw new TypeError(NOT_JSON)
        copy[key] = copyJsonValue(value[key], containers)
    }
    return copy
}

export { cloneJson, cloneJsonObject, copyJsonValue, isJsonObject, jsonValueSchema, serializeJson }
export type { JsonPrimitive, JsonObject, JsonValue }
