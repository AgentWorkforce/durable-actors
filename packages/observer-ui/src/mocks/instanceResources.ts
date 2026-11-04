import type { ActorInstance } from "../types/Actors.js"
import type { InstanceResources } from "../types/Resources.js"

const megabyte = 1_000_000
const days = 7

export function mockInstanceResources({ actorId, status }: ActorInstance): InstanceResources {
    const seed = hash(actorId)
    const bytes = (4 + (seed % 400) / 10) * megabyte
    return {
        storage: {
            bytes,
            tables: 2 + (seed % 7),
            rows: 1_000 + (seed % 50_000),
            dailyBytes: Array.from({ length: days }, (_, day) => bytes * (0.6 + (0.4 * (day + 1)) / days))
        },
        memory: status === "live" ? { bytes: (12 + (seed % 90)) * megabyte, limitBytes: 128 * megabyte } : null
    }
}

function hash(value: string): number {
    let result = 0
    for (const char of value) result = (result * 31 + char.charCodeAt(0)) >>> 0
    return result
}
