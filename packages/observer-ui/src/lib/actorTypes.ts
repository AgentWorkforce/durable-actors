import type { ActorInstance, ActorType } from "../types/Actors.js"

export type ActorTypeSummary = {
    actorName: string
    total: number
    live: number
    dormant: number
}

// Relative so the routes work both standalone and nested under a host app's route.
export const actorTypePath = (actorName: string) => `actors/${encodeURIComponent(actorName)}`
export const instancePath = (actorId: string) => `instances/${encodeURIComponent(actorId)}`

export function summarizeActorType({ actorName, instances }: ActorType): ActorTypeSummary {
    return {
        actorName,
        total: instances.length,
        live: instances.filter(instance => instance.status === "live").length,
        dormant: instances.filter(instance => instance.status === "dormant").length
    }
}

export function byLastActive(instances: ActorInstance[]): ActorInstance[] {
    return [...instances].sort((a, b) => (b.lastActiveAtMs ?? 0) - (a.lastActiveAtMs ?? 0))
}
