export interface ActorType {
    actorName: string
    methods?: string[]
    live: number
    dormant: number
    unknown: number
    instances: ActorInstance[]
}

export interface ActorInventory {
    actors: ActorType[]
}

export type ActorResidency = "live" | "dormant" | "unknown"

export interface ActorInstance {
    actorId: string
    status: ActorResidency
    lastActiveAtMs?: number | null
    connections: ActorConnection[]
    waiting?: { id: string; operation: string }[] | null
}

interface ActorConnection {
    id: string
    metadata: unknown
}
