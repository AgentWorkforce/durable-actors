import type { ActorResidency } from "./Actors.js"

export interface ResidencyPeriod {
    actorId: string
    status: ActorResidency
    fromMs: number
    toMs: number
}

export interface ResidencyHistory {
    truncated: boolean
    periods: ResidencyPeriod[]
}
