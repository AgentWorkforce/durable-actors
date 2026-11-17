import type { ActorKill } from "../types/Kills.js"
import type { TimeWindow } from "../types/TimeRange.js"

/** The window's sandbox kills, in the order given: a history lists them newest first. */
export function killEvents(kills: ActorKill[], { fromMs }: TimeWindow): ActorKill[] {
    return kills.filter(kill => kill.atMs >= fromMs)
}
