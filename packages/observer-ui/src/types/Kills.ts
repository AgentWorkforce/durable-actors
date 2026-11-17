export type KillReason = "out_of_memory" | "out_of_disk" | "evicted" | "killed"

/** A sandbox that stopped under an actor without the platform asking it to. */
export interface ActorKill {
    actorName: string
    actorId: string
    atMs: number
    reason: KillReason
}
