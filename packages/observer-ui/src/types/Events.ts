import type { ActorResidency, WaitingOperation } from "./Actors.js"
import type { ActorKill } from "./Kills.js"
import type { ActorLog } from "./Logs.js"
import type { RequestTrace } from "./Requests.js"

/** An instance as its latest lifecycle report leaves it. */
export interface InstanceChange {
    actorName: string
    actorId: string
    status: ActorResidency
    occurredAtMs: number
    reportedAtMs: number
    /** When the status turns unknown unless another report arrives; null once stopped. */
    staleAtMs: number | null
    lastActiveAtMs: number | null
    waiting: WaitingOperation[] | null
    /** The SQLite database size, sampled at most every 10 seconds; null when not yet read. */
    databaseBytes: number | null
}

/** One sample of the CPU and memory of the sandbox an instance runs in. */
export interface ActorUsage {
    actorName: string
    actorId: string
    atMs: number
    cpuCores: number | null
    memoryBytes: number | null
}

export interface ConnectionOpened {
    connectionId: string
    actorName: string
    actorId: string
    gatewayId: string
    openedAtMs: number
    metadata: unknown
}

export interface ConnectionClosed {
    connectionId: string
    actorName: string
    actorId: string
    gatewayId: string
    openedAtMs: number
    closedAtMs: number
    closeCode: number | null
    closeReason: string | null
    messagesIn: number | null
    messagesOut: number | null
}

export interface GatewayAlive {
    gatewayId: string
    atMs: number
}

/** The actor classes a deployment leaves deployed; empty once it is removed. */
export interface Deployment {
    atMs: number
    actors: { actorName: string; methods: string[] }[]
}

/** One change on the project's live channels, as `observe/events` sends it. */
export type ObserveEvent =
    | { type: "request"; item: RequestTrace }
    | { type: "log"; item: ActorLog }
    | { type: "instance"; item: InstanceChange }
    | { type: "connectionOpened"; item: ConnectionOpened }
    | { type: "connectionClosed"; item: ConnectionClosed }
    | { type: "gatewayAlive"; item: GatewayAlive }
    | { type: "deployment"; item: Deployment }
    | { type: "kill"; item: ActorKill }
    | { type: "usage"; item: ActorUsage }

export const observeEventTypes: ObserveEvent["type"][] = ["request", "log", "instance", "connectionOpened", "connectionClosed", "gatewayAlive", "deployment", "kill", "usage"]

/** An event `observe/events/history` keeps: one that happens once. The other types have their own views. */
export type RecordedEvent = Extract<ObserveEvent, { type: "kill" }>

/** Past events, newest first, each as the stream sent it. */
export interface EventHistory {
    truncated: boolean
    events: RecordedEvent[]
}
