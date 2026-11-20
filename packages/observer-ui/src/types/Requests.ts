import type { Histogram } from "./Histogram.js"

export type RequestOutcome = "completed" | "failed" | "rejected" | "rerouted" | "interrupted"

export interface RequestTrace {
    sequence: number
    eventId: string
    hostId: string
    sessionId: string
    requestId: string
    actorName: string
    actorId: string
    kind: "method" | "websocket"
    operation: string
    connectionId: string | null
    startedAtMs: number
    routingMs: number
    durationMs: number
    queueWaitMs: number | null
    hostState: "cold" | "warm"
    outcome: RequestOutcome
}

export interface RequestTracePage {
    records: RequestTrace[]
    nextCursor: string | null
}

export type RequestHistoryQuery = {
    fromMs: number
    toMs: number
    limit?: number
    cursor?: string
    actorName?: string
    actorId?: string
}

/** Requests by outcome with summed timings; rerouted attempts count under their final outcome. */
export interface RequestCounts {
    ok: number
    failed: number
    rejected: number
    interrupted: number
    /** Summed queue waits of the requests that waited, and how many did. */
    queueWaitMs: number
    queueWaits: number
    /** Summed time excluding the queue wait. */
    runMs: number
}

export type RequestHistogram = Histogram<RequestCounts>
