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
    nextCursor?: string | null
    resumeCursor: string
    snapshot?: boolean
}

export type RequestHistoryQuery = {
    fromMs: number
    toMs: number
    limit?: number
    cursor?: string
    actorName?: string
    actorId?: string
}
