export interface SocketSession {
    connectionId: string
    actorName: string
    actorId: string
    hostId: string | null
    openedAtMs: number | null
    closedAtMs: number | null
    lastSeenMs: number | null
    messages: number
    failures: number
    metadata?: unknown
}
