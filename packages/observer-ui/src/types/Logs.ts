export type LogLevel = "debug" | "info" | "warn" | "error"

export interface ActorLog {
    logId: string
    actorName: string
    actorId: string
    requestId: string | null
    timestampMs: number
    level: LogLevel
    message: string
}

export interface ActorLogPage {
    records: ActorLog[]
    nextCursor: string | null
}

export type LevelCounts = Record<LogLevel, number>

export interface LogHistogram {
    fromMs: number
    bucketMs: number
    buckets: LevelCounts[]
}

export interface LogFilter {
    actorName: string
    actorId: string
    requestId: string | null
    level: LogLevel | null
    search: string
}
