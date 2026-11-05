import type { LogFilter } from "../types/Logs.js"

// Every key carries the base URL so observers for different projects never share cached data.

export const actorsKey = (baseUrl: string) => ["actors", baseUrl] as const

export const actorEventsKey = (baseUrl: string) => ["actors/events", baseUrl] as const

export const requestsKey = (baseUrl: string) => ["requests", baseUrl] as const

export const requestEventsKey = (baseUrl: string, durationMs: number) => ["requests/events", baseUrl, durationMs] as const

export const residencyKey = (baseUrl: string, actorName: string) => ["residency", baseUrl, actorName] as const

export const webSocketsKey = (baseUrl: string) => ["websockets", baseUrl] as const

export const logsKey = (baseUrl: string, { actorName, actorId, requestId, level, search }: LogFilter) => ["logs", baseUrl, actorName, actorId, requestId, level, search] as const

export const logHistogramKey = (baseUrl: string, { actorName, actorId, requestId, search }: LogFilter) => ["logs/histogram", baseUrl, actorName, actorId, requestId, search] as const

export const logEventsKey = (baseUrl: string, filter: LogFilter) => ["logs/events", ...logsKey(baseUrl, filter).slice(1)] as const
