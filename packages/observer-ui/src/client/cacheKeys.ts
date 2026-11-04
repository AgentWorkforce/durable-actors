// Every key carries the base URL so observers for different projects never share cached data.

export const actorsKey = (baseUrl: string) => ["actors", baseUrl] as const

export const actorEventsKey = (baseUrl: string) => ["actors/events", baseUrl] as const

export const requestsKey = (baseUrl: string) => ["requests", baseUrl] as const

export const requestEventsKey = (baseUrl: string, durationMs: number) => ["requests/events", baseUrl, durationMs] as const

export const residencyKey = (baseUrl: string, actorName: string) => ["residency", baseUrl, actorName] as const

export const webSocketsKey = (baseUrl: string) => ["websockets", baseUrl] as const
