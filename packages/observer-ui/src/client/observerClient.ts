import type { ActorInventory } from "../types/Actors.js"
import type { ActorLogPage, LogFilter, LogHistogram } from "../types/Logs.js"
import type { RequestHistoryQuery, RequestTrace, RequestTracePage } from "../types/Requests.js"
import type { ResidencyHistory } from "../types/Residency.js"
import type { SocketSession } from "../types/Sockets.js"
import type { TimeBounds } from "../types/TimeRange.js"

const requestPageSize = 500
const maxRequestPages = 20

export const defaultBaseUrl = "/api/observe"

const logPageSize = 200

type totalActorInstancesPayload = Map<string, ActorInventory>

export interface ObserverClient {
    listActorTypes(): Promise<ActorInventory>
    listRequests(query: RequestHistoryQuery): Promise<RequestTracePage>
    listResidency(actorName: string, bounds: TimeBounds): Promise<ResidencyHistory>
    listWebSockets(): Promise<SocketSession[]>
    listLogs(filter: LogFilter, bounds: TimeBounds, cursor: string | null): Promise<ActorLogPage>
    getLogHistogram(filter: LogFilter, bounds: TimeBounds, bucketMs: number): Promise<LogHistogram>
}

export class HttpObserverClient implements ObserverClient {
    constructor(private readonly baseUrl: string = defaultBaseUrl) {}

    async listActorTypes(): Promise<ActorInventory> {
        return await this.get<ActorInventory>("actors")
    }

    async listRequests({ fromMs, toMs, limit = requestPageSize, cursor, actorName, actorId }: RequestHistoryQuery): Promise<RequestTracePage> {
        const query = new URLSearchParams({ fromMs: String(fromMs), toMs: String(toMs), limit: String(limit) })

        if (cursor) query.set("cursor", cursor)
        if (actorName) query.set("actorName", actorName)
        if (actorId) query.set("actorId", actorId)

        return await this.get<RequestTracePage>(`requests?${query}`)
    }

    async listResidency(actorName: string, { fromMs, toMs }: TimeBounds): Promise<ResidencyHistory> {
        const query = new URLSearchParams({ actorName, fromMs: String(fromMs), toMs: String(toMs) })
        return await this.get<ResidencyHistory>(`residency?${query}`)
    }

    async listWebSockets(): Promise<SocketSession[]> {
        return await this.get<SocketSession[]>("websockets")
    }

    async listLogs(filter: LogFilter, { fromMs, toMs }: TimeBounds, cursor: string | null): Promise<ActorLogPage> {
        const query = logFilterParams(filter)
        query.set("fromMs", String(fromMs))
        query.set("toMs", String(toMs))
        query.set("limit", String(logPageSize))
        if (cursor) query.set("cursor", cursor)
        return await this.get<ActorLogPage>(`logs?${query}`)
    }

    // Counts every level so the level control can show what each choice would keep.
    async getLogHistogram(filter: LogFilter, { fromMs, toMs }: TimeBounds, bucketMs: number): Promise<LogHistogram> {
        const query = logFilterParams({ ...filter, level: null })
        query.set("fromMs", String(fromMs))
        query.set("toMs", String(toMs))
        query.set("bucketMs", String(bucketMs))
        return await this.get<LogHistogram>(`logs/histogram?${query}`)
    }

    async collectRequests(bounds: TimeBounds, filter: { actorName?: string; actorId?: string } = {}): Promise<RequestTrace[]> {
        const records: RequestTrace[] = []
        let cursor: string | undefined
        for (let page = 0; page < maxRequestPages; page++) {
            const result = await this.listRequests({ ...bounds, ...filter, cursor })
            records.push(...result.records)
            if (!result.nextCursor || result.nextCursor === cursor || result.records.length === 0) break
            cursor = result.nextCursor
        }
        return records
    }

    private async get<T>(path: string): Promise<T> {
        const response = await fetch(`${this.baseUrl.replace(/\/$/u, "")}/${path}`, {
            method: "GET",
            // Hosts may serve the API from another origin and authenticate with cookies.
            credentials: "include",
            redirect: "error",
            headers: { accept: "application/json" }
        })

        if (!response.ok) {
            throw new Error(`Connection check failed (HTTP ${response.status}).`)
        }

        return (await response.json()) as T
    }
}

export function logFilterParams({ actorName, actorId, requestId, level, search }: LogFilter): URLSearchParams {
    const query = new URLSearchParams({ actorName, actorId })
    if (requestId) query.set("requestId", requestId)
    if (level) query.set("level", level)
    if (search) query.set("search", search)
    return query
}
