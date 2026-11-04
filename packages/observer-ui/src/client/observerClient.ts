import type { ActorInventory } from "../types/Actors.js"
import type { RequestTracePage } from "../types/Requests.js"
import type { ResidencyHistory } from "../types/Residency.js"
import type { SocketSession } from "../types/Sockets.js"
import type { TimeBounds } from "../types/TimeRange.js"

export const defaultBaseUrl = "/api/observe"

type totalActorInstancesPayload = Map<string, ActorInventory>

export interface ObserverClient {
    listActorTypes(): Promise<ActorInventory>
    listRequests(bounds: TimeBounds): Promise<RequestTracePage>
    listResidency(actorName: string, bounds: TimeBounds): Promise<ResidencyHistory>
    listWebSockets(): Promise<SocketSession[]>
}

export class HttpObserverClient implements ObserverClient {
    constructor(private readonly baseUrl: string = defaultBaseUrl) {}

    async listActorTypes(): Promise<ActorInventory> {
        return await this.get<ActorInventory>("actors")
    }

    async listRequests({ fromMs, toMs }: TimeBounds): Promise<RequestTracePage> {
        const query = new URLSearchParams({ fromMs: String(fromMs), toMs: String(toMs), limit: "500" })
        return await this.get<RequestTracePage>(`requests?${query}`)
    }

    async listResidency(actorName: string, { fromMs, toMs }: TimeBounds): Promise<ResidencyHistory> {
        const query = new URLSearchParams({ actorName, fromMs: String(fromMs), toMs: String(toMs) })
        return await this.get<ResidencyHistory>(`residency?${query}`)
    }

    async listWebSockets(): Promise<SocketSession[]> {
        return await this.get<SocketSession[]>("websockets")
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
