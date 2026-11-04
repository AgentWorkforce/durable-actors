import useSWR from "swr"

import { webSocketsKey } from "../client/cacheKeys.js"
import { HttpObserverClient } from "../client/observerClient.js"
import type { SocketSession } from "../types/Sockets.js"

// There is no WebSocket event stream; live open connections come from the actor inventory stream instead.
const refreshIntervalMs = 15_000

export function useWebSockets(baseUrl: string) {
    const { data, error, isLoading } = useSWR<SocketSession[]>(webSocketsKey(baseUrl), () => new HttpObserverClient(baseUrl).listWebSockets(), { refreshInterval: refreshIntervalMs })

    return {
        sessions: data ?? null,
        isLoading,
        isError: !data && Boolean(error),
        error
    }
}
