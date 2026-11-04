import { type ReactNode, createContext, useContext } from "react"

import { useActors } from "../hooks/useActors.js"
import { useRequests } from "../hooks/useRequests.js"
import { useWebSockets } from "../hooks/useWebSockets.js"
import type { ActorInventory } from "../types/Actors.js"
import type { RequestTrace } from "../types/Requests.js"
import type { SocketSession } from "../types/Sockets.js"

import { useTimeWindow } from "./TimeRangeContext.js"
import { defaultBaseUrl } from "./observerClient.js"

type DataContextType = {
    baseUrl: string
    actorTypes: ActorInventory | null
    isError: boolean
    requests: RequestTrace[] | null
    isRequestsError: boolean
    sessions: SocketSession[] | null
    isSocketsError: boolean
}

const DataContext = createContext<DataContextType | undefined>(undefined)

export const DataProvider = ({ baseUrl = defaultBaseUrl, children }: { baseUrl?: string; children: ReactNode }) => {
    const { actorTypes, isError } = useActors(baseUrl)
    const { requests, isError: isRequestsError } = useRequests(baseUrl, useTimeWindow())
    const { sessions, isError: isSocketsError } = useWebSockets(baseUrl)

    return <DataContext.Provider value={{ baseUrl, actorTypes, isError, requests, isRequestsError, sessions, isSocketsError }}>{children}</DataContext.Provider>
}

export const useData = (): DataContextType => {
    const context = useContext(DataContext)
    if (!context) {
        throw new Error("useData must be used within a DataProvider")
    }
    return context
}
