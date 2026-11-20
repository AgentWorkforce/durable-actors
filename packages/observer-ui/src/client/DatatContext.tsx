import { type ReactNode, createContext, useContext } from "react"

import { useActors } from "../hooks/useActors.js"
import { useEventStream } from "../hooks/useLiveView.js"
import { useNow } from "../hooks/useNow.js"
import { useRequestHistogram } from "../hooks/useRequests.js"
import { useWebSockets } from "../hooks/useWebSockets.js"
import { currentInventory } from "../lib/views/actors.js"
import { currentSessions } from "../lib/views/sockets.js"
import type { ActorInventory } from "../types/Actors.js"
import type { RequestHistogram } from "../types/Requests.js"
import type { SocketSession } from "../types/Sockets.js"

import { useTimeWindow } from "./TimeRangeContext.js"
import type { EventBus } from "./events.js"
import { defaultBaseUrl } from "./observerClient.js"

// Statuses that change without an event (unknown, lost) are worked out on this tick.
const statusTickMs = 5_000

const projectEvents = new URLSearchParams({ signals: "requests,platform" })

type DataContextType = {
    baseUrl: string
    events: EventBus | null
    actorTypes: ActorInventory | null
    isError: boolean
    requestHistogram: RequestHistogram | undefined
    isRequestsError: boolean
    sessions: SocketSession[] | null
    isSocketsError: boolean
    /** The time current statuses are worked out at: the clock while live, held while paused. */
    nowMs: number
}

const DataContext = createContext<DataContextType | undefined>(undefined)

export const DataProvider = ({ baseUrl = defaultBaseUrl, children }: { baseUrl?: string; children: ReactNode }) => {
    const timeWindow = useTimeWindow()
    // Pausing closes the stream, so every view keeps what it showed until live updates resume.
    const events = useEventStream(baseUrl, timeWindow.live ? projectEvents : null)
    const now = useNow(timeWindow.live ? statusTickMs : null)
    const { inventory, isError } = useActors(baseUrl, events)
    const { histogram: requestHistogram, isError: isRequestsError } = useRequestHistogram(baseUrl, timeWindow, {}, events)
    const { sessions, isError: isSocketsError } = useWebSockets(baseUrl, events)

    return (
        <DataContext.Provider
            value={{
                baseUrl,
                events,
                actorTypes: inventory && currentInventory(inventory, now),
                isError,
                requestHistogram,
                isRequestsError,
                sessions: sessions && currentSessions(sessions, inventory?.gateways ?? {}, now),
                isSocketsError,
                nowMs: now
            }}
        >
            {children}
        </DataContext.Provider>
    )
}

export const useData = (): DataContextType => {
    const context = useContext(DataContext)
    if (!context) {
        throw new Error("useData must be used within a DataProvider")
    }
    return context
}
