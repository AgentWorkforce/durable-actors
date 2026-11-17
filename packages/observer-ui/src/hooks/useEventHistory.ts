import { useData } from "../client/DatatContext.js"
import { eventHistoryKey } from "../client/cacheKeys.js"
import { HttpObserverClient } from "../client/observerClient.js"
import { currentBounds, windowKey } from "../lib/timeRange.js"
import { applyRecordedEvents } from "../lib/views/eventHistory.js"
import type { RecordedEvent } from "../types/Events.js"
import type { TimeWindow } from "../types/TimeRange.js"

import { useLiveView } from "./useLiveView.js"

/** The project's past events of these types, or only one class's or instance's when named. */
export function useEventHistory(types: RecordedEvent["type"][], timeWindow: TimeWindow, actorName?: string, actorId?: string) {
    const { baseUrl, events } = useData()
    const { data, error, isLoading } = useLiveView(
        [...eventHistoryKey(baseUrl, types, actorName, actorId), ...windowKey(timeWindow)],
        () => new HttpObserverClient(baseUrl).listEventHistory(types, currentBounds(timeWindow), actorName, actorId),
        applyRecordedEvents(types, actorName, actorId),
        timeWindow.live ? events : null
    )
    return {
        history: data ?? null,
        isLoading,
        isError: !data && Boolean(error),
        error
    }
}
