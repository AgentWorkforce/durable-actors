import { useData } from "../client/DatatContext.js"
import { residencyKey } from "../client/cacheKeys.js"
import { HttpObserverClient } from "../client/observerClient.js"
import { currentBounds, windowKey } from "../lib/timeRange.js"
import { applyResidencyEvents } from "../lib/views/residency.js"
import type { TimeWindow } from "../types/TimeRange.js"

import { useLiveView } from "./useLiveView.js"

/** One instance's residency when `actorId` is set, otherwise the whole class's. */
export function useResidency(actorName: string, timeWindow: TimeWindow, actorId?: string) {
    const { baseUrl, events } = useData()
    const { data, error, isLoading } = useLiveView(
        [...residencyKey(baseUrl, actorName, actorId), ...windowKey(timeWindow)],
        () => new HttpObserverClient(baseUrl).listResidency(actorName, currentBounds(timeWindow), actorId),
        applyResidencyEvents(actorName, actorId),
        timeWindow.live ? events : null
    )
    return {
        residency: data ?? null,
        isLoading,
        isError: !data && Boolean(error),
        error
    }
}
