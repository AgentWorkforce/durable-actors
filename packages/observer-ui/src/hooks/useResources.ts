import { useData } from "../client/DatatContext.js"
import { resourcesKey } from "../client/cacheKeys.js"
import { HttpObserverClient } from "../client/observerClient.js"
import { currentBounds, windowKey } from "../lib/timeRange.js"
import { applyResourceEvents } from "../lib/views/resources.js"
import type { TimeWindow } from "../types/TimeRange.js"

import { useLiveView } from "./useLiveView.js"

/** One instance's CPU, memory and database size, in the window's buckets. */
export function useResources(actorName: string, actorId: string, timeWindow: TimeWindow) {
    const { baseUrl, events } = useData()
    const { data, error } = useLiveView(
        [...resourcesKey(baseUrl, actorName, actorId), ...windowKey(timeWindow)],
        () => new HttpObserverClient(baseUrl).getResources(actorName, actorId, { ...currentBounds(timeWindow), fromMs: timeWindow.fromMs }, timeWindow.stepMs),
        applyResourceEvents(actorName, actorId),
        timeWindow.live ? events : null
    )
    return { resources: data ?? null, isError: !data && Boolean(error) }
}
