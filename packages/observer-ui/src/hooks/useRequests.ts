import { requestHistogramKey } from "../client/cacheKeys.js"
import type { EventBus } from "../client/events.js"
import { HttpObserverClient } from "../client/observerClient.js"
import { emptyRequestCounts } from "../lib/requestStats.js"
import { type RequestFilter, applyRequestCounts } from "../lib/views/requests.js"
import type { TimeWindow } from "../types/TimeRange.js"

import { useLiveHistogram } from "./useLiveHistogram.js"

/** Request counts per window step, from one store query rather than every request in the window. */
export function useRequestHistogram(baseUrl: string, timeWindow: TimeWindow, filter: RequestFilter, events: EventBus | null) {
    const client = new HttpObserverClient(baseUrl)
    return useLiveHistogram(
        { key: requestHistogramKey(baseUrl, filter), read: (bounds, bucketMs) => client.getRequestHistogram(filter, bounds, bucketMs), apply: applyRequestCounts(filter), empty: emptyRequestCounts },
        timeWindow,
        timeWindow.stepMs,
        events
    )
}
