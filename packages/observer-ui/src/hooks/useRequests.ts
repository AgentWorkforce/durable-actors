import useSWRSubscription, { type SWRSubscriptionOptions } from "swr/subscription"

import { requestEventsKey, requestsKey } from "../client/cacheKeys.js"
import { HttpObserverClient } from "../client/observerClient.js"
import { subscribeToRequests } from "../client/streams/requestStream.js"
import type { RequestTrace } from "../types/Requests.js"
import type { TimeWindow } from "../types/TimeRange.js"

import { useWindowedSWR } from "./useWindowedSWR.js"

export function useRequests(baseUrl: string, timeWindow: TimeWindow) {
    const { live, fromMs, toMs } = timeWindow
    const durationMs = toMs - fromMs
    const { data, error, isLoading } = useWindowedSWR(requestsKey(baseUrl), timeWindow, bounds => new HttpObserverClient(baseUrl).collectRequests(bounds))

    const { data: streamed } = useSWRSubscription(live ? requestEventsKey(baseUrl, durationMs) : null, (_key, { next }: SWRSubscriptionOptions<RequestTrace[]>) =>
        subscribeToRequests(baseUrl, page => next(null, traces => mergeTraces(traces ?? [], page.records, durationMs)))
    )

    return {
        requests: live && (data || streamed) ? mergeTraces(data ?? [], streamed ?? [], durationMs) : (data ?? null),
        isLoading,
        isError: !streamed && Boolean(error),
        error
    }
}

// The stream sends incremental pages rather than full snapshots, so records accumulate until they leave the window.
function mergeTraces(traces: RequestTrace[], incoming: RequestTrace[], durationMs: number): RequestTrace[] {
    const since = Date.now() - durationMs
    const byEvent = new Map([...traces, ...incoming].map(trace => [trace.eventId, trace]))
    return [...byEvent.values()].filter(trace => trace.startedAtMs >= since)
}
