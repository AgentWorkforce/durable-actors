import { useData } from "../client/DatatContext.js"
import { useTimeWindow } from "../client/TimeRangeContext.js"
import { requestsKey } from "../client/cacheKeys.js"
import { HttpObserverClient } from "../client/observerClient.js"
import type { RequestTrace } from "../types/Requests.js"

import { useWindowedSWR } from "./useWindowedSWR.js"

const liveRefreshMs = 15_000

type RequestFilter = { actorName: string; actorId?: string }

// The shared request sample is capped project-wide, so busy actors can fall out of it.
// Actor pages fetch their own history and top it up with streamed traces between polls.
export function useActorTypeRequests(actorName: string) {
    return useFilteredRequests({ actorName })
}

export function useInstanceRequests(actorName: string, actorId: string) {
    return useFilteredRequests({ actorName, actorId })
}

function useFilteredRequests(filter: RequestFilter) {
    const { baseUrl, requests: streamed } = useData()
    const timeWindow = useTimeWindow()
    const { data, error, isLoading } = useWindowedSWR(
        [...requestsKey(baseUrl), filter.actorName, filter.actorId],
        timeWindow,
        bounds => new HttpObserverClient(baseUrl).collectRequests(bounds, filter),
        { refreshInterval: timeWindow.live ? liveRefreshMs : 0 }
    )
    const live = streamed?.filter(trace => matches(trace, filter)) ?? []
    const requests = data ? uniqueByEvent([...data, ...live]) : null
    return {
        requests,
        isLoading,
        isError: !data && Boolean(error),
        error
    }
}

function matches(trace: RequestTrace, { actorName, actorId }: RequestFilter): boolean {
    return trace.actorName === actorName && (actorId === undefined || trace.actorId === actorId)
}

function uniqueByEvent(traces: RequestTrace[]): RequestTrace[] {
    return [...new Map(traces.map(trace => [trace.eventId, trace])).values()]
}
