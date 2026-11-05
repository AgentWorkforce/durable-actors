import useSWR from "swr"
import useSWRInfinite from "swr/infinite"
import useSWRSubscription, { type SWRSubscriptionOptions } from "swr/subscription"

import { logEventsKey, logHistogramKey, logsKey } from "../client/cacheKeys.js"
import { HttpObserverClient } from "../client/observerClient.js"
import { subscribeToLogs } from "../client/streams/logStream.js"
import { alignBounds, chartBucketMs } from "../lib/logs.js"
import type { ActorLog, ActorLogPage, LogFilter } from "../types/Logs.js"
import type { TimeWindow } from "../types/TimeRange.js"

import { currentBounds, windowKey } from "./useWindowedSWR.js"

const liveRefreshMs = 5_000
const streamedLimit = 2_000

export function useLogs(baseUrl: string, timeWindow: TimeWindow, filter: LogFilter) {
    const history = useLogHistory(baseUrl, timeWindow, filter)
    const streamed = useLogStream(baseUrl, timeWindow.live, filter)
    const { data: histogram } = useLogHistogram(baseUrl, timeWindow, filter)
    const pages = history.data
    return {
        lines: pages ? newestFirst([...(streamed ?? []), ...pages.flatMap(page => page.records)]) : null,
        hasOlder: Boolean(pages?.at(-1)?.nextCursor),
        loadingOlder: Boolean(pages && history.size > pages.length),
        loadOlder: () => history.setSize(size => size + 1),
        histogram,
        isError: !pages && Boolean(history.error)
    }
}

function useLogHistory(baseUrl: string, timeWindow: TimeWindow, filter: LogFilter) {
    const client = new HttpObserverClient(baseUrl)
    return useSWRInfinite<ActorLogPage>(
        (index, previous: ActorLogPage | null) => (previous && !previous.nextCursor ? null : [...logsKey(baseUrl, filter), ...windowKey(timeWindow), index === 0 ? null : previous!.nextCursor]),
        (key: readonly unknown[]) => client.listLogs(filter, currentBounds(timeWindow), key.at(-1) as string | null),
        { revalidateFirstPage: false, revalidateOnFocus: false }
    )
}

function useLogStream(baseUrl: string, live: boolean, filter: LogFilter): ActorLog[] | undefined {
    const { data } = useSWRSubscription(live ? logEventsKey(baseUrl, filter) : null, (_key, { next }: SWRSubscriptionOptions<ActorLog[]>) =>
        subscribeToLogs(baseUrl, filter, records => next(null, lines => [...records, ...(lines ?? [])].slice(0, streamedLimit)))
    )
    return data
}

function useLogHistogram(baseUrl: string, timeWindow: TimeWindow, filter: LogFilter) {
    const client = new HttpObserverClient(baseUrl)
    const bucketMs = chartBucketMs(timeWindow.toMs - timeWindow.fromMs)
    return useSWR([...logHistogramKey(baseUrl, filter), ...windowKey(timeWindow), bucketMs], () => client.getLogHistogram(filter, alignBounds(currentBounds(timeWindow), bucketMs), bucketMs), {
        refreshInterval: timeWindow.live ? liveRefreshMs : 0,
        keepPreviousData: true
    })
}

function newestFirst(lines: ActorLog[]): ActorLog[] {
    const unique = new Map(lines.map(line => [line.logId, line]))
    return [...unique.values()].sort((a, b) => b.timestampMs - a.timestampMs || (a.logId < b.logId ? 1 : -1))
}
