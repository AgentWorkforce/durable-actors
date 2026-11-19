import useSWRInfinite from "swr/infinite"

import { logHistogramKey, logLinesKey, logsKey } from "../client/cacheKeys.js"
import { HttpObserverClient, logFilterParams } from "../client/observerClient.js"
import { alignBounds, chartBucketMs } from "../lib/logs.js"
import { currentBounds, windowKey } from "../lib/timeRange.js"
import { applyHistogramEvents, applyLogEvents, histogramFor, liveHistogram } from "../lib/views/logs.js"
import type { ActorLog, ActorLogPage, LogFilter } from "../types/Logs.js"
import type { TimeWindow } from "../types/TimeRange.js"

import { useEventStream, useLiveView } from "./useLiveView.js"

export function useLogs(baseUrl: string, timeWindow: TimeWindow, filter: LogFilter) {
    // Every level streams so the histogram counts them all; the list shows the chosen ones.
    const events = useEventStream(baseUrl, timeWindow.live ? logStreamQuery(filter) : null)
    const history = useLogHistory(baseUrl, timeWindow, filter)
    const { data: streamed } = useLiveView(logLinesKey(baseUrl, filter), async (): Promise<ActorLog[]> => [], applyLogEvents(filter.level), events)
    const histogram = useLogHistogram(baseUrl, timeWindow, filter, events)
    const pages = history.data
    return {
        lines: pages ? newestFirst([...(streamed ?? []), ...pages.flatMap(page => page.records)]) : null,
        hasOlder: Boolean(pages?.at(-1)?.nextCursor),
        loadingOlder: Boolean(pages && history.size > pages.length && (!history.error || history.isValidating)),
        // After a failed page, asking for one past the loaded pages retries it instead of skipping ahead.
        loadOlder: () => history.setSize((pages?.length ?? 0) + 1),
        histogram: histogram.data,
        isError: (!pages && Boolean(history.error)) || (!histogram.data && Boolean(histogram.error))
    }
}

function logStreamQuery(filter: LogFilter): URLSearchParams {
    const query = logFilterParams({ ...filter, level: null })
    query.set("signals", "logs")
    return query
}

function useLogHistory(baseUrl: string, timeWindow: TimeWindow, filter: LogFilter) {
    const client = new HttpObserverClient(baseUrl)
    return useSWRInfinite<ActorLogPage>(
        (index, previous: ActorLogPage | null) => (previous && !previous.nextCursor ? null : [...logsKey(baseUrl, filter), ...windowKey(timeWindow), index === 0 ? null : previous!.nextCursor]),
        (key: readonly unknown[]) => client.listLogs(filter, currentBounds(timeWindow), key.at(-1) as string | null),
        { revalidateFirstPage: false, revalidateOnFocus: false }
    )
}

function useLogHistogram(baseUrl: string, timeWindow: TimeWindow, filter: LogFilter, events: ReturnType<typeof useEventStream>) {
    const bucketMs = chartBucketMs(timeWindow.toMs - timeWindow.fromMs)
    const { data, error } = useLiveView(
        [...logHistogramKey(baseUrl, filter), ...windowKey(timeWindow), bucketMs],
        async () => {
            const readAtMs = Date.now()
            const read = await new HttpObserverClient(baseUrl).getLogHistogram(filter, alignBounds(currentBounds(timeWindow), bucketMs), bucketMs)
            return liveHistogram(read, readAtMs)
        },
        applyHistogramEvents,
        events
    )
    return { data: data && histogramFor(data, alignBounds(currentBounds(timeWindow), bucketMs)), error }
}

function newestFirst(lines: ActorLog[]): ActorLog[] {
    const unique = new Map(lines.map(line => [line.logId, line]))
    return [...unique.values()].sort((a, b) => b.timestampMs - a.timestampMs || (a.logId < b.logId ? 1 : -1))
}
