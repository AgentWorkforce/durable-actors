import type { ApplyEvents } from "../../hooks/useLiveView.js"
import type { RequestCounts, RequestTrace } from "../../types/Requests.js"
import { addRequestCounts, countsOf } from "../requestStats.js"

import { type LiveHistogram, type SecondCounts, addCounts } from "./histogram.js"

export type RequestFilter = { actorName?: string; actorId?: string }

/** Adds streamed requests that match the filter, dropping those older than the live window. */
export function applyRequestEvents(durationMs: number, { actorName, actorId }: RequestFilter = {}): ApplyEvents<RequestTrace[]> {
    return (traces, events) => {
        const added: RequestTrace[] = []
        for (const event of events) {
            if (event.type !== "request") continue
            const trace = event.item
            if ((actorName === undefined || trace.actorName === actorName) && (actorId === undefined || trace.actorId === actorId)) added.push(trace)
        }
        if (added.length === 0) return traces
        const since = Date.now() - durationMs
        return [...new Map([...traces, ...added].map(trace => [trace.eventId, trace])).values()].filter(trace => trace.startedAtMs >= since)
    }
}

/**
 * Counts streamed requests that match the filter, in the bucket they started in, unless the read
 * already did: the read covered requests that started before it and were stored when it came back,
 * and hosts export a request when it finishes, so one that finished later was not stored yet.
 */
export function applyRequestCounts({ actorName, actorId }: RequestFilter = {}): ApplyEvents<LiveHistogram<RequestCounts>> {
    return (histogram, events) => {
        const added: SecondCounts<RequestCounts>[] = []
        for (const event of events) {
            if (event.type !== "request") continue
            const trace = event.item
            const counts = countsOf(trace)
            const unread = trace.startedAtMs >= histogram.readToMs || trace.startedAtMs + trace.durationMs > histogram.respondedAtMs
            if (counts && unread && (actorName === undefined || trace.actorName === actorName) && (actorId === undefined || trace.actorId === actorId)) added.push({ atMs: trace.startedAtMs, counts })
        }
        return addCounts(histogram, added, addRequestCounts)
    }
}
