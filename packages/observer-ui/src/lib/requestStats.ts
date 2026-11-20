import type { RequestCounts, RequestHistogram, RequestTrace } from "../types/Requests.js"
import type { TimeSpan, TimeWindow } from "../types/TimeRange.js"

import { bucketCount, bucketIndex } from "./timeRange.js"

export type TimingPick = (trace: RequestTrace) => number | null

export const queueTime: TimingPick = trace => trace.queueWaitMs

// durationMs includes the queue wait, so latency excluding the queue subtracts it.
export const serviceTime: TimingPick = trace => trace.durationMs - (trace.queueWaitMs ?? 0)

export function requestCounts(requests: RequestTrace[], timeWindow: TimeWindow): number[] {
    const counts = Array<number>(bucketCount(timeWindow)).fill(0)
    for (const trace of countable(requests)) {
        const index = bucketIndex(timeWindow, trace.startedAtMs)
        if (index !== null) counts[index]! += 1
    }
    return counts
}

// Rerouted attempts are retried under the same request, so counting them would double-count.
export function countable(requests: RequestTrace[]): RequestTrace[] {
    return requests.filter(trace => trace.outcome !== "rerouted")
}

export function requestsDuring(requests: RequestTrace[], span: TimeSpan): RequestTrace[] {
    return countable(requests).filter(trace => overlaps(trace, span))
}

export function websocketActivity(requests: RequestTrace[], connectionId: string, span: TimeSpan): { messages: number; failures: number } {
    const during = requests.filter(trace => trace.kind === "websocket" && trace.connectionId === connectionId && overlaps(trace, span))
    return {
        messages: during.filter(trace => trace.operation === "onMessage").length,
        failures: during.filter(trace => trace.outcome !== "completed" && trace.outcome !== "rerouted").length
    }
}

function overlaps(trace: RequestTrace, span: TimeSpan): boolean {
    return trace.startedAtMs <= span.toMs && trace.startedAtMs + trace.durationMs >= span.fromMs
}

export function averageOf(requests: RequestTrace[], pick: TimingPick): number | null {
    const values = picked(requests, pick)
    return values.length ? average(values) : null
}

function average(values: number[]): number {
    return values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : 0
}

function picked(requests: RequestTrace[], pick: TimingPick): number[] {
    return requests.map(pick).filter((value): value is number => value !== null)
}

export function emptyRequestCounts(): RequestCounts {
    return { ok: 0, failed: 0, rejected: 0, interrupted: 0, queueWaitMs: 0, queueWaits: 0, runMs: 0 }
}

export function addRequestCounts(total: RequestCounts | undefined, more: RequestCounts): RequestCounts {
    const sum = total ?? emptyRequestCounts()
    return {
        ok: sum.ok + more.ok,
        failed: sum.failed + more.failed,
        rejected: sum.rejected + more.rejected,
        interrupted: sum.interrupted + more.interrupted,
        queueWaitMs: sum.queueWaitMs + more.queueWaitMs,
        queueWaits: sum.queueWaits + more.queueWaits,
        runMs: sum.runMs + more.runMs
    }
}

export function requestTotal({ ok, failed, rejected, interrupted }: RequestCounts): number {
    return ok + failed + rejected + interrupted
}

/** One request as counts; null for a rerouted attempt, which counts under its final outcome. */
export function countsOf(trace: RequestTrace): RequestCounts | null {
    if (trace.outcome === "rerouted") return null
    const counts = emptyRequestCounts()
    counts[trace.outcome === "completed" ? "ok" : trace.outcome] = 1
    return { ...counts, queueWaitMs: trace.queueWaitMs ?? 0, queueWaits: trace.queueWaitMs === null ? 0 : 1, runMs: serviceTime(trace) ?? trace.durationMs }
}

export function histogramTotals({ buckets }: RequestHistogram): RequestCounts {
    return buckets.reduce(addRequestCounts, emptyRequestCounts())
}

/** An average from summed counts; null when nothing was counted. */
export type CountsAverage = (counts: RequestCounts) => number | null

export const averageQueueWait: CountsAverage = ({ queueWaitMs, queueWaits }) => (queueWaits ? queueWaitMs / queueWaits : null)

export const averageRunTime: CountsAverage = counts => (requestTotal(counts) ? counts.runMs / requestTotal(counts) : null)
