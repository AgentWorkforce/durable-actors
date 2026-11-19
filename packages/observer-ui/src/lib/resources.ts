import type { TimeWindow } from "../types/TimeRange.js"

import { bucketCount, bucketIndex } from "./timeRange.js"

/**
 * The highest value in each bucket of the window. Empty buckets are 0, or with `carry` repeat the
 * last value before them, starting from `carry`'s own value.
 */
export function bucketMax<T extends { atMs: number }>(points: T[], timeWindow: TimeWindow, value: (point: T) => number | null, carry?: number): number[] {
    const series = Array<number | null>(bucketCount(timeWindow)).fill(null)
    for (const point of points) {
        const index = bucketIndex(timeWindow, point.atMs)
        const current = value(point)
        if (index !== null && current !== null) series[index] = Math.max(series[index] ?? 0, current)
    }
    let previous = carry ?? 0
    return series.map(entry => (entry === null ? (carry === undefined ? 0 : previous) : (previous = entry)))
}

/** The latest point in each bucket of the window, carried across empty buckets (the first one before any). */
export function bucketLatest<T extends { atMs: number }>(points: T[], timeWindow: TimeWindow): (T | null)[] {
    const series = Array<T | undefined>(bucketCount(timeWindow)).fill(undefined)
    for (const point of points) {
        const index = bucketIndex(timeWindow, point.atMs) ?? (point.atMs < timeWindow.fromMs ? 0 : null)
        if (index !== null) series[index] = point
    }
    let previous = series.find(entry => entry !== undefined) ?? null
    return series.map(entry => (previous = entry ?? previous))
}
