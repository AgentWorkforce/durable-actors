import { formatClock } from "../format.js"

export const trendWindowMs = 60 * 60 * 1000
export const bucketCount = 30
export const bucketMs = trendWindowMs / bucketCount
export const emptySeries = Array<number>(bucketCount).fill(0)

export function trendAxis(now: number): string[] {
    return [formatClock(now - trendWindowMs), formatClock(now - trendWindowMs / 2), "Now"]
}
