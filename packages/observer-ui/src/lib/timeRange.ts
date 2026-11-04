import { formatDuration, formatTick, formatTimeSpan } from "../format.js"
import type { TimeRange, TimeWindow } from "../types/TimeRange.js"

const second = 1000
const minute = 60 * second
const hour = 60 * minute
const targetBuckets = 30
const niceSteps = [10 * second, 30 * second, minute, 2 * minute, 5 * minute, 15 * minute, 30 * minute, hour, 3 * hour, 6 * hour, 12 * hour, 24 * hour]

export const lastHour: TimeRange = { mode: "live", durationMs: hour }

export function pinnedLastHour(): TimeRange {
    const toMs = Date.now()
    return { mode: "static", fromMs: toMs - hour, toMs }
}

export const timePresets = [
    { label: "Last 15 minutes", durationMs: 15 * minute },
    { label: "Last 1 hour", durationMs: hour },
    { label: "Last 6 hours", durationMs: 6 * hour },
    { label: "Last 24 hours", durationMs: 24 * hour },
    { label: "Last 7 days", durationMs: 7 * 24 * hour }
]

export function liveLabel(durationMs: number): string {
    return timePresets.find(preset => preset.durationMs === durationMs)?.label ?? `Last ${Math.round(durationMs / minute)} minutes`
}

export function resolveWindow(range: TimeRange, now: number): TimeWindow {
    const [fromMs, toMs] = range.mode === "live" ? [now - range.durationMs, now] : [range.fromMs, range.toMs]
    return { fromMs, toMs, stepMs: pickStep(toMs - fromMs), live: range.mode === "live" }
}

export function atTime(day: Date, time: string): number {
    const [hours = 0, minutes = 0] = time.split(":").map(Number)
    return new Date(day.getFullYear(), day.getMonth(), day.getDate(), hours, minutes).getTime()
}

export function toggleLive({ fromMs, toMs, live }: TimeWindow): TimeRange {
    return live ? { mode: "static", fromMs, toMs } : { mode: "live", durationMs: toMs - fromMs }
}

export function describeWindow({ fromMs, toMs, live }: TimeWindow): string {
    return live ? `last ${formatDuration(toMs - fromMs)}` : formatTimeSpan(fromMs, toMs)
}

export function windowTicks({ fromMs, toMs, live }: TimeWindow, intervals: number): string[] {
    const spanMs = toMs - fromMs
    return Array.from({ length: intervals + 1 }, (_, index) => (live && index === intervals ? "Now" : formatTick(fromMs + (index / intervals) * spanMs, spanMs)))
}

export function bucketLabel(timeWindow: TimeWindow, index: number): string {
    return formatTick(bucketEnd(timeWindow, index), timeWindow.toMs - timeWindow.fromMs)
}

export function bucketEnd({ fromMs, toMs, stepMs }: TimeWindow, index: number): number {
    return Math.min(fromMs + (index + 1) * stepMs, toMs)
}

export function bucketCount({ fromMs, toMs, stepMs }: TimeWindow): number {
    return Math.max(1, Math.ceil((toMs - fromMs) / stepMs))
}

export function bucketIndex(timeWindow: TimeWindow, atMs: number): number | null {
    if (atMs < timeWindow.fromMs || atMs > timeWindow.toMs) return null
    return Math.min(Math.floor((atMs - timeWindow.fromMs) / timeWindow.stepMs), bucketCount(timeWindow) - 1)
}

function pickStep(durationMs: number): number {
    return niceSteps.find(step => durationMs / step <= targetBuckets) ?? niceSteps.at(-1)!
}
