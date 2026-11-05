import type { LevelCounts, LogHistogram, LogLevel } from "../types/Logs.js"
import type { TimeBounds } from "../types/TimeRange.js"

type Json = string | number | boolean | null | Json[] | { [key: string]: Json }

export type ParsedMessage = { firstLine: string; moreLines: number; json: Json[] | { [key: string]: Json } | undefined }

export type JsonSummary = { headline: string | undefined; fields: [string, string][] }

export const logLevels: LogLevel[] = ["debug", "info", "warn", "error"]

// The runtime tags output written while an actor is being constructed with this request ID.
export const constructorRequestId = "activate"

const second = 1000
const bucketSteps = [1, 2, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 10_800, 21_600, 43_200, 86_400].map(seconds => seconds * second)
const chartBars = 60
const headlineKeys = ["message", "msg", "event"]
const summaryFields = 6

export function chartBucketMs(spanMs: number): number {
    return bucketSteps.find(step => spanMs / step <= chartBars) ?? bucketSteps.at(-1)!
}

// Bucket edges sit on multiples of the bucket size so a moving live window keeps its bars steady.
export function alignBounds({ fromMs, toMs }: TimeBounds, bucketMs: number): TimeBounds {
    const start = Math.floor(fromMs / bucketMs) * bucketMs
    return { fromMs: start, toMs: Math.max(Math.ceil(toMs / bucketMs) * bucketMs, start + bucketMs) }
}

export function levelTotals({ buckets }: LogHistogram): LevelCounts {
    const totals: LevelCounts = { debug: 0, info: 0, warn: 0, error: 0 }
    for (const counts of buckets) for (const level of logLevels) totals[level] += counts[level]
    return totals
}

export function countAtLeast(counts: LevelCounts, level: LogLevel | null): number {
    return logLevels.slice(level ? logLevels.indexOf(level) : 0).reduce((sum, kept) => sum + counts[kept], 0)
}

export function parseLogMessage(message: string): ParsedMessage {
    const lines = message.split("\n")
    return { firstLine: lines[0] ?? "", moreLines: lines.length - 1, json: parseJson(message) }
}

export function summarizeJson(value: Json[] | { [key: string]: Json }): JsonSummary {
    if (Array.isArray(value)) return { headline: `${value.length} items`, fields: [] }
    const headlineKey = headlineKeys.find(key => typeof value[key] === "string")
    const fields = Object.entries(value)
        .filter(([key]) => key !== headlineKey)
        .slice(0, summaryFields)
        .map(([key, field]): [string, string] => [key, previewJson(field)])
    return { headline: headlineKey && (value[headlineKey] as string), fields }
}

export function shortRequestId(requestId: string): string {
    if (/^[0-9a-f]{8}-[0-9a-f]{4}-/iu.test(requestId)) return requestId.slice(0, 8)
    return requestId.length > 14 ? `${requestId.slice(0, 4)}…${requestId.slice(-8)}` : requestId
}

function parseJson(message: string): ParsedMessage["json"] {
    const text = message.trim()
    if (!text.startsWith("{") && !text.startsWith("[")) return undefined
    try {
        const value: Json = JSON.parse(text)
        return typeof value === "object" && value !== null ? value : undefined
    } catch {
        return undefined
    }
}

function previewJson(value: Json): string {
    if (Array.isArray(value)) return "[…]"
    if (typeof value === "object" && value !== null) return "{…}"
    const text = JSON.stringify(value)
    return text.length > 40 ? `${text.slice(0, 39)}…` : text
}
