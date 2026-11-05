import { type PointerEvent, useState } from "react"

import { formatClock, formatCount, formatDuration, formatPlural, formatPreciseClock, formatTick } from "../../format.js"
import { countAtLeast, logLevels } from "../../lib/logs.js"
import { cn } from "../../lib/utils.js"
import type { LevelCounts, LogHistogram, LogLevel } from "../../types/Logs.js"
import type { TimeSpan } from "../../types/TimeRange.js"
import { Skeleton } from "../ui/skeleton.js"

export const levelFill: Record<LogLevel, string> = {
    debug: "bg-muted-foreground/25",
    info: "bg-muted-foreground/55",
    warn: "bg-warning",
    error: "bg-danger"
}

const scaleSteps = [2, 4, 6, 10]
const tickCount = 6

type Drag = { from: number; to: number }

export type LogChartProps = {
    histogram: LogHistogram | undefined
    level: LogLevel | null
    live: boolean
    onZoom: (span: TimeSpan) => void
}

export function LogChart({ histogram, level, live, onZoom }: LogChartProps) {
    const [hovered, setHovered] = useState<number | null>(null)
    const [drag, setDrag] = useState<Drag | null>(null)
    if (!histogram) return <Skeleton className="mt-4 mb-3.5 h-[122px] w-full rounded-sm" />
    const { buckets, fromMs, bucketMs } = histogram
    const scale = niceScale(Math.max(...buckets.map(counts => countAtLeast(counts, null)), 1))
    const bucketAt = (event: PointerEvent<HTMLDivElement>) => {
        const box = event.currentTarget.getBoundingClientRect()
        return Math.min(buckets.length - 1, Math.max(0, Math.floor(((event.clientX - box.left) / box.width) * buckets.length)))
    }
    const finishDrag = (event: PointerEvent<HTMLDivElement>) => {
        const end = bucketAt(event)
        setDrag(null)
        if (!drag || drag.from === end) return
        onZoom({ fromMs: fromMs + Math.min(drag.from, end) * bucketMs, toMs: fromMs + (Math.max(drag.from, end) + 1) * bucketMs })
    }
    return (
        <div className="mt-4 border-b pb-3.5">
            <div className="relative pt-4">
                <Gridlines scale={scale} />
                <div
                    role="img"
                    aria-label={`Log lines per ${formatDuration(bucketMs)}; drag across the chart to zoom into a time range`}
                    className="relative ml-[42px] h-24 cursor-crosshair touch-none select-none"
                    onPointerDown={event => {
                        event.currentTarget.setPointerCapture(event.pointerId)
                        setDrag({ from: bucketAt(event), to: bucketAt(event) })
                    }}
                    onPointerMove={event => {
                        setHovered(bucketAt(event))
                        if (drag) setDrag({ ...drag, to: bucketAt(event) })
                    }}
                    onPointerUp={finishDrag}
                    onPointerLeave={() => setHovered(null)}
                >
                    <div className="absolute inset-0 flex items-end gap-0.5">
                        {buckets.map((counts, index) => (
                            <Bar key={index} counts={counts} scale={scale} level={level} hovered={index === hovered} />
                        ))}
                    </div>
                    {drag && <Selection drag={drag} buckets={buckets.length} />}
                    {hovered !== null && !drag && countAtLeast(buckets[hovered]!, null) > 0 && <BucketTooltip index={hovered} histogram={histogram} />}
                </div>
            </div>
            <TimeLabels histogram={histogram} live={live} />
        </div>
    )
}

function niceScale(peak: number): number {
    for (let magnitude = 1; ; magnitude *= 10) {
        const step = scaleSteps.find(candidate => candidate * magnitude >= peak)
        if (step) return step * magnitude
    }
}

// Lines span the card's full width; each value sits just above its line, left of the bars.
function Gridlines({ scale }: { scale: number }) {
    return (
        <div aria-hidden className="pointer-events-none absolute inset-x-0 top-4 h-24">
            {[scale, scale / 2, 0].map((value, index) => (
                <div key={index} className="absolute inset-x-0 border-t border-dashed border-border/80" style={{ top: `${index * 50}%` }}>
                    <span className="absolute bottom-0.5 left-0 text-[11px] leading-none text-muted-foreground tabular-nums">{formatCount(value)}</span>
                </div>
            ))}
        </div>
    )
}

type BarProps = { counts: LevelCounts; scale: number; level: LogLevel | null; hovered: boolean }

function Bar({ counts, scale, level, hovered }: BarProps) {
    const kept = level ? logLevels.indexOf(level) : 0
    if (!countAtLeast(counts, null)) return <span className="h-px flex-1 bg-border/60" />
    return (
        <span className={cn("flex h-full flex-1 flex-col-reverse rounded-[1px]", hovered && "outline outline-offset-1 outline-foreground")}>
            {logLevels.map((name, index) =>
                counts[name] ? (
                    <span key={name} className={cn("block w-full last:rounded-t-[1px]", levelFill[name], index < kept && "opacity-25")} style={{ height: `${(counts[name] / scale) * 100}%` }} />
                ) : null
            )}
        </span>
    )
}

function Selection({ drag, buckets }: { drag: Drag; buckets: number }) {
    const first = Math.min(drag.from, drag.to)
    const span = Math.abs(drag.to - drag.from) + 1
    return (
        <div
            className="pointer-events-none absolute inset-y-0 rounded-sm bg-foreground/10 ring-1 ring-foreground/40"
            style={{ left: `${(first / buckets) * 100}%`, width: `${(span / buckets) * 100}%` }}
        />
    )
}

function BucketTooltip({ index, histogram: { buckets, fromMs, bucketMs } }: { index: number; histogram: LogHistogram }) {
    const counts = buckets[index]!
    const startMs = fromMs + index * bucketMs
    const clock = bucketMs < 60_000 ? formatPreciseClock : formatClock
    const right = index >= buckets.length / 2
    return (
        <div
            className="pointer-events-none absolute top-1 z-10 rounded-lg border bg-card px-3 py-2 text-xs whitespace-nowrap text-muted-foreground shadow-(--shadow-popover)"
            style={right ? { right: `calc(${100 - (index / buckets.length) * 100}% + 8px)` } : { left: `calc(${((index + 1) / buckets.length) * 100}% + 8px)` }}
        >
            <b className="mb-1 block font-medium text-foreground tabular-nums">
                {clock(startMs)} – {clock(startMs + bucketMs)} · {formatPlural(countAtLeast(counts, null), "line")}
            </b>
            <span className="flex gap-3.5 tabular-nums">
                {[...logLevels].reverse().map(level => (
                    <span key={level} className="flex items-center gap-1.5">
                        <i className={cn("size-1.5 rounded-full", levelFill[level])} />
                        {formatCount(counts[level])} {level}
                    </span>
                ))}
            </span>
        </div>
    )
}

function TimeLabels({ histogram: { fromMs, bucketMs, buckets }, live }: { histogram: LogHistogram; live: boolean }) {
    const spanMs = bucketMs * buckets.length
    return (
        <div aria-hidden className="mt-1.5 ml-[42px] flex justify-between text-[11px] text-muted-foreground tabular-nums">
            {Array.from({ length: tickCount + 1 }, (_, index) => (
                <span key={index}>{live && index === tickCount ? "Now" : formatTick(fromMs + (index / tickCount) * spanMs, spanMs)}</span>
            ))}
        </div>
    )
}
