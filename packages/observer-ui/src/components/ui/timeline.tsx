import type { ReactNode } from "react"

import { windowTicks } from "../../lib/timeRange.js"
import { cn } from "../../lib/utils.js"
import type { TimeSpan, TimeWindow } from "../../types/TimeRange.js"

const tickCount = 6
const trackGrid = "bg-[linear-gradient(90deg,color-mix(in_oklab,var(--border)_55%,transparent)_1px,transparent_1px)] bg-size-[calc(100%/6)_100%] shadow-[inset_-1px_0_0_var(--border)]"

export type TimelineSegment = TimeSpan & { left: number; width: number; now: boolean }

export type TimelineAxisProps = {
    timeWindow: TimeWindow
    grid: string
    label?: string
    trailing?: string
}

export function TimelineAxis({ timeWindow, grid, label, trailing }: TimelineAxisProps) {
    return (
        <div aria-hidden className={`${grid} h-6 border-b`}>
            <span className="text-xs text-muted-foreground">{label}</span>
            <div className="relative h-full text-[11px] text-muted-foreground tabular-nums">
                {windowTicks(timeWindow, tickCount).map((tick, index) => (
                    <span
                        key={index}
                        className={cn("absolute top-1", index === 0 ? "" : index === tickCount ? "-translate-x-full text-foreground" : "-translate-x-1/2")}
                        style={{ left: `${(index / tickCount) * 100}%` }}
                    >
                        {tick}
                    </span>
                ))}
            </div>
            {trailing !== undefined && <span className="text-right text-xs text-muted-foreground">{trailing}</span>}
        </div>
    )
}

export function TimelineTrack({ label, children }: { label: string; children: ReactNode }) {
    return (
        <div role="img" aria-label={label} className={`relative h-full ${trackGrid} before:absolute before:inset-x-0 before:top-1/2 before:border-t before:border-dashed before:border-border`}>
            {children}
        </div>
    )
}

// Open spans end at fetch time, which trails the live clock between polls, so an ongoing last span runs to the window's end.
export function timelineSegments(spans: TimeSpan[], { fromMs: start, toMs: end }: TimeWindow, ongoing: boolean): TimelineSegment[] {
    const spanMs = end - start
    const isOngoing = (index: number) => ongoing && index === spans.length - 1
    return spans
        .map((span, index) => ({ fromMs: Math.max(span.fromMs, start), toMs: isOngoing(index) ? end : Math.min(span.toMs, end), now: isOngoing(index) }))
        .filter(span => span.toMs > span.fromMs)
        .map(span => ({ ...span, left: (span.fromMs - start) / spanMs, width: (span.toMs - span.fromMs) / spanMs }))
}
