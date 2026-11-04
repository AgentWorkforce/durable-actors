import { useState } from "react"
import { Link } from "react-router"

import { formatClock, formatDuration, formatPlural, formatPreciseTimeSpan } from "../../format.js"
import { byLastActive, instancePath } from "../../lib/actorTypes.js"
import { describeWindow } from "../../lib/timeRange.js"
import { cn } from "../../lib/utils.js"
import type { ActorInstance, ActorType } from "../../types/Actors.js"
import type { RequestTrace } from "../../types/Requests.js"
import type { ResidencyHistory, ResidencyPeriod } from "../../types/Residency.js"
import type { TimeWindow } from "../../types/TimeRange.js"
import { ExpandFooter } from "../ui/expandFooter.js"
import { SectionCard } from "../ui/sectionCard.js"
import { Skeleton } from "../ui/skeleton.js"
import { TimelineAxis, type TimelineSegment, TimelineTrack, timelineSegments } from "../ui/timeline.js"
import { Tooltip, TooltipContent, TooltipTrigger } from "../ui/tooltip.js"

const previewRows = 10
const rowGrid = "grid grid-cols-[minmax(0,210px)_1fr_minmax(0,128px)] items-center gap-6"

export type ResidencyCardProps = {
    actor: ActorType | null
    history: ResidencyHistory | null
    requests: RequestTrace[] | null
    failed: boolean
    timeWindow: TimeWindow
}

export function ResidencyCard({ actor, history, requests, failed, timeWindow }: ResidencyCardProps) {
    const [expanded, setExpanded] = useState(false)
    const instances = actor && byLastActive(actor.instances)
    const livePeriods = history && groupLivePeriods(history)
    const shown = expanded ? instances : instances?.slice(0, previewRows)
    return (
        <SectionCard title="Residency" aside={<ResidencyKeys />} busy={!shown && !failed} className="pb-3">
            {failed ? (
                <p className="py-4 text-[13px] text-muted-foreground">Couldn't reach the observer API.</p>
            ) : (
                <>
                    <div className="mt-3.5">
                        <TimelineAxis timeWindow={timeWindow} grid={rowGrid} label="Instance" trailing="Time live" />
                        {shown && livePeriods ? (
                            shown.map(instance => (
                                <ResidencyRow
                                    key={instance.actorId}
                                    instance={instance}
                                    periods={livePeriods.get(instance.actorId) ?? []}
                                    requests={requests && requests.filter(trace => trace.actorId === instance.actorId && trace.outcome !== "rerouted")}
                                    timeWindow={timeWindow}
                                />
                            ))
                        ) : (
                            <RowSkeletons />
                        )}
                    </div>
                    {instances && (
                        <ExpandFooter
                            shown={shown?.length ?? 0}
                            total={instances.length}
                            noun="instance"
                            expandLabel="Show All Instances"
                            expanded={expanded}
                            onToggle={() => setExpanded(!expanded)}
                        />
                    )}
                </>
            )}
        </SectionCard>
    )
}

export function groupLivePeriods({ periods }: ResidencyHistory): Map<string, ResidencyPeriod[]> {
    const grouped = new Map<string, ResidencyPeriod[]>()
    for (const period of periods) {
        if (period.status === "live") grouped.set(period.actorId, [...(grouped.get(period.actorId) ?? []), period])
    }
    return grouped
}

function ResidencyKeys() {
    return (
        <div className="flex gap-4.5 text-xs text-muted-foreground">
            <span className="flex items-center gap-1.75">
                <span className="h-2 w-4 rounded-full bg-success" />
                Live
            </span>
            <span className="flex items-center gap-1.75">
                <span className="w-4 border-t border-dashed border-muted-foreground/60" />
                Dormant
            </span>
        </div>
    )
}

type ResidencyRowProps = { instance: ActorInstance; periods: ResidencyPeriod[]; requests: RequestTrace[] | null; timeWindow: TimeWindow }

function ResidencyRow({ instance, periods, requests, timeWindow }: ResidencyRowProps) {
    const live = instance.status === "live"
    const segments = timelineSegments(periods, timeWindow, live && timeWindow.live)
    const liveMs = liveDuration(segments)
    return (
        <Link
            to={instancePath(instance.actorId)}
            className={`${rowGrid} h-11.5 border-b border-border/50 transition-colors outline-none last:border-b-0 hover:bg-muted/40 focus-visible:ring-2 focus-visible:ring-ring`}
        >
            <div className="flex min-w-0 items-center gap-2.5">
                <span aria-hidden className={cn("size-2 shrink-0 rounded-full", live ? "bg-success" : "border border-muted-foreground/50")} />
                <div className="min-w-0">
                    <b translate="no" className="block truncate text-[13px] font-medium">
                        {instance.actorId}
                    </b>
                    <span className="mt-0.5 block truncate text-[11px] text-muted-foreground tabular-nums">{statusLabel(live, periods, instance, timeWindow.fromMs)}</span>
                </div>
            </div>
            <ResidencyTrack segments={segments} requests={requests} label={`${instance.actorId}: ${liveMs ? `live ${formatDuration(liveMs)}` : "dormant"}, ${describeWindow(timeWindow)}`} />
            <span className={cn("text-right text-[13px] tabular-nums", liveMs ? "text-foreground/85" : "text-muted-foreground")}>{formatDuration(liveMs)}</span>
        </Link>
    )
}

export function liveDuration(segments: TimelineSegment[]): number {
    return segments.reduce((sum, segment) => sum + segment.toMs - segment.fromMs, 0)
}

function statusLabel(live: boolean, periods: ResidencyPeriod[], instance: ActorInstance, windowStartMs: number): string {
    if (live) {
        const woke = periods.at(-1)?.fromMs
        return woke && woke > windowStartMs ? `Woke ${formatClock(woke)}` : "Live"
    }
    const lastLive = periods.at(-1)?.toMs ?? instance.lastActiveAtMs
    return lastLive ? `Slept ${formatClock(lastLive)}` : "Dormant"
}

export function ResidencyTrack({ segments, requests, label }: { segments: TimelineSegment[]; requests: RequestTrace[] | null; label: string }) {
    return (
        <TimelineTrack label={label}>
            {segments.map(segment => (
                <LiveSegment key={segment.left} segment={segment} requests={requests} />
            ))}
        </TimelineTrack>
    )
}

function LiveSegment({ segment, requests }: { segment: TimelineSegment; requests: RequestTrace[] | null }) {
    const handled = requests?.filter(trace => trace.startedAtMs <= segment.toMs && trace.startedAtMs + trace.durationMs >= segment.fromMs)
    return (
        <Tooltip>
            <TooltipTrigger asChild>
                <span
                    className={cn(
                        "absolute top-1/2 h-2.5 -translate-y-1/2 rounded-full bg-success shadow-[0_0_12px_color-mix(in_oklab,var(--success)_20%,transparent)] before:absolute before:-inset-x-1.5 before:-inset-y-2",
                        segment.now && "rounded-r-[1px] after:absolute after:top-0.75 after:right-1 after:size-1 after:rounded-full after:bg-background"
                    )}
                    style={{ left: `${segment.left * 100}%`, width: `${segment.width * 100}%` }}
                />
            </TooltipTrigger>
            <TooltipContent className="tabular-nums">
                <div className="font-medium">{formatPreciseTimeSpan(segment.fromMs, segment.toMs)}</div>
                <div className="mt-1 opacity-80">
                    Active {formatDuration(segment.toMs - segment.fromMs)} · {handled ? formatPlural(handled.length, "request") : "requests unavailable"}
                </div>
            </TooltipContent>
        </Tooltip>
    )
}

function RowSkeletons() {
    return Array.from({ length: 4 }, (_, index) => (
        <div key={index} className={`${rowGrid} h-11.5 border-b border-border/50 last:border-b-0`}>
            <div className="flex items-center gap-2.5">
                <Skeleton className="size-2 rounded-full" />
                <div>
                    <Skeleton className="h-3.5 w-32" />
                    <Skeleton className="mt-1.5 h-2.5 w-16" />
                </div>
            </div>
            <Skeleton className="h-2.5 rounded-full" style={{ width: `${70 - index * 15}%` }} />
            <Skeleton className="ml-auto h-3.5 w-12" />
        </div>
    ))
}
