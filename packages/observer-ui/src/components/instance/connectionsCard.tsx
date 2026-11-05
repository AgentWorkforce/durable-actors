import { useState } from "react"

import { formatClock, formatDuration, formatPlural, formatPreciseTimeSpan } from "../../format.js"
import { websocketActivity } from "../../lib/requestStats.js"
import { sessionSpan } from "../../lib/sockets.js"
import { describeWindow } from "../../lib/timeRange.js"
import { cn } from "../../lib/utils.js"
import type { ActorInstance } from "../../types/Actors.js"
import type { RequestTrace } from "../../types/Requests.js"
import type { ResidencyPeriod } from "../../types/Residency.js"
import type { SocketSession } from "../../types/Sockets.js"
import type { TimeSpan, TimeWindow } from "../../types/TimeRange.js"
import { ResidencyTrack, liveDuration } from "../actorType/residencyCard.js"
import { ExpandFooter } from "../ui/expandFooter.js"
import { SectionCard } from "../ui/sectionCard.js"
import { Skeleton } from "../ui/skeleton.js"
import { TimelineAxis, type TimelineSegment, TimelineTrack, timelineSegments } from "../ui/timeline.js"
import { Tooltip, TooltipContent, TooltipTrigger } from "../ui/tooltip.js"

const previewRows = 10
const rowGrid = "grid grid-cols-[minmax(0,150px)_1fr] items-center gap-6"

type Connection = { session: SocketSession; open: boolean; span: TimeSpan; segment: TimelineSegment }

export type ConnectionsCardProps = {
    instance: ActorInstance | null
    sessions: SocketSession[] | null
    livePeriods: ResidencyPeriod[] | null
    requests: RequestTrace[] | null
    failed: boolean
    timeWindow: TimeWindow
}

export function ConnectionsCard({ instance, sessions, livePeriods, requests, failed, timeWindow }: ConnectionsCardProps) {
    const [expanded, setExpanded] = useState(false)
    const sockets = instance && sessions && socketRows(sessions, instance, timeWindow)
    const shown = expanded ? sockets : sockets?.slice(0, previewRows)
    return (
        <SectionCard title="WebSocket Connections" aside={<ConnectionKeys closed={sockets?.some(row => !row.open) ?? false} />} busy={!shown && !failed} className="pb-3">
            {failed ? (
                <p className="py-4 text-[13px] text-muted-foreground">Couldn't reach the observer API.</p>
            ) : (
                <>
                    <div className="mt-3.5">
                        <TimelineAxis timeWindow={timeWindow} grid={rowGrid} />
                        {instance && sockets && shown && livePeriods ? (
                            <>
                                <InstanceRow instance={instance} periods={livePeriods} requests={requests} timeWindow={timeWindow} />
                                {shown.map(row => (
                                    <SocketRow key={row.session.connectionId} row={row} requests={requests} timeWindow={timeWindow} />
                                ))}
                                {!sockets.length && <p className="pb-2 text-[13px] text-muted-foreground">No connections, {describeWindow(timeWindow)}.</p>}
                            </>
                        ) : (
                            <RowSkeletons />
                        )}
                    </div>
                    {sockets && sockets.length > previewRows && (
                        <ExpandFooter
                            shown={shown?.length ?? 0}
                            total={sockets.length}
                            noun="connection"
                            expandLabel="Show All Connections"
                            expanded={expanded}
                            onToggle={() => setExpanded(!expanded)}
                        />
                    )}
                </>
            )}
        </SectionCard>
    )
}

function socketRows(sessions: SocketSession[], instance: ActorInstance, timeWindow: TimeWindow): Connection[] {
    const openIds = new Set(instance.connections.map(connection => connection.id))
    return sessions
        .flatMap(session => {
            const open = openIds.has(session.connectionId)
            const span = sessionSpan(session, open)
            const [segment] = span ? timelineSegments([span], timeWindow, open && timeWindow.live) : []
            return span && segment ? [{ session, open, span, segment }] : []
        })
        .sort((a, b) => a.span.fromMs - b.span.fromMs)
}

function ConnectionKeys({ closed }: { closed: boolean }) {
    return (
        <div className="flex gap-4.5 text-xs text-muted-foreground">
            <span className="flex items-center gap-1.75">
                <span className="h-1.25 w-4 rounded-full bg-success" />
                Instance live
            </span>
            <span className="flex items-center gap-1.75">
                <span className="h-2 w-4 rounded-full bg-chart-2" />
                Open connection
            </span>
            {closed && (
                <span className="flex items-center gap-1.75">
                    <span className="h-2 w-4 rounded-full bg-chart-2/55" />
                    Closed
                </span>
            )}
        </div>
    )
}

type InstanceRowProps = { instance: ActorInstance; periods: ResidencyPeriod[]; requests: RequestTrace[] | null; timeWindow: TimeWindow }

function InstanceRow({ instance, periods, requests, timeWindow }: InstanceRowProps) {
    const live = instance.status === "live"
    const segments = timelineSegments(periods, timeWindow, live && timeWindow.live)
    const liveMs = liveDuration(segments)
    return (
        <div className={`${rowGrid} mt-3 mb-4.5 h-10.5 overflow-hidden rounded-lg border border-border/70 bg-background/60`}>
            <div className="pl-3">
                <b className="block text-[13px] font-medium">Residency</b>
                {live && (
                    <span className="mt-0.5 block text-[11px] text-muted-foreground tabular-nums">
                        <span className="font-medium text-success">Live</span> · {formatDuration(liveMs)}
                    </span>
                )}
            </div>
            <ResidencyTrack segments={segments} requests={requests} label={`Residency: live ${formatDuration(liveMs)}, ${describeWindow(timeWindow)}`} />
        </div>
    )
}

function SocketRow({ row: { session, open, span, segment }, requests, timeWindow }: { row: Connection; requests: RequestTrace[] | null; timeWindow: TimeWindow }) {
    const activity = requests ? websocketActivity(requests, session.connectionId, segment) : null
    const messages = activity?.messages ?? session.messages
    const failures = activity?.failures ?? session.failures
    const failedClose = !open && failures > 0
    return (
        <div className={`${rowGrid} h-11 border-b border-border/50 last:border-b-0`}>
            <div className="min-w-0">
                <b translate="no" className="block truncate text-[13px] font-medium">
                    {session.connectionId}
                </b>
                <span className={cn("mt-0.5 block truncate text-[11px] text-muted-foreground tabular-nums", failedClose && "text-danger")}>
                    {open ? `${formatPlural(messages, "message")} · ${formatDuration(timeWindow.toMs - span.fromMs)}` : `Closed ${formatClock(span.toMs)}`}
                    {failedClose && ` · ${formatPlural(failures, "failure")}`}
                </span>
            </div>
            <TimelineTrack label={`${session.connectionId}: ${open ? "open" : "closed"}, ${formatPreciseTimeSpan(segment.fromMs, segment.toMs)}`}>
                <SocketSegment messages={messages} failures={failures} segment={segment} open={open} failed={failedClose} />
            </TimelineTrack>
        </div>
    )
}

type SocketSegmentProps = { messages: number; failures: number; segment: TimelineSegment; open: boolean; failed: boolean }

function SocketSegment({ messages, failures, segment, open, failed }: SocketSegmentProps) {
    return (
        <Tooltip>
            <TooltipTrigger asChild>
                <span
                    className={cn(
                        "absolute top-1/2 h-2.5 min-w-1 -translate-y-1/2 rounded-full before:absolute before:-inset-x-1.5 before:-inset-y-2",
                        open ? "bg-chart-2 shadow-[0_0_12px_color-mix(in_oklab,var(--chart-2)_20%,transparent)]" : "bg-chart-2/55",
                        segment.now && "rounded-r-[1px] after:absolute after:top-0.75 after:right-1 after:size-1 after:rounded-full after:bg-background",
                        !open && "after:absolute after:-top-0.75 after:right-0 after:h-4 after:w-0.5 after:rounded-[1px]",
                        !open && (failed ? "after:bg-danger" : "after:bg-chart-2")
                    )}
                    style={{ left: `${segment.left * 100}%`, width: `${segment.width * 100}%` }}
                />
            </TooltipTrigger>
            <TooltipContent className="tabular-nums">
                <div className="font-medium">{formatPreciseTimeSpan(segment.fromMs, segment.toMs)}</div>
                <div className="mt-1 opacity-80">
                    {formatPlural(messages, "message")} · {formatPlural(failures, "failure")}
                </div>
            </TooltipContent>
        </Tooltip>
    )
}

function RowSkeletons() {
    return Array.from({ length: 4 }, (_, index) => (
        <div key={index} className={`${rowGrid} h-11 border-b border-border/50 last:border-b-0`}>
            <div>
                <Skeleton className="h-3.5 w-24" />
                <Skeleton className="mt-1.5 h-2.5 w-14" />
            </div>
            <Skeleton className="h-2.5 rounded-full" style={{ width: `${60 - index * 12}%`, marginLeft: `${index * 10}%` }} />
        </div>
    ))
}
