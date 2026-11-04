import { useState } from "react"

import { formatClock, formatCount, formatDecimal, formatDuration, formatMs, formatPlural, formatPreciseClock } from "../../format.js"
import { averageOf, countable, queueTime, requestCounts, serviceTime } from "../../lib/requestStats.js"
import { bucketIndex, windowTicks } from "../../lib/timeRange.js"
import { cn } from "../../lib/utils.js"
import type { RequestOutcome, RequestTrace } from "../../types/Requests.js"
import type { TimeWindow } from "../../types/TimeRange.js"
import { BarChart } from "../ui/barChart.js"
import { SectionCard } from "../ui/sectionCard.js"
import { Skeleton } from "../ui/skeleton.js"
import { Tooltip, TooltipContent, TooltipTrigger } from "../ui/tooltip.js"

const callLimit = 40
const labelRoom = 0.9
const tickSteps = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000, 30_000, 60_000, 300_000, 900_000, 3_600_000, 21_600_000]
const axisUnits = [
    { label: "ms", ms: 1, below: 1_000 },
    { label: "s", ms: 1_000, below: 60_000 },
    { label: "min", ms: 60_000, below: Infinity }
]
const waterfallGrid = "grid grid-cols-[52px_minmax(0,150px)_1fr_52px_70px_70px] items-center gap-4.5"
const queueStripes = "bg-[repeating-linear-gradient(135deg,var(--chart-1)_0_2px,transparent_2px_4px)] shadow-[inset_0_0_0_1px_color-mix(in_oklab,var(--chart-1)_50%,transparent)]"
const outcomeLabels: Record<RequestOutcome, string> = { completed: "ok", failed: "failed", rejected: "rejected", rerouted: "rerouted", interrupted: "interrupted" }
const kindLabels: Record<RequestTrace["kind"], string> = { method: "rpc", websocket: "ws" }

type Scale = { spanMs: number; ticks: number[]; unit: (typeof axisUnits)[number] }
type MethodCalls = { key: string; kind: RequestTrace["kind"]; operation: string; calls: RequestTrace[] }

export type RequestsCardProps = {
    requests: RequestTrace[] | null
    failed: boolean
    timeWindow: TimeWindow
}

export function RequestsCard({ requests, failed, timeWindow }: RequestsCardProps) {
    const [picked, setPicked] = useState<number | null>(null)
    const traces = requests && countable(requests)
    const counts = traces && requestCounts(traces, timeWindow)
    const selected = counts && (picked !== null && picked < counts.length ? picked : latestActive(counts))
    const inSlice = traces && selected !== null ? sliceCalls(traces, timeWindow, selected) : null
    const calls = inSlice?.slice(-callLimit)
    return (
        <SectionCard title="Requests" aside={<WaterfallKeys />} busy={!counts && !failed} className="pb-3.5">
            {failed ? (
                <p className="py-4 text-[13px] text-muted-foreground">Couldn't reach the observer API.</p>
            ) : counts && selected !== null && inSlice && calls ? (
                <>
                    <SliceHistogram counts={counts} selected={selected} onSelect={setPicked} timeWindow={timeWindow} />
                    <Waterfall calls={calls} total={inSlice.length} />
                </>
            ) : (
                <WaterfallSkeleton />
            )}
        </SectionCard>
    )
}

function latestActive(counts: number[]): number {
    const fromEnd = [...counts].reverse().findIndex(count => count > 0)
    return counts.length - 1 - Math.max(fromEnd, 0)
}

function sliceCalls(traces: RequestTrace[], timeWindow: TimeWindow, slice: number): RequestTrace[] {
    return traces.filter(trace => bucketIndex(timeWindow, trace.startedAtMs) === slice).sort((a, b) => a.startedAtMs - b.startedAtMs)
}

function WaterfallKeys() {
    return (
        <div className="flex gap-4.5 text-xs text-muted-foreground">
            <span className="flex items-center gap-1.75">
                <span className={`h-2 w-4 rounded-xs ${queueStripes}`} />
                Queue wait
            </span>
            <span className="flex items-center gap-1.75">
                <span className="h-2 w-4 rounded-xs bg-foreground" />
                Running
            </span>
        </div>
    )
}

type SliceHistogramProps = { counts: number[]; selected: number; onSelect: (index: number) => void; timeWindow: TimeWindow }

function SliceHistogram({ counts, selected, onSelect, timeWindow }: SliceHistogramProps) {
    const sliceStart = (index: number) => timeWindow.fromMs + index * timeWindow.stepMs
    const sliceEnd = Math.min(sliceStart(selected + 1), timeWindow.toMs)
    const clock = timeWindow.stepMs < 60_000 ? formatPreciseClock : formatClock
    return (
        <div className="mt-3.5 grid items-end gap-x-6 gap-y-3 border-b pt-3 pb-3.5 md:grid-cols-[282px_1fr]">
            <div className="text-xs text-muted-foreground tabular-nums">
                <b className="mb-0.5 block text-[13px] font-medium text-foreground">Requests per {formatDuration(timeWindow.stepMs)}</b>
                {clock(sliceStart(selected))} – {clock(sliceEnd)}
            </div>
            <div>
                <BarChart
                    values={counts}
                    highlighted={selected}
                    onSelect={onSelect}
                    label="Requests over the selected time range; pick a bar to inspect its requests"
                    barLabel={index => `${formatPlural(counts[index] ?? 0, "request")} from ${formatClock(sliceStart(index))}`}
                    className="h-10"
                />
                <div aria-hidden className="mt-1.5 flex justify-between text-[11px] text-muted-foreground tabular-nums">
                    {windowTicks(timeWindow, 3).map((tick, index) => (
                        <span key={index}>{tick}</span>
                    ))}
                </div>
            </div>
        </div>
    )
}

function Waterfall({ calls, total }: { calls: RequestTrace[]; total: number }) {
    if (!calls.length) return <p className="py-6 text-[13px] text-muted-foreground">No requests in this slice.</p>
    const origin = calls[0]!.startedAtMs
    const scale = waterfallScale(extentMs(calls))
    const rows = methodRows(calls)
    return (
        <>
            <WaterfallAxis scale={scale} />
            <ol>
                {rows.map(row => (
                    <MethodRow key={row.key} row={row} calls={calls} origin={origin} scale={scale} />
                ))}
            </ol>
            <WaterfallFooter calls={calls} methods={rows.length} total={total} />
        </>
    )
}

function extentMs(calls: RequestTrace[]): number {
    return Math.max(...calls.map(call => call.startedAtMs + call.durationMs)) - calls[0]!.startedAtMs
}

// Bars stop short of the axis end so the outcome label after a failed call still fits.
function waterfallScale(extent: number): Scale {
    const span = extent / labelRoom
    const step = tickSteps.find(step => span / step <= 6) ?? tickSteps.at(-1)!
    const spanMs = Math.max(step, Math.ceil(span / step) * step)
    return { spanMs, ticks: Array.from({ length: spanMs / step + 1 }, (_, index) => index * step), unit: axisUnits.find(unit => spanMs < unit.below) ?? axisUnits.at(-1)! }
}

function methodRows(calls: RequestTrace[]): MethodCalls[] {
    const rows = new Map<string, MethodCalls>()
    for (const call of calls) {
        const key = `${call.kind}:${call.operation}`
        const row = rows.get(key) ?? { key, kind: call.kind, operation: call.operation, calls: [] }
        row.calls.push(call)
        rows.set(key, row)
    }
    return [...rows.values()].sort((a, b) => b.calls.length - a.calls.length || a.calls[0]!.startedAtMs - b.calls[0]!.startedAtMs)
}

function WaterfallAxis({ scale: { spanMs, ticks, unit } }: { scale: Scale }) {
    return (
        <div aria-hidden className={`${waterfallGrid} h-7.5 border-b text-xs text-muted-foreground`}>
            <span>Kind</span>
            <span>Method</span>
            <div className="relative h-full text-[11px] tabular-nums">
                {ticks.map((tick, index) => (
                    <span
                        key={tick}
                        className={cn("absolute top-2", index === 0 ? "" : index === ticks.length - 1 ? "-translate-x-full" : "-translate-x-1/2")}
                        style={{ left: `${(tick / spanMs) * 100}%` }}
                    >
                        {index === 0 ? `+0\u00a0${unit.label}` : formatDecimal(tick / unit.ms)}
                    </span>
                ))}
            </div>
            <span className="text-right">Calls</span>
            <span className="text-right">Avg wait</span>
            <span className="text-right">Avg run</span>
        </div>
    )
}

type MethodRowProps = { row: MethodCalls; calls: RequestTrace[]; origin: number; scale: Scale }

function MethodRow({ row, calls, origin, scale }: MethodRowProps) {
    const failed = row.calls.some(isFailed)
    const avgWait = averageOf(row.calls, queueTime) ?? 0
    const avgRun = averageOf(row.calls, serviceTime) ?? 0
    return (
        <li className={`${waterfallGrid} h-10 border-b border-border/50 text-xs tabular-nums last:border-b-0`}>
            <KindBadge kind={row.kind} />
            <span translate="no" className={cn("truncate text-[13px]", failed ? "text-danger" : "text-foreground/90")}>
                {row.operation}
            </span>
            <MethodTrack
                label={`${row.operation}: ${formatPlural(row.calls.length, "call")}, average wait ${formatMs(avgWait)}, average run ${formatMs(avgRun)}`}
                row={row}
                calls={calls}
                origin={origin}
                scale={scale}
            />
            <span className={cn("text-right", failed ? "text-danger" : "text-foreground/90")}>{formatCount(row.calls.length)}</span>
            <span className="text-right text-muted-foreground">{formatMs(avgWait)}</span>
            <span className="text-right text-foreground/90">{formatMs(avgRun)}</span>
        </li>
    )
}

function isFailed(call: RequestTrace): boolean {
    return call.outcome !== "completed"
}

function KindBadge({ kind }: { kind: RequestTrace["kind"] }) {
    return (
        <span className={cn("rounded-sm border py-0.5 text-center text-[10px] font-medium tracking-wide uppercase", kind === "websocket" ? "border-chart-2/35 text-chart-2" : "text-foreground")}>
            {kindLabels[kind]}
        </span>
    )
}

type MethodTrackProps = MethodRowProps & { label: string }

function MethodTrack({ label, row, calls, origin, scale: { spanMs, ticks } }: MethodTrackProps) {
    const at = (ms: number) => `${(ms / spanMs) * 100}%`
    return (
        <div
            role="img"
            aria-label={label}
            className="relative h-full bg-[linear-gradient(90deg,color-mix(in_oklab,var(--border)_55%,transparent)_1px,transparent_1px)]"
            style={{ backgroundSize: `${100 / (ticks.length - 1)}% 100%` }}
        >
            {row.calls.map(call => (
                <CallBar key={call.eventId} call={call} blocker={blockerOf(call, calls)} left={at(call.startedAtMs - origin)} width={at(call.durationMs)} />
            ))}
        </div>
    )
}

function blockerOf(call: RequestTrace, calls: RequestTrace[]): RequestTrace | undefined {
    if (!call.queueWaitMs) return undefined
    return calls.find(other => other !== call && other.startedAtMs + (other.queueWaitMs ?? 0) <= call.startedAtMs && call.startedAtMs < other.startedAtMs + other.durationMs)
}

type CallBarProps = { call: RequestTrace; blocker: RequestTrace | undefined; left: string; width: string }

function CallBar({ call, blocker, left, width }: CallBarProps) {
    const queueShare = call.durationMs > 0 ? (call.queueWaitMs ?? 0) / call.durationMs : 0
    const failed = isFailed(call)
    return (
        <Tooltip>
            <TooltipTrigger asChild>
                <span className="group absolute top-1/2 flex h-3 min-w-0.75 -translate-y-1/2 before:absolute before:-inset-y-2 before:inset-x-0" style={{ left, width }}>
                    {queueShare > 0 && <span className={`h-full rounded-l-[2px] ${queueStripes}`} style={{ width: `${queueShare * 100}%` }} />}
                    <span
                        className={cn(
                            "h-full min-w-0.75 flex-1 rounded-[2px] outline-offset-2 group-hover:outline-2 group-hover:outline-foreground",
                            queueShare > 0 && "rounded-l-none",
                            failed ? "bg-danger" : "bg-foreground"
                        )}
                    />
                    {failed && <span className="absolute top-1/2 left-full ml-2 -translate-y-1/2 text-[11px] whitespace-nowrap text-danger">{outcomeLabels[call.outcome]}</span>}
                </span>
            </TooltipTrigger>
            <TooltipContent className="tabular-nums">
                <CallDetail call={call} blocker={blocker} />
            </TooltipContent>
        </Tooltip>
    )
}

function CallDetail({ call, blocker }: { call: RequestTrace; blocker: RequestTrace | undefined }) {
    const runMs = serviceTime(call) ?? call.durationMs
    return (
        <span>
            {call.queueWaitMs ? (
                <>
                    Waited <b className="font-medium">{formatMs(call.queueWaitMs)}</b>
                    {blocker && (
                        <>
                            {" "}
                            behind{" "}
                            <b translate="no" className="font-medium">
                                {blocker.operation}
                            </b>
                        </>
                    )}{" "}
                    · ran <b className="font-medium">{formatMs(runMs)}</b>
                </>
            ) : (
                <>
                    Ran <b className="font-medium">{formatMs(runMs)}</b>
                </>
            )}
            {isFailed(call) && ` · ${outcomeLabels[call.outcome]}`}
        </span>
    )
}

function WaterfallFooter({ calls, methods, total }: { calls: RequestTrace[]; methods: number; total: number }) {
    const waited = calls.filter(call => (call.queueWaitMs ?? 0) > 0).length
    const errors = calls.filter(isFailed).length
    return (
        <div className="flex justify-between border-t pt-3 text-xs text-muted-foreground tabular-nums">
            <span>
                {formatPlural(calls.length, "call")} across {formatPlural(methods, "method")} · {formatCount(waited)} waited in queue
                {errors > 0 && ` · ${formatPlural(errors, "error")}`}
            </span>
            {calls.length < total && (
                <span>
                    Showing the latest {formatCount(calls.length)} of {formatCount(total)}
                </span>
            )}
        </div>
    )
}

function WaterfallSkeleton() {
    return (
        <div className="mt-3.5 pb-1">
            <Skeleton className="h-10 w-full rounded-sm" />
            {Array.from({ length: 5 }, (_, index) => (
                <div key={index} className={`${waterfallGrid} h-10`}>
                    <Skeleton className="h-4" />
                    <Skeleton className="h-3.5 w-28" />
                    <Skeleton className="h-3 rounded-[2px]" style={{ width: `${20 + index * 12}%`, marginLeft: `${index * 14}%` }} />
                    <Skeleton className="ml-auto h-3.5 w-6" />
                    <Skeleton className="ml-auto h-3.5 w-10" />
                    <Skeleton className="ml-auto h-3.5 w-10" />
                </div>
            ))}
        </div>
    )
}
