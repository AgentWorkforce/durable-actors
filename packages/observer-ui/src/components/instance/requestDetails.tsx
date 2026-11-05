import { formatMs, formatMsValue, formatPreciseDateTime } from "../../format.js"
import { serviceTime } from "../../lib/requestStats.js"
import { cn } from "../../lib/utils.js"
import type { RequestOutcome, RequestTrace } from "../../types/Requests.js"
import { DrawerDescription, DrawerHeader, DrawerTitle } from "../ui/drawer.js"
import { QueueStripes } from "../ui/queueStripes.js"

const outcomeLabels: Record<RequestOutcome, string> = { completed: "Completed", failed: "Failed", rejected: "Rejected", rerouted: "Rerouted", interrupted: "Interrupted" }
const kindLabels: Record<RequestTrace["kind"], string> = { method: "Method", websocket: "WebSocket" }

export type RequestDetailsProps = {
    call: RequestTrace
    blocker: RequestTrace | undefined
}

export function RequestDetails({ call, blocker }: RequestDetailsProps) {
    const failed = call.outcome !== "completed"
    return (
        <>
            <DrawerHeader className="shrink-0 gap-2 border-b px-5 py-4 pr-14">
                <div className="flex items-center gap-2">
                    <HostState state={call.hostState} />
                    <span className={cn("text-xs", failed ? "text-danger" : "text-muted-foreground")}>{outcomeLabels[call.outcome]}</span>
                </div>
                <DrawerTitle translate="no" className="truncate text-lg tracking-tight">
                    {call.operation}
                </DrawerTitle>
                <DrawerDescription className="text-[13px] tabular-nums">
                    {kindLabels[call.kind]} · {formatPreciseDateTime(call.startedAtMs)}
                </DrawerDescription>
            </DrawerHeader>
            <RequestFacts call={call} blocker={blocker} />
        </>
    )
}

function RequestFacts({ call, blocker }: { call: RequestTrace; blocker: RequestTrace | undefined }) {
    const queued = call.queueWaitMs
    const running = queued === null ? null : serviceTime(call)
    const queueShare = queued !== null && call.durationMs > 0 ? (queued / call.durationMs) * 100 : 0
    return (
        <div className="min-h-0 flex-1 overflow-y-auto px-5 py-4">
            <div className="grid grid-cols-3 gap-3">
                <TimingValue label="Queue" ms={queued} />
                <TimingValue label="In actor" ms={running} />
                <TimingValue label="Overall" ms={call.durationMs} />
            </div>
            <div className="mt-3.5 flex h-2 overflow-hidden rounded-full bg-muted" aria-hidden>
                {queueShare > 0 && <QueueStripes style={{ width: `${queueShare}%` }} />}
                <span className={cn("min-w-0.5 flex-1", call.outcome === "completed" ? "bg-foreground" : "bg-danger")} />
            </div>
            {queued === null ? (
                <p className="mt-2.5 text-xs text-muted-foreground">Did not begin processing.</p>
            ) : (
                blocker && (
                    <p className="mt-2.5 text-xs text-muted-foreground">
                        Waited behind{" "}
                        <b translate="no" className="font-medium text-foreground">
                            {blocker.operation}
                        </b>
                        .
                    </p>
                )
            )}
            <dl className="mt-5 border-t">
                <Detail label="Request ID" value={call.requestId} mono />
                <Detail label="Routing" value={formatMs(call.routingMs)} />
                <Detail label="Kind" value={kindLabels[call.kind]} />
                <Detail label="Outcome" value={outcomeLabels[call.outcome]} danger={call.outcome !== "completed"} />
                <Detail label="Actor" value={call.actorName} mono />
                <Detail label="Instance" value={call.actorId} mono />
                <Detail label="Host" value={call.hostId} mono />
                <Detail label="Session" value={call.sessionId} mono />
                {call.connectionId && <Detail label="Connection" value={call.connectionId} mono />}
            </dl>
        </div>
    )
}

function TimingValue({ label, ms }: { label: string; ms: number | null }) {
    return (
        <div>
            <div className="text-xs text-muted-foreground">{label}</div>
            <div className="mt-1.5 flex items-baseline gap-1.5 text-[22px] leading-none font-medium tracking-tight tabular-nums">
                {ms === null ? (
                    "—"
                ) : (
                    <>
                        {formatMsValue(ms)}
                        <span className="text-xs font-medium tracking-normal text-muted-foreground">ms</span>
                    </>
                )}
            </div>
        </div>
    )
}

function HostState({ state }: { state: RequestTrace["hostState"] }) {
    const warm = state === "warm"
    return (
        <span
            className={cn(
                "inline-flex items-center gap-1.5 rounded-full border px-2 py-0.5 text-[11px] font-medium tracking-wide uppercase",
                warm ? "border-success/25 bg-success/15 text-success" : "border-warning/30 bg-warning/15 text-warning"
            )}
        >
            <span className={cn("size-1.5 rounded-full", warm ? "bg-success" : "bg-warning")} />
            {warm ? "Warm" : "Cold"}
        </span>
    )
}

function Detail({ label, value, mono, danger }: { label: string; value: string; mono?: boolean; danger?: boolean }) {
    return (
        <div className="grid grid-cols-[108px_minmax(0,1fr)] gap-3 border-b border-border/60 py-2.5">
            <dt className="text-xs text-muted-foreground">{label}</dt>
            <dd translate={mono ? "no" : undefined} className={cn("text-[13px] text-foreground/90", mono && "font-mono text-xs break-all", danger && "text-danger")}>
                {value}
            </dd>
        </div>
    )
}
