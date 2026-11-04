import { useId, useState } from "react"
import { Link } from "react-router"

import { formatAgo, formatCount, formatMs, formatPlural } from "../../format.js"
import { byLastActive, instancePath, summarizeActorType } from "../../lib/actorTypes.js"
import { averageOf, queueTime, serviceTime } from "../../lib/requestStats.js"
import { cn } from "../../lib/utils.js"
import type { ActorInstance, ActorType } from "../../types/Actors.js"
import type { RequestTrace } from "../../types/Requests.js"
import { SectionCard } from "../ui/sectionCard.js"
import { SegmentedControl } from "../ui/segmentedControl.js"
import { Skeleton } from "../ui/skeleton.js"

import { InstanceStatus } from "./instanceStatus.js"

type InstanceFilter = "all" | "live" | "dormant"

const columns = 24
const cellLimit = columns * 10

export type InstancesCardProps = {
    actor: ActorType | null
    requests: RequestTrace[] | null
    failed: boolean
    now: number
}

export function InstancesCard({ actor, requests, failed, now }: InstancesCardProps) {
    const [filter, setFilter] = useState<InstanceFilter>("all")
    const instances = actor && byLastActive(actor.instances)
    const visible = instances?.filter(instance => filter === "all" || instance.status === filter) ?? null
    return (
        <SectionCard
            title="Instances"
            description="Sorted by last active. Number shows open sockets."
            aside={actor && <InstanceFilters actor={actor} value={filter} onChange={setFilter} />}
            busy={!actor && !failed}
            className="pb-4.5"
        >
            {failed ? (
                <p className="py-4 text-[13px] text-muted-foreground">Couldn't reach the observer API.</p>
            ) : actor && visible ? (
                <InstanceGrid actor={actor} instances={visible} requests={requests} now={now} />
            ) : (
                <GridSkeleton />
            )}
        </SectionCard>
    )
}

function InstanceFilters({ actor, value, onChange }: { actor: ActorType; value: InstanceFilter; onChange: (filter: InstanceFilter) => void }) {
    const { total, live, dormant } = summarizeActorType(actor)
    return (
        <SegmentedControl
            label="Filter instances"
            value={value}
            onChange={onChange}
            options={[
                { value: "all", label: "All", badge: formatCount(total) },
                { value: "live", label: "Live", badge: formatCount(live) },
                { value: "dormant", label: "Dormant", badge: formatCount(dormant) }
            ]}
        />
    )
}

function InstanceGrid({ actor, instances, requests, now }: { actor: ActorType; instances: ActorInstance[]; requests: RequestTrace[] | null; now: number }) {
    const [activeId, setActiveId] = useState<string | null>(null)
    if (!instances.length) return <p className="py-4 text-[13px] text-muted-foreground">No instances match this filter.</p>
    const shown = instances.slice(0, cellLimit)
    return (
        <>
            <ColumnRuler />
            <ul aria-label={`${actor.actorName} instances, most recently active first`} className="mt-1.5 grid grid-cols-24 gap-1.5 tabular-nums">
                {shown.map((instance, index) => (
                    <InstanceCell
                        key={instance.actorId}
                        instance={instance}
                        active={instance.actorId === activeId}
                        alignRight={index % columns >= columns / 2}
                        onActive={setActiveId}
                        requests={requests}
                        now={now}
                    />
                ))}
            </ul>
            <GridFooter actor={actor} shown={shown.length} matching={instances.length} />
        </>
    )
}

function ColumnRuler() {
    return (
        <div aria-hidden className="mt-4.5 grid grid-cols-24 gap-1.5 text-[11px] text-muted-foreground tabular-nums">
            <span>001</span>
            <span className="col-start-12">012</span>
            <span className="col-start-24 text-right">024</span>
        </div>
    )
}

type InstanceCellProps = {
    instance: ActorInstance
    active: boolean
    alignRight: boolean
    onActive: (actorId: string | null) => void
    requests: RequestTrace[] | null
    now: number
}

function InstanceCell({ instance, active, alignRight, onActive, requests, now }: InstanceCellProps) {
    const tooltipId = useId()
    const live = instance.status === "live"
    const sockets = instance.connections.length
    return (
        <li className="relative">
            <Link
                to={instancePath(instance.actorId)}
                aria-label={`${instance.actorId}, ${instance.status}, ${formatPlural(sockets, "open socket")}`}
                aria-describedby={active ? tooltipId : undefined}
                onMouseEnter={() => onActive(instance.actorId)}
                onMouseLeave={() => onActive(null)}
                onFocus={() => onActive(instance.actorId)}
                onBlur={() => onActive(null)}
                className={cn(
                    "grid h-6.5 w-full place-items-center rounded-[5px] text-[10px] font-medium outline-none",
                    live ? "bg-success text-background" : "bg-muted shadow-[inset_0_0_0_1px_var(--border)]",
                    active && "outline-2 outline-offset-2 outline-foreground"
                )}
            >
                {sockets > 0 ? formatCount(sockets) : !live && <span className="size-1 rounded-full bg-muted-foreground/40" />}
            </Link>
            {active && <InstanceTooltip id={tooltipId} instance={instance} requests={requests} now={now} alignRight={alignRight} />}
        </li>
    )
}

function InstanceTooltip({ id, instance, requests, now, alignRight }: { id: string; instance: ActorInstance; requests: RequestTrace[] | null; now: number; alignRight: boolean }) {
    const own = requests?.filter(trace => trace.actorId === instance.actorId) ?? []
    const queue = averageOf(own, queueTime)
    const latency = averageOf(own, serviceTime)
    const live = instance.status === "live"
    return (
        <div
            id={id}
            role="tooltip"
            className={cn("absolute top-full z-10 mt-2 w-64 rounded-lg border bg-popover px-3.5 py-3 text-xs text-popover-foreground shadow-(--shadow-popover)", alignRight ? "right-0" : "left-0")}
        >
            <div className="flex items-center justify-between gap-3">
                <span translate="no" className="truncate">
                    {instance.actorId}
                </span>
                <InstanceStatus live={live} className="text-[11px] font-medium">
                    {live ? "Live" : "Dormant"}
                </InstanceStatus>
            </div>
            <TooltipRow label="Open sockets" value={formatCount(instance.connections.length)} />
            <TooltipRow label="Avg queue time" value={queue === null ? "—" : formatMs(queue)} />
            <TooltipRow label="Avg latency" value={latency === null ? "—" : formatMs(latency)} />
            <TooltipRow label="Last active" value={instance.lastActiveAtMs ? formatAgo(instance.lastActiveAtMs, now) : "—"} />
        </div>
    )
}

function TooltipRow({ label, value }: { label: string; value: string }) {
    return (
        <div className="mt-1.5 flex justify-between text-muted-foreground">
            <span>{label}</span>
            <b className="font-medium text-foreground tabular-nums">{value}</b>
        </div>
    )
}

function GridFooter({ actor, shown, matching }: { actor: ActorType; shown: number; matching: number }) {
    const { live, dormant } = summarizeActorType(actor)
    return (
        <div className="mt-4 flex flex-wrap items-center justify-between gap-3 text-[13px] text-muted-foreground tabular-nums">
            <div className="flex flex-wrap gap-5">
                <FooterKey swatch="bg-success" label="Live" count={live} />
                <FooterKey swatch="bg-muted shadow-[inset_0_0_0_1px_var(--border)]" label="Dormant" count={dormant} />
                {shown < matching && (
                    <span>
                        Showing {formatCount(shown)} of {formatCount(matching)}
                    </span>
                )}
            </div>
            <span className="text-xs">Most recent → Least recent</span>
        </div>
    )
}

function FooterKey({ swatch, label, count }: { swatch: string; label: string; count: number }) {
    return (
        <span className="flex items-center gap-1.75">
            <span className={`size-2.5 rounded-[3px] ${swatch}`} />
            {label} <b className="font-bold text-foreground">{formatCount(count)}</b>
        </span>
    )
}

function GridSkeleton() {
    return (
        <div className="mt-6 grid grid-cols-24 gap-1.5">
            {Array.from({ length: columns * 3 }, (_, index) => (
                <Skeleton key={index} className="h-6.5 rounded-[5px]" />
            ))}
        </div>
    )
}
