import { Link } from "react-router"

import { formatPreciseDateTime } from "../../format.js"
import { instancePath } from "../../lib/actorTypes.js"
import { cn } from "../../lib/utils.js"
import type { ActorKill, KillReason } from "../../types/Kills.js"
import type { LogLevel } from "../../types/Logs.js"
import { ExpandFooter } from "../ui/expandFooter.js"
import { Skeleton } from "../ui/skeleton.js"

import { LevelBadge } from "./logList.js"

const previewRows = 10
const labels: Record<KillReason, string> = {
    out_of_memory: "Out of memory",
    out_of_disk: "Out of disk",
    evicted: "Evicted",
    killed: "Killed"
}
const levels: Record<KillReason, LogLevel> = {
    out_of_memory: "error",
    out_of_disk: "error",
    evicted: "warn",
    killed: "warn"
}
const messages: Record<KillReason, string> = {
    out_of_memory: "Actor used more memory than it's limit",
    out_of_disk: "Sandbox used more disk than its limit.",
    evicted: "Sandbox evicted by its node.",
    killed: "Sandbox killed by a signal the platform didn't send."
}

export type EventListProps = {
    events: ActorKill[]
    noun: string
    /** Adds a column that names each event's instance, for a list that covers more than one. */
    showInstance?: boolean
    expanded: boolean
    onToggle: () => void
}

export function EventList({ events, noun, showInstance = false, expanded, onToggle }: EventListProps) {
    if (!events.length) return <p className="py-8 text-center text-[13px] text-muted-foreground">No {noun}s in this time range.</p>
    const rows = expanded ? events : events.slice(0, previewRows)
    return (
        <>
            <div className={`${rowGrid(showInstance)} mt-3.5 h-7.5 border-b px-2 text-xs text-muted-foreground`}>
                <span>Time</span>
                <span>Event</span>
                {showInstance && <span>Instance</span>}
                <span>Message</span>
            </div>
            <ol>
                {rows.map(event => (
                    <EventRow key={`${event.actorId}:${event.reason}:${event.atMs}`} event={event} showInstance={showInstance} />
                ))}
            </ol>
            {events.length > previewRows && <ExpandFooter shown={rows.length} total={events.length} noun={noun} expandLabel="Show All" expanded={expanded} onToggle={onToggle} />}
        </>
    )
}

export function EventRowSkeletons({ showInstance = false }: { showInstance?: boolean }) {
    return (
        <div className="mt-3.5 py-2">
            {Array.from({ length: 4 }, (_, index) => (
                <div key={index} className={`${rowGrid(showInstance)} h-8.5 px-2`}>
                    <Skeleton className="h-3.5 w-24" />
                    <Skeleton className="h-4 w-16" />
                    {showInstance && <Skeleton className="h-3.5 w-28" />}
                    <Skeleton className="h-3.5" style={{ width: `${40 + ((index * 23) % 40)}%` }} />
                </div>
            ))}
        </div>
    )
}

function rowGrid(showInstance: boolean): string {
    return cn("grid items-center gap-x-4", showInstance ? "grid-cols-[128px_112px_minmax(0,200px)_minmax(0,1fr)]" : "grid-cols-[128px_112px_minmax(0,1fr)]")
}

function EventRow({ event, showInstance }: { event: ActorKill; showInstance: boolean }) {
    return (
        <li className={cn(rowGrid(showInstance), "min-h-8.5 border-b border-border/55 px-2 text-[12.5px] last:border-b-0")}>
            <span className="font-mono text-xs text-muted-foreground tabular-nums">{formatPreciseDateTime(event.atMs)}</span>
            <LevelBadge level={levels[event.reason]}>{labels[event.reason]}</LevelBadge>
            {showInstance && <InstanceLink actorId={event.actorId} />}
            <span className="min-w-0 py-1.5 text-foreground/95">{messages[event.reason]}</span>
        </li>
    )
}

function InstanceLink({ actorId }: { actorId: string }) {
    return (
        <Link
            to={instancePath(actorId)}
            translate="no"
            className="max-w-full min-w-0 justify-self-start truncate rounded-sm font-medium outline-none hover:underline focus-visible:ring-2 focus-visible:ring-ring"
        >
            {actorId}
        </Link>
    )
}
