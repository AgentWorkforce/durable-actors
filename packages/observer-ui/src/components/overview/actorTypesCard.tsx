import { Link } from "react-router"

import { useData } from "../../client/DatatContext.js"
import { formatCount, formatPercent, formatPlural } from "../../format.js"
import { type ActorTypeSummary, actorTypePath, summarizeActorType } from "../../lib/actorTypes.js"
import { ResidencyBar } from "../ui/residencyBar.js"
import { SectionCard } from "../ui/sectionCard.js"
import { Skeleton } from "../ui/skeleton.js"

export function ActorTypesCard() {
    const { actorTypes, isError } = useData()
    const rows = actorTypes && actorTypes.actors.map(summarizeActorType).sort((a, b) => b.total - a.total)
    return (
        <SectionCard title="Actor Types" aside={<ResidencyKey />} busy={!rows && !isError} className="pb-2.5">
            <div className="mt-1.5">
                <ActorTypeLanes rows={rows} failed={isError} />
            </div>
        </SectionCard>
    )
}

function ResidencyKey() {
    return (
        <div className="flex gap-4 text-xs text-muted-foreground">
            <span className="flex items-center gap-1.5">
                <span className="size-2 rounded-xs bg-success" />
                Live
            </span>
            <span className="flex items-center gap-1.5">
                <span className="size-2 rounded-xs bg-border" />
                Dormant
            </span>
        </div>
    )
}

function ActorTypeLanes({ rows, failed }: { rows: ActorTypeSummary[] | null; failed: boolean }) {
    if (failed) return <p className="py-4 text-[13px] text-muted-foreground">Couldn't reach the observer API.</p>
    if (!rows) return <LaneSkeletons />
    if (!rows.length) return <p className="py-4 text-[13px] text-muted-foreground">No actor types deployed yet.</p>
    const largest = Math.max(...rows.map(row => row.total), 1)
    return (
        <ul>
            {rows.map(row => (
                <ActorTypeLane key={row.actorName} row={row} largest={largest} />
            ))}
        </ul>
    )
}

const laneGrid = "grid items-center gap-x-7 gap-y-3 py-4 md:grid-cols-[190px_1fr_168px]"
const laneDivider = "border-t border-border/60 first:border-t-0"

function ActorTypeLane({ row, largest }: { row: ActorTypeSummary; largest: number }) {
    const liveShare = row.total ? row.live / row.total : 0
    return (
        <li className={laneDivider}>
            <Link
                to={actorTypePath(row.actorName)}
                className={`${laneGrid} -mx-3 rounded-lg px-3 tabular-nums transition-colors outline-none hover:bg-muted/50 focus-visible:ring-2 focus-visible:ring-ring`}
            >
                <div className="min-w-0">
                    <b translate="no" className="block truncate text-sm font-medium">
                        {row.actorName}
                    </b>
                    <span className="mt-0.75 block text-xs text-muted-foreground">{formatPlural(row.total, "instance")}</span>
                </div>
                <ResidencyTrack row={row} liveShare={liveShare} width={row.total / largest} />
                <div className="grid grid-cols-2 gap-5 text-right">
                    <div>
                        <b className="block text-lg leading-tight font-bold tracking-tight">{formatCount(row.live)}</b>
                        <span className="mt-0.75 block text-xs text-success">live · {formatPercent(liveShare)}</span>
                    </div>
                    <div>
                        <b className="block text-lg leading-tight font-medium tracking-tight text-foreground/85">{formatCount(row.dormant)}</b>
                        <span className="mt-0.75 block text-xs text-muted-foreground">dormant</span>
                    </div>
                </div>
            </Link>
        </li>
    )
}

function ResidencyTrack({ row, liveShare, width }: { row: ActorTypeSummary; liveShare: number; width: number }) {
    return (
        <div className="relative flex h-5.5 items-center before:absolute before:inset-x-0 before:top-1/2 before:h-px before:bg-border/60">
            <ResidencyBar
                liveShare={liveShare}
                label={`${formatCount(row.live)} of ${formatCount(row.total)} live, ${formatPercent(liveShare)}`}
                className="h-5.5 min-w-2.5 rounded-[3px]"
                style={{ width: `${width * 100}%` }}
            />
        </div>
    )
}

function LaneSkeletons() {
    return (
        <ul>
            {[100, 60, 20].map(width => (
                <li key={width} className={`${laneGrid} ${laneDivider}`}>
                    <div>
                        <Skeleton className="h-4 w-28" />
                        <Skeleton className="mt-1.5 h-3 w-20" />
                    </div>
                    <Skeleton className="h-5.5 rounded-[3px]" style={{ width: `${width}%` }} />
                    <Skeleton className="ml-auto h-8 w-32" />
                </li>
            ))}
        </ul>
    )
}
