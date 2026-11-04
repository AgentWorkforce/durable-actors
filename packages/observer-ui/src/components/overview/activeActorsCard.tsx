import { useData } from "../../client/DatatContext.js"
import { formatCount } from "../../format.js"
import type { ActorInventory } from "../../types/Actors.js"
import type { ActorResidencyMetrics } from "../../types/Overview.js"
import { ResidencyBar, ResidencyBarSkeleton, ResidencyLegend } from "../ui/residencyBar.js"
import { Skeleton } from "../ui/skeleton.js"
import { StatCard } from "../ui/statCard.js"

export function ActiveActorsCard() {
    const { actorTypes, isError } = useData()
    const metrics = actorTypes && getMetrics(actorTypes)
    const loading = !metrics && !isError
    const value = metrics ? formatCount(metrics.live) : loading ? <Skeleton className="h-11 w-28" /> : "—"
    const unit = loading ? <Skeleton className="h-3.5 w-7" /> : "live"
    return (
        <StatCard label="Active Actors" value={value} unit={unit} busy={loading}>
            {metrics ? <Residency {...metrics} /> : <ResidencySkeleton failed={isError} />}
        </StatCard>
    )
}

function getMetrics(actorTypes: ActorInventory): ActorResidencyMetrics {
    const actorInstances = actorTypes.actors.flatMap(actor => actor.instances)
    const live = actorInstances.filter(instance => instance.status === "live").length
    const dormant = actorInstances.filter(instance => instance.status === "dormant").length
    return { live, dormant }
}

function Residency({ live, dormant }: ActorResidencyMetrics) {
    const total = live + dormant
    const liveShare = total ? live / total : 0
    return (
        <>
            <p className="mt-2 text-[13px] text-muted-foreground tabular-nums">
                {formatCount(total)} {total === 1 ? "actor" : "actors"} total
            </p>
            <ResidencyBar liveShare={liveShare} label={`${formatCount(live)} live actors, ${formatCount(dormant)} dormant actors`} pulse className="mt-auto h-10 rounded-sm" />
            <ResidencyLegend live={live} dormant={dormant} showCounts />
        </>
    )
}

function ResidencySkeleton({ failed }: { failed: boolean }) {
    if (failed) return <p className="mt-2 text-[13px] text-muted-foreground">Couldn't reach the observer API.</p>
    return (
        <>
            <Skeleton className="mt-2.5 h-3.5 w-52 max-w-full" />
            <ResidencyBarSkeleton />
        </>
    )
}
