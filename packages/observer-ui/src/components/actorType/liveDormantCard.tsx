import { formatCount, formatPercent } from "../../format.js"
import type { ActorTypeSummary } from "../../lib/actorTypes.js"
import { ResidencyBar, ResidencyBarSkeleton, ResidencyLegend } from "../ui/residencyBar.js"
import { Skeleton } from "../ui/skeleton.js"
import { StatCard, StatFigure } from "../ui/statCard.js"

export function LiveDormantCard({ summary, failed }: { summary: ActorTypeSummary | null; failed: boolean }) {
    const total = summary && <span className="shrink-0 text-xs font-normal tabular-nums">{formatCount(summary.total)} total</span>
    return (
        <StatCard label="Live & Dormant" aside={total} busy={!summary && !failed}>
            {summary ? <Residency {...summary} /> : <ResidencySkeleton failed={failed} />}
        </StatCard>
    )
}

function Residency({ live, dormant, total }: ActorTypeSummary) {
    const liveShare = total ? live / total : 0
    return (
        <>
            <div className="mt-3.5 flex flex-wrap gap-x-10 gap-y-2">
                <StatFigure value={formatCount(live)} unit="live" />
                <StatFigure value={formatCount(dormant)} unit="dormant" className="text-muted-foreground/75" />
            </div>
            <ResidencyBar liveShare={liveShare} label={`${formatCount(live)} live, ${formatCount(dormant)} dormant, ${formatPercent(liveShare)} live`} pulse className="mt-auto h-10 rounded-sm" />
            <ResidencyLegend live={live} dormant={dormant} />
        </>
    )
}

function ResidencySkeleton({ failed }: { failed: boolean }) {
    if (failed) return <p className="mt-3.5 text-[13px] text-muted-foreground">Couldn't reach the observer API.</p>
    return (
        <>
            <Skeleton className="mt-3.5 h-11 w-48 max-w-full" />
            <ResidencyBarSkeleton />
        </>
    )
}
