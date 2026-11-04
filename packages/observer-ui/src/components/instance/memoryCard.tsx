import { formatMegabytes, formatPercent } from "../../format.js"
import type { MemoryUsage } from "../../types/Resources.js"
import { ResidencyBar } from "../ui/residencyBar.js"
import { Skeleton } from "../ui/skeleton.js"
import { StatCard } from "../ui/statCard.js"

import { ResourceSkeleton } from "./storageCard.js"

export type MemoryCardProps = {
    memory: MemoryUsage | null
    loading: boolean
    failed: boolean
}

export function MemoryCard({ memory, loading, failed }: MemoryCardProps) {
    const value = memory ? formatMegabytes(memory.bytes) : loading ? <Skeleton className="h-11 w-20" /> : failed ? "—" : "0"
    return (
        <StatCard label="Memory" aside={<span className="shrink-0 text-xs font-normal">while live</span>} value={value} unit="MB" busy={loading}>
            {memory ? <MemoryMeter {...memory} /> : loading || failed ? <ResourceSkeleton failed={failed} /> : <p className="mt-2 text-[13px] text-muted-foreground">Not in memory while dormant.</p>}
        </StatCard>
    )
}

function MemoryMeter({ bytes, limitBytes }: MemoryUsage) {
    const share = bytes / limitBytes
    return (
        <>
            <div className="mt-2 text-[13px] text-muted-foreground tabular-nums">
                of <b className="font-bold text-foreground">{formatMegabytes(limitBytes)}&nbsp;MB</b> limit · {formatPercent(share)}
            </div>
            <ResidencyBar liveShare={share} label={`${formatMegabytes(bytes)} of ${formatMegabytes(limitBytes)} MB memory in use`} className="mt-auto h-10 rounded-sm" />
            <div className="mt-1.5 flex justify-between text-xs text-muted-foreground tabular-nums">
                <span>0</span>
                <span>{formatMegabytes(limitBytes)}&nbsp;MB</span>
            </div>
        </>
    )
}
