import type { CSSProperties } from "react"

import { formatCount, formatPercent } from "../../format.js"
import { cn } from "../../lib/utils.js"

import { Skeleton } from "./skeleton.js"

export type ResidencyBarProps = {
    liveShare: number
    label: string
    pulse?: boolean
    className?: string
    style?: CSSProperties
}

export function ResidencyBar({ liveShare, label, pulse, className, style }: ResidencyBarProps) {
    return (
        <div role="img" aria-label={label} className={cn("relative flex overflow-hidden", className)} style={style}>
            <div className="relative bg-success" style={{ width: `${liveShare * 100}%` }}>
                {pulse && liveShare > 0 && <span className="absolute inset-y-0 right-0.75 w-0.75 animate-pulse bg-success motion-reduce:animate-none" />}
            </div>
            <div className="flex-1 bg-border" />
        </div>
    )
}

export function ResidencyLegend({ live, dormant, showCounts }: { live: number; dormant: number; showCounts?: boolean }) {
    const total = live + dormant
    const liveShare = total ? live / total : 0
    return (
        <div className="mt-3 flex justify-between gap-4 text-[13px] tabular-nums">
            <LegendItem swatch="bg-success" label="Live" count={showCounts ? live : undefined} share={liveShare} />
            <LegendItem swatch="bg-border" label="Dormant" count={showCounts ? dormant : undefined} share={total ? 1 - liveShare : 0} />
        </div>
    )
}

export function ResidencyBarSkeleton() {
    return (
        <>
            <Skeleton className="mt-auto h-10 w-full rounded-sm" />
            <div className="mt-3 flex justify-between gap-4">
                <Skeleton className="h-3.5 w-28" />
                <Skeleton className="h-3.5 w-28" />
            </div>
        </>
    )
}

function LegendItem({ swatch, label, count, share }: { swatch: string; label: string; count?: number; share: number }) {
    return (
        <span className="flex items-center gap-1.75 text-muted-foreground">
            <span className={`size-2 rounded-xs ${swatch}`} />
            {label} {count !== undefined && <b className="font-bold text-foreground">{formatCount(count)}</b>} · {formatPercent(share)}
        </span>
    )
}
