import type { ReactNode } from "react"

import { Skeleton } from "./skeleton.js"
import { type PointDescription, type Reference, Sparkline } from "./sparkline.js"
import { StatCard } from "./statCard.js"

export type TrendCardProps = {
    label: string
    value: ReactNode
    unit: ReactNode
    busy?: boolean
    failed?: boolean
    series: number[]
    axis: string[]
    color: string
    chartLabel: string
    describePoint: (index: number) => PointDescription
    references?: Reference[]
    /** A row under the figure, such as the chart's legend. */
    children?: ReactNode
}

export function TrendCard({ label, value, unit, busy, failed, series, axis, color, chartLabel, describePoint, references, children }: TrendCardProps) {
    return (
        <StatCard label={label} value={value} unit={busy ? <Skeleton className="h-3.5 w-9" /> : unit} busy={busy}>
            {failed && <p className="mt-2 text-[13px] text-muted-foreground">Couldn't reach the observer API.</p>}
            {children && !busy && <div className="mt-2 flex flex-wrap items-center gap-x-4.5 gap-y-1 text-[13px] text-muted-foreground tabular-nums">{children}</div>}
            {busy ? (
                <TrendSkeleton ticks={axis.length} />
            ) : (
                <>
                    <Sparkline values={series} color={color} label={chartLabel} describePoint={describePoint} references={references} />
                    <div className="mt-1.5 flex justify-between text-xs text-muted-foreground tabular-nums">
                        {axis.map(tick => (
                            <span key={tick}>{tick}</span>
                        ))}
                    </div>
                </>
            )}
        </StatCard>
    )
}

function TrendSkeleton({ ticks }: { ticks: number }) {
    return (
        <>
            <Skeleton className="mt-auto h-16 w-full rounded-sm" />
            <div className="mt-1.5 flex h-4 items-center justify-between">
                {Array.from({ length: ticks }, (_, index) => (
                    <Skeleton key={index} className="h-3 w-9" />
                ))}
            </div>
        </>
    )
}
