import { formatCount, formatMegabytes } from "../../format.js"
import type { StorageUsage } from "../../types/Resources.js"
import { BarChart } from "../ui/barChart.js"
import { Skeleton } from "../ui/skeleton.js"
import { StatCard } from "../ui/statCard.js"

export function StorageCard({ storage, failed }: { storage: StorageUsage | null; failed: boolean }) {
    const loading = !storage && !failed
    const value = storage ? formatMegabytes(storage.bytes) : loading ? <Skeleton className="h-11 w-20" /> : "—"
    return (
        <StatCard label="SQLite Storage" aside={<span className="shrink-0 text-xs font-normal">on disk</span>} value={value} unit="MB" busy={loading}>
            {storage ? <StorageHistory {...storage} /> : <ResourceSkeleton failed={failed} />}
        </StatCard>
    )
}

function StorageHistory({ tables, rows, dailyBytes }: StorageUsage) {
    const first = dailyBytes[0] ?? 0
    const latest = dailyBytes.at(-1) ?? 0
    const today = latest - (dailyBytes.at(-2) ?? latest)
    return (
        <>
            <div className="mt-2 text-[13px] text-muted-foreground tabular-nums">
                <b className="font-bold text-foreground">{formatCount(tables)}</b> tables · <b className="font-bold text-foreground">{formatCount(rows)}</b> rows
            </div>
            <BarChart
                values={dailyBytes}
                highlighted={dailyBytes.length - 1}
                label={`Storage grew from ${formatMegabytes(first)} MB to ${formatMegabytes(latest)} MB over the last ${dailyBytes.length} days`}
                className="mt-auto h-16"
            />
            <div className="mt-1.5 flex justify-between text-xs text-muted-foreground tabular-nums">
                <span>+{formatMegabytes(today)}&nbsp;MB today</span>
                <span>{dailyBytes.length}&nbsp;d</span>
            </div>
        </>
    )
}

export function ResourceSkeleton({ failed }: { failed: boolean }) {
    if (failed) return <p className="mt-2 text-[13px] text-muted-foreground">Couldn't reach the observer API.</p>
    return (
        <>
            <Skeleton className="mt-2 h-3.5 w-32" />
            <Skeleton className="mt-auto h-16 w-full rounded-sm" />
            <Skeleton className="mt-1.5 h-3 w-full" />
        </>
    )
}
