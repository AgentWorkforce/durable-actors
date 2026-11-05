import { useData } from "../../client/DatatContext.js"
import { useTimeWindow } from "../../client/TimeRangeContext.js"
import { formatClock, formatCount, formatDecimal, formatPlural } from "../../format.js"
import { requestCounts } from "../../lib/requestStats.js"
import { bucketCount, bucketEnd, bucketLabel, describeWindow, windowTicks } from "../../lib/timeRange.js"
import type { RequestCountMetrics } from "../../types/Overview.js"
import type { RequestTrace } from "../../types/Requests.js"
import type { TimeWindow } from "../../types/TimeRange.js"
import { Skeleton } from "../ui/skeleton.js"
import { TrendCard, TrendStat, TrendStatsSkeleton } from "../ui/trendCard.js"

export function RequestCountCard() {
    const { requests, isRequestsError } = useData()
    const timeWindow = useTimeWindow()
    const metrics = requests && getMetrics(requests, timeWindow)
    const loading = !metrics && !isRequestsError
    const value = <TotalValue total={metrics?.total} loading={loading} />
    const window = describeWindow(timeWindow)
    const series = metrics?.series ?? Array<number>(bucketCount(timeWindow)).fill(0)
    return (
        <TrendCard
            label="Total Requests"
            window={window}
            value={value}
            unit="requests"
            busy={loading}
            series={series}
            axis={windowTicks(timeWindow, 2)}
            color="var(--chart-1)"
            chartLabel={metrics ? `Requests, ${window}, ${formatCount(metrics.total)} in total` : `Requests, ${window}`}
            describePoint={index => ({ at: bucketLabel(timeWindow, index), value: formatPlural(series[index] ?? 0, "request") })}
        >
            {metrics ? <CountStats {...metrics} /> : <TrendStatsSkeleton failed={isRequestsError} />}
        </TrendCard>
    )
}

function TotalValue({ total, loading }: { total: number | undefined; loading: boolean }) {
    if (total !== undefined) return <>{formatCount(total)}</>
    if (loading) return <Skeleton className="h-11 w-28" />
    return <>—</>
}

function getMetrics(requests: RequestTrace[], timeWindow: TimeWindow): RequestCountMetrics {
    const series = requestCounts(requests, timeWindow)
    const total = series.reduce((sum, count) => sum + count, 0)
    const peak = Math.max(...series)
    return {
        total,
        perSecond: total / ((timeWindow.toMs - timeWindow.fromMs) / 1000),
        peak,
        peakAt: formatClock(bucketEnd(timeWindow, series.indexOf(peak))),
        series
    }
}

function CountStats({ perSecond, peak, peakAt }: RequestCountMetrics) {
    return (
        <>
            <TrendStat label="Avg" value={formatDecimal(perSecond)} suffix="req/s" />
            <TrendStat label="Peak" value={formatCount(peak)} suffix={`at ${peakAt}`} />
        </>
    )
}
