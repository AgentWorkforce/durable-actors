import { useData } from "../../client/DatatContext.js"
import { useTimeWindow } from "../../client/TimeRangeContext.js"
import { formatClock, formatDecimal } from "../../format.js"
import { average, requestRates } from "../../lib/requestStats.js"
import { bucketCount, bucketEnd, bucketLabel, describeWindow, windowTicks } from "../../lib/timeRange.js"
import type { RequestRateMetrics } from "../../types/Overview.js"
import type { RequestTrace } from "../../types/Requests.js"
import type { TimeWindow } from "../../types/TimeRange.js"
import { Skeleton } from "../ui/skeleton.js"
import { TrendCard, TrendStat, TrendStatsSkeleton } from "../ui/trendCard.js"

export function RequestRateCard() {
    const { requests, isRequestsError } = useData()
    return <RequestRateTrend requests={requests} failed={isRequestsError} timeWindow={useTimeWindow()} color="var(--chart-1)" />
}

export type RequestRateTrendProps = {
    requests: RequestTrace[] | null
    failed: boolean
    timeWindow: TimeWindow
    color: string
}

export function RequestRateTrend({ requests, failed, timeWindow, color }: RequestRateTrendProps) {
    const metrics = requests && getMetrics(requests, timeWindow)
    const loading = !metrics && !failed
    const value = metrics ? formatDecimal(metrics.current) : loading ? <Skeleton className="h-11 w-28" /> : "—"
    const window = describeWindow(timeWindow)
    const series = metrics?.series ?? Array<number>(bucketCount(timeWindow)).fill(0)
    return (
        <TrendCard
            label="Requests / Second"
            window={window}
            value={value}
            unit="req/s"
            busy={loading}
            series={series}
            axis={windowTicks(timeWindow, 2)}
            color={color}
            chartLabel={metrics ? `Requests per second, ${window}, peaking at ${formatDecimal(metrics.peak)}` : `Requests per second, ${window}`}
            describePoint={index => ({ at: bucketLabel(timeWindow, index), value: `${formatDecimal(series[index] ?? 0)} req/s` })}
        >
            {metrics ? <RateStats {...metrics} /> : <TrendStatsSkeleton failed={failed} />}
        </TrendCard>
    )
}

function getMetrics(requests: RequestTrace[], timeWindow: TimeWindow): RequestRateMetrics {
    const series = requestRates(requests, timeWindow)
    const peak = Math.max(...series)
    return {
        current: series.at(-1) ?? 0,
        average: average(series),
        peak,
        peakAt: formatClock(bucketEnd(timeWindow, series.indexOf(peak))),
        series
    }
}

function RateStats({ average, peak, peakAt }: RequestRateMetrics) {
    return (
        <>
            <TrendStat label="Avg" value={formatDecimal(average)} />
            <TrendStat label="Peak" value={formatDecimal(peak)} suffix={`at ${peakAt}`} />
        </>
    )
}
