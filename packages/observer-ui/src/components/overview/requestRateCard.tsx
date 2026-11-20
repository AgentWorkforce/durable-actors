import { formatDecimal } from "../../format.js"
import { requestTotal } from "../../lib/requestStats.js"
import { bucketCount, describeWindow, windowTicks } from "../../lib/timeRange.js"
import { bucketEndLabel } from "../../lib/views/histogram.js"
import type { RequestHistogram } from "../../types/Requests.js"
import type { TimeWindow } from "../../types/TimeRange.js"
import { Skeleton } from "../ui/skeleton.js"
import { TrendCard } from "../ui/trendCard.js"

export type RequestRateTrendProps = {
    histogram: RequestHistogram | undefined
    failed: boolean
    timeWindow: TimeWindow
    color: string
}

export function RequestRateTrend({ histogram, failed, timeWindow, color }: RequestRateTrendProps) {
    const series = histogram?.buckets.map(counts => requestTotal(counts) / (histogram.bucketMs / 1000))
    const loading = !series && !failed
    const value = series ? formatDecimal(series.at(-1) ?? 0) : loading ? <Skeleton className="h-11 w-28" /> : "—"
    const shown = series ?? Array<number>(bucketCount(timeWindow)).fill(0)
    return (
        <TrendCard
            label="Requests / Second"
            value={value}
            unit="req/s"
            busy={loading}
            failed={!series && failed}
            series={shown}
            axis={windowTicks(timeWindow, 2)}
            color={color}
            chartLabel={`Requests per second, ${describeWindow(timeWindow)}`}
            describePoint={index => ({ at: histogram ? bucketEndLabel(histogram, index) : "", value: `${formatDecimal(shown[index] ?? 0)} req/s` })}
        />
    )
}
