import { useData } from "../../client/DatatContext.js"
import { useTimeWindow } from "../../client/TimeRangeContext.js"
import { formatCount, formatPlural } from "../../format.js"
import { requestTotal } from "../../lib/requestStats.js"
import { bucketCount, describeWindow, windowTicks } from "../../lib/timeRange.js"
import { bucketEndLabel } from "../../lib/views/histogram.js"
import { Skeleton } from "../ui/skeleton.js"
import { TrendCard } from "../ui/trendCard.js"

export function RequestCountCard() {
    const { requestHistogram, isRequestsError } = useData()
    const timeWindow = useTimeWindow()
    const series = requestHistogram?.buckets.map(requestTotal)
    const total = series?.reduce((sum, count) => sum + count, 0)
    const loading = !series && !isRequestsError
    const window = describeWindow(timeWindow)
    const shown = series ?? Array<number>(bucketCount(timeWindow)).fill(0)
    return (
        <TrendCard
            label="Total Requests"
            value={<TotalValue total={total} loading={loading} />}
            unit="requests"
            busy={loading}
            failed={!series && isRequestsError}
            series={shown}
            axis={windowTicks(timeWindow, 2)}
            color="var(--chart-1)"
            chartLabel={total !== undefined ? `Requests, ${window}, ${formatCount(total)} in total` : `Requests, ${window}`}
            describePoint={index => ({ at: requestHistogram ? bucketEndLabel(requestHistogram, index) : "", value: formatPlural(shown[index] ?? 0, "request") })}
        />
    )
}

function TotalValue({ total, loading }: { total: number | undefined; loading: boolean }) {
    if (total !== undefined) return <>{formatCount(total)}</>
    if (loading) return <Skeleton className="h-11 w-28" />
    return <>—</>
}
