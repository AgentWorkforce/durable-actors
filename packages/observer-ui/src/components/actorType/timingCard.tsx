import { formatMs, formatMsParts } from "../../format.js"
import { type CountsAverage, histogramTotals } from "../../lib/requestStats.js"
import { bucketCount, windowTicks } from "../../lib/timeRange.js"
import { bucketEndLabel } from "../../lib/views/histogram.js"
import type { RequestHistogram } from "../../types/Requests.js"
import type { TimeWindow } from "../../types/TimeRange.js"
import { Skeleton } from "../ui/skeleton.js"
import { TrendCard } from "../ui/trendCard.js"

export type TimingCardProps = {
    label: string
    pick: CountsAverage
    color: string
    chartLabel: string
    histogram: RequestHistogram | undefined
    failed: boolean
    timeWindow: TimeWindow
}

export function TimingCard({ label, pick, color, chartLabel, histogram, failed, timeWindow }: TimingCardProps) {
    const series = histogram?.buckets.map(counts => pick(counts) ?? 0)
    const loading = !series && !failed
    const [average, unit] = formatMsParts((histogram && pick(histogramTotals(histogram))) ?? 0)
    const value = series ? average : loading ? <Skeleton className="h-11 w-20" /> : "—"
    const shown = series ?? Array<number>(bucketCount(timeWindow)).fill(0)
    return (
        <TrendCard
            label={label}
            value={value}
            unit={unit}
            busy={loading}
            failed={!series && failed}
            series={shown}
            axis={windowTicks(timeWindow, 2)}
            color={color}
            chartLabel={chartLabel}
            describePoint={index => ({ at: histogram ? bucketEndLabel(histogram, index) : "", value: formatMs(shown[index] ?? 0) })}
        />
    )
}
