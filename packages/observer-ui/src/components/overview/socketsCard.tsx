import { useData } from "../../client/DatatContext.js"
import { formatClock, formatCount, formatPlural } from "../../format.js"
import { sessionSpan } from "../../lib/sockets.js"
import { bucketCount, emptySeries, trendAxis, trendWindowMs } from "../../lib/trendWindow.js"
import type { ActorInventory } from "../../types/Actors.js"
import type { SocketMetrics } from "../../types/Overview.js"
import type { SocketSession } from "../../types/Sockets.js"
import { Skeleton } from "../ui/skeleton.js"
import { TrendCard } from "../ui/trendCard.js"

export function SocketsCard() {
    const { actorTypes, sessions, isError, isSocketsError, nowMs: now } = useData()
    const failed = isError || isSocketsError
    const metrics = actorTypes && sessions && getMetrics(actorTypes, sessions, now)
    const loading = !metrics && !failed
    const value = metrics ? formatCount(metrics.open) : loading ? <Skeleton className="h-11 w-28" /> : "—"
    const series = metrics?.series ?? emptySeries
    return (
        <TrendCard
            label="WebSocket Connections"
            value={value}
            unit="open"
            busy={loading}
            failed={!metrics && failed}
            series={series}
            axis={trendAxis(now)}
            color="var(--chart-2)"
            chartLabel={metrics ? `Open WebSocket connections over the last hour, now ${formatCount(metrics.open)}` : "Open WebSocket connections over the last hour"}
            describePoint={index => ({ at: formatClock(sampleTime(now, index)), value: formatPlural(series[index] ?? 0, "open connection") })}
        />
    )
}

function getMetrics(inventory: ActorInventory, sessions: SocketSession[], now: number): SocketMetrics {
    const instances = inventory.actors.flatMap(actor => actor.instances)
    const open = instances.reduce((sum, instance) => sum + instance.connections.length, 0)
    return { open, series: [...openSeries(sessions, now).slice(0, -1), open] }
}

function openSeries(sessions: SocketSession[], now: number): number[] {
    return Array.from({ length: bucketCount }, (_, index) => sessions.filter(session => isOpenAt(session, sampleTime(now, index))).length)
}

function sampleTime(now: number, index: number): number {
    return now - trendWindowMs + index * (trendWindowMs / (bucketCount - 1))
}

function isOpenAt(session: SocketSession, at: number): boolean {
    const span = sessionSpan(session)
    return span.fromMs <= at && at < span.toMs
}
