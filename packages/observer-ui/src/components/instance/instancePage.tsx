import { useState } from "react"
import { useParams } from "react-router"

import { useData } from "../../client/DatatContext.js"
import { useTimeWindow } from "../../client/TimeRangeContext.js"
import { formatAgo, formatClock, formatDuration } from "../../format.js"
import { useInstanceRequests } from "../../hooks/useActorRequests.js"
import { useEventHistory } from "../../hooks/useEventHistory.js"
import { useRequestHistogram } from "../../hooks/useRequests.js"
import { useResidency } from "../../hooks/useResidency.js"
import { useResources } from "../../hooks/useResources.js"
import { averageQueueWait, averageRunTime } from "../../lib/requestStats.js"
import { describeWindow } from "../../lib/timeRange.js"
import type { ActorInstance } from "../../types/Actors.js"
import type { ResidencyPeriod } from "../../types/Residency.js"
import type { TimeWindow } from "../../types/TimeRange.js"
import { InstanceStatus } from "../actorType/instanceStatus.js"
import { groupLivePeriods } from "../actorType/residencyCard.js"
import { TimingCard } from "../actorType/timingCard.js"
import { RequestRateTrend } from "../overview/requestRateCard.js"
import { TimeRangeControl } from "../timeRange/timeRangeControl.js"
import { PageHeader } from "../ui/pageHeader.js"
import { Skeleton } from "../ui/skeleton.js"

import { ConnectionsCard } from "./connectionsCard.js"
import { EventsCard } from "./eventsCard.js"
import { LogsCard, logsCardId } from "./logsCard.js"
import { RequestsCard } from "./requestsCard.js"
import { CpuCard, MemoryCard, StorageCard } from "./resourceCards.js"

export function InstancePage() {
    const { actorName = "", actorId = "" } = useParams()
    const { baseUrl, events, actorTypes, isError, sessions, isSocketsError } = useData()
    const timeWindow = useTimeWindow()
    const { requests: ownRequests, isError: isRequestsError } = useInstanceRequests(actorName, actorId)
    const { histogram, isError: isHistogramError } = useRequestHistogram(baseUrl, timeWindow, { actorName, actorId }, events)
    const { residency, isError: isResidencyError } = useResidency(actorName, timeWindow, actorId)
    const { resources, isError: isResourcesError } = useResources(actorName, actorId, timeWindow)
    const { history: killHistory } = useEventHistory(["kill"], timeWindow, actorName, actorId)
    const kills = (killHistory?.events ?? []).map(event => event.item).reverse()
    const instance = actorTypes?.actors.find(type => type.actorName === actorName)?.instances.find(candidate => candidate.actorId === actorId) ?? null
    const ownSessions = sessions?.filter(session => session.actorName === actorName && session.actorId === actorId) ?? null
    const livePeriods = residency && (groupLivePeriods(residency).get(actorId) ?? [])
    const [logRequestId, setLogRequestId] = useState<string | null>(null)
    function showRequestLogs(requestId: string) {
        setLogRequestId(requestId)
        document.getElementById(logsCardId)?.scrollIntoView({ behavior: "smooth", block: "start" })
    }

    return (
        <section aria-labelledby="instance-heading" className="flex flex-col gap-5">
            <InstanceHeader actorName={actorName} actorId={actorId} instance={instance} livePeriods={livePeriods} timeWindow={timeWindow} loading={!actorTypes && !isError} />
            {actorTypes && !instance ? (
                <p className="text-[13px] text-muted-foreground">
                    No {actorName} instance named {actorId} is deployed.
                </p>
            ) : (
                <div className="flex flex-col gap-4">
                    <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
                        <RequestRateTrend histogram={histogram} failed={isHistogramError} timeWindow={timeWindow} color="var(--foreground)" />
                        <TimingCard
                            label="Avg Queue Wait"
                            pick={averageQueueWait}
                            color="var(--chart-1)"
                            chartLabel={`Average queue wait, ${describeWindow(timeWindow)}`}
                            histogram={histogram}
                            failed={isHistogramError}
                            timeWindow={timeWindow}
                        />
                        <TimingCard
                            label="Avg Request Time"
                            pick={averageRunTime}
                            color="var(--foreground)"
                            chartLabel={`Average request time excluding the queue, ${describeWindow(timeWindow)}`}
                            histogram={histogram}
                            failed={isHistogramError}
                            timeWindow={timeWindow}
                        />
                        <CpuCard resources={resources} failed={isResourcesError} timeWindow={timeWindow} />
                        <MemoryCard resources={resources} kills={kills} failed={isResourcesError} timeWindow={timeWindow} />
                        <StorageCard resources={resources} failed={isResourcesError} timeWindow={timeWindow} />
                    </div>
                    <RequestsCard requests={ownRequests} failed={isRequestsError} timeWindow={timeWindow} onShowLogs={showRequestLogs} />
                    <ConnectionsCard
                        instance={instance}
                        sessions={ownSessions}
                        livePeriods={livePeriods}
                        requests={ownRequests}
                        failed={isError || isSocketsError || isResidencyError}
                        timeWindow={timeWindow}
                    />
                    <LogsCard actorName={actorName} actorId={actorId} requests={ownRequests} requestId={logRequestId} onRequestChange={setLogRequestId} timeWindow={timeWindow} />
                    <EventsCard actorName={actorName} actorId={actorId} timeWindow={timeWindow} />
                </div>
            )}
        </section>
    )
}

type InstanceHeaderProps = {
    actorName: string
    actorId: string
    instance: ActorInstance | null
    livePeriods: ResidencyPeriod[] | null
    timeWindow: TimeWindow
    loading: boolean
}

function InstanceHeader({ actorName, actorId, instance, livePeriods, timeWindow, loading }: InstanceHeaderProps) {
    const live = instance?.status === "live"
    return (
        <PageHeader
            titleId="instance-heading"
            title={actorId}
            back={{ to: "../..", relative: "path", label: actorName }}
            badge={instance && <StatusPill live={live} />}
            meta={
                loading ? (
                    <Skeleton className="h-3.5 w-64" />
                ) : (
                    <>
                        <span translate="no">{actorName}</span>
                        {instance && (
                            <>
                                <span aria-hidden>·</span>
                                <span>{residencyLabel(instance, livePeriods, timeWindow)}</span>
                            </>
                        )}
                    </>
                )
            }
            aside={<TimeRangeControl />}
        />
    )
}

function StatusPill({ live }: { live: boolean }) {
    return (
        <InstanceStatus
            live={live}
            className={live ? "rounded-full border border-success/25 bg-success/15 py-0.75 pr-2.25 pl-2 text-xs font-medium" : "rounded-full border px-2.25 py-0.75 text-xs font-medium"}
        >
            {live ? "Live" : "Dormant"}
        </InstanceStatus>
    )
}

function residencyLabel(instance: ActorInstance, livePeriods: ResidencyPeriod[] | null, { fromMs, toMs }: TimeWindow): string {
    if (instance.status !== "live") return instance.lastActiveAtMs ? `Last active ${formatAgo(instance.lastActiveAtMs, toMs)}` : "Dormant"
    const since = livePeriods?.at(-1)?.fromMs
    if (since === undefined) return "Live"
    return since <= fromMs ? "Live for the whole window" : `Live since ${formatClock(since)} (${formatDuration(toMs - since)})`
}
