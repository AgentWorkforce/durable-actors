import type { ReactNode } from "react"

import { formatBytes, formatClock, formatCpu, formatMebibytes, scaleBytes } from "../../format.js"
import { bucketLatest, bucketMax } from "../../lib/resources.js"
import { bucketLabel, describeWindow, windowTicks } from "../../lib/timeRange.js"
import { cn } from "../../lib/utils.js"
import type { ActorKill, KillReason } from "../../types/Kills.js"
import type { ResourceUsage, SandboxResources, UsagePoint } from "../../types/Resources.js"
import type { TimeWindow } from "../../types/TimeRange.js"
import { Skeleton } from "../ui/skeleton.js"
import type { Reference } from "../ui/sparkline.js"
import { TrendCard } from "../ui/trendCard.js"

const mebibyte = 1_048_576
const noSamples = "No samples in this window. Local development doesn't measure CPU or memory."

const killLabels: Record<KillReason, string> = {
    out_of_memory: "Out of memory",
    out_of_disk: "Out of disk",
    evicted: "Evicted",
    killed: "Killed"
}

export type ResourceCardProps = {
    resources: ResourceUsage | null
    failed: boolean
    timeWindow: TimeWindow
}

export function CpuCard({ resources, failed, timeWindow }: ResourceCardProps) {
    const samples = resources?.usage ?? []
    const latest = latestValue(samples, sample => sample.cpuCores)
    const cpus = (cores: number) => `${formatCpu(cores)} CPU`
    const reserved = reservations(resources, timeWindow, {
        request: sandbox => sandbox.cpuMillis / 1000,
        limit: sandbox => (sandbox.cpuLimitMillis === undefined ? null : sandbox.cpuLimitMillis / 1000),
        format: cpus
    })
    const color = "var(--chart-3)"
    return (
        <ResourceCard
            label="CPU"
            shown={latest === undefined ? undefined : { value: formatCpu(latest), unit: "CPU" }}
            describe={(cores, index) => [`${cpus(cores)} used`, ...reserved.describe(index)]}
            series={bucketMax(samples, timeWindow, sample => sample.cpuCores)}
            references={reserved.references}
            color={color}
            resources={resources}
            failed={failed}
            timeWindow={timeWindow}
        >
            {latest === undefined && <span>{noSamples}</span>}
            <Legend color={color} reserved={reserved} />
        </ResourceCard>
    )
}

export function MemoryCard({ resources, kills, failed, timeWindow }: ResourceCardProps & { kills: ActorKill[] }) {
    const samples = resources?.usage ?? []
    const latest = latestValue(samples, sample => sample.memoryBytes)
    const mebibytes = (bytes: number) => `${formatMebibytes(bytes)} MiB`
    const reserved = reservations(resources, timeWindow, {
        request: sandbox => sandbox.memoryMib * mebibyte,
        limit: sandbox => (sandbox.memoryLimitMib === undefined ? null : sandbox.memoryLimitMib * mebibyte),
        format: mebibytes
    })
    const color = "var(--chart-1)"
    return (
        <ResourceCard
            label="Memory"
            shown={latest === undefined ? undefined : { value: formatMebibytes(latest), unit: "MiB" }}
            describe={(bytes, index) => [`${mebibytes(bytes)} used`, ...reserved.describe(index)]}
            series={bucketMax(samples, timeWindow, sample => sample.memoryBytes)}
            references={reserved.references}
            color={color}
            resources={resources}
            failed={failed}
            timeWindow={timeWindow}
        >
            {latest === undefined && <span>{noSamples}</span>}
            <Legend color={color} reserved={reserved} />
            <Kills kills={kills} />
        </ResourceCard>
    )
}

export function StorageCard({ resources, failed, timeWindow }: ResourceCardProps) {
    const loading = !resources && !failed
    const database = resources?.database ?? []
    const latest = resources?.databaseBytes ?? database.at(-1)?.bytes ?? null
    // A database keeps its size between reports, so each bucket holds the last size seen.
    const series = bucketMax(database, timeWindow, point => point.bytes, database[0]?.bytes ?? latest ?? 0)
    const shown = latest === null ? null : scaleBytes(latest)
    return (
        <TrendCard
            label="SQLite Storage"
            value={shown ? shown.value : loading ? <Skeleton className="h-11 w-20" /> : "—"}
            unit={shown?.unit ?? ""}
            busy={loading}
            failed={!resources && failed}
            series={series}
            axis={windowTicks(timeWindow, 2)}
            color="var(--chart-4)"
            chartLabel={`SQLite database size, ${describeWindow(timeWindow)}`}
            describePoint={index => ({ at: bucketLabel(timeWindow, index), value: formatBytes(series[index] ?? 0) })}
        />
    )
}

function latestValue(samples: UsagePoint[], value: (sample: UsagePoint) => number | null): number | undefined {
    for (let index = samples.length - 1; index >= 0; index--) {
        const found = value(samples[index]!)
        if (found !== null) return found
    }
    return undefined
}

type Reading = {
    request: (sandbox: SandboxResources) => number
    /** The ceiling the sandbox is held to, or null when uncapped. */
    limit: (sandbox: SandboxResources) => number | null
    format: (value: number) => string
}

type Reserved = {
    references: Reference[]
    describe: (index: number) => string[]
    /** The request and limit of the latest sandbox, for the legend. */
    current: { request: string; limit: string | null } | null
}

/** The request and limit in force at each point of the chart, from the sandbox that ran then. */
function reservations(resources: ResourceUsage | null, timeWindow: TimeWindow, reading: Reading): Reserved {
    const sandboxes = resources?.sandboxes ?? []
    const inForce = bucketLatest(
        sandboxes.map(sandbox => ({ ...sandbox, atMs: sandbox.claimedAtMs })),
        timeWindow
    )
    const describeAt = (sandbox: SandboxResources) => {
        const limit = reading.limit(sandbox)
        return { request: reading.format(reading.request(sandbox)), limit: limit === null ? null : reading.format(limit) }
    }
    // Where the limit is the request, the dotted limit line stands for both.
    const requestLine = (sandbox: SandboxResources) => (reading.limit(sandbox) === reading.request(sandbox) ? null : reading.request(sandbox))
    const latest = sandboxes.at(-1)
    return {
        references: [{ values: inForce.map(sandbox => sandbox && requestLine(sandbox)) }, { values: inForce.map(sandbox => sandbox && reading.limit(sandbox)), dotted: true }],
        describe: index => {
            const sandbox = inForce[index]
            if (!sandbox) return []
            const { request, limit } = describeAt(sandbox)
            return [`Requested ${request}`, limit ? `Limit ${limit}` : "No limit"]
        },
        current: latest ? describeAt(latest) : null
    }
}

type CardProps = ResourceCardProps & {
    label: string
    shown: { value: string; unit: string } | undefined
    /** The hover lines for one point, the value first. */
    describe: (value: number, index: number) => string[]
    series: number[]
    references: Reference[]
    color: string
    children: ReactNode
}

function ResourceCard({ label, shown, describe, series, references, color, resources, failed, timeWindow, children }: CardProps) {
    const loading = !resources && !failed
    return (
        <TrendCard
            label={label}
            value={shown ? shown.value : loading ? <Skeleton className="h-11 w-20" /> : "—"}
            unit={shown?.unit ?? ""}
            busy={loading}
            failed={!resources && failed}
            series={series}
            axis={windowTicks(timeWindow, 2)}
            color={color}
            chartLabel={`${label}, ${describeWindow(timeWindow)}`}
            describePoint={index => {
                const [value = "", ...details] = describe(series[index] ?? 0, index)
                return { at: bucketLabel(timeWindow, index), value, details }
            }}
            references={references}
        >
            {resources && children}
        </TrendCard>
    )
}

/** Names the chart's lines: usage in color, the request solid grey, the limit dotted grey. */
function Legend({ color, reserved }: { color: string; reserved: Reserved }) {
    const { current } = reserved
    return (
        <>
            <Key label="Used" swatch={<i className="w-3 border-t-2" style={{ borderColor: color }} />} />
            {current && <Key label={`Requested ${current.request}`} swatch={<i className="w-3 border-t-2 border-muted-foreground" />} />}
            {current && (
                <Key
                    label={current.limit ? `Limit ${current.limit}` : "No limit"}
                    swatch={<i className={cn("w-3 border-t-2 border-dotted border-muted-foreground", !current.limit && "opacity-40")} />}
                />
            )}
        </>
    )
}

function Key({ label, swatch }: { label: string; swatch: ReactNode }) {
    return (
        <span className="inline-flex items-center gap-1.5">
            <span aria-hidden className="inline-flex">
                {swatch}
            </span>
            {label}
        </span>
    )
}

function Kills({ kills }: { kills: ActorKill[] }) {
    const last = kills.at(-1)
    if (!last) return null
    return (
        <span className="text-destructive">
            {killLabels[last.reason]} <b className="font-bold">{formatClock(last.atMs)}</b>
            {kills.length > 1 && ` +${kills.length - 1} more`}
        </span>
    )
}
