import { useState } from "react"

import { Columns3, Search, X } from "lucide-react"

import { useData } from "../../client/DatatContext.js"
import { useSetTimeRange } from "../../client/TimeRangeContext.js"
import { formatCount } from "../../format.js"
import { useDebouncedValue } from "../../hooks/useDebouncedValue.js"
import { useLogs } from "../../hooks/useLogs.js"
import { countAtLeast, levelTotals, shortRequestId } from "../../lib/logs.js"
import { cn } from "../../lib/utils.js"
import type { LevelCounts, LogLevel } from "../../types/Logs.js"
import type { RequestTrace } from "../../types/Requests.js"
import type { TimeWindow } from "../../types/TimeRange.js"
import { Button } from "../ui/button.js"
import { Popover, PopoverContent, PopoverTrigger } from "../ui/popover.js"
import { SectionCard } from "../ui/sectionCard.js"
import { SegmentedControl } from "../ui/segmentedControl.js"

import { LogChart, levelFill } from "./logChart.js"
import { type LogColumns, LogList } from "./logList.js"

type LevelChoice = "all" | Exclude<LogLevel, "debug">

const levelChoices: { value: LevelChoice; label: string }[] = [
    { value: "all", label: "All" },
    { value: "info", label: "Info+" },
    { value: "warn", label: "Warn+" },
    { value: "error", label: "Error" }
]
export const logsCardId = "logs"
const searchDelayMs = 300
const defaultColumns: LogColumns = { level: true, request: false }
const columnsStorageKey = "durable-actors-observer:log-columns"
const columnLabels: [keyof LogColumns, string][] = [
    ["level", "Level"],
    ["request", "Request"]
]

export type LogsCardProps = {
    actorName: string
    actorId: string
    requests: RequestTrace[] | null
    requestId: string | null
    onRequestChange: (requestId: string | null) => void
    timeWindow: TimeWindow
}

export function LogsCard({ actorName, actorId, requests, requestId, onRequestChange, timeWindow }: LogsCardProps) {
    const { baseUrl } = useData()
    const setRange = useSetTimeRange()
    const [level, setLevel] = useState<LevelChoice>("all")
    const [searchText, setSearchText] = useState("")
    const [columns, setColumns] = useLogColumns()
    const search = useDebouncedValue(searchText.trim(), searchDelayMs)
    const minimum = level === "all" ? null : level
    const logs = useLogs(baseUrl, timeWindow, { actorName, actorId, requestId, level: minimum, search })
    const calls = new Map(requests?.map(request => [request.requestId, request]))
    const clearFilters = () => {
        onRequestChange(null)
        setSearchText("")
        setLevel("all")
    }
    return (
        <SectionCard
            id={logsCardId}
            title="Logs"
            aside={
                <div className="flex flex-wrap items-center gap-2">
                    <SearchField value={searchText} onChange={setSearchText} />
                    <LevelControl value={level} onChange={setLevel} totals={logs.histogram && levelTotals(logs.histogram)} />
                    <ColumnsMenu columns={columns} onChange={setColumns} />
                </div>
            }
            busy={!logs.lines && !logs.isError}
            className="pb-3.5"
        >
            {requestId && <RequestChip requestId={requestId} call={calls.get(requestId)} onRemove={() => onRequestChange(null)} onClearFilters={clearFilters} />}
            <LogChart histogram={logs.histogram} level={minimum} live={timeWindow.live} onZoom={span => setRange({ mode: "static", ...span })} />
            <LogList
                lines={logs.lines}
                failed={logs.isError}
                search={search}
                filtered={Boolean(requestId || search || minimum)}
                onFilterRequest={onRequestChange}
                onClearFilters={clearFilters}
                onReachEnd={logs.hasOlder && !logs.loadingOlder ? logs.loadOlder : undefined}
                columns={columns}
            />
        </SectionCard>
    )
}

function SearchField({ value, onChange }: { value: string; onChange: (value: string) => void }) {
    return (
        <label className="flex h-8 w-64 items-center gap-2 rounded-lg border bg-background/40 px-2.5 text-[12.5px] focus-within:border-ring focus-within:ring-2 focus-within:ring-ring/40">
            <Search aria-hidden className="size-3.5 shrink-0 text-muted-foreground" />
            <input
                value={value}
                maxLength={256}
                placeholder="Search messages"
                aria-label="Search log messages"
                onChange={event => onChange(event.target.value)}
                onKeyDown={event => {
                    if (event.key === "Escape") onChange("")
                }}
                className="min-w-0 flex-1 bg-transparent text-foreground outline-none placeholder:text-muted-foreground"
            />
            {value && (
                <button
                    type="button"
                    aria-label="Clear search"
                    onClick={() => onChange("")}
                    className="grid size-4.5 shrink-0 place-items-center rounded-full bg-muted text-muted-foreground hover:text-foreground"
                >
                    <X className="size-2.5" />
                </button>
            )}
        </label>
    )
}

function LevelControl({ value, onChange, totals }: { value: LevelChoice; onChange: (value: LevelChoice) => void; totals: LevelCounts | undefined }) {
    return (
        <SegmentedControl
            label="Minimum log level"
            value={value}
            onChange={onChange}
            options={levelChoices.map(choice => ({
                ...choice,
                icon: (choice.value === "warn" || choice.value === "error") && <i className={cn("size-1.5 rounded-full", levelFill[choice.value])} />,
                badge: totals && formatCount(countAtLeast(totals, choice.value === "all" ? null : choice.value))
            }))}
        />
    )
}

function ColumnsMenu({ columns, onChange }: { columns: LogColumns; onChange: (columns: LogColumns) => void }) {
    return (
        <Popover>
            <PopoverTrigger asChild>
                <Button variant="outline" size="sm" className="h-8 text-xs">
                    <Columns3 />
                    Columns
                </Button>
            </PopoverTrigger>
            <PopoverContent align="end" className="w-44 p-1.5">
                {columnLabels.map(([key, label]) => (
                    <label key={key} className="flex cursor-pointer items-center gap-2.5 rounded-sm px-2 py-1.5 text-[13px] hover:bg-muted">
                        <input type="checkbox" checked={columns[key]} onChange={event => onChange({ ...columns, [key]: event.target.checked })} className="accent-primary" />
                        {label}
                    </label>
                ))}
            </PopoverContent>
        </Popover>
    )
}

// A per-browser preference; storage can be unavailable, so it falls back to the defaults.
function useLogColumns(): [LogColumns, (columns: LogColumns) => void] {
    const [columns, setColumns] = useState(readColumns)
    const update = (next: LogColumns) => {
        setColumns(next)
        try {
            localStorage.setItem(columnsStorageKey, JSON.stringify(next))
        } catch {
            // Keep the choice for this page only.
        }
    }
    return [columns, update]
}

function readColumns(): LogColumns {
    try {
        return { ...defaultColumns, ...JSON.parse(localStorage.getItem(columnsStorageKey) ?? "{}") }
    } catch {
        return defaultColumns
    }
}

type RequestChipProps = { requestId: string; call: RequestTrace | undefined; onRemove: () => void; onClearFilters: () => void }

function RequestChip({ requestId, call, onRemove, onClearFilters }: RequestChipProps) {
    return (
        <div className="mt-3.5 flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
            <span>Filtered to</span>
            <span className="inline-flex h-7 items-center gap-2 rounded-full border border-chart-2/45 bg-chart-2/12 pr-1.5 pl-2.5 text-foreground">
                <span className="text-muted-foreground">Request</span>
                <code title={requestId} className="font-mono text-[11.5px] text-chart-2">
                    {shortRequestId(requestId)}
                </code>
                {call && (
                    <span className="text-muted-foreground">
                        {call.operation}
                        {call.outcome !== "completed" && ` · ${call.outcome}`}
                    </span>
                )}
                <button
                    type="button"
                    aria-label="Remove the request filter"
                    onClick={onRemove}
                    className="grid size-4.5 place-items-center rounded-full bg-background/40 text-muted-foreground hover:text-foreground"
                >
                    <X className="size-2.5" />
                </button>
            </span>
            <button type="button" onClick={onClearFilters} className="text-foreground underline decoration-border underline-offset-3 hover:decoration-foreground">
                Clear filters
            </button>
        </div>
    )
}
