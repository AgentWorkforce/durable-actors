import { Fragment, type ReactNode, useState } from "react"

import { Check, ChevronRight, ChevronsDownUp, ChevronsUpDown, Copy } from "lucide-react"

import { formatLogClock, formatPlural } from "../../format.js"
import { type ParsedMessage, constructorRequestId, parseLogMessage, shortRequestId, summarizeJson } from "../../lib/logs.js"
import { cn } from "../../lib/utils.js"
import type { ActorLog, LogLevel } from "../../types/Logs.js"
import { Button } from "../ui/button.js"
import { Skeleton } from "../ui/skeleton.js"

export type LogColumns = { level: boolean; request: boolean }

type Layout = { columns: LogColumns; gridTemplateColumns: string; messageOffset: number }

// "Expand all" also covers lines that load later; toggled rows are the exceptions to it.
type Expansion = { all: boolean; toggled: ReadonlySet<string> }

const rowGrid = "grid items-center gap-x-4"
const columnGap = 16
const loadAheadPx = 240
const widths = { time: 104, level: 58, request: 104 }
const badge: Record<LogLevel, string> = {
    debug: "border-border/60 text-muted-foreground/70",
    info: "text-muted-foreground",
    warn: "border-warning/35 bg-warning/10 text-warning",
    error: "border-danger/40 bg-danger/15 text-danger"
}
const rowTint: Record<LogLevel, string> = {
    debug: "",
    info: "",
    warn: "shadow-[inset_2px_0_0_color-mix(in_oklab,var(--warning)_75%,transparent)]",
    error: "bg-[linear-gradient(90deg,color-mix(in_oklab,var(--danger)_10%,transparent),transparent_70%)] shadow-[inset_2px_0_0_var(--danger)]"
}
// A quoted key with its colon, a quoted string, or a number, boolean, or null in pretty-printed JSON.
const jsonToken = /("(?:\\.|[^"\\])*")(?=\s*:)\s*:|("(?:\\.|[^"\\])*")|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?|\b(?:true|false|null)\b/gu

export type LogListProps = {
    lines: ActorLog[] | null
    failed: boolean
    search: string
    filtered: boolean
    onFilterRequest: (requestId: string) => void
    onClearFilters: () => void
    onReachEnd: (() => void) | undefined
    columns: LogColumns
}

export function LogList({ lines, failed, search, filtered, onFilterRequest, onClearFilters, onReachEnd, columns }: LogListProps) {
    const layout = layoutFor(columns)
    const [expansion, setExpansion] = useState<Expansion>({ all: false, toggled: new Set() })
    if (failed) return <p className="py-6 text-[13px] text-muted-foreground">Couldn't reach the observer API.</p>
    if (!lines) return <ListSkeleton layout={layout} />
    if (!lines.length) return <EmptyList filtered={filtered} onClearFilters={onClearFilters} />
    const isExpanded = (logId: string) => expansion.all !== expansion.toggled.has(logId)
    const anyExpanded = lines.some(line => isExpanded(line.logId))
    const toggle = (logId: string) =>
        setExpansion(({ all, toggled }) => {
            const next = new Set(toggled)
            if (!next.delete(logId)) next.add(logId)
            return { all, toggled: next }
        })
    return (
        <div
            className="max-h-[640px] overflow-y-auto"
            onScroll={event => {
                const box = event.currentTarget
                if (onReachEnd && box.scrollHeight - box.scrollTop - box.clientHeight < loadAheadPx) onReachEnd()
            }}
        >
            <div className={`${rowGrid} sticky top-0 z-[1] h-7.5 border-b bg-card px-2 text-xs text-muted-foreground`} style={{ gridTemplateColumns: layout.gridTemplateColumns }}>
                <span>Time</span>
                {columns.level && <span>Level</span>}
                {columns.request && <span>Request</span>}
                <span className="col-span-2 flex items-center justify-between">
                    Message
                    <button
                        type="button"
                        onClick={() => setExpansion({ all: !anyExpanded, toggled: new Set() })}
                        className="flex items-center gap-1 rounded-sm outline-none hover:text-foreground focus-visible:ring-2 focus-visible:ring-ring"
                    >
                        {anyExpanded ? <ChevronsDownUp className="size-3.5" /> : <ChevronsUpDown className="size-3.5" />}
                        {anyExpanded ? "Collapse all" : "Expand all"}
                    </button>
                </span>
            </div>
            <ol>
                {lines.map(line => (
                    <LogRow key={line.logId} line={line} layout={layout} expanded={isExpanded(line.logId)} onToggle={() => toggle(line.logId)} search={search} onFilterRequest={onFilterRequest} />
                ))}
            </ol>
        </div>
    )
}

function layoutFor(columns: LogColumns): Layout {
    const fixed = [widths.time, ...(columns.level ? [widths.level] : []), ...(columns.request ? [widths.request] : [])]
    return {
        columns,
        gridTemplateColumns: `${fixed.map(width => `${width}px`).join(" ")} minmax(0,1fr) 16px`,
        messageOffset: fixed.reduce((offset, width) => offset + width + columnGap, 0)
    }
}

function ListSkeleton({ layout }: { layout: Layout }) {
    return (
        <div className="py-2">
            {Array.from({ length: 6 }, (_, index) => (
                <div key={index} className={`${rowGrid} h-8.5 px-2`} style={{ gridTemplateColumns: layout.gridTemplateColumns }}>
                    <Skeleton className="h-3.5 w-20" />
                    {layout.columns.level && <Skeleton className="h-4 w-11" />}
                    {layout.columns.request && <Skeleton className="h-4 w-16" />}
                    <Skeleton className="h-3.5" style={{ width: `${35 + ((index * 23) % 50)}%` }} />
                </div>
            ))}
        </div>
    )
}

function EmptyList({ filtered, onClearFilters }: { filtered: boolean; onClearFilters: () => void }) {
    return (
        <p className="py-8 text-center text-[13px] text-muted-foreground">
            {filtered ? (
                <>
                    No lines match these filters.{" "}
                    <button type="button" onClick={onClearFilters} className="font-medium text-foreground underline decoration-border underline-offset-3 hover:decoration-foreground">
                        Clear filters
                    </button>
                </>
            ) : (
                "No logs in this time range."
            )}
        </p>
    )
}

type LogRowProps = {
    line: ActorLog
    layout: Layout
    expanded: boolean
    onToggle: () => void
    search: string
    onFilterRequest: (requestId: string) => void
}

function LogRow({ line, layout, expanded, onToggle, search, onFilterRequest }: LogRowProps) {
    const parsed = parseLogMessage(line.message)
    return (
        <li className={cn("border-b border-border/55 px-2 last:border-b-0", rowTint[line.level])}>
            <div onClick={onToggle} className={`${rowGrid} min-h-8.5 cursor-pointer text-[12.5px]`} style={{ gridTemplateColumns: layout.gridTemplateColumns }}>
                <span className="font-mono text-xs text-muted-foreground tabular-nums">{formatLogClock(line.timestampMs)}</span>
                {layout.columns.level && (
                    <span className={cn("justify-self-start rounded-sm border px-1.5 py-px text-[9.5px] font-bold tracking-[0.07em] uppercase", badge[line.level])}>{line.level}</span>
                )}
                {layout.columns.request && <RequestCell requestId={line.requestId} onFilter={onFilterRequest} />}
                <MessagePreview parsed={parsed} level={line.level} search={search} />
                <button
                    type="button"
                    aria-expanded={expanded}
                    aria-label={expanded ? "Collapse line" : "Expand line"}
                    onClick={event => {
                        event.stopPropagation()
                        onToggle()
                    }}
                    className="rounded-sm text-muted-foreground outline-none hover:text-foreground focus-visible:ring-2 focus-visible:ring-ring"
                >
                    <ChevronRight className={cn("size-3.5 transition-transform", expanded && "rotate-90")} />
                </button>
            </div>
            {expanded && <LogDetail line={line} parsed={parsed} indent={layout.messageOffset} />}
        </li>
    )
}

function RequestCell({ requestId, onFilter }: { requestId: string | null; onFilter: (requestId: string) => void }) {
    if (requestId === null)
        return (
            <span className="font-mono text-muted-foreground" title="Logged outside a request, for example by a timer that fired after its call returned">
                —
            </span>
        )
    if (requestId === constructorRequestId)
        return (
            <span className="justify-self-start rounded-sm border border-dashed px-1.5 text-[11px] text-muted-foreground" title="Logged by the constructor while the actor was loading">
                constructor
            </span>
        )
    return (
        <button
            type="button"
            title={`Show only request ${requestId}`}
            onClick={event => {
                event.stopPropagation()
                onFilter(requestId)
            }}
            className="max-w-full justify-self-start truncate rounded-sm bg-chart-2/12 px-1.5 py-px font-mono text-[11.5px] text-chart-2 outline-none hover:bg-chart-2/22 focus-visible:ring-2 focus-visible:ring-ring"
        >
            {shortRequestId(requestId)}
        </button>
    )
}

function MessagePreview({ parsed, level, search }: { parsed: ParsedMessage; level: LogLevel; search: string }) {
    if (parsed.json) return <JsonPreview value={parsed.json} search={search} />
    return (
        <span className={cn("flex min-w-0 items-center gap-2 font-mono", level === "debug" ? "text-muted-foreground" : "text-foreground/95")}>
            <span className="truncate">{highlight(parsed.firstLine, search)}</span>
            {parsed.moreLines > 0 && <span className="shrink-0 rounded-full border px-2 font-sans text-[11px] text-muted-foreground">+{formatPlural(parsed.moreLines, "line")}</span>}
        </span>
    )
}

function JsonPreview({ value, search }: { value: NonNullable<ParsedMessage["json"]>; search: string }) {
    const { headline, fields } = summarizeJson(value)
    return (
        <span className="flex min-w-0 items-center gap-2 font-mono">
            <span className="shrink-0 rounded-sm border border-chart-2/40 px-1 font-sans text-[9px] font-bold tracking-[0.07em] text-chart-2">JSON</span>
            <span className="truncate">
                {headline && <span className="mr-2.5 font-medium text-foreground">{highlight(headline, search)}</span>}
                {fields.map(([key, field], index) => (
                    <Fragment key={key}>
                        {index > 0 && <span className="text-muted-foreground"> · </span>}
                        <span className="text-muted-foreground">{key} </span>
                        <span className="text-foreground/95">{highlight(field, search)}</span>
                    </Fragment>
                ))}
            </span>
        </span>
    )
}

type LogDetailProps = { line: ActorLog; parsed: ParsedMessage; indent: number }

function LogDetail({ line, parsed, indent }: LogDetailProps) {
    const entry = {
        timestamp: new Date(line.timestampMs).toISOString(),
        level: line.level,
        actorName: line.actorName,
        actorId: line.actorId,
        requestId: line.requestId,
        message: parsed.json ?? line.message
    }
    return (
        <div className="pr-6 pb-4" style={{ paddingLeft: indent }}>
            <JsonBlock value={entry} />
            <div className="mt-3">
                <CopyButton text={JSON.stringify(entry, null, 2)} />
            </div>
        </div>
    )
}

function JsonBlock({ value }: { value: unknown }) {
    return (
        <pre className="max-h-96 overflow-auto rounded-lg border bg-(--code-bg) px-3.5 py-2.5 font-mono text-xs leading-relaxed break-words whitespace-pre-wrap">
            {highlightJson(JSON.stringify(value, null, 2))}
        </pre>
    )
}

function highlightJson(text: string): ReactNode[] {
    const parts: ReactNode[] = []
    let at = 0
    for (const match of text.matchAll(jsonToken)) {
        const [token, key, string] = match
        parts.push(
            text.slice(at, match.index),
            <span key={match.index} className={key ? "text-(--tk-plain)" : string ? "text-(--tk-string)" : "text-(--tk-type)"}>
                {key ?? token}
            </span>
        )
        if (key) parts.push(token.slice(key.length))
        at = match.index + token.length
    }
    parts.push(text.slice(at))
    return parts
}

function CopyButton({ text }: { text: string }) {
    const [copied, setCopied] = useState(false)
    return (
        <Button
            variant="outline"
            size="xs"
            onClick={() =>
                void navigator.clipboard.writeText(text).then(() => {
                    setCopied(true)
                    setTimeout(() => setCopied(false), 1500)
                })
            }
        >
            {copied ? <Check /> : <Copy />}
            {copied ? "Copied" : "Copy JSON"}
        </Button>
    )
}

function highlight(text: string, search: string): ReactNode {
    if (!search) return text
    const lower = text.toLowerCase()
    const needle = search.toLowerCase()
    const parts: ReactNode[] = []
    let at = 0
    for (let index = lower.indexOf(needle); index >= 0; index = lower.indexOf(needle, at)) {
        parts.push(
            text.slice(at, index),
            <mark key={index} className="rounded-[2px] bg-primary/25 text-foreground shadow-[inset_0_-1px_0_var(--primary)]">
                {text.slice(index, index + needle.length)}
            </mark>
        )
        at = index + needle.length
    }
    parts.push(text.slice(at))
    return parts
}
