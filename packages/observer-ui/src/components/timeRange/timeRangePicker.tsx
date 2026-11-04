import { useId, useState } from "react"
import type { DateRange } from "react-day-picker"

import { CalendarIcon, ChevronDownIcon, ChevronLeftIcon, ChevronRightIcon } from "lucide-react"

import { formatClock, formatTimeSpan } from "../../format.js"
import { atTime, liveLabel, timePresets } from "../../lib/timeRange.js"
import { cn } from "../../lib/utils.js"
import type { TimeRange, TimeWindow } from "../../types/TimeRange.js"
import { Button } from "../ui/button.js"
import { Calendar } from "../ui/calendar.js"
import { Input } from "../ui/input.js"
import { Popover, PopoverContent, PopoverTrigger } from "../ui/popover.js"

type PickerView = "quick" | "custom"

export function TimeRangePicker({ timeWindow, onChange }: { timeWindow: TimeWindow; onChange: (range: TimeRange) => void }) {
    const [open, setOpen] = useState(false)
    const [view, setView] = useState<PickerView>("quick")
    const toggle = (next: boolean) => {
        setOpen(next)
        if (next) setView(timeWindow.live ? "quick" : "custom")
    }
    const choose = (range: TimeRange) => {
        onChange(range)
        setOpen(false)
    }
    return (
        <Popover open={open} onOpenChange={toggle}>
            <PopoverTrigger asChild>
                <Button variant="outline" size="sm" className="text-xs tabular-nums">
                    <WindowLabel timeWindow={timeWindow} />
                    <ChevronDownIcon className="opacity-60" />
                </Button>
            </PopoverTrigger>
            <PopoverContent align="end" className="w-auto p-0">
                {view === "quick" ? (
                    <QuickRanges timeWindow={timeWindow} onSelect={choose} onCustom={() => setView("custom")} />
                ) : (
                    <AbsoluteRange timeWindow={timeWindow} onApply={choose} onBack={() => setView("quick")} />
                )}
            </PopoverContent>
        </Popover>
    )
}

function WindowLabel({ timeWindow: { fromMs, toMs, live } }: { timeWindow: TimeWindow }) {
    if (!live) {
        return (
            <>
                <CalendarIcon />
                {formatTimeSpan(fromMs, toMs)}
            </>
        )
    }
    return (
        <>
            <span className="size-1.5 animate-pulse rounded-full bg-success motion-reduce:animate-none" />
            {liveLabel(toMs - fromMs)}
        </>
    )
}

function QuickRanges({ timeWindow, onSelect, onCustom }: { timeWindow: TimeWindow; onSelect: (range: TimeRange) => void; onCustom: () => void }) {
    const current = timeWindow.live ? timeWindow.toMs - timeWindow.fromMs : null
    return (
        <div className="flex w-48 flex-col gap-0.5 p-1.5">
            {timePresets.map(preset => (
                <Button
                    key={preset.durationMs}
                    variant="ghost"
                    size="sm"
                    aria-pressed={preset.durationMs === current}
                    onClick={() => onSelect({ mode: "live", durationMs: preset.durationMs })}
                    className={cn("justify-start text-xs font-normal", preset.durationMs === current && "bg-accent text-accent-foreground")}
                >
                    {preset.label}
                </Button>
            ))}
            <div className="my-1 border-t" />
            <Button variant="ghost" size="sm" onClick={onCustom} className="justify-start text-xs font-normal">
                <CalendarIcon />
                Custom range
                <ChevronRightIcon className="ml-auto opacity-60" />
            </Button>
        </div>
    )
}

function AbsoluteRange({ timeWindow, onApply, onBack }: { timeWindow: TimeWindow; onApply: (range: TimeRange) => void; onBack: () => void }) {
    const [days, setDays] = useState<DateRange | undefined>({ from: new Date(timeWindow.fromMs), to: new Date(timeWindow.toMs) })
    const [fromTime, setFromTime] = useState(formatClock(timeWindow.fromMs))
    const [toTime, setToTime] = useState(formatClock(timeWindow.toMs))
    const range = toStaticRange(days, fromTime, toTime)
    return (
        <div>
            <Button variant="ghost" size="xs" onClick={onBack} className="mt-2 ml-2 text-xs font-normal text-muted-foreground">
                <ChevronLeftIcon />
                Quick ranges
            </Button>
            <Calendar mode="range" selected={days} onSelect={setDays} defaultMonth={days?.from} disabled={{ after: new Date() }} />
            <div className="grid grid-cols-2 gap-3 border-t p-3">
                <TimeField label="Start" value={fromTime} onChange={setFromTime} />
                <TimeField label="End" value={toTime} onChange={setToTime} />
            </div>
            <div className="flex items-center justify-between gap-3 border-t p-3 text-xs text-muted-foreground tabular-nums">
                <span>{range ? formatTimeSpan(range.fromMs, range.toMs) : "End must be after start"}</span>
                <Button size="sm" disabled={!range} onClick={() => range && onApply(range)}>
                    Apply
                </Button>
            </div>
        </div>
    )
}

function TimeField({ label, value, onChange }: { label: string; value: string; onChange: (value: string) => void }) {
    const id = useId()
    return (
        <div className="grid gap-1.5">
            <label htmlFor={id} className="text-xs text-muted-foreground">
                {label}
            </label>
            <Input id={id} type="time" value={value} onChange={event => onChange(event.target.value)} className="h-8 text-xs tabular-nums" />
        </div>
    )
}

// A single click in range mode selects only a start day; treat that as a range within that day.
function toStaticRange(days: DateRange | undefined, fromTime: string, toTime: string): Extract<TimeRange, { mode: "static" }> | null {
    if (!days?.from || !fromTime || !toTime) return null
    const fromMs = atTime(days.from, fromTime)
    const toMs = atTime(days.to ?? days.from, toTime)
    return fromMs < toMs ? { mode: "static", fromMs, toMs } : null
}
