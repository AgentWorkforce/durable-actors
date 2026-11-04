import { type ReactNode, createContext, useContext, useEffect, useState } from "react"

import { lastHour, resolveWindow } from "../lib/timeRange.js"
import type { TimeRange, TimeWindow } from "../types/TimeRange.js"

const liveTickMs = 5_000

type TimeRangeContextType = { timeWindow: TimeWindow; setRange: (range: TimeRange) => void }

const TimeRangeContext = createContext<TimeRangeContextType | undefined>(undefined)

export function TimeRangeProvider({ initialRange = lastHour, children }: { initialRange?: TimeRange | (() => TimeRange); children: ReactNode }) {
    const [range, setRange] = useState(initialRange)
    const now = useNow(range.mode === "live")
    return <TimeRangeContext.Provider value={{ timeWindow: resolveWindow(range, now), setRange }}>{children}</TimeRangeContext.Provider>
}

export function useTimeWindow(): TimeWindow {
    return useTimeRange().timeWindow
}

export function useSetTimeRange(): (range: TimeRange) => void {
    return useTimeRange().setRange
}

function useTimeRange(): TimeRangeContextType {
    const context = useContext(TimeRangeContext)
    if (!context) throw new Error("Time range hooks must be used within a TimeRangeProvider")
    return context
}

function useNow(ticking: boolean): number {
    const [now, setNow] = useState(Date.now)
    useEffect(() => {
        if (!ticking) return
        setNow(Date.now())
        const timer = setInterval(() => setNow(Date.now()), liveTickMs)
        return () => clearInterval(timer)
    }, [ticking])
    return now
}
