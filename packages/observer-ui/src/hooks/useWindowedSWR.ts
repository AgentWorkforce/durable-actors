import useSWR, { type SWRConfiguration } from "swr"

import type { TimeBounds, TimeWindow } from "../types/TimeRange.js"

export function useWindowedSWR<T>(key: readonly unknown[], timeWindow: TimeWindow, fetch: (bounds: TimeBounds) => Promise<T>, config?: SWRConfiguration<T>) {
    return useSWR<T>([...key, ...windowKey(timeWindow)], () => fetch(currentBounds(timeWindow)), config)
}

// Live keys use the duration rather than the moving bounds so the clock tick doesn't refetch.
export function windowKey({ fromMs, toMs, live }: TimeWindow): readonly unknown[] {
    return live ? ["live", toMs - fromMs] : [fromMs, toMs]
}

// Live fetches send the client's now explicitly; the server's default "now" can drift from the window being drawn.
export function currentBounds({ fromMs, toMs, live }: TimeWindow): TimeBounds {
    if (!live) return { fromMs, toMs }
    const now = Date.now()
    return { fromMs: now - (toMs - fromMs), toMs: now }
}
