import useSWR, { type SWRConfiguration } from "swr"

import type { TimeBounds, TimeWindow } from "../types/TimeRange.js"

// Live keys use the duration rather than the moving bounds so the clock tick doesn't refetch.
export function useWindowedSWR<T>(key: readonly unknown[], timeWindow: TimeWindow, fetch: (bounds: TimeBounds) => Promise<T>, config?: SWRConfiguration<T>) {
    const { fromMs, toMs, live } = timeWindow
    return useSWR<T>(live ? [...key, "live", toMs - fromMs] : [...key, fromMs, toMs], () => fetch(live ? trailingBounds(toMs - fromMs) : { fromMs, toMs }), config)
}

// Live fetches send the client's now explicitly; the server's default "now" can drift from the window being drawn.
function trailingBounds(durationMs: number): TimeBounds {
    const toMs = Date.now()
    return { fromMs: toMs - durationMs, toMs }
}
