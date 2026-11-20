import { useEffect, useRef, useState } from "react"

import useSWR, { type Key, type SWRConfiguration } from "swr"

import { EventBus, openEventStream } from "../client/events.js"
import type { ObserveEvent } from "../types/Events.js"

export type ApplyEvents<T> = (data: T, events: ObserveEvent[]) => T

/**
 * The standard way a view stays current: read the store when the view opens, then apply each
 * event from the stream. Without a bus the view is a plain read.
 */
export function useLiveView<T>(key: Key, read: () => Promise<T>, apply: ApplyEvents<T>, bus: EventBus | null, config?: SWRConfiguration<T>) {
    const applyRef = useRef(apply)
    applyRef.current = apply
    const reading = useRef(0)
    const view = useSWR<T>(
        key,
        async () => {
            reading.current++
            try {
                const data = await read()
                return bus ? applyRef.current(data, bus.recentEvents()) : data
            } finally {
                reading.current--
            }
        },
        // A paused view keeps what it read; a live one is kept current by the stream and re-reads when it reopens.
        { revalidateOnFocus: false, revalidateOnReconnect: false, ...config }
    )
    const { mutate } = view
    useEffect(() => {
        if (!bus) return
        // SWR discards a read that any mutation overlaps, and the read applies recent events itself.
        const unsubscribe = bus.subscribe(events => {
            if (reading.current === 0) void mutate(data => (data === undefined ? data : applyRef.current(data, events)), { revalidate: false })
        })
        const unwatch = bus.onReconnect(() => void mutate())
        return () => {
            unsubscribe()
            unwatch()
        }
    }, [bus, mutate])
    return view
}

/** One `observe/events` stream for as long as the caller is mounted with a query. */
export function useEventStream(baseUrl: string, query: URLSearchParams | null): EventBus | null {
    const [bus] = useState(() => new EventBus())
    const serialized = query?.toString() ?? null
    useEffect(() => {
        if (serialized === null) return
        bus.reset()
        return openEventStream(baseUrl, new URLSearchParams(serialized), bus)
    }, [baseUrl, bus, serialized])
    return serialized === null ? null : bus
}
