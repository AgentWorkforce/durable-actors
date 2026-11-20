import { type ObserveEvent, observeEventTypes } from "../types/Events.js"

// Covers the time between an event reaching the stream and its write reaching the store.
const recentMs = 10_000
// Messages that arrive together reach views as one update. A timer, not an animation frame,
// which never fires in a background tab while the stream keeps delivering.
const flushMs = 50

type Listener = (events: ObserveEvent[]) => void

/**
 * The events one `observe/events` stream delivers. Views read the store when they open, then
 * apply each event; the recent ones are applied again on top of every read so a change the store
 * has yet to receive is not lost.
 */
export class EventBus {
    private readonly listeners = new Set<Listener>()
    private readonly reconnectListeners = new Set<() => void>()
    private recent: { atMs: number; event: ObserveEvent }[] = []
    private openedBefore = false
    private pending: ObserveEvent[] = []
    private flushTimer: ReturnType<typeof setTimeout> | null = null

    subscribe(listener: Listener): () => void {
        this.listeners.add(listener)
        return () => this.listeners.delete(listener)
    }

    /** Runs when the stream opens again, as after a pause or a dropped connection, when events may have been missed. */
    onReconnect(listener: () => void): () => void {
        this.reconnectListeners.add(listener)
        return () => this.reconnectListeners.delete(listener)
    }

    /** Forgets the recent events when the stream starts over with a different query. */
    reset(): void {
        this.recent = []
        this.pending = []
    }

    recentEvents(): ObserveEvent[] {
        const since = Date.now() - recentMs
        return this.recent.filter(entry => entry.atMs >= since).map(entry => entry.event)
    }

    publish(events: ObserveEvent[]): void {
        for (const event of events) this.pending.push(event)
        this.flushTimer ??= setTimeout(() => this.flush(), flushMs)
    }

    /** `afterGap` marks a first open that came well after the views read the store. */
    opened(afterGap = false): void {
        if (this.openedBefore || afterGap) for (const listener of this.reconnectListeners) listener()
        this.openedBefore = true
    }

    private flush(): void {
        this.flushTimer = null
        const events = this.pending
        if (events.length === 0) return
        this.pending = []
        const now = Date.now()
        this.recent = [...this.recent.filter(entry => entry.atMs >= now - recentMs), ...events.map(event => ({ atMs: now, event }))]
        for (const listener of this.listeners) listener(events)
    }
}

/**
 * Streams while the page is visible. A hidden tab closes the stream; showing it again reopens it
 * and the views read the store again, as after any reopen.
 */
export function openEventStream(baseUrl: string, query: URLSearchParams, bus: EventBus): () => void {
    let source: EventSource | null = null
    const connect = () => {
        source = new EventSource(`${baseUrl}/events?${query}`, { withCredentials: true })
        forward(source, bus)
        source.addEventListener("open", () => bus.opened(startedHidden))
    }
    const followVisibility = () => {
        if (!document.hidden) {
            if (!source) connect()
            return
        }
        source?.close()
        source = null
    }
    // A tab opened in the background connects only once shown, long after its views first read.
    const startedHidden = document.hidden
    followVisibility()
    document.addEventListener("visibilitychange", followVisibility)
    return () => {
        document.removeEventListener("visibilitychange", followVisibility)
        source?.close()
    }
}

function forward(source: EventSource, bus: EventBus) {
    for (const type of observeEventTypes) {
        source.addEventListener(type, message => bus.publish(JSON.parse(message.data).map((item: unknown) => ({ type, item }) as ObserveEvent)))
    }
}
