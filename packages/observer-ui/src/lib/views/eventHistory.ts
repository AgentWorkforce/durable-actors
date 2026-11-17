import type { ApplyEvents } from "../../hooks/useLiveView.js"
import type { EventHistory, ObserveEvent, RecordedEvent } from "../../types/Events.js"

// The newest events the server returns; older ones fall off as new ones arrive.
const eventLimit = 500

/** Adds the events of these types, under the actor when one is named. */
export function applyRecordedEvents(types: RecordedEvent["type"][], actorName?: string, actorId?: string): ApplyEvents<EventHistory> {
    return (history, events) => {
        // A pool manager that restarts sends an event again, so one already held isn't added.
        const held = new Map(history.events.map(event => [identity(event), event]))
        for (const event of events) {
            if (isRecorded(event, types) && isUnder(event, actorName, actorId)) held.set(identity(event), event)
        }
        if (held.size === history.events.length) return history
        const newestFirst = [...held.values()].sort((a, b) => b.item.atMs - a.item.atMs)
        return { truncated: history.truncated || newestFirst.length > eventLimit, events: newestFirst.slice(0, eventLimit) }
    }
}

function identity({ type, item }: RecordedEvent): string {
    return `${type}:${JSON.stringify(item)}`
}

function isRecorded(event: ObserveEvent, types: RecordedEvent["type"][]): event is RecordedEvent {
    return types.some(type => type === event.type)
}

function isUnder({ item }: RecordedEvent, actorName: string | undefined, actorId: string | undefined): boolean {
    return (actorName === undefined || item.actorName === actorName) && (actorId === undefined || item.actorId === actorId)
}
