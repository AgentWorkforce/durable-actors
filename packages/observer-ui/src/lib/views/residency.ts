import type { ApplyEvents } from "../../hooks/useLiveView.js"
import type { ObserveEvent } from "../../types/Events.js"
import type { ResidencyHistory, ResidencyPeriod } from "../../types/Residency.js"

/** Extends each instance's timeline with its lifecycle reports, as the server builds it. */
export function applyResidencyEvents(actorName: string, actorId?: string): ApplyEvents<ResidencyHistory> {
    return (history, events) => {
        const periods = [...history.periods]
        for (const event of events) if (event.type === "instance" && reportsFor(event, actorName, actorId)) applyReport(periods, event)
        return { ...history, periods }
    }
}

function reportsFor({ item }: Extract<ObserveEvent, { type: "instance" }>, actorName: string, actorId: string | undefined): boolean {
    return item.actorName === actorName && (actorId === undefined || item.actorId === actorId)
}

function applyReport(periods: ResidencyPeriod[], { item }: Extract<ObserveEvent, { type: "instance" }>) {
    const { actorId, status, occurredAtMs } = item
    const index = lastIndexOf(periods, actorId)
    const last = periods[index]
    if (last && occurredAtMs <= last.fromMs) return
    if (last?.status === status) {
        periods[index] = { ...last, toMs: Math.max(last.toMs, occurredAtMs) }
        return
    }
    if (last) periods[index] = { ...last, toMs: occurredAtMs }
    periods.push({ actorId, status, fromMs: occurredAtMs, toMs: occurredAtMs })
}

function lastIndexOf(periods: ResidencyPeriod[], actorId: string): number {
    for (let index = periods.length - 1; index >= 0; index--) if (periods[index]!.actorId === actorId) return index
    return -1
}
