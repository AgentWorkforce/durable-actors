import type { ApplyEvents } from "../../hooks/useLiveView.js"
import type { DatabasePoint, ResourceUsage, UsagePoint } from "../../types/Resources.js"

/** Adds the instance's live CPU and memory samples and the database sizes its reports carry. */
export function applyResourceEvents(actorName: string, actorId: string): ApplyEvents<ResourceUsage> {
    const owns = (item: { actorName: string; actorId: string }) => item.actorName === actorName && item.actorId === actorId
    return (resources, events) => {
        const usage: UsagePoint[] = []
        const database: DatabasePoint[] = []
        for (const event of events) {
            if (event.type === "usage" && owns(event.item)) usage.push({ atMs: event.item.atMs, cpuCores: event.item.cpuCores, memoryBytes: event.item.memoryBytes })
            if (event.type === "instance" && owns(event.item) && event.item.databaseBytes !== null) database.push({ atMs: event.item.occurredAtMs, bytes: event.item.databaseBytes })
        }
        if (usage.length === 0 && database.length === 0) return resources
        return {
            ...resources,
            usage: [...resources.usage, ...usage],
            database: [...resources.database, ...database],
            databaseBytes: database.at(-1)?.bytes ?? resources.databaseBytes
        }
    }
}
