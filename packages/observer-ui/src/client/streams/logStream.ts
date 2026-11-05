import type { ActorLog, LogFilter } from "../../types/Logs.js"
import { logFilterParams } from "../observerClient.js"

export function subscribeToLogs(baseUrl: string, filter: LogFilter, onRecords: (records: ActorLog[]) => void): () => void {
    const source = new EventSource(`${baseUrl}/logs/events?${logFilterParams(filter)}`, { withCredentials: true })
    source.addEventListener("logs", event => onRecords(JSON.parse(event.data).records))
    return () => source.close()
}
