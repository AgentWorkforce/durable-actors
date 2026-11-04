import type { RequestTracePage } from "../../types/Requests.js"

export function subscribeToRequests(baseUrl: string, onPage: (page: RequestTracePage) => void): () => void {
    const source = new EventSource(`${baseUrl}/requests/events`, { withCredentials: true })
    source.addEventListener("requests", event => onPage(JSON.parse(event.data)))
    return () => source.close()
}
