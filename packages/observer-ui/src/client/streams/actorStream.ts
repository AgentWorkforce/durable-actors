import type { ActorInventory } from "../../types/Actors.js"

export function subscribeToActors(baseUrl: string, onInventory: (inventory: ActorInventory) => void): () => void {
    const source = new EventSource(`${baseUrl}/events`, { withCredentials: true })
    source.addEventListener("inventory", event => onInventory(JSON.parse(event.data)))
    return () => source.close()
}
