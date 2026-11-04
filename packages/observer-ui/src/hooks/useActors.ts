import useSWR from "swr"
import useSWRSubscription, { type SWRSubscriptionOptions } from "swr/subscription"

import { actorEventsKey, actorsKey } from "../client/cacheKeys.js"
import { HttpObserverClient } from "../client/observerClient.js"
import { subscribeToActors } from "../client/streams/actorStream.js"
import type { ActorInventory } from "../types/Actors.js"

export function useActors(baseUrl: string) {
    const { data, error, isLoading } = useSWR<ActorInventory>(actorsKey(baseUrl), () => new HttpObserverClient(baseUrl).listActorTypes())
    const { data: live } = useSWRSubscription(actorEventsKey(baseUrl), (_key, { next }: SWRSubscriptionOptions<ActorInventory>) => subscribeToActors(baseUrl, inventory => next(null, inventory)))

    return {
        actorTypes: live ?? data ?? null,
        isLoading,
        isError: !live && Boolean(error),
        error
    }
}
