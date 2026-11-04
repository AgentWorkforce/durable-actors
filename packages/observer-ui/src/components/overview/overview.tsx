import { ActiveActorsCard } from "./activeActorsCard.js"
import { ActorTypesCard } from "./actorTypesCard.js"
import { RequestRateCard } from "./requestRateCard.js"
import { SocketsCard } from "./socketsCard.js"

export function Overview() {
    return (
        <section aria-labelledby="overview-heading" className="flex flex-col gap-5">
            <header className="flex flex-wrap items-baseline justify-between gap-2">
                <h1 id="overview-heading" className="text-2xl font-bold tracking-tight">
                    Overview
                </h1>
            </header>
            <div className="flex flex-col gap-4">
                <div className="grid gap-4 lg:grid-cols-3">
                    <ActiveActorsCard />
                    <RequestRateCard />
                    <SocketsCard />
                </div>
                <ActorTypesCard />
            </div>
        </section>
    )
}
