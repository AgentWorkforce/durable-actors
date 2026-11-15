import { ActorRuntime as Runtime } from "../../src/host/actor-runtime.js"
import { DatabaseProxy } from "../../src/host/database.js"

import { HostDatabase } from "./host-database.js"

export { DatabaseProxy } from "../../src/host/database.js"
export { hydrateActorState, snapshotActorState } from "../../src/host/actor-runtime.js"

type RuntimeArguments = ConstructorParameters<typeof Runtime>
export class ActorRuntime extends Runtime {
    constructor(
        definition: RuntimeArguments[0],
        allowNextInvocation: RuntimeArguments[1],
        publish?: RuntimeArguments[3],
        connections?: RuntimeArguments[4],
        database: RuntimeArguments[2] = new DatabaseProxy(new HostDatabase().call)
    ) {
        super(definition, allowNextInvocation, database, publish, connections)
    }
}
