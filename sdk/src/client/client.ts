import { AsyncLocalStorage } from "node:async_hooks"

import type { ActorConnection, ActorSocketMessage } from "../actor/socket.js"
import type { ActorSchemas } from "../actor/socketValidation.js"

interface ActorClientTransport {
    invoke(actorName: string, actorId: string, method: string, args: readonly unknown[]): Promise<unknown>
    connect(actorName: string, actorId: string, metadata: unknown, schemas?: ActorSchemas): Promise<ActorConnection>
    broadcast(actorName: string, actorId: string, message: ActorSocketMessage): Promise<void>
}

const scopedClients = new AsyncLocalStorage<ActorClientTransport>()

let defaultClient: Promise<ActorClientTransport> | undefined

async function actorClient(): Promise<ActorClientTransport> {
    return scopedClients.getStore() ?? remoteActorClient()
}

function remoteActorClient(): Promise<ActorClientTransport> {
    defaultClient ??= import("./remoteClient.js")
        .then(({ RemoteActorClient }) => new RemoteActorClient())
        .catch(error => {
            defaultClient = undefined
            throw error
        })
    return defaultClient
}

function runWithActorClient<T>(client: ActorClientTransport, operation: () => T): T {
    return scopedClients.run(client, operation)
}

export { actorClient, remoteActorClient, runWithActorClient }
export type { ActorClientTransport }
