import { validateActorComponent } from "../actor/identity.js"
import type { ActorConnection, ActorSocketMessage } from "../actor/socket.js"
import type { ActorSchemas } from "../actor/socketValidation.js"
import { parseActorHostReply } from "../client-runtime/http.js"
import { cloneJson } from "../client-runtime/json.js"
import type { ActorClientTransport } from "../client/client.js"
import { ActorInvocationError, ActorProtocolError } from "../errors.js"

import type { ActorCallRequester } from "./types.js"

/** Invokes actors through the Rust host, which scopes calls to the caller's project. */
class HostActorClient implements ActorClientTransport {
    constructor(
        private readonly call: ActorCallRequester,
        private readonly remote: () => Promise<ActorClientTransport>
    ) {}

    async invoke(actorName: string, actorId: string, method: string, args: readonly unknown[]): Promise<unknown> {
        const requestId = globalThis.crypto.randomUUID()
        const encoded = cloneJson(args, "actor arguments")
        if (!Array.isArray(encoded)) throw new ActorProtocolError("actor arguments must be a JSON array")
        const reply = parseActorHostReply(
            await this.call({
                request_id: requestId,
                actor_name: validateActorComponent("actor name", actorName),
                actor_id: validateActorComponent("actor ID", actorId),
                method: validateActorComponent("actor method", method),
                args: encoded
            })
        )
        if (reply.type === "completed") return reply.result
        if (reply.type === "failed") throw new ActorInvocationError(reply.code, requestId, reply.message)
        throw new ActorInvocationError("unavailable", requestId, "actor host rejected the invocation before execution")
    }

    async connect(
        actorName: string,
        actorId: string,
        metadata: unknown,
        schemas?: ActorSchemas
    ): Promise<ActorConnection> {
        return (await this.remote()).connect(actorName, actorId, metadata, schemas)
    }

    async broadcast(actorName: string, actorId: string, message: ActorSocketMessage): Promise<void> {
        return (await this.remote()).broadcast(actorName, actorId, message)
    }
}

export { HostActorClient }
