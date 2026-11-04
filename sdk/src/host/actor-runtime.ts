import { isDeepStrictEqual } from "node:util"

import type { ActorDefinition, AnyActor } from "../actor/actor.js"
import { RuntimeActorContext } from "../actor/context.js"
import { runWithActorDatabase } from "../actor/database.js"
import type { ActorIdentity } from "../actor/identity.js"
import { ActorResource } from "../actor/resource.js"
import type { ActorSchema } from "../actor/schema.js"
import { Persistence } from "../actor/schema.js"
import type { ActorSocketScope } from "../actor/socket.js"
import { decodeSocketMessage, runWithActorSockets } from "../actor/socket.js"
import type { SocketEffect } from "../actor/socketProtocol.js"
import type { ActorSchemas } from "../actor/socketValidation.js"
import { ActorDefinitionError, ActorProtocolError, ActorSerializationError, errorMessage } from "../errors.js"
import type { JsonObject, JsonValue } from "../json.js"
import { cloneJson, cloneJsonObject, isJsonObject } from "../json.js"

import type { DatabaseProxy } from "./database.js"
import { DatabaseCommitError } from "./database.js"
import { bindPersistedFields, isPersistedField } from "./persisted-fields.js"
import type { ActorExecutorReply, ActorWorkerCommand, InvokeCommand, WebSocketEventCommand } from "./protocol.js"
import { failedReply } from "./protocol.js"
import type { SocketPublisher, SocketSource } from "./types.js"

class ActorRuntime {
    private instance: AnyActor | undefined
    private readonly schemas: ActorSchemas
    private sequence = 0
    private lastCompletedState: JsonObject | undefined
    private fatal: ActorExecutorReply | undefined

    constructor(
        private readonly definition: ActorDefinition,
        private readonly allowNextInvocation: () => void,
        private readonly database: DatabaseProxy,
        private readonly publish?: SocketPublisher,
        private readonly connections: SocketSource = async () => {
            throw new Error("actor connection lookup is unavailable")
        }
    ) {
        this.schemas = { ...definition.schemas, contract: definition.state.contract }
        if (this.interleaved)
            database.rejectTransactions(
                `${definition.actorName} uses @Interleave, so overlapping calls cannot share an explicit database transaction`
            )
    }

    async handle(command: ActorWorkerCommand): Promise<ActorExecutorReply> {
        if (this.fatal !== undefined) return this.fatal
        try {
            if (command.type === "hydrate") {
                await this.prepare(command)
                return { type: "hydrated" }
            }
            if (command.type === "invoke") return await this.invoke(command)
            if (command.type === "evict") {
                const isGracefuleShutdown = command.graceful
                await this.close(isGracefuleShutdown)
                this.reset()
                return { type: "evicted" }
            }
            return await this.handleSocketEvent(command)
        } catch (error) {
            reportFailure(this.definition.actorName, command.type, error)
            this.reset()
            const failure = failedReply("invalid_actor_state", errorMessage(error))
            if (this.interleaved) this.fatal = failure
            return failure
        }
    }

    async close(isGracefuleShutdown: boolean): Promise<void> {
        if (isGracefuleShutdown) await this.shutdown()
        this.reset()
    }

    async shutdown(): Promise<void> {
        if (this.instance === undefined) return

        const fields = this.definition.state.fields.filter(field => field.resource).reverse()

        await runWithActorDatabase(this.instance, async () => {
            for (const field of fields) {
                const resource = Reflect.get(this.instance!, field.name) as ActorResource
                await resource.onGracefulShutdown()
            }
        })
    }

    private get interleaved(): boolean {
        return (this.definition.state.interleaveMethods?.length ?? 0) > 0
    }

    private completionOrder(): { sequence?: number } {
        return this.interleaved ? { sequence: ++this.sequence } : {}
    }

    private async invoke(command: InvokeCommand): Promise<ActorExecutorReply> {
        const instance = await this.prepare(command)
        if (!this.definition.methods.has(command.method) && command.method !== "onAlarm") {
            return failedReply(
                "method_not_found",
                `actor method ${this.definition.actorName}.${command.method} was not found`
            )
        }
        const method: unknown =
            Reflect.get(instance, command.method) ?? (command.method === "onAlarm" ? ignoreAlarm : undefined)
        if (typeof method !== "function") {
            return failedReply(
                "method_not_callable",
                `actor method ${this.definition.actorName}.${command.method} is not callable`
            )
        }

        const before = this.database.fields()
        try {
            this.admitNext(command.method)
            const execution = await runWithActorSockets(
                instance,
                this.connections,
                async () =>
                    runWithActorDatabase(
                        instance,
                        async () => Reflect.apply(method, instance, command.args) as Promise<unknown>
                    ),
                this.publish,
                this.schemas
            )
            const result: JsonValue = execution.value === undefined ? null : cloneJson(execution.value, "actor result")
            if (this.fatal !== undefined) return this.fatal
            validateActorState(instance, this.definition.state)
            const state = this.database.fields()
            const effects = [...execution.effects, ...this.stateUpdates(before, state)]
            this.database.commit()

            return {
                type: "invoked",
                result,
                ...this.completionOrder(),
                ...(effects.length === 0 ? {} : { effects })
            }
        } catch (error) {
            reportFailure(this.definition.actorName, command.method, error)
            if (error instanceof DatabaseCommitError) {
                this.fatal = failedReply("actor_database_failed", errorMessage(error))
                return this.fatal
            }
            if (!this.interleaved) await this.restoreInstance(command.actor)
            return failedReply("actor_method_failed", errorMessage(error))
        }
    }

    private async handleSocketEvent(command: WebSocketEventCommand): Promise<ActorExecutorReply> {
        const instance = await this.prepare(command)
        const methodName = lifecycleMethod(command)
        const method: unknown = Reflect.get(instance, methodName)
        const before = this.database.fields()
        try {
            this.admitNext(methodName)
            const execution = await runWithActorSockets(
                instance,
                this.connections,
                async scope => {
                    const args = lifecycleArguments(command, scope, this.schemas)
                    if (method === undefined) return
                    if (typeof method !== "function")
                        throw new ActorProtocolError(
                            `actor lifecycle hook ${this.definition.actorName}.${methodName} is not callable`
                        )
                    await runWithActorDatabase(
                        instance,
                        async () => Reflect.apply(method, instance, args) as Promise<unknown>
                    )
                },
                command.event.type === "connect" ? undefined : this.publish,
                this.schemas,
                command.connections
            )

            if (this.fatal !== undefined) return this.fatal
            validateActorState(instance, this.definition.state)
            const state = this.database.fields()
            const effects = [
                ...execution.effects,
                ...this.stateUpdates(
                    before,
                    state,
                    command.event.type === "connect" ? command.event.connection.id : undefined
                )
            ]
            this.database.commit()

            return {
                type: "websocket_handled",
                ...this.completionOrder(),
                effects: socketEffects(command, state, effects, this.definition.state)
            }
        } catch (error) {
            reportFailure(this.definition.actorName, methodName, error)
            if (error instanceof DatabaseCommitError) {
                this.fatal = failedReply("actor_database_failed", errorMessage(error))
                return this.fatal
            }
            if (!this.interleaved) await this.restoreInstance(command.actor)
            return failedReply("actor_socket_failed", errorMessage(error))
        }
    }

    private admitNext(method: string): void {
        if (this.definition.state.interleaveMethods?.includes(method)) this.allowNextInvocation()
    }

    private async prepare(command: ActorWorkerCommand): Promise<AnyActor> {
        if (this.instance !== undefined) return this.instance
        return await this.createInstance(command.actor, this.database.fields())
    }

    private async restoreInstance(identity: ActorIdentity): Promise<void> {
        this.database.rollback()
        await this.createInstance(identity, this.database.fields())
    }

    private stateUpdates(before: JsonObject, state: JsonObject, except?: string): SocketEffect[] {
        const previous = this.interleaved ? (this.lastCompletedState ?? before) : before
        if (this.interleaved) this.lastCompletedState = state
        return stateUpdates(previous, state, this.definition.state, except)
    }

    private reset(): void {
        this.instance = undefined
    }

    private async createInstance(identity: ActorIdentity, state: JsonValue | null): Promise<AnyActor> {
        const context = RuntimeActorContext.runtime(identity.actor_id, this.database)
        const instance = context.construct(this.definition.actorClass) as AnyActor
        validateActorState(instance, this.definition.state)
        if (state !== null) hydrateActorState(instance, persistedState(state), this.definition.state)
        this.database.initializeFields(snapshotActorState(instance, this.definition.state))
        bindPersistedFields(instance, context, this.definition.state, this.database)
        if (this.interleaved) this.lastCompletedState = this.database.fields()
        this.instance = instance

        const resources = this.definition.state.fields.filter(field => field.resource)
        await runWithActorDatabase(instance, async () => {
            for (const field of resources) {
                const resource = Reflect.get(instance, field.name) as ActorResource
                await resource.onResume()
            }
        })
        return instance
    }
}

function socketEffects(
    command: WebSocketEventCommand,
    state: JsonObject,
    effects: readonly SocketEffect[],
    schema: ActorSchema
): readonly SocketEffect[] {
    if (command.event.type !== "connect" || connectionWasRejected(command.event.connection.id, effects)) return effects
    const fields = emittableFields(schema)
    if (fields.length === 0) return effects
    return [
        ...effects,
        {
            type: "state_snapshot",
            connection_id: command.event.connection.id,
            state: Object.fromEntries(
                fields.filter(field => Object.hasOwn(state, field.name)).map(field => [field.name, state[field.name]])
            )
        }
    ]
}

function stateUpdates(before: JsonObject, after: JsonObject, schema: ActorSchema, except?: string): SocketEffect[] {
    const changed = emittableFields(schema).filter(
        field =>
            Object.hasOwn(before, field.name) !== Object.hasOwn(after, field.name) ||
            !isDeepStrictEqual(before[field.name], after[field.name])
    )
    if (changed.length === 0) return []
    return [
        {
            type: "state_update",
            changes: Object.fromEntries(
                changed.filter(field => Object.hasOwn(after, field.name)).map(field => [field.name, after[field.name]])
            ),
            removed: changed.filter(field => !Object.hasOwn(after, field.name)).map(field => field.name),
            ...(except === undefined ? {} : { except_connection_ids: [except] })
        }
    ]
}

function emittableFields(schema: ActorSchema) {
    return schema.fields.filter(
        field => field.emittable && !field.visibility && !field.private && field.persistence === Persistence.Persisted
    )
}

function connectionWasRejected(connectionId: string, effects: readonly SocketEffect[]): boolean {
    return effects.some(effect => effect.type === "reject" && effect.connection_id === connectionId)
}

function reportFailure(actorName: string, operation: string, error: unknown): void {
    console.error(`${actorName}.${operation} failed:`, error)
}

async function ignoreAlarm(): Promise<void> {}

function lifecycleMethod(command: WebSocketEventCommand): "onConnect" | "onMessage" | "onDisconnect" {
    switch (command.event.type) {
        case "connect":
            return "onConnect"
        case "message":
            return "onMessage"
        case "disconnect":
            return "onDisconnect"
    }
}

function lifecycleArguments(
    command: WebSocketEventCommand,
    scope: ActorSocketScope,
    schemas: ActorDefinition["schemas"]
): readonly unknown[] {
    switch (command.event.type) {
        case "connect":
            return [scope.eventSocket(command.event.connection, "connecting")]
        case "message":
            return [scope.connection(command.event.connection_id), decodeSocketMessage(command.event.message, schemas)]
        case "disconnect":
            return [
                scope.eventSocket(command.event.connection, "closed"),
                command.event.code,
                command.event.reason,
                command.event.was_clean
            ]
    }
}

function persistedState(value: JsonValue): JsonObject {
    if (!isJsonObject(value)) {
        throw new ActorProtocolError("persisted actor state must be a JSON object")
    }
    return value
}

function snapshotActorState(instance: object, schema: ActorSchema): JsonObject {
    validateActorState(instance, schema)
    const state = Object.fromEntries(
        schema.fields
            .filter(field => field.persistence === Persistence.Persisted && Object.hasOwn(instance, field.name))
            .map(field => [field.name, Reflect.get(instance, field.name)])
    )
    return cloneJsonObject(state, "actor state")
}

function hydrateActorState(instance: object, state: JsonObject, schema: ActorSchema): void {
    validateActorState(instance, schema)
    const restored = cloneJsonObject(
        Object.fromEntries(
            schema.fields
                .filter(field => field.persistence === Persistence.Persisted && Object.hasOwn(state, field.name))
                .map(field => [field.name, state[field.name]])
        ),
        "actor state"
    )
    for (const [key, value] of Object.entries(restored)) {
        if (!Reflect.defineProperty(instance, key, { configurable: true, enumerable: true, writable: true, value }))
            throw new ActorSerializationError(`actor field ${key} cannot be restored`)
    }
}

// Make sure every public field in the Actor has a @Persisted or @Ephemeral decorator! Throws otherwise
function validateActorState(instance: object, schema: ActorSchema): void {
    const fields = new Set(schema.fields.filter(field => !field.private).map(field => field.name))
    for (const key of Reflect.ownKeys(instance)) {
        if (typeof key !== "string" || !fields.has(key))
            throw new ActorDefinitionError(
                `actor field ${schema.actorName}.${String(key)} must declare @Persisted or @Ephemeral`
            )
        const descriptor = Object.getOwnPropertyDescriptor(instance, key)!
        if (!("value" in descriptor) && !isPersistedField(instance, key, descriptor))
            throw new ActorDefinitionError(`actor field ${schema.actorName}.${key} must be a data property`)
    }
}

export { ActorRuntime, hydrateActorState, snapshotActorState }
