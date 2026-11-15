import { findActorDefinition } from "../actor/actor.js"
import type { ActorDefinition } from "../actor/actor.js"
import type { ActorIdentity } from "../actor/identity.js"
import type { HostSocketEffect, SocketLookup, SocketQuery } from "../actor/socketProtocol.js"
import { remoteActorClient, runWithActorClient } from "../client/client.js"
import { errorMessage } from "../errors.js"
import type { JsonValue } from "../json.js"

import { loadActorEntrypoint } from "./actor-entrypoint.js"
import { ActorRuntime } from "./actor-runtime.js"
import { captureConsole } from "./console-capture.js"
import { DatabaseProxy } from "./database.js"
import type { DatabaseCall, DatabaseResult } from "./database.js"
import { HostActorClient } from "./host-actor-client.js"
import { currentInvocation, runInvocation } from "./invocation.js"
import type { Invocation } from "./invocation.js"
import { failedReply } from "./protocol.js"
import type {
    ActorCallRequest,
    ActorExecutionCommand,
    ActorExecutorCommand,
    ActorExecutorReply,
    ActorLog,
    AssignedCommand,
    EvictCommand
} from "./protocol.js"
import type { ActorCallRequester, InvocationDatabase, LogWriter, ResidentActorOptions } from "./types.js"

// The actor's own code caused these, and its state was rolled back. Any other failure means the state in memory
// cannot be trusted, so the actor starts again from its database.
const RECOVERABLE = new Set(["actor_method_failed", "actor_socket_failed", "method_not_found", "method_not_callable"])

/**
 * The actor this process runs. Its code runs on the thread that reads the session, so a database call holds the
 * whole process until the Rust host answers.
 */
class ResidentActor {
    private loading: Promise<readonly string[]> | undefined
    private actorNames: readonly string[] | undefined
    private identity: ActorIdentity | undefined
    private calls: ActorCallRequester = unavailableActorCalls
    private logs: LogWriter = writeToStdio
    private activation: Activation | undefined
    private lastResident = false
    private lastSequence = 0
    private closed = false
    private readonly residencyListeners = new Set<() => void>()
    private readonly client = new HostActorClient(call => this.callActor(call), remoteActorClient)
    private readonly releaseConsole: () => void

    constructor(private readonly options: ResidentActorOptions) {
        this.releaseConsole = captureConsole((level, message) =>
            this.logs({ messageId: currentInvocation()?.id, level, message })
        )
        process.on("uncaughtException", this.crashed)
        process.on("unhandledRejection", this.crashed)
    }

    /** Runs the customer's module, once. Resolves with the names of the actors it exports. */
    ready(): Promise<readonly string[]> {
        if (this.closed) return Promise.reject(new Error("actor host is closed"))
        this.loading ??= loadActorEntrypoint(this.options.actorEntrypointUrl).then(names => (this.actorNames = names))
        return this.loading
    }

    assign(actor: ActorIdentity, calls: ActorCallRequester, logs: LogWriter = writeToStdio): void {
        if (!this.actorNames?.includes(actor.actor_name))
            throw new Error(`actor name ${actor.actor_name} is not loaded in this customer process`)
        this.identity = { ...actor }
        this.calls = calls
        this.logs = logs
    }

    async handle(command: ActorExecutorCommand, invocation: Invocation): Promise<ActorExecutorReply> {
        if (this.closed) return failedReply("actor_worker_terminated", "actor host is closed")
        try {
            return command.type === "evict"
                ? await this.evict(command, invocation)
                : await this.execute(command, invocation)
        } catch (error) {
            return failedReply("actor_worker_failed", errorMessage(error))
        }
    }

    isResident(): boolean {
        return this.activation !== undefined
    }

    onResidencyChange(listener: () => void): () => void {
        this.residencyListeners.add(listener)
        return () => {
            this.residencyListeners.delete(listener)
        }
    }

    close(): void {
        if (this.closed) return
        this.closed = true
        this.stop(failedReply("actor_worker_terminated", "actor host is closed"))
        process.off("uncaughtException", this.crashed)
        process.off("unhandledRejection", this.crashed)
        this.releaseConsole()
    }

    private async execute(command: ActorExecutionCommand, invocation: Invocation): Promise<ActorExecutorReply> {
        const actor = this.assignedActor()
        const activation = this.activate(actor)
        const reply = await activation.run({ ...command, actor }, invocation)
        if (reply.type === "failed" && !RECOVERABLE.has(reply.code) && this.activation === activation)
            this.stop(failedReply("actor_worker_terminated", "the actor was stopped by a failed invocation"))
        return this.inOrder(reply, activation.sequenceBase)
    }

    private async evict(command: EvictCommand, invocation: Invocation): Promise<ActorExecutorReply> {
        // The actor leaves memory even when its shutdown fails.
        await this.activation?.run({ ...command, actor: this.assignedActor() }, invocation)
        this.stop(failedReply("actor_worker_terminated", "the actor was evicted by the Rust host"))
        return { type: "evicted" }
    }

    private activate(actor: ActorIdentity): Activation {
        if (this.activation !== undefined) return this.activation
        const definition = findActorDefinition(actor.actor_name)
        if (definition === undefined)
            throw new Error(`actor entrypoint ${this.options.actorEntrypointUrl} does not export ${actor.actor_name}`)
        this.activation = new Activation(definition, this.options.database, this.client, this.lastSequence)
        this.notifyResidencyChange()
        return this.activation
    }

    private stop(reply: ActorExecutorReply): void {
        const activation = this.activation
        this.activation = undefined
        activation?.stop(reply)
        this.notifyResidencyChange()
    }

    // An activation numbers its replies from one, so the numbers of the one before are carried over.
    private inOrder(reply: ActorExecutorReply, base: number): ActorExecutorReply {
        if ((reply.type !== "invoked" && reply.type !== "websocket_handled") || reply.sequence === undefined)
            return reply
        const sequence = base + reply.sequence
        this.lastSequence = Math.max(this.lastSequence, sequence)
        return { ...reply, sequence }
    }

    private assignedActor(): ActorIdentity {
        if (this.identity === undefined) throw new Error("actor host has no assigned actor")
        return this.identity
    }

    private notifyResidencyChange(): void {
        const current = this.isResident()
        if (current === this.lastResident) return
        this.lastResident = current
        for (const listener of this.residencyListeners) listener()
    }

    private async callActor(call: ActorCallRequest): Promise<JsonValue> {
        try {
            return await this.calls(call)
        } catch (error) {
            return { type: "failed", code: "outcome_unknown", message: errorMessage(error) }
        }
    }

    // An error nothing caught leaves no way to know what the actor's code was in the middle of.
    private readonly crashed = (error: unknown): void => {
        console.error("actor code failed outside a call:", error)
        this.stop(failedReply("actor_worker_failed", errorMessage(error)))
    }
}

/** One stay of the actor in memory: from its first command until it is evicted or can no longer be trusted. */
class Activation {
    private readonly runtime: ActorRuntime
    private readonly running = new Map<Invocation, (reply: ActorExecutorReply) => void>()
    private stopped = false

    constructor(
        definition: ActorDefinition,
        private readonly database: InvocationDatabase,
        private readonly client: HostActorClient,
        readonly sequenceBase: number
    ) {
        this.runtime = new ActorRuntime(
            definition,
            () => invocationOf("invocation admission").allowNext(),
            new DatabaseProxy(call => this.callDatabase(call)),
            effects => this.publish(effects),
            query => this.connections(query)
        )
    }

    /** Resolves when the command replies, or as soon as the activation stops. */
    run(command: AssignedCommand, invocation: Invocation): Promise<ActorExecutorReply> {
        return new Promise(resolve => {
            const settle = (reply: ActorExecutorReply): void => {
                this.running.delete(invocation)
                resolve(reply)
            }
            this.running.set(invocation, settle)
            runInvocation(invocation, () => {
                void runWithActorClient(this.client, () => this.runtime.handle(command)).then(settle, error =>
                    settle(failedReply("actor_worker_failed", errorMessage(error)))
                )
            })
        })
    }

    /**
     * Answers what is still running with `reply`. Its code cannot be interrupted, so whatever it does afterwards
     * is kept from the database and the sockets.
     */
    stop(reply: ActorExecutorReply): void {
        this.stopped = true
        for (const settle of [...this.running.values()]) settle(reply)
    }

    // The Rust host refuses a call that names a command it has already answered, so code that outlives its
    // command, such as a timer, names none and reaches the activation's database.
    private callDatabase(call: DatabaseCall): DatabaseResult {
        if (this.stopped) throw new Error("the actor was stopped, so its database is closed to code still running")
        const invocation = currentInvocation()
        return this.database(call, invocation !== undefined && this.running.has(invocation) ? invocation.id : undefined)
    }

    private publish(effects: readonly HostSocketEffect[]): void {
        const invocation = invocationOf("socket output")
        if (this.running.has(invocation)) invocation.publish(effects)
    }

    private async connections(query?: SocketQuery): Promise<SocketLookup> {
        const invocation = invocationOf("connection lookup")
        if (!this.running.has(invocation)) throw new Error("actor connection lookup is unavailable")
        return invocation.connections(query)
    }
}

function invocationOf(operation: string): Invocation {
    const invocation = currentInvocation()
    if (invocation === undefined) throw new Error(`${operation} has no active invocation`)
    return invocation
}

// Before the host session attaches there is no socket, so output keeps going to the terminal.
function writeToStdio(log: ActorLog): void {
    const stream = log.level === "warn" || log.level === "error" ? process.stderr : process.stdout
    stream.write(`${log.message}\n`)
}

async function unavailableActorCalls(): Promise<never> {
    throw new Error("actor calls are unavailable before the actor is assigned")
}

export { ResidentActor }
