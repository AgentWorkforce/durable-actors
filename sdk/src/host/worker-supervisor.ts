import { Worker } from "node:worker_threads"

import type { ActorIdentity } from "../actor/identity.js"
import type { SocketQuery } from "../actor/socketProtocol.js"
import { errorMessage } from "../errors.js"
import type { JsonValue } from "../json.js"

import type { DatabaseRequest } from "./database-bridge.js"
import { completeDatabaseCall } from "./database-bridge.js"
import type { DatabaseCall } from "./database.js"
import type {
    ActorCallRequest,
    ActorExecutionCommand,
    ActorExecutorCommand,
    ActorExecutorReply,
    ActorLog,
    ActorWorkerCommand,
    ActorWorkerData,
    ActorWorkerMessage,
    ActorWorkerRequest,
    EvictCommand
} from "./protocol.js"
import { failedReply } from "./protocol.js"
import type {
    ActorCallRequester,
    ActorWorkerFactory,
    ActorWorkerHandle,
    ActorWorkerState,
    ActorWorkerSupervisorOptions,
    DatabaseRequester,
    LogWriter,
    ResidentActorWorkerOptions,
    SocketPublisher,
    SocketSource
} from "./types.js"

class ActorWorker implements ActorWorkerHandle {
    private readonly worker: Worker
    private readonly readyPromise: Promise<readonly string[]>
    private readyResolve: ((actorNames: readonly string[]) => void) | undefined
    private readyReject: ((error: Error) => void) | undefined
    private readonly pending = new Map<
        number,
        {
            resolve: (reply: ActorExecutorReply) => void
            reject: (error: Error) => void
            allowNextInvocation: (() => void) | undefined
            database: DatabaseRequester
            publish: SocketPublisher | undefined
            connections: SocketSource | undefined
        }
    >()
    private readonly databaseCalls = new Set<DatabaseRequest>()
    private nextMessageId = 0
    private lifecycleState: ActorWorkerState = "starting"
    private terminalError: Error | undefined

    private readonly warmPromise: Promise<void>
    private warmResolve: (() => void) | undefined
    private warmReject: ((error: Error) => void) | undefined
    private assigned: boolean

    constructor(
        data?: ActorWorkerData,
        private onStateChange: () => void = () => {},
        private activationDatabase: DatabaseRequester = unavailableDatabase,
        private actorCalls: ActorCallRequester = unavailableActorCalls,
        private logs: LogWriter = writeToStdio
    ) {
        this.assigned = data !== undefined
        this.warmPromise = new Promise((resolve, reject) => {
            this.warmResolve = resolve
            this.warmReject = reject
        })
        void this.warmPromise.catch(() => undefined)
        this.readyPromise = new Promise<readonly string[]>((resolve, reject) => {
            this.readyResolve = resolve
            this.readyReject = reject
        })
        void this.readyPromise.catch(() => undefined)
        this.worker = new Worker(new URL("./actor-worker.js", import.meta.url), { workerData: data })
        this.worker.on("message", (message: ActorWorkerMessage) => this.receive(message))
        this.worker.once("error", error => this.stop(error))
        this.worker.once("exit", code => this.exited(code))
        this.worker.unref()
    }

    private receive(message: ActorWorkerMessage): void {
        if (this.lifecycleState === "stopping" || this.lifecycleState === "stopped") return
        switch (message.type) {
            case "warm":
                this.warmResolve?.()
                this.warmResolve = undefined
                this.warmReject = undefined
                return
            case "ready":
                if (this.lifecycleState !== "starting") return
                this.lifecycleState = "ready"
                this.readyResolve?.(message.actorNames)
                this.readyResolve = undefined
                this.readyReject = undefined
                this.onStateChange()
                return
            case "ready_for_invocation": {
                const pending = this.pending.get(message.messageId)
                if (pending?.allowNextInvocation === undefined)
                    return this.stop(new Error("actor sent an unexpected invocation admission"))
                const allowNextInvocation = pending.allowNextInvocation
                pending.allowNextInvocation = undefined
                allowNextInvocation()
                return
            }
            case "database_call":
                void this.callDatabase(message.messageId, message.call, message.request)
                return
            case "get_connections":
                void this.loadConnections(message.messageId, message.query)
                return
            case "actor_call":
                void this.callActor(message.callId, message.call)
                return
            case "socket_effects":
                this.pending.get(message.messageId)?.publish?.(message.effects)
                return
            case "log":
                this.logs(message)
                return
            case "failed":
                this.stop(new Error(message.message))
                return
            case "reply":
                if (this.readyResolve !== undefined)
                    return this.stop(new Error(`actor Worker sent ${message.type} before ready`))
                this.reply(message.messageId, message.reply)
                return
            default:
                throw message satisfies never
        }
    }

    warm(): Promise<void> {
        this.worker.ref()
        return this.warmPromise
    }

    load(
        data: ActorWorkerData,
        onStateChange: () => void,
        database: DatabaseRequester,
        calls: ActorCallRequester,
        logs: LogWriter = writeToStdio
    ): void {
        if (this.assigned) throw new Error("customer code already assigned")
        this.onStateChange = onStateChange
        this.activationDatabase = database
        this.actorCalls = calls
        this.logs = logs
        this.assigned = true
        this.post({ type: "load", data })
    }

    get state(): ActorWorkerState {
        return this.lifecycleState
    }

    ready(): Promise<readonly string[]> {
        if (this.lifecycleState === "stopping" || this.lifecycleState === "stopped")
            return Promise.reject(this.terminalError)
        this.worker.ref()
        return this.readyPromise.finally(() => {
            if (this.pending.size === 0) this.worker.unref()
        })
    }

    async execute(
        command: ActorWorkerCommand,
        allowNextInvocation: () => void,
        database: DatabaseRequester,
        publish?: SocketPublisher,
        connections?: SocketSource
    ): Promise<ActorExecutorReply> {
        if (this.terminalError !== undefined) throw this.terminalError
        this.worker.ref()
        try {
            await this.readyPromise
            if (this.terminalError !== undefined) throw this.terminalError
            return await new Promise<ActorExecutorReply>((resolve, reject) => {
                const messageId = ++this.nextMessageId
                this.pending.set(messageId, {
                    resolve,
                    reject,
                    allowNextInvocation,
                    database,
                    publish,
                    connections
                })
                this.post({ type: "execute", messageId, command })
            })
        } finally {
            if (this.pending.size === 0) this.worker.unref()
        }
    }

    terminate(reason: string): void {
        this.stop(new ActorWorkerTerminatedError(reason))
    }

    private async callDatabase(
        messageId: number | undefined,
        call: DatabaseCall,
        request: DatabaseRequest
    ): Promise<void> {
        this.databaseCalls.add(request)
        try {
            const database =
                (messageId === undefined ? undefined : this.pending.get(messageId)?.database) ?? this.activationDatabase
            completeDatabaseCall(request, { result: await database(call) })
        } catch (error) {
            completeDatabaseCall(request, { error: errorMessage(error) })
        } finally {
            this.databaseCalls.delete(request)
        }
    }

    private async callActor(callId: number, call: ActorCallRequest): Promise<void> {
        let outcome: JsonValue
        try {
            outcome = await this.actorCalls(call)
        } catch (error) {
            outcome = { type: "failed", code: "outcome_unknown", message: errorMessage(error) }
        }
        if (this.lifecycleState === "ready") this.post({ type: "actor_result", callId, outcome })
    }

    private async loadConnections(messageId: number, query?: SocketQuery): Promise<void> {
        try {
            const source = this.pending.get(messageId)?.connections
            if (source === undefined) throw new Error("actor connection lookup is unavailable")
            const connections = await source(query)
            this.post({ type: "socket_connections", messageId, connections })
        } catch (error) {
            this.post({ type: "socket_connections", messageId, connections: [], error: errorMessage(error) })
        }
    }

    private reply(messageId: number, reply: ActorExecutorReply): void {
        const pending = this.pending.get(messageId)
        if (pending === undefined) return this.stop(new Error("actor replied to an unknown invocation"))
        this.pending.delete(messageId)
        pending.resolve(reply)
        if (this.pending.size === 0) this.worker.unref()
    }

    private post(message: ActorWorkerRequest): void {
        this.worker.postMessage(message)
    }

    private stop(error: Error): void {
        if (this.lifecycleState === "stopping" || this.lifecycleState === "stopped") return
        this.lifecycleState = "stopping"
        this.fail(error)
        void this.worker.terminate()
        this.onStateChange()
    }

    private exited(code: number): void {
        this.lifecycleState = "stopped"
        this.fail(new Error(`actor Worker exited with code ${code}`))
        this.onStateChange()
    }

    private fail(error: Error): void {
        this.warmReject?.(error)
        this.terminalError ??= error
        this.readyReject?.(this.terminalError)
        this.readyResolve = undefined
        this.readyReject = undefined
        for (const request of this.databaseCalls) completeDatabaseCall(request, { error: this.terminalError.message })
        this.databaseCalls.clear()
        for (const pending of this.pending.values()) pending.reject(this.terminalError)
        this.pending.clear()
        this.worker.unref()
    }
}

class ActorWorkerSupervisor {
    private readonly actorEntrypointUrl: string
    private readonly createWorker: ActorWorkerFactory
    private resident: ResidentActorWorker | undefined
    private lastResident = false
    private readonly residencyListeners = new Set<() => void>()
    private speculativeWorker: ActorWorkerHandle | undefined
    private identity: ActorIdentity | undefined
    private activationDatabase: DatabaseRequester = unavailableDatabase
    private actorCalls: ActorCallRequester = unavailableActorCalls
    private logs: LogWriter = writeToStdio
    private closed = false
    private actorNames: readonly string[] | undefined
    private lastSequence = 0

    constructor(options: ActorWorkerSupervisorOptions) {
        this.actorEntrypointUrl = options.actorEntrypointUrl
        this.createWorker =
            options.createWorker ??
            ((data, onStateChange, database, calls, logs) =>
                new ActorWorker(data, onStateChange, database, calls, logs))
        this.preload()
    }

    async ready(): Promise<readonly string[]> {
        if (this.closed) throw new Error("actor supervisor is closed")
        if (this.actorNames !== undefined) return this.actorNames
        const worker = this.speculativeWorker ?? this.preload()
        this.actorNames = await worker.ready()
        return this.actorNames
    }

    assign(
        actor: ActorIdentity,
        database: DatabaseRequester,
        calls: ActorCallRequester,
        logs: LogWriter = writeToStdio
    ): void {
        if (!this.actorNames?.includes(actor.actor_name))
            throw new Error(`actor name ${actor.actor_name} is not loaded in this customer process`)
        this.identity = { ...actor }
        this.activationDatabase = database
        this.actorCalls = calls
        this.logs = logs
    }

    async handle(
        command: ActorExecutorCommand,
        allowNextInvocation: () => void,
        database: DatabaseRequester,
        publish?: SocketPublisher,
        connections?: SocketSource
    ): Promise<ActorExecutorReply> {
        if (this.closed) return failedReply("actor_worker_terminated", "actor supervisor is closed")
        switch (command.type) {
            case "hydrate":
            case "invoke":
            case "websocket_event":
                try {
                    return await this.execute(command, allowNextInvocation, database, publish, connections)
                } catch (error) {
                    return failedReply("actor_worker_failed", errorMessage(error))
                }
            case "evict":
                return this.evict(command, allowNextInvocation, database)
            default:
                throw command satisfies never
        }
    }

    onResidencyChange(listener: () => void): () => void {
        this.residencyListeners.add(listener)
        return () => {
            this.residencyListeners.delete(listener)
        }
    }

    private notifyResidencyChange(): void {
        const current = this.isResident()
        if (current === this.lastResident) return
        this.lastResident = current
        for (const listener of this.residencyListeners) listener()
    }

    isResident(): boolean {
        return this.resident?.isActive() ?? false
    }

    close(): void {
        this.closed = true
        this.takeSpeculativeWorker()?.terminate("actor supervisor is closed")
        this.resident?.terminate("actor supervisor is closed")
        this.resident = undefined
    }

    private preload(): ActorWorkerHandle {
        const worker = this.createWorker(
            { moduleUrl: this.actorEntrypointUrl },
            () => this.notifyResidencyChange(),
            this.callActivationDatabase,
            this.callActor,
            this.writeLog
        )
        this.speculativeWorker = worker
        void worker.ready().catch(() => this.discardPreload(worker))
        return worker
    }

    private discardPreload(worker: ActorWorkerHandle): void {
        if (this.speculativeWorker !== worker) return
        this.takeSpeculativeWorker()?.terminate("actor preload failed")
    }

    private execute(
        command: ActorExecutionCommand,
        allowNextInvocation: () => void,
        database: DatabaseRequester,
        publish?: SocketPublisher,
        connections?: SocketSource
    ): Promise<ActorExecutorReply> {
        const workerCommand = this.workerCommand(command)
        let actor = this.resident
        if (actor === undefined) {
            actor = new ResidentActorWorker({
                sequenceBase: this.lastSequence,
                moduleUrl: this.actorEntrypointUrl,
                worker: this.takeSpeculativeWorker(),
                createWorker: this.createWorker,
                onResidencyChange: () => this.notifyResidencyChange(),
                database: this.callActivationDatabase,
                calls: this.callActor,
                logs: this.writeLog
            })
            this.resident = actor
        }
        const base = actor.sequenceBase
        return actor.execute(workerCommand, allowNextInvocation, database, publish, connections).then(reply => {
            if ((reply.type === "invoked" || reply.type === "websocket_handled") && reply.sequence !== undefined) {
                const sequence = base + reply.sequence
                this.lastSequence = Math.max(this.lastSequence, sequence)
                return { ...reply, sequence }
            }
            return reply
        })
    }

    private async evict(
        command: EvictCommand,
        allowNextInvocation: () => void,
        database: DatabaseRequester
    ): Promise<ActorExecutorReply> {
        const actor = this.resident
        if (actor === undefined) return { type: "evicted" }
        try {
            await actor.execute(this.workerCommand(command), allowNextInvocation, database)
        } catch {
            // The thread is still stopped when onSleep fails.
        }
        this.resident = undefined
        actor.terminate("resident actor was evicted by the Rust host")
        return { type: "evicted" }
    }

    private readonly callActivationDatabase: DatabaseRequester = call => this.activationDatabase(call)

    private readonly callActor: ActorCallRequester = call => this.actorCalls(call)

    private readonly writeLog: LogWriter = log => this.logs(log)

    private workerCommand(command: ActorExecutorCommand): ActorWorkerCommand {
        if (this.identity === undefined) throw new Error("actor supervisor has no assigned actor")
        return { ...command, actor: this.identity }
    }

    private takeSpeculativeWorker(): ActorWorkerHandle | undefined {
        const worker = this.speculativeWorker
        this.speculativeWorker = undefined
        return worker
    }
}

class ResidentActorWorker {
    readonly sequenceBase: number
    readonly moduleUrl: string
    readonly createWorker: ActorWorkerFactory
    private readonly onResidencyChange: () => void
    private readonly database: DatabaseRequester
    private readonly calls: ActorCallRequester
    private readonly logs: LogWriter
    private worker: ActorWorkerHandle | undefined

    constructor(options: ResidentActorWorkerOptions) {
        this.sequenceBase = options.sequenceBase
        this.moduleUrl = options.moduleUrl
        this.createWorker = options.createWorker
        this.onResidencyChange = options.onResidencyChange
        this.database = options.database
        this.calls = options.calls
        this.logs = options.logs
        this.worker = options.worker
    }

    async execute(
        command: ActorWorkerCommand,
        allowNextInvocation: () => void,
        database: DatabaseRequester,
        publish?: SocketPublisher,
        connections?: SocketSource
    ): Promise<ActorExecutorReply> {
        this.worker ??= this.createWorker(
            { moduleUrl: this.moduleUrl },
            this.onResidencyChange,
            this.database,
            this.calls,
            this.logs
        )
        const worker = this.worker
        this.onResidencyChange()
        let reply: ActorExecutorReply
        try {
            reply = await worker.execute(command, allowNextInvocation, database, publish, connections)
        } catch (error) {
            reply = failedReply(
                error instanceof ActorWorkerTerminatedError ? "actor_worker_terminated" : "actor_worker_failed",
                errorMessage(error)
            )
        }
        if (this.worker !== worker)
            return failedReply("actor_worker_terminated", "resident actor was terminated during invocation")
        if (
            reply.type === "failed" &&
            !["actor_method_failed", "actor_socket_failed", "method_not_found", "method_not_callable"].includes(
                reply.code
            )
        ) {
            worker.terminate("actor invocation failed")
            this.worker = undefined
            this.onResidencyChange()
        }
        return reply
    }

    isActive(): boolean {
        return this.worker?.state === "ready"
    }

    terminate(reason: string): void {
        this.worker?.terminate(reason)
        this.worker = undefined
        this.onResidencyChange()
    }
}

// Before the host session attaches there is no socket, so output keeps going to the terminal.
function writeToStdio(log: ActorLog): void {
    const stream = log.level === "warn" || log.level === "error" ? process.stderr : process.stdout
    stream.write(`${log.message}\n`)
}

async function unavailableDatabase(): Promise<never> {
    throw new Error("actor database is unavailable before the actor is assigned")
}

async function unavailableActorCalls(): Promise<never> {
    throw new Error("actor calls are unavailable before the actor is assigned")
}

class ActorWorkerTerminatedError extends Error {
    constructor(message: string) {
        super(message)
        this.name = "ActorWorkerTerminatedError"
    }
}

export { ActorWorker, ActorWorkerSupervisor }
