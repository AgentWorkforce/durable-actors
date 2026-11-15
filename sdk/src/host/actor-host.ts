import { stat } from "node:fs/promises"
import { type Socket, createConnection } from "node:net"
import path from "node:path"
import { pathToFileURL } from "node:url"
import { z } from "zod"

import type { SocketLookup, SocketQuery } from "../actor/socketProtocol.js"
import { ActorConfigurationError, ActorProtocolError, ActorSessionError } from "../errors.js"
import type { JsonValue } from "../json.js"
import { DescriptorStream } from "../payload/blocking-stream.js"
import { DatabaseChannel, DatabaseChannelError } from "../payload/database-channel.js"
import { GarbageCollection } from "../payload/garbage-collection.js"
import { SessionLines } from "../payload/session-lines.js"
import { SessionWriter } from "../payload/session-writer.js"
import type { SessionText } from "../payload/session-writer.js"

import type { Invocation } from "./invocation.js"
import { detachEffects, detachReply, parseActorSessionServerMessage } from "./protocol.js"
import type { ActorCallRequest, ActorExecutorReply, ActorSessionClientMessage, SessionPart } from "./protocol.js"
import { ResidentActor } from "./resident-actor.js"
import type { ActorHostSettings, InvocationDatabase, SessionActor, SessionActorFactory } from "./types.js"

async function runActorHost(): Promise<never> {
    const session = new ActorSession()
    await session.start()

    await session.waitUntilDisconnected()
    throw new ActorSessionError("Rust host disconnected from actor session")
}

class ActorSession {
    private startup: Promise<void> | undefined
    private connection: ActorSessionConnection | undefined
    // One count of what has passed, whichever connection it passed on.
    private readonly collection = new GarbageCollection()

    constructor(
        private readonly settings: ActorHostSettings = parseHostSettings(process.env),
        private readonly createActor: SessionActorFactory = options => new ResidentActor(options),
        private readonly socket?: Socket
    ) {}

    start(): Promise<void> {
        this.startup ??= this.initialize()
        return this.startup
    }

    waitUntilDisconnected(): Promise<void> {
        if (this.connection === undefined) throw new ActorSessionError("actor session has not started")
        return this.connection.closed()
    }

    private async initialize(): Promise<void> {
        const actorEntrypointUrl = await resolveActorEntrypoint(this.settings.actorEntrypoint)
        const actor = this.createActor({ actorEntrypointUrl, database: this.openDatabase() })
        try {
            const actorNames = await discoverActorNames(actor, this.settings.startupTimeoutMs)
            if (actorNames.length === 0)
                throw new ActorSessionError("the actor entrypoint does not export any actor classes")
            const socket = this.socket ?? (await connectSocket(this.settings.socketPath))
            // Known before the socket delivers a command, so a database failure inside one can end the session.
            this.connection = new ActorSessionConnection(socket, actor, this.collection)
            await this.connection.attach(actorNames, this.settings.startupTimeoutMs)
            void this.connection.closed().then(() => actor.close())
        } catch (error) {
            actor.close()
            throw error
        }
    }

    private openDatabase(): InvocationDatabase {
        const descriptor = this.settings.databaseDescriptor
        if (descriptor === undefined) return unavailableDatabase
        const channel = new DatabaseChannel(new DescriptorStream(descriptor), this.collection)
        return (call, invocation) => {
            try {
                return channel.call(call, invocation)
            } catch (error) {
                // No later call can be answered, so the process ends and the Rust host starts another.
                if (error instanceof DatabaseChannelError) this.connection?.fail(error)
                throw error
            }
        }
    }
}

async function discoverActorNames(actor: Pick<SessionActor, "ready">, timeoutMs: number): Promise<readonly string[]> {
    let timer: NodeJS.Timeout | undefined
    try {
        return await Promise.race([
            actor.ready(),
            new Promise<never>((_, reject) => {
                timer = setTimeout(
                    () => reject(new ActorSessionError(`actor module loading timed out after ${timeoutMs}ms`)),
                    timeoutMs
                )
            })
        ])
    } finally {
        clearTimeout(timer)
    }
}

class ActorSessionConnection {
    private unsubscribeActivity: (() => void) | undefined
    private activityTimer: NodeJS.Timeout | undefined
    private readonly lines = new SessionLines()
    private readonly writer: SessionWriter
    private attachedResolve: (() => void) | undefined
    private attachedReject: ((error: Error) => void) | undefined
    private readonly attachedPromise: Promise<void>
    private closedResolve: (() => void) | undefined
    private readonly closedPromise: Promise<void>
    private readonly actorCalls = new Map<
        number,
        { resolve: (outcome: JsonValue) => void; reject: (error: Error) => void }
    >()
    private nextActorCallId = 0
    private readonly loadingConnections = new Map<
        number,
        { resolve: (connections: SocketLookup) => void; reject: (error: Error) => void }
    >()

    constructor(
        private readonly socket: Socket,
        private readonly actor: SessionActor,
        collection: GarbageCollection
    ) {
        this.attachedPromise = new Promise<void>((resolve, reject) => {
            this.attachedResolve = resolve
            this.attachedReject = reject
        })
        this.closedPromise = new Promise<void>(resolve => {
            this.closedResolve = resolve
        })
        this.writer = new SessionWriter(socket, collection)
        this.bindSocket()
    }

    async attach(actorNames: readonly string[], timeoutMs: number): Promise<void> {
        this.send({ type: "attach", protocol: 33, actor_names: actorNames })
        await this.waitUntilAttached(timeoutMs)
    }

    closed(): Promise<void> {
        return this.closedPromise
    }

    fail(error: Error): void {
        this.attachedReject?.(error)
        this.attachedResolve = undefined
        this.attachedReject = undefined
        this.socket.destroy()
    }

    private waitUntilAttached(timeoutMs: number): Promise<void> {
        return new Promise<void>((resolve, reject) => {
            const timeout = setTimeout(() => {
                const error = new ActorSessionError(`actor session attachment timed out after ${timeoutMs}ms`)
                this.fail(error)
                reject(error)
            }, timeoutMs)
            void this.attachedPromise.then(
                () => {
                    clearTimeout(timeout)
                    resolve()
                },
                error => {
                    clearTimeout(timeout)
                    reject(error)
                }
            )
        })
    }

    private bindSocket(): void {
        this.socket.on("data", (chunk: Buffer) => this.acceptChunk(chunk))
        this.socket.once("error", error => this.fail(error))
        this.socket.once("close", () => this.close())
    }

    private acceptChunk(chunk: Buffer): void {
        try {
            for (const line of this.lines.accept(chunk)) void this.handle(line)
        } catch (error) {
            this.fail(sessionError(error))
        }
    }

    private async handle(line: unknown): Promise<void> {
        try {
            const message = parseActorSessionServerMessage(line)
            switch (message.type) {
                case "attached":
                    this.actor.assign(
                        message.actor,
                        call => this.callActor(call),
                        log =>
                            this.send({
                                type: "log",
                                message_id: log.messageId,
                                level: log.level,
                                message: log.message
                            })
                    )
                    this.reportResidency()
                    this.attachedResolve?.()
                    this.attachedResolve = undefined
                    this.attachedReject = undefined
                    break
                case "command":
                    this.reply(
                        message.message_id,
                        await this.actor.handle(message.command, this.invocation(message.message_id))
                    )
                    break
                case "actor_result": {
                    const pending = this.actorCalls.get(message.call_id)
                    if (pending === undefined) throw new ActorProtocolError("Rust host replied to unknown actor call")
                    this.actorCalls.delete(message.call_id)
                    pending.resolve(message.outcome)
                    break
                }
                case "socket_connections": {
                    const pending = this.loadingConnections.get(message.message_id)
                    if (pending === undefined)
                        throw new ActorProtocolError("Rust host replied to unknown connection lookup")
                    this.loadingConnections.delete(message.message_id)
                    if (message.error === undefined) pending.resolve(message.connections)
                    else pending.reject(new ActorSessionError(message.error))
                    break
                }
                default:
                    throw message satisfies never
            }
        } catch (error) {
            this.fail(sessionError(error))
        }
    }

    private reportResidency(): void {
        if (this.activityTimer) return
        const report = () => {
            try {
                this.send({ type: "residency", resident: this.actor.isResident() })
            } catch (error) {
                this.fail(sessionError(error))
            }
        }
        this.unsubscribeActivity = this.actor.onResidencyChange(report)
        report()
        this.activityTimer = setInterval(report, 1_000)
        this.activityTimer.unref()
    }

    private invocation(messageId: number): Invocation {
        return {
            id: messageId,
            allowNext: () => this.send({ type: "ready_for_invocation", message_id: messageId }),
            publish: effects => this.send({ type: "socket_effects", message_id: messageId }, detachEffects(effects)),
            connections: query => this.getConnections(messageId, query)
        }
    }

    private callActor(call: ActorCallRequest): Promise<JsonValue> {
        return new Promise((resolve, reject) => {
            if (this.socket.destroyed) throw new ActorSessionError("Rust host disconnected before the actor call")
            const callId = ++this.nextActorCallId
            this.actorCalls.set(callId, { resolve, reject })
            try {
                this.send({ type: "actor_call", call_id: callId, call })
            } catch (error) {
                this.actorCalls.delete(callId)
                reject(sessionError(error))
            }
        })
    }

    private getConnections(messageId: number, query?: SocketQuery): Promise<SocketLookup> {
        return new Promise((resolve, reject) => {
            if (this.loadingConnections.has(messageId))
                throw new ActorProtocolError("actor connections are already being loaded")
            this.loadingConnections.set(messageId, { resolve, reject })
            try {
                this.send({ type: "get_connections", message_id: messageId, query })
            } catch (error) {
                this.loadingConnections.delete(messageId)
                reject(sessionError(error))
            }
        })
    }

    private reply(messageId: number, reply: ActorExecutorReply): void {
        const { parts, ...head } = detachReply(reply)
        this.send({ type: "reply", message_id: messageId, reply: head }, parts)
    }

    // The line announces the size of each part that follows it, in bytes of UTF-8.
    private send(message: ActorSessionClientMessage, parts: readonly SessionPart[] = []): void {
        const head = parts.length === 0 ? message : { ...message, parts: parts.map(partBytes) }
        this.writer.write([`${JSON.stringify(head)}\n`, ...parts.flatMap(partTexts)])
    }

    private close(): void {
        clearInterval(this.activityTimer)
        this.unsubscribeActivity?.()
        this.unsubscribeActivity = undefined
        for (const pending of this.actorCalls.values())
            pending.reject(new ActorSessionError("Rust host disconnected during an actor call"))
        this.actorCalls.clear()
        for (const pending of this.loadingConnections.values())
            pending.reject(new ActorSessionError("Rust host disconnected while loading connections"))
        this.loadingConnections.clear()
        this.attachedReject?.(new ActorSessionError("Rust host disconnected from actor session"))
        this.attachedResolve = undefined
        this.attachedReject = undefined
        this.closedResolve?.()
        this.closedResolve = undefined
    }
}

function connectSocket(socketPath: string): Promise<Socket> {
    return new Promise((resolve, reject) => {
        const socket = createConnection(socketPath)
        const onError = (error: Error): void => {
            socket.off("connect", onConnect)
            socket.destroy()
            reject(new ActorSessionError(`could not attach to Rust host at ${socketPath}`, { cause: error }))
        }
        const onConnect = (): void => {
            socket.off("error", onError)
            resolve(socket)
        }
        socket.once("error", onError)
        socket.once("connect", onConnect)
    })
}

function partBytes(part: SessionPart): number {
    return typeof part === "string" ? Buffer.byteLength(part) : part.byteLength
}

function partTexts(part: SessionPart): readonly SessionText[] {
    return typeof part === "string" ? [part] : part.chunks
}

function sessionError(error: unknown): Error {
    return error instanceof Error ? error : new ActorSessionError(String(error))
}

function unavailableDatabase(): never {
    throw new ActorSessionError("actor database is unavailable: the Rust host gave this process no database channel")
}

async function resolveActorEntrypoint(configured: string | undefined): Promise<string> {
    const entrypointPath = path.resolve(configured ?? DEFAULT_ACTOR_ENTRYPOINT)
    await requireFile(
        entrypointPath,
        configured === undefined
            ? `default actor entrypoint ${DEFAULT_ACTOR_ENTRYPOINT}`
            : `configured actor entrypoint ${configured}`
    )
    return pathToFileURL(entrypointPath).href
}

async function requireFile(filePath: string, label: string): Promise<void> {
    if (!(await isFile(filePath))) throw new ActorConfigurationError(`${label} is not a file`)
}

async function isFile(filePath: string): Promise<boolean> {
    try {
        return (await stat(filePath)).isFile()
    } catch {
        return false
    }
}

function parseHostSettings(environment: NodeJS.ProcessEnv): ActorHostSettings {
    const result = actorSessionSettingsSchema.safeParse(environment)
    if (!result.success)
        throw new ActorConfigurationError(`actor-host session settings are invalid: ${result.error.message}`)
    return {
        socketPath: result.data.DURABLE_ACTORS_EXECUTOR_SOCKET,
        actorEntrypoint: result.data.DURABLE_ACTORS_ENTRYPOINT,
        startupTimeoutMs: parseStartupTimeout(environment.DURABLE_ACTORS_HOST_STARTUP_MS),
        databaseDescriptor: result.data.DURABLE_ACTORS_DATABASE_FD
    }
}

function parseStartupTimeout(value: string | undefined): number {
    if (value === undefined) return DEFAULT_ACTOR_STARTUP_TIMEOUT_MS
    const parsed = Number(value)
    if (!Number.isInteger(parsed) || parsed <= 0)
        throw new ActorConfigurationError("DURABLE_ACTORS_HOST_STARTUP_MS must be a positive integer")
    return parsed
}

const DEFAULT_ACTOR_STARTUP_TIMEOUT_MS = 10_000

const actorSessionSettingsSchema = z.object({
    DURABLE_ACTORS_EXECUTOR_SOCKET: z.string().trim().min(1),
    DURABLE_ACTORS_ENTRYPOINT: z.string().trim().min(1).optional(),
    DURABLE_ACTORS_DATABASE_FD: z.coerce.number().int().nonnegative().optional()
})

const DEFAULT_ACTOR_ENTRYPOINT = "dist/actors.mjs"

export { ActorSession, connectSocket, parseHostSettings, resolveActorEntrypoint, runActorHost }
