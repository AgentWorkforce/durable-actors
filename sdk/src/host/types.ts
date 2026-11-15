import type { HostSocketEffect, SocketSource } from "../actor/socketProtocol.js"
import type { JsonValue } from "../json.js"

import type { DatabaseCall, DatabaseResult } from "./database.js"
import type { ActorCallRequest, ActorLog } from "./protocol.js"
import type { ResidentActor } from "./resident-actor.js"

interface ActorHostSettings {
    readonly socketPath: string
    readonly actorEntrypoint: string | undefined
    readonly startupTimeoutMs: number
    /** The inherited connection that database calls block on. Absent outside the Rust host. */
    readonly databaseDescriptor: number | undefined
}

type SocketPublisher = (effects: readonly HostSocketEffect[]) => void

/** Holds the thread until the Rust host has answered. `invocation` names the command whose database is called. */
type InvocationDatabase = (call: DatabaseCall, invocation?: number) => DatabaseResult

type ActorCallRequester = (call: ActorCallRequest) => Promise<JsonValue>

type LogWriter = (log: ActorLog) => void

interface ResidentActorOptions {
    readonly actorEntrypointUrl: string
    readonly database: InvocationDatabase
}

type SessionActor = Pick<ResidentActor, "ready" | "assign" | "handle" | "isResident" | "onResidencyChange" | "close">

type SessionActorFactory = (options: ResidentActorOptions) => SessionActor

export type {
    ActorCallRequester,
    ActorHostSettings,
    InvocationDatabase,
    LogWriter,
    ResidentActorOptions,
    SessionActor,
    SessionActorFactory,
    SocketPublisher,
    SocketSource
}
