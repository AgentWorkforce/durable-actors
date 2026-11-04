import type { SocketEffect, SocketSource } from "../actor/socketProtocol.js"
import type { JsonValue } from "../json.js"

import type { DatabaseCall, DatabaseResult } from "./database.js"
import type { ActorCallRequest, ActorExecutorReply, ActorWorkerCommand, ActorWorkerData } from "./protocol.js"
import type { ActorWorkerSupervisor } from "./worker-supervisor.js"

interface ActorHostSettings {
    readonly socketPath: string
    readonly actorEntrypoint: string | undefined
    readonly startupTimeoutMs: number
}

type SocketPublisher = (effects: readonly SocketEffect[]) => void

type DatabaseRequester = (call: DatabaseCall) => Promise<DatabaseResult>

type ActorCallRequester = (call: ActorCallRequest) => Promise<JsonValue>

type ActorSessionSupervisor = Pick<ActorWorkerSupervisor, "assign" | "handle" | "isResident" | "onResidencyChange">

type ActorWorkerSupervisorFactory = (
    options: ActorWorkerSupervisorOptions
) => ActorSessionSupervisor & Pick<ActorWorkerSupervisor, "ready" | "close">

interface ActorWorkerSupervisorOptions {
    readonly actorEntrypointUrl: string
    readonly createWorker?: ActorWorkerFactory
}

interface ResidentActorWorkerOptions {
    readonly sequenceBase: number
    readonly onResidencyChange: () => void
    readonly database: DatabaseRequester
    readonly calls: ActorCallRequester
    readonly moduleUrl: string
    readonly worker?: ActorWorkerHandle
    readonly createWorker: ActorWorkerFactory
}

type ActorWorkerState = "starting" | "ready" | "stopping" | "stopped"

interface ActorWorkerHandle {
    readonly state: ActorWorkerState
    ready(): Promise<readonly string[]>
    execute(
        command: ActorWorkerCommand,
        allowNextInvocation: () => void,
        database: DatabaseRequester,
        publish?: SocketPublisher,
        connections?: SocketSource
    ): Promise<ActorExecutorReply>
    terminate(reason: string): void
}

type ActorWorkerFactory = (
    data: ActorWorkerData,
    onStateChange: () => void,
    database: DatabaseRequester,
    calls: ActorCallRequester
) => ActorWorkerHandle

export type {
    ActorCallRequester,
    ActorHostSettings,
    ActorSessionSupervisor,
    ActorWorkerFactory,
    ActorWorkerHandle,
    ActorWorkerState,
    ActorWorkerSupervisorFactory,
    ActorWorkerSupervisorOptions,
    DatabaseRequester,
    ResidentActorWorkerOptions,
    SocketPublisher,
    SocketSource
}
