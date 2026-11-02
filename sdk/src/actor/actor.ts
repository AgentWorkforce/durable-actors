import { actorClient } from "../client/client.js"
import { ActorDefinitionError } from "../errors.js"
import type { JsonObject, JsonValue } from "../json.js"

import type { ActorAlarm } from "./alarm.js"
import type { ActorContext } from "./context.js"
import { RuntimeActorContext } from "./context.js"
import type { ActorDatabase } from "./database.js"
import { validateActorComponent } from "./identity.js"
import type { ActorSchema } from "./schema.js"
import type { ActorBroadcastOptions, ActorConnection, ActorSocket, ActorSocketMessage } from "./socket.js"
import { actorConnectionCount, actorConnections, broadcastActor, setActorAutoResponse } from "./socket.js"
import type { ActorSchemas } from "./socketValidation.js"
import { outgoingMessage, socketMetadata } from "./socketValidation.js"

declare const actorTypes: unique symbol

const actorDefinitions = new Map<string, ActorDefinition>()
const asyncFunction = Object.getPrototypeOf(async () => {}).constructor
const referenceClassKey = Symbol("actor reference class")
let actorContext: (value: object) => RuntimeActorContext | undefined

interface Actor<Metadata = JsonValue, Incoming = JsonValue, Outgoing = Incoming, Tag extends string = string> {
    /** Accepts a joining connection on success. Call `socket.reject(4003, reason)` to deny it. */
    onConnect?(socket: ActorSocket<Metadata, Outgoing, Tag>): Promise<void>
    /** Handles a parsed JSON message. */
    onMessage?(socket: ActorSocket<Metadata, Outgoing, Tag>, message: Incoming): Promise<void>
    /** Runs after disconnection; may be skipped on server failure. */
    onDisconnect?(
        socket: ActorSocket<Metadata, Outgoing, Tag>,
        code: number,
        reason: string,
        wasClean: boolean
    ): Promise<void>
    /** Runs when the actor's scheduled alarm becomes due. The alarm is cleared afterward unless this callback sets a new one. */
    onAlarm?(): Promise<void>
}

/**
 * Base class for durable actors. Extend directly; explicit constructors must pass their `ActorContext` to `super`.
 * Application methods must be async; instance fields require `@Persisted` or `@Ephemeral`.
 * Initialized private `ActorResource` fields are implicitly ephemeral.
 * @typeParam Metadata - Connection metadata supplied by the backend.
 * @typeParam Incoming - Application messages received by the actor.
 * @typeParam Outgoing - Application messages sent by the actor.
 * @typeParam Tag - Allowed connection tags.
 */
abstract class Actor<Metadata = JsonValue, Incoming = JsonValue, Outgoing = Incoming, Tag extends string = string> {
    /** @internal */
    declare readonly [actorTypes]: { metadata: Metadata; incoming: Incoming; outgoing: Outgoing; tag: Tag }
    readonly #context: RuntimeActorContext

    static {
        actorContext = value => (#context in value ? value.#context : undefined)
    }

    protected constructor(context: ActorContext) {
        this.#context = RuntimeActorContext.bind(context, this)
    }

    /** Actor-local SQLite. Statements autocommit unless grouped with `db.transaction`. */
    get db(): ActorDatabase {
        return this.#context.database
    }

    /** The actor's single durable alarm slot. */
    get alarm(): ActorAlarm {
        return this.#context.alarm
    }

    /**
     * Returns a backend reference. The first call starts the actor if needed.
     * @param actorId - 1–128 ASCII letters, digits, dots, underscores, or hyphens.
     */
    static get<TActorClass extends ActorClass>(
        this: ValidActorClass<TActorClass>,
        actorId: string
    ): ActorReference<TActorClass["prototype"]> {
        return getActorReference(this, validateActorComponent("actor ID", actorId))
    }

    /** The bound actor ID, available from field initializers. */
    protected get id(): string {
        return this.#context.actorId
    }

    /**
     * Lists connections during an actor call. Includes a joining socket; excludes a disconnected one.
     */
    protected getConnections(tag?: Tag): Promise<readonly ActorSocket<Metadata, Outgoing, Tag>[]> {
        return actorConnections<Metadata, Outgoing, Tag>(this, tag)
    }

    protected getConnectionCount(): Promise<number> {
        return actorConnectionCount(this)
    }

    /** Matches raw text frames and responds without waking the actor. Omit to clear. */
    protected setWebSocketAutoResponse(pair?: { readonly request: string; readonly response: string }): void {
        setActorAutoResponse(this, pair)
    }

    /** Sends JSON to matching connections, including the sender by default. Messages are not saved. */
    protected broadcast(message: Outgoing, options?: ActorBroadcastOptions<Tag>): void {
        broadcastActor(this, message, options)
    }
}

function registerActorClass<Instance extends AnyActor>(
    actorClass: ActorClass<Instance>,
    state: ActorSchema
): ActorDefinition {
    const actorName = actorClassName(actorClass)
    const reserved = state.fields.find(field => field.name === "db" || field.name === "alarm")
    if (reserved !== undefined)
        throw new ActorDefinitionError(`actor field ${reserved.name} is reserved for the actor runtime`)
    const existing = actorDefinitions.get(actorName)
    if (existing !== undefined) {
        if (existing.actorClass !== actorClass) throw new ActorDefinitionError(`duplicate actor name ${actorName}`)
        existing.state = state
        return existing
    }

    const definition = { ...describeActorClass(actorClass), state }
    actorDefinitions.set(actorName, definition)
    return definition
}

function findActorDefinition(actorName: string): ActorDefinition | undefined {
    return actorDefinitions.get(actorName)
}

function getActorReference<TActorClass extends ActorClass>(
    actorClass: TActorClass,
    actorId: string
): ActorReference<TActorClass["prototype"]> {
    const Reference = referenceClass(actorClass)
    return new Reference(actorId) as unknown as ActorReference<TActorClass["prototype"]>
}

function referenceClass(actorClass: ActorClass): ActorReferenceClass {
    if (Object.hasOwn(actorClass, referenceClassKey)) return Reflect.get(actorClass, referenceClassKey)
    const definition = describeActorClass(actorClass)

    class ActorReference extends Actor {
        constructor(actorId: string) {
            super(RuntimeActorContext.reference(actorId))
        }
    }

    Object.defineProperty(ActorReference.prototype, "connect", {
        configurable: false,
        enumerable: false,
        writable: false,
        value: function connectActor(this: Actor, metadata: unknown): Promise<ActorConnection> {
            const actorId = actorIdOf(this)
            const attachment = socketMetadata(metadata, definition.schemas)
            return actorClient().then(client =>
                client.connect(definition.actorName, actorId, attachment, definition.schemas)
            )
        }
    })

    Object.defineProperty(ActorReference.prototype, "broadcast", {
        configurable: false,
        enumerable: false,
        writable: false,
        value: function broadcastActorMessage(this: Actor, message: ActorSocketMessage): Promise<void> {
            const actorId = actorIdOf(this)
            const outgoing = outgoingMessage(message, definition.schemas)
            return actorClient().then(client => client.broadcast(definition.actorName, actorId, outgoing))
        }
    })

    definition.methods.forEach(method => {
        Object.defineProperty(ActorReference.prototype, method, {
            configurable: false,
            enumerable: false,
            writable: false,
            value: function forwardActorMethod(this: Actor, ...args: unknown[]): Promise<unknown> {
                const actorId = actorIdOf(this)
                return actorClient().then(client => client.invoke(definition.actorName, actorId, method, args))
            }
        })
    })
    Object.defineProperty(actorClass, referenceClassKey, { value: ActorReference })
    return ActorReference
}

function actorIdOf(instance: object): string {
    const context = actorContext(instance)
    if (context === undefined)
        throw new ActorDefinitionError("actor identity is unavailable outside an actor invocation")
    return context.actorId
}

function describeActorClass(actorClass: ActorClass): ActorClassDescription {
    const actorName = actorClassName(actorClass)
    validateActorClass(actorClass, actorName)
    return {
        actorName: validateActorComponent("actor name", actorName),
        actorClass,
        schemas: actorClass.schemas ?? {},
        methods: new Set(discoverMethods(actorClass, actorName))
    }
}

function discoverMethods(actorClass: ActorClass, actorName: string): string[] {
    if (Object.getOwnPropertySymbols(actorClass.prototype).length > 0)
        throw new ActorDefinitionError(`actor class ${actorName} cannot define symbol methods`)

    return Object.entries(Object.getOwnPropertyDescriptors(actorClass.prototype)).flatMap(([name, descriptor]) => {
        if (name === "constructor") return []
        if (descriptor.get !== undefined || descriptor.set !== undefined)
            throw new ActorDefinitionError(`actor class ${actorName} cannot define accessor ${name}`)
        if (typeof descriptor.value !== "function") return []
        validateActorComponent("actor method", name)
        if (name === "then") throw new ActorDefinitionError(`actor class ${actorName} cannot define method then`)
        if (name === "connect" || name === "broadcast" || name === "db" || name === "alarm")
            throw new ActorDefinitionError(`actor class ${actorName} cannot define reserved method ${name}`)
        if (!(descriptor.value instanceof asyncFunction))
            throw new ActorDefinitionError(
                `actor method ${actorName}.${name} must be async. For a sync helper, use a \`#private\` method or a module-level function.`
            )
        if (name === "onAlarm" && descriptor.value.length !== 0)
            throw new ActorDefinitionError(`actor lifecycle hook ${actorName}.onAlarm must take no arguments`)
        if (lifecycleMethods.has(name)) return []
        return [name]
    })
}

const lifecycleMethods = new Set(["onConnect", "onMessage", "onDisconnect", "onAlarm"])

function validateActorClass(actorClass: ActorClass, actorName: string): void {
    if (Object.getPrototypeOf(actorClass.prototype) !== Actor.prototype)
        throw new ActorDefinitionError(`actor class ${actorName} must extend Actor directly`)
}

function actorClassName(actorClass: ActorClass): string {
    if (actorClass.name.length === 0) throw new ActorDefinitionError("actor classes must be named")
    return actorClass.name
}

interface ActorDefinition extends ActorClassDescription {
    state: ActorSchema
}

interface ActorClassDescription {
    readonly actorName: string
    readonly actorClass: ActorClass
    readonly schemas: ActorSchemas
    readonly methods: ReadonlySet<string>
}

type AnyActor = Actor<unknown, unknown, unknown>
type ActorClass<Instance extends AnyActor = AnyActor> = Function & {
    readonly prototype: Instance
    readonly schemas?: ActorSchemas
}
type ActorReferenceClass = new (actorId: string) => Actor
type AsyncMethod = (...args: never[]) => Promise<unknown>
type InvalidActorMethod<Instance extends AnyActor> = {
    [Key in keyof Instance]-?: NonNullable<Instance[Key]> extends (...args: never[]) => unknown
        ? NonNullable<Instance[Key]> extends AsyncMethod
            ? never
            : Key
        : never
}[keyof Instance]
type ValidActorClass<TActorClass extends ActorClass> =
    InvalidActorMethod<TActorClass["prototype"]> extends never
        ? TActorClass extends {
              readonly schemas: ActorSchemas<
                  SocketMetadata<TActorClass["prototype"]>,
                  SocketIncoming<TActorClass["prototype"]>,
                  SocketOutgoing<TActorClass["prototype"]>,
                  SocketTag<TActorClass["prototype"]>
              >
          }
            ? TActorClass
            : TActorClass extends { readonly schemas: unknown }
              ? never
              : TActorClass
        : never
type ActorReference<Instance extends AnyActor> = {
    [
        Key in keyof Instance as Instance[Key] extends AsyncMethod
            ? Key extends ActorLifecycleMethod
                ? never
                : Key
            : never
    ]: Instance[Key]
} & {
    /** Opens a backend socket. Attach listeners immediately; acceptance may still be pending. */
    connect(
        metadata: SocketMetadata<Instance>
    ): Promise<ActorConnection<SocketIncoming<Instance>, SocketOutgoing<Instance>, JsonObject>>
    /** Sends to all connections without running actor code. Messages are not saved. */
    broadcast(message: SocketOutgoing<Instance>): Promise<void>
}
type ActorLifecycleMethod = "onConnect" | "onMessage" | "onDisconnect" | "onAlarm"
type SocketMetadata<Instance extends AnyActor> = Instance[typeof actorTypes]["metadata"]
type SocketIncoming<Instance extends AnyActor> = Instance[typeof actorTypes]["incoming"]
type SocketOutgoing<Instance extends AnyActor> = Instance[typeof actorTypes]["outgoing"]
type SocketTag<Instance extends AnyActor> = Instance[typeof actorTypes]["tag"]
/** Socket type for an actor instance type, for example `ActorSocketOf<ChatRoom>`. */
type ActorSocketOf<Instance extends AnyActor> = ActorSocket<
    SocketMetadata<Instance>,
    SocketOutgoing<Instance>,
    SocketTag<Instance>
>
/** Incoming message type for an actor instance type. */
type ActorMessageOf<Instance extends AnyActor> = SocketIncoming<Instance>

export { Actor, actorContext, findActorDefinition, registerActorClass }
export type { ActorClass, ActorContext, ActorDefinition, ActorMessageOf, ActorReference, ActorSocketOf, AnyActor }
