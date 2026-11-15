import { Actor, registerActorClass } from "../actor/actor.js"
import type { ActorClass } from "../actor/actor.js"
import { ACTOR_ARTIFACT_VERSION } from "../actor/schema.js"
import type { ActorSchema } from "../actor/schema.js"
import { ActorConfigurationError, ActorDefinitionError } from "../errors.js"

/** Runs the customer's module and registers the actors it exports. Resolves with their names. */
async function loadActorEntrypoint(moduleUrl: string): Promise<string[]> {
    const artifact = await import(moduleUrl)
    if (artifact.version !== ACTOR_ARTIFACT_VERSION || !Array.isArray(artifact.schemas) || !artifact.actors)
        throw new ActorConfigurationError("invalid actor artifact; redeploy using matching SDK and runtime versions")
    return registerActors(artifact.actors, artifact.schemas)
}

function registerActors(actorModule: Record<string, unknown>, schemas: readonly ActorSchema[]): string[] {
    const actorNames: string[] = []
    for (const [exportName, value] of Object.entries(actorModule)) {
        if (!isActorClass(value)) continue
        if (exportName === "default") {
            throw new ActorDefinitionError("actor entrypoint must use named exports, not a default export")
        }
        if (Object.getPrototypeOf(value.prototype) !== Actor.prototype) {
            throw new ActorDefinitionError(
                `actor entrypoint export ${exportName} must be a class that directly extends Actor`
            )
        }
        if (value.name !== exportName) {
            throw new ActorDefinitionError(`actor entrypoint export ${exportName} must have the same class name`)
        }
        const schema = schemas.find(schema => schema.actorName === exportName)
        if (schema === undefined)
            throw new ActorDefinitionError(`actor ${exportName} has no validated schema; restart the actor host`)
        actorNames.push(registerActorClass(value, schema).actorName)
    }
    if (actorNames.length === 0) throw new ActorDefinitionError("actor entrypoint has no named actor exports")
    if (actorNames.length !== schemas.length)
        throw new ActorDefinitionError("actor exports do not match validated schemas; restart the actor host")
    actorNames.sort()
    return actorNames
}

function isActorClass(value: unknown): value is ActorClass {
    return typeof value === "function" && value.prototype instanceof Actor
}

export { loadActorEntrypoint }
