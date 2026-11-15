import { actorContext } from "../actor/actor.js"
import type { RuntimeActorContext } from "../actor/context.js"
import { Persistence } from "../actor/schema.js"
import type { ActorSchema } from "../actor/schema.js"

import type { DatabaseProxy } from "./database.js"

function bindPersistedFields(
    instance: object,
    context: RuntimeActorContext,
    schema: ActorSchema,
    database: DatabaseProxy
): void {
    for (const field of schema.fields) {
        if (field.persistence !== Persistence.Persisted || !Object.hasOwn(instance, field.name)) continue
        let value: unknown = Reflect.get(instance, field.name)
        const descriptor: PropertyDescriptor = {
            configurable: true,
            enumerable: true,
            get: () => value,
            set: next => {
                context.assertAccess()
                database.persistField(field.name, next)
                value = next
            }
        }
        database.onFieldRestore(field.name, restored => (value = restored))
        Object.defineProperty(instance, field.name, descriptor)
        context.persistedFields.set(field.name, descriptor)
    }
}

function isPersistedField(instance: object, name: string, descriptor: PropertyDescriptor): boolean {
    const bound = actorContext(instance)?.persistedFields.get(name)
    return bound !== undefined && bound.get === descriptor.get && bound.set === descriptor.set
}

export { bindPersistedFields, isPersistedField }
