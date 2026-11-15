import assert from "node:assert/strict"
import { mkdir, mkdtemp, realpath, rm, writeFile } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { test } from "node:test"
import { fileURLToPath, pathToFileURL } from "node:url"

import { ACTOR_ARTIFACT_VERSION } from "../../src/actor/schema.js"
import { ActorConfigurationError, ActorDefinitionError } from "../../src/errors.js"
import { loadActorEntrypoint } from "../../src/host/actor-entrypoint.js"
import { resolveActorEntrypoint } from "../../src/host/actor-host.js"

test("resolves the conventional built actor entrypoint", async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), "durable-actors-entrypoint-"))
    const previousDirectory = process.cwd()
    try {
        await mkdir(path.join(root, "dist"))
        const entrypoint = path.join(root, "dist/actors.mjs")
        await writeFile(entrypoint, "export {}\n")
        process.chdir(root)
        assert.equal(await realpath(fileURLToPath(await resolveActorEntrypoint(undefined))), await realpath(entrypoint))
    } finally {
        process.chdir(previousDirectory)
        await rm(root, { recursive: true, force: true })
    }
})

test("rejects a configured actor entrypoint that does not exist", async () => {
    await assert.rejects(resolveActorEntrypoint("./missing-actors.mjs"), ActorConfigurationError)
})

test("rejects an incompatible built actor artifact", async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), "durable-actors-artifact-"))
    try {
        const entrypoint = path.join(root, "actors.mjs")
        await writeFile(entrypoint, "export const version = 999; export const actors = {}; export const schemas = []")
        await assert.rejects(loadActorEntrypoint(pathToFileURL(entrypoint).href), /invalid actor artifact/)
    } finally {
        await rm(root, { recursive: true, force: true })
    }
})

test("loads only actors from an entrypoint with mixed exports", async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), "durable-actors-mixed-entrypoint-"))
    try {
        const entrypoint = path.join(root, "mixed.mjs")
        const sdk = new URL("../../src/index.js", import.meta.url).href
        await writeFile(
            entrypoint,
            `import { Actor } from ${JSON.stringify(sdk)}
            const limit = 10
            class Utility { value = 1 }
            class MixedRoom extends Actor {}
            class MixedCounter extends Actor {}
            export const actors = { Actor, limit, empty: null, callback: () => 1, helper: () => limit, Utility, default: { limit }, MixedRoom, MixedCounter }
            export const version = ${ACTOR_ARTIFACT_VERSION}
            export const schemas = ${JSON.stringify(["MixedCounter", "MixedRoom"].map(actorName => ({ actorName, fields: [] })))}`
        )
        assert.deepEqual(await loadActorEntrypoint(pathToFileURL(entrypoint).href), ["MixedCounter", "MixedRoom"])
    } finally {
        await rm(root, { recursive: true, force: true })
    }
})

test("rejects invalid actor exports while ignoring unrelated exports", async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), "durable-actors-invalid-entrypoint-"))
    try {
        const sdk = new URL("../../src/index.js", import.meta.url).href
        for (const [name, declaration, actors, message] of [
            ["default", "class Counter extends Actor {}", "{ default: Counter }", /named exports/],
            ["alias", "class Counter extends Actor {}", "{ Renamed: Counter }", /same class name/],
            [
                "indirect",
                "class Base extends Actor {}; class Counter extends Base {}",
                "{ Counter }",
                /directly extends Actor/
            ],
            ["non-actor", "class Utility {}", "{ Utility }", /named actor exports/]
        ] as const) {
            const entrypoint = path.join(root, `${name}.mjs`)
            await writeFile(
                entrypoint,
                `import { Actor } from ${JSON.stringify(sdk)}
                ${declaration}
                export const version = ${ACTOR_ARTIFACT_VERSION}
                export const schemas = []
                export const actors = { helper: 1, ...${actors} }`
            )
            await assert.rejects(loadActorEntrypoint(pathToFileURL(entrypoint).href), error => {
                assert.ok(error instanceof ActorDefinitionError)
                assert.match(error.message, message)
                return true
            })
        }
    } finally {
        await rm(root, { recursive: true, force: true })
    }
})
