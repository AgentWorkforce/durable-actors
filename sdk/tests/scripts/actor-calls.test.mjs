import assert from "node:assert/strict"
import { copyFile, mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { test } from "node:test"
import { fileURLToPath } from "node:url"

import { createActorTransport } from "../../dist/backend.js"
import { startLocalActors } from "../../dist/localRuntime.js"

const sdk = fileURLToPath(new URL("../..", import.meta.url))

test("onAlarm calls a cold actor in its own project without client settings", { timeout: 120_000 }, async t => {
    for (const name of ["DURABLE_ACTORS_PROJECT_ID", "DURABLE_ACTORS_SECRET", "DURABLE_ACTORS_API_KEY"]) delete process.env[name]
    const root = await mkdtemp(path.join(os.tmpdir(), "actor-calls-e2e-"))
    let runtime
    t.after(async () => {
        await runtime?.stop()
        await rm(root, { recursive: true, force: true })
    })
    await mkdir(path.join(root, "node_modules"))
    await symlink(sdk, path.join(root, "node_modules/durable-actors"))
    await copyFile(new URL("../fixtures/actor-calls-actor.ts", import.meta.url), path.join(root, "actors.ts"))
    await writeFile(path.join(root, "package.json"), JSON.stringify({ type: "module" }))
    await writeFile(
        path.join(root, "tsconfig.json"),
        JSON.stringify({
            compilerOptions: {
                target: "ES2022",
                module: "NodeNext",
                strict: true,
                skipLibCheck: true,
                typeRoots: [path.join(sdk, "node_modules/@types")]
            }
        })
    )
    // A project other than "local" keeps the environment-configured client from reaching it.
    runtime = await startLocalActors({
        projectId: "actor-calls",
        project: root,
        entrypoint: "actors.ts",
        quiet: process.env.ACTOR_CALLS_TEST_LOGS !== "1"
    })
    const { projectId, controlPlaneUrl, apiKey } = runtime.connection
    const transport = createActorTransport({ projectId, controlPlaneUrl, apiKey })

    await transport.invoke("Document", "doc-1", "publish", [])
    assert.deepEqual(await eventually(() => transport.invoke("Document", "doc-1", "read", [])), ["doc-1"])
    assert.deepEqual(await transport.invoke("DocIndex", "main", "list", []), ["doc-1"])
})

async function eventually(read) {
    const deadline = Date.now() + 30_000
    while (Date.now() < deadline) {
        const value = await read()
        if (value !== null) return value
        await new Promise(resolve => setTimeout(resolve, 100))
    }
    throw new Error("onAlarm did not index the document")
}
