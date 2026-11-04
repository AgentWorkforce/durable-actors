import assert from "node:assert/strict"
import path from "node:path"
import { pathToFileURL } from "node:url"

import type { SocketEffect } from "../../src/actor/socketProtocol.js"
import type { ActorWorkerSupervisor as Supervisor } from "../../src/host/worker-supervisor.js"

import { HostDatabase } from "./host-database.js"

const [sdk, artifact] = process.argv.slice(2)
const { ActorWorkerSupervisor } = await import(pathToFileURL(path.join(sdk!, "dist/host/worker-supervisor.js")).href)
const supervisor: Supervisor = new ActorWorkerSupervisor({ actorEntrypointUrl: pathToFileURL(artifact!).href })
try {
    assert.deepEqual(await supervisor.ready(), ["Counter"])
    supervisor.assign(
        { project_id: "local", actor_name: "Counter", actor_id: "counter-1" },
        async () => {
            throw new Error("unexpected activation database call")
        },
        async () => {
            throw new Error("unexpected actor call")
        }
    )
    const effects: SocketEffect[] = []
    const database = new HostDatabase()
    const reply = await supervisor.handle(
        {
            type: "invoke",
            request_id: "request",
            method: "read",
            args: []
        },
        () => {},
        async call => database.call(call),
        async published => {
            effects.push(...published)
        },
        async () => []
    )
    assert.equal(reply.type, "invoked")
    if (reply.type === "invoked") assert.equal(reply.result, "counter-1:0")
    assert.equal(effects[0]?.type, "broadcast")
    console.log("invocation passed")
} finally {
    supervisor.close()
}
