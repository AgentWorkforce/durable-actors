import assert from "node:assert/strict"
import path from "node:path"
import { pathToFileURL } from "node:url"

import type { HostSocketEffect } from "../../src/actor/socketProtocol.js"
import type { ResidentActor as Resident } from "../../src/host/resident-actor.js"

import { HostDatabase } from "./host-database.js"
import { resultOf } from "./reply.js"
import { command } from "./resident-actor.js"

const [sdk, artifact] = process.argv.slice(2)
const { ResidentActor } = await import(pathToFileURL(path.join(sdk!, "dist/host/resident-actor.js")).href)
const database = new HostDatabase()
const actor: Resident = new ResidentActor({
    actorEntrypointUrl: pathToFileURL(artifact!).href,
    database: database.call
})
try {
    assert.deepEqual(await actor.ready(), ["Counter"])
    actor.assign({ project_id: "local", actor_name: "Counter", actor_id: "counter-1" }, async () => {
        throw new Error("unexpected actor call")
    })
    const effects: HostSocketEffect[] = []
    const reply = await actor.handle(
        { type: "invoke", request_id: "request", method: "read", args: [] },
        command(1, { publish: published => effects.push(...published) })
    )
    assert.equal(resultOf(reply), "counter-1:0")
    assert.equal(effects[0]?.type, "broadcast")
    console.log("invocation passed")
} finally {
    actor.close()
    database.close()
}
