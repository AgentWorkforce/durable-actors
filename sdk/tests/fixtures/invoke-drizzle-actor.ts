import assert from "node:assert/strict"
import path from "node:path"
import { pathToFileURL } from "node:url"

import type { ResidentActor as Resident } from "../../src/host/resident-actor.js"
import type { JsonValue } from "../../src/json.js"

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
    assert.deepEqual(await actor.ready(), ["Library"])
    actor.assign({ project_id: "local", actor_name: "Library", actor_id: "library-1" }, async () => {
        throw new Error("unexpected actor call")
    })
    let commands = 0
    const invoke = async (method: string, ...args: JsonValue[]) =>
        resultOf(await actor.handle({ type: "invoke", request_id: method, method, args }, command(++commands)))
    assert.equal(await invoke("add", "Ada", "Notes"), 1)
    assert.equal(await invoke("add", "Ada", "Notes"), null)
    assert.deepEqual(await invoke("list"), [{ authorId: 1, bookId: 1, title: "Notes" }])
    assert.deepEqual(
        database.query("SELECT created_at FROM __drizzle_migrations").map(row => Number(row.created_at)),
        [1700000000000]
    )
    console.log("drizzle invocation passed")
} finally {
    actor.close()
    database.close()
}
