import assert from "node:assert/strict"
import path from "node:path"
import { pathToFileURL } from "node:url"

import type { ActorWorkerSupervisor as Supervisor } from "../../src/host/worker-supervisor.js"
import type { JsonValue } from "../../src/json.js"

import { HostDatabase } from "./host-database.js"

const [sdk, artifact] = process.argv.slice(2)
const { ActorWorkerSupervisor } = await import(pathToFileURL(path.join(sdk!, "dist/host/worker-supervisor.js")).href)
const supervisor: Supervisor = new ActorWorkerSupervisor({ actorEntrypointUrl: pathToFileURL(artifact!).href })
const database = new HostDatabase()
try {
    assert.deepEqual(await supervisor.ready(), ["Library"])
    supervisor.assign(
        { project_id: "local", actor_name: "Library", actor_id: "library-1" },
        async call => database.call(call),
        async () => {
            throw new Error("unexpected actor call")
        }
    )
    const invoke = async (method: string, ...args: JsonValue[]) => {
        const reply = await supervisor.handle(
            { type: "invoke", request_id: method, method, args },
            () => {},
            async call => database.call(call),
            async () => {},
            async () => []
        )
        assert.equal(reply.type, "invoked", JSON.stringify(reply))
        return reply.type === "invoked" ? reply.result : undefined
    }
    assert.equal(await invoke("add", "Ada", "Notes"), 1)
    assert.equal(await invoke("add", "Ada", "Notes"), null)
    assert.deepEqual(await invoke("list"), [{ authorId: 1, bookId: 1, title: "Notes" }])
    assert.deepEqual(
        database.query("SELECT created_at FROM __drizzle_migrations").map(row => Number(row.created_at)),
        [1700000000000]
    )
    console.log("drizzle invocation passed")
} finally {
    supervisor.close()
    database.close()
}
