import assert from "node:assert/strict"
import { spawn } from "node:child_process"
import { once } from "node:events"
import { mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises"
import { createServer } from "node:net"
import path from "node:path"
import { createInterface } from "node:readline"
import { pathToFileURL } from "node:url"

const sdk = path.resolve(process.argv[2] ?? path.dirname(process.env.DURABLE_ACTORS_SDK_HOST))
const directory = await mkdtemp("/tmp/actor-image-")
const server = createServer()
let child
let socket
const timeout = setTimeout(() => {
    child?.kill()
    console.error("executor image smoke test timed out")
    process.exit(1)
}, 20_000)
try {
    await mkdir(path.join(directory, "node_modules"))
    await symlink(sdk, path.join(directory, "node_modules/durable-actors"))
    const entrypoint = path.join(directory, "actors.mjs")
    await writeFile(
        entrypoint,
        `
        import { Actor } from "durable-actors";
        class Counter extends Actor { count = 0; async add() { return ++this.count } }
        export const actors = { Counter };
        export const schemas = [{ actorName: "Counter", fields: [{ name: "count", persistence: "persisted" }] }];
        export const version = 1;
    `
    )
    const socketPath = path.join(directory, "executor.sock")
    server.listen(socketPath)
    await once(server, "listening")
    const connected = once(server, "connection")
    child = spawn(process.execPath, ["--eval", `await (await import(${JSON.stringify(pathToFileURL(path.join(sdk, "host.js")).href)})).runGenericHost()`], {
        env: { ...process.env, DURABLE_ACTORS_EXECUTOR_SOCKET: socketPath },
        stdio: ["ignore", "inherit", "inherit"]
    })
    const exited = once(child, "exit")
    ;[socket] = await Promise.race([
        connected,
        exited.then(() => {
            throw new Error("executor exited before connecting")
        })
    ])
    const lines = createInterface({ input: socket })[Symbol.asyncIterator]()
    const send = message => socket.write(JSON.stringify(message) + "\n")
    let committed = {}
    let pending = {}
    const call = request => {
        switch (request.op) {
            case "fields":
                return { type: "fields", fields: pending }
            case "initialize_fields":
                pending = { ...request.fields, ...pending }
                break
            case "persist_field":
                pending = { ...pending, [request.name]: request.value }
                if (request.immediate) committed = pending
                break
            case "commit":
                committed = pending
                break
            case "rollback":
                pending = committed
                break
            default:
                throw new Error(`unexpected database call ${request.op}`)
        }
        return { type: "done" }
    }
    const receive = async () => {
        for (;;) {
            const { value, done } = await lines.next()
            assert.equal(done, false, "executor closed its connection")
            const message = JSON.parse(value)
            if (message.type === "database_call") send({ type: "database_result", call_id: message.call_id, result: call(message.call) })
            else if (message.type !== "residency") return message
        }
    }
    assert.deepEqual(await receive(), { type: "warm", protocol: 32 })
    send({ type: "load", entrypoint, environment: {} })
    assert.deepEqual(await receive(), { type: "attach", protocol: 32, actor_names: ["Counter"] })
    send({ type: "attached", protocol: 32, actor: { project_id: "default", actor_name: "Counter", actor_id: "one" } })
    for (let expected = 1; expected <= 2; expected++) {
        send({
            type: "command",
            message_id: expected,
            command: {
                type: "invoke",
                request_id: `request-${expected}`,
                method: "add",
                args: []
            }
        })
        const result = await receive()
        assert.equal(result.reply.type, "invoked", JSON.stringify(result))
        assert.equal(result.reply.result, expected)
        send({ type: "command", message_id: 10 + expected, command: { type: "evict", graceful: false } })
        assert.equal((await receive()).reply.type, "evicted")
    }
    child.kill()
    await exited
    console.log("TypeScript image: generic warmup, actor invocation, and database recovery passed")
} finally {
    clearTimeout(timeout)
    child?.kill()
    socket?.destroy()
    server.close()
    await rm(directory, { recursive: true, force: true })
}
