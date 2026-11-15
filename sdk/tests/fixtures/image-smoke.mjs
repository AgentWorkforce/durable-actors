import assert from "node:assert/strict"
import { spawn } from "node:child_process"
import { once } from "node:events"
import { mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises"
import { createServer } from "node:net"
import path from "node:path"
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
        env: { ...process.env, DURABLE_ACTORS_EXECUTOR_SOCKET: socketPath, DURABLE_ACTORS_DATABASE_FD: "3" },
        stdio: ["ignore", "inherit", "inherit", "pipe"]
    })
    const exited = once(child, "exit")
    ;[socket] = await Promise.race([
        connected,
        exited.then(() => {
            throw new Error("executor exited before connecting")
        })
    ])
    const frames = readFrames(socket)
    const send = message => socket.write(JSON.stringify(message) + "\n")
    let committed = {}
    let pending = {}
    const call = (request, values) => {
        switch (request.op) {
            case "fields":
                return { type: "fields", fields: pending }
            case "initialize_fields":
                pending = { ...Object.fromEntries(request.names.map((name, index) => [name, values[index]])), ...pending }
                break
            case "persist_field":
                pending = { ...pending, [request.name]: values[0] }
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
    serveDatabase(child.stdio[3], call)
    const receive = async () => {
        for (;;) {
            const { value, done } = await frames.next()
            assert.equal(done, false, "executor closed its connection")
            const { head: message, parts } = value
            if (message.type !== "residency") return attach(message, parts)
        }
    }
    assert.deepEqual(await receive(), { type: "warm", protocol: 33 })
    send({ type: "load", entrypoint, environment: {} })
    assert.deepEqual(await receive(), { type: "attach", protocol: 33, actor_names: ["Counter"] })
    send({ type: "attached", protocol: 33, actor: { project_id: "default", actor_name: "Counter", actor_id: "one" } })
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

// The executor's thread blocks on its fourth descriptor for each database call. A call is a size and that much
// JSON, then the JSON of each field it writes. Its answer is a row per field it reads, then how the call ended.
function serveDatabase(channel, call) {
    let asked = Buffer.alloc(0)
    channel.on("data", chunk => {
        asked = Buffer.concat([asked, chunk])
        for (let request = takeRequest(asked); request !== undefined; request = takeRequest(asked)) {
            asked = asked.subarray(request.length)
            channel.write(answer(() => call(request.call, request.values)))
        }
    })
}

// The only values these calls carry are text: a tag, a size and that many bytes of a field's JSON.
function takeRequest(asked) {
    if (asked.length < 4 || asked.length < 4 + asked.readUInt32LE(0)) return undefined
    let at = 4 + asked.readUInt32LE(0)
    const head = JSON.parse(asked.subarray(4, at).toString())
    const values = []
    for (let index = 0; index < head.bindings; index++) {
        if (asked.length < at + 5) return undefined
        const end = at + 5 + asked.readUInt32LE(at + 1)
        if (asked.length < end) return undefined
        values.push(JSON.parse(asked.subarray(at + 5, end).toString()))
        at = end
    }
    return { call: head.call, values, length: at }
}

function answer(call) {
    try {
        const result = call()
        if (result.type !== "fields") return record(0x12, JSON.stringify(result))
        const rows = Object.entries(result.fields).map(([name, value]) => Buffer.concat([Buffer.from([0x11, 3]), sized(name), Buffer.from([3]), sized(JSON.stringify(value))]))
        return Buffer.concat([Buffer.from([0x10, 2, 0, 0, 0]), sized("name"), sized("value"), ...rows, record(0x12, '{"type":"rows"}')])
    } catch (error) {
        return record(0x13, error.message)
    }
}

function record(kind, body) {
    return Buffer.concat([Buffer.from([kind]), sized(body)])
}

function sized(text) {
    const size = Buffer.alloc(4)
    size.writeUInt32LE(Buffer.byteLength(text))
    return Buffer.concat([size, Buffer.from(text)])
}

// A message is a line of JSON, then the parts whose sizes the line announces.
async function* readFrames(input) {
    let pending = Buffer.alloc(0)
    for await (const chunk of input) {
        pending = Buffer.concat([pending, chunk])
        for (let frame = takeFrame(pending); frame !== undefined; frame = takeFrame(pending)) {
            pending = pending.subarray(frame.length)
            yield frame
        }
    }
}

// The result of an invocation is the first part of its reply.
function attach(message, parts) {
    if (message.type === "reply" && message.reply.type === "invoked") message.reply.result = JSON.parse(parts[0].toString())
    return message
}

function takeFrame(pending) {
    const end = pending.indexOf(10)
    if (end === -1) return undefined
    const head = JSON.parse(pending.subarray(0, end).toString())
    let start = end + 1
    const parts = (head.parts ?? []).map(size => pending.subarray(start, (start += size)))
    return start > pending.length ? undefined : { head, parts, length: start }
}
