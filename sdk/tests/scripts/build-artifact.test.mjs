import assert from "node:assert/strict"
import { execFile, spawn } from "node:child_process"
import { once } from "node:events"
import { cp, mkdir, mkdtemp, readFile, realpath, rm, symlink, writeFile } from "node:fs/promises"
import { createServer } from "node:net"
import os from "node:os"
import path from "node:path"
import { test } from "node:test"
import { fileURLToPath } from "node:url"
import { promisify } from "node:util"

import { buildActor } from "../../dist/compiler/actor-build.js"
import { serveDatabase } from "../fixtures/database-peer.ts"
import { HostDatabase } from "../fixtures/host-database.ts"
import { sessionReader, writeSession } from "../fixtures/session-peer.ts"

const sdk = fileURLToPath(new URL("../../", import.meta.url))
const run = promisify(execFile)

test("one actor build returns the matching public contract", async t => {
    const root = await project(t)
    await writeFile(path.join(root, "src/actors.ts"), 'import { Actor } from "durable-actors"; export class Counter extends Actor { async read(): Promise<number> { return 7 } }')
    const contract = await buildActor(path.join(root, "src/actors.ts"), path.join(root, "actors.mjs"))
    assert.equal(contract?.actors[0].actorName, "Counter")
    assert.equal(contract.actors[0].rpc.methods[0].name, "read")
    assert.match(await readFile(path.join(root, "actors.mjs"), "utf8"), /Counter/)
})

test("the installed compiler generates declarations using only its runtime dependencies", async t => {
    const installed = await mkdtemp(path.join(os.tmpdir(), "installed-actor-sdk-"))
    t.after(() => rm(installed, { recursive: true, force: true }))
    await cp(path.join(sdk, "bin"), path.join(installed, "bin"), { recursive: true })
    await cp(path.join(sdk, "dist"), path.join(installed, "dist"), { recursive: true })
    await cp(path.join(sdk, "package.json"), path.join(installed, "package.json"))
    const manifest = JSON.parse(await readFile(path.join(installed, "package.json"), "utf8"))
    for (const name of Object.keys(manifest.dependencies)) {
        const destination = path.join(installed, "node_modules", name)
        await mkdir(path.dirname(destination), { recursive: true })
        await symlink(await realpath(path.join(sdk, "node_modules", name)), destination, "dir")
    }
    const root = await project(t, installed)
    await writeFile(path.join(root, "src/actors.ts"), 'import { Actor } from "durable-actors"; export class Counter extends Actor { async read(): Promise<number> { return 7 } }')
    const output = path.join(root, "generated")
    await run("bun", [path.join(installed, manifest.bin["durable-actors"]), "generate", "src/actors.ts", "--out-dir", output], { cwd: root })
    assert.match(await readFile(path.join(output, "types.d.ts"), "utf8"), /read\(\): Promise<number>/)
})

test("bundling uses the analyzed source even when files change during a build", async t => {
    const root = await project(t)
    const entrypoint = path.join(root, "src/actors.ts")
    await writeFile(entrypoint, 'import { Actor } from "durable-actors"; export class Counter extends Actor { async read(): Promise<number> { return 7 } }')
    const { ActorCompiler } = await import("../../dist/compiler/actor-compiler.js")
    const compilation = new ActorCompiler().compileDeployment(entrypoint)
    await writeFile(entrypoint, 'import { Actor } from "durable-actors"; export class Replacement extends Actor {}')
    const { bundleActor } = await import("../../dist/compiler/actor-build.js")
    const outfile = path.join(root, "actors.mjs")
    await bundleActor(entrypoint, outfile, compilation)
    const artifact = await readFile(outfile, "utf8")
    assert.match(artifact, /Counter/)
    assert.doesNotMatch(artifact, /Replacement/)
    assert.equal(compilation.contract.actors[0].actorName, "Counter")
})

test("deployment builds produce code and a contract without executing customer code", async t => {
    const root = await project(t)
    await writeFile(
        path.join(root, "src/actors.ts"),
        'import { Actor } from "durable-actors"; export class Counter extends Actor { async get(): Promise<number> { return 42 } }; throw new Error("customer code executed during build")'
    )
    const output = path.join(root, "published")
    const { stdout } = await run("bun", [path.join(sdk, "dist/compiler/deployment-build.js"), root, "src/actors.ts", output])
    const contract = JSON.parse(stdout)
    assert.equal(contract.actors[0].actorName, "Counter")
    assert.equal(contract.actors[0].rpc.methods[0].name, "get")
    assert.match(await readFile(path.join(output, "actors.mjs"), "utf8"), /Counter/)
    await writeFile(path.join(root, "src/actors.ts"), 'import { Actor } from "durable-actors"; export class Counter extends Actor { async get(): Promise<Date> { return new Date() } }')
    await assert.rejects(run("bun", [path.join(sdk, "dist/compiler/deployment-build.js"), root, "src/actors.ts", output]), /JSON-compatible/)
})

test("local bundles resolve the same SDK even outside the project directory", async t => {
    const root = await project(t)
    const output = await mkdtemp(path.join(os.tmpdir(), "local-actor-code-"))
    t.after(() => rm(output, { recursive: true, force: true }))
    await writeFile(path.join(root, "src/actors.ts"), 'import { Actor } from "durable-actors"; export class Counter extends Actor { async read(): Promise<number> { return 7 } }')
    await run("bun", [path.join(sdk, "dist/compiler/deployment-build.js"), root, "src/actors.ts", output, "local"])
    await run("bun", [
        "--eval",
        `import assert from 'node:assert/strict';
        import { Actor } from ${JSON.stringify(path.join(sdk, "dist/index.js"))};
        const { actors } = await import(${JSON.stringify(path.join(output, "actors.mjs"))});
        assert.equal(Object.getPrototypeOf(actors.Counter.prototype), Actor.prototype);`
    ])
})

test("bundles skip Bun's runtime transpiler and keep CommonJS globals", async t => {
    const root = await project(t)
    const dependency = path.join(root, "node_modules/commonjs-dependency")
    await mkdir(path.join(dependency, "lib"), { recursive: true })
    await writeFile(path.join(dependency, "package.json"), JSON.stringify({ name: "commonjs-dependency", main: "index.js", types: "index.d.ts" }))
    await writeFile(path.join(dependency, "index.d.ts"), "declare const dependency: Record<string, string>\nexport = dependency\n")
    await writeFile(path.join(dependency, "lib/value.js"), 'module.exports = "bundled"\n')
    await writeFile(
        path.join(dependency, "index.js"),
        `module.exports = {
            events: typeof require("node:events").EventEmitter,
            value: require("./lib/value.js"),
            dirname: __dirname,
            filename: __filename
        }`
    )
    await writeFile(
        path.join(root, "src/actors.ts"),
        'import { Actor } from "durable-actors"; import dependency from "commonjs-dependency"; export class Counter extends Actor { static readonly dependency = dependency }'
    )
    const artifact = path.join(root, "dist/actors.mjs")
    await buildActor(path.join(root, "src/actors.ts"), artifact)
    assert.equal((await readFile(artifact, "utf8")).split("\n")[0], "// @bun")
    const { stdout } = await run("bun", ["--eval", `const { actors } = await import(${JSON.stringify(artifact)}); console.log(JSON.stringify(actors.Counter.dependency))`])
    const { dirname, filename, ...values } = JSON.parse(stdout)
    assert.deepEqual(values, { events: "function", value: "bundled" })
    assert.equal(path.basename(filename), "actors.mjs")
    assert.equal(path.dirname(filename), dirname)
})

test("actors keep their class names when a dependency declares the same name", async t => {
    const root = await project(t)
    const dependency = path.join(root, "node_modules/tally")
    await mkdir(dependency)
    await writeFile(path.join(dependency, "package.json"), JSON.stringify({ name: "tally", version: "1.0.0", type: "module", main: "index.js", types: "index.d.ts" }))
    await writeFile(path.join(dependency, "index.d.ts"), "export declare class Counter {}\n")
    await writeFile(path.join(dependency, "index.js"), "export class Counter {}\n")
    await writeFile(
        path.join(root, "src/actors.ts"),
        'import { Actor } from "durable-actors"; import { Counter as Tally } from "tally"; export class Counter extends Actor { static readonly tally = Tally }'
    )
    const artifact = path.join(root, "dist/actors.mjs")
    await buildActor(path.join(root, "src/actors.ts"), artifact)
    await run("bun", [path.join(sdk, "dist/host/validate-artifact.js"), artifact], { timeout: 30_000 })
})

test("built actors run without source, compiler, or TypeScript loader", { timeout: 30_000 }, async t => {
    const root = await project(t)
    await writeFile(path.join(root, "src/increment.ts"), "export const increment = (amount: number) => amount + 1\n")
    await writeFile(
        path.join(root, "src/actors.ts"),
        `import { Actor, Persisted, Ephemeral, Emittable } from "durable-actors"
        import { increment } from "./increment.js"
        export class BuiltCounter extends Actor {
            @Persisted @Emittable count = 0
            @Ephemeral calls = 0
            async add(amount: number) {
                this.count += increment(amount)
                return { count: this.count, calls: ++this.calls }
            }
            async invalidState() { this.count = "invalid" as never }
        }`
    )
    await buildActor(path.join(root, "src/actors.ts"), path.join(root, "dist/actors.mjs"))
    assert.match(await readFile(path.join(root, "dist/actors.mjs"), "utf8"), /BuiltCounter/)

    const deployed = path.join(root, "deployed")
    await mkdir(deployed)
    await cp(path.join(root, "dist"), path.join(deployed, "dist"), { recursive: true })
    await rm(path.join(root, "src"), { recursive: true })
    await rm(path.join(root, "tsconfig.json"))
    await rm(path.join(root, "dist"), { recursive: true })
    const socketPath = path.join(root, "host.sock")
    const bootstrap = path.join(root, "host.mjs")
    await writeFile(bootstrap, `import { runActorHost } from ${JSON.stringify(new URL("../../dist/host.js", import.meta.url).href)}; await runActorHost()`)
    const server = createServer()
    t.after(() => server.close())
    const connected = once(server, "connection")
    server.listen(socketPath)
    await once(server, "listening")
    const environment = { ...process.env, DURABLE_ACTORS_EXECUTOR_SOCKET: socketPath, DURABLE_ACTORS_DATABASE_FD: "3" }
    delete environment.DURABLE_ACTORS_ENTRYPOINT
    // The fourth descriptor is the database channel, which the host's thread blocks on.
    const host = spawn("bun", [bootstrap], {
        cwd: deployed,
        env: environment,
        stdio: ["ignore", "pipe", "pipe", "pipe"]
    })
    const database = new HostDatabase()
    t.after(() => database.close())
    serveDatabase(host.stdio[3], database.call)
    let stderr = ""
    host.stderr.on("data", data => (stderr += data))
    const exited = once(host, "exit")
    t.after(async () => {
        host.kill()
        await exited
    })
    const [socket] = await Promise.race([
        connected,
        exited.then(() => {
            throw new Error(`actor host exited before connecting: ${stderr}`)
        })
    ])
    t.after(() => socket.destroy())
    const read = sessionReader(socket)
    const receive = async () => {
        for (;;) {
            const message = await read()
            if (message.type !== "residency") return message
        }
    }
    assert.deepEqual(await receive(), { type: "attach", protocol: 34, actor_names: ["BuiltCounter"] })
    const send = message => writeSession(socket, message)
    send({ type: "attached", protocol: 34, actor: { project_id: "default", actor_name: "BuiltCounter", actor_id: "counter" } })
    const invoke = messageId =>
        send({
            type: "command",
            message_id: messageId,
            command: { type: "invoke", request_id: `request-${messageId}`, method: "add", args: [2] }
        })
    invoke(1)
    const first = await receive()
    assert.deepEqual(first, {
        type: "reply",
        message_id: 1,
        reply: {
            type: "invoked",
            result: { count: 3, calls: 1 },
            effects: [{ type: "state_update", changes: { count: 3 }, removed: [] }]
        }
    })
    send({ type: "command", message_id: 2, command: { type: "evict", graceful: false } })
    assert.equal((await receive()).reply.type, "evicted")
    invoke(3)
    const resumed = await receive()
    assert.deepEqual(resumed, {
        type: "reply",
        message_id: 3,
        reply: {
            type: "invoked",
            result: { count: 6, calls: 1 },
            effects: [{ type: "state_update", changes: { count: 6 }, removed: [] }]
        }
    })
    send({
        type: "command",
        message_id: 4,
        command: {
            type: "invoke",
            request_id: "invalid-state",
            method: "invalidState",
            args: []
        }
    })
    const unchecked = (await receive()).reply
    assert.equal(unchecked.type, "invoked")
    assert.deepEqual(database.fields(), { count: "invalid" })
})

test("actor builds report invalid persistence annotations before deployment", async t => {
    const root = await project(t)
    await writeFile(path.join(root, "src/actors.ts"), `import { Actor } from "durable-actors"; export class Counter extends Actor { count = 0 }`)
    await assert.rejects(buildActor(path.join(root, "src/actors.ts"), path.join(root, "dist/actors.mjs")), /must declare exactly one of @Persisted or @Ephemeral/)
})

async function project(t, installed = sdk) {
    const root = await mkdtemp(path.join(os.tmpdir(), "actor-build-"))
    t.after(() => rm(root, { recursive: true, force: true }))
    await mkdir(path.join(root, "src"))
    await mkdir(path.join(root, "node_modules"))
    await symlink(installed, path.join(root, "node_modules/durable-actors"), "dir")
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
    return root
}
