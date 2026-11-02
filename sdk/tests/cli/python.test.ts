import assert from "node:assert/strict"
import { execFile, spawn } from "node:child_process"
import { once } from "node:events"
import { mkdir, mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises"
import { createServer } from "node:http"
import { tmpdir } from "node:os"
import path from "node:path"
import { type TestContext, test } from "node:test"
import { fileURLToPath } from "node:url"
import { promisify } from "node:util"

const run = promisify(execFile)
const cli = fileURLToPath(new URL("../../../dist/cli.js", import.meta.url))
const python = process.env.DURABLE_ACTORS_TEST_PYTHON
const runtime = process.env.DURABLE_ACTORS_TEST_RUNTIME
const environment = Object.fromEntries(
    Object.entries(process.env).filter(([key]) => !key.startsWith("DURABLE_ACTORS_"))
)
const source = `from durable_actors import Actor, emitted, persisted
class Counter(Actor):
    count: int = emitted(persisted(0))
    def increment(self, amount: int = 1) -> int:
        self.count += amount
        return self.count
`

test("init python points to the Python CLI", async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "actors-python-init-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    await assert.rejects(
        run(process.execPath, [cli, "init", "counter", "--template", "python"], { cwd: directory }),
        /uvx --from 'durable-actors\[cli\]'/u
    )
})

test(
    "generate checks Python definitions, generates independent clients and rejects typing errors",
    { skip: !python },
    async t => {
        const directory = await mkdtemp(path.join(tmpdir(), "actors-python-generate-"))
        t.after(() => rm(directory, { recursive: true, force: true }))
        await writeFile(path.join(directory, "actors.py"), source)
        await symlink(path.dirname(path.dirname(python!)), path.join(directory, ".venv"), "dir")
        const env = { ...environment, VIRTUAL_ENV: undefined }
        const result = await run(process.execPath, [cli, "generate", "actors.py"], { cwd: directory, env })
        assert.match(result.stdout, /Generated 1 actor contract/u)
        assert.match(await readFile(path.join(directory, "generated/_counter.py"), "utf8"), /def increment/u)
        await readFile(path.join(directory, "generated/py.typed"))
        await writeFile(
            path.join(directory, "usage.py"),
            `from generated import actors
def use() -> None:
    counter = actors.Counter.get("one")
    result: int = counter.increment(2)
    subscription = counter.subscribe(lambda state: print(state.count))
    subscription.close()
    counter.increment("bad")
`
        )
        await assert.rejects(
            run(python!, ["-m", "mypy", "--strict", "usage.py"], { cwd: directory }),
            (error: Error & { stdout?: string }) => {
                assert.match(error.stdout ?? "", /incompatible type/u)
                return true
            }
        )
        await writeFile(path.join(directory, "actors.py"), source.replace("return self.count", 'return "wrong"'))
        await assert.rejects(
            run(process.execPath, [cli, "generate", "actors.py"], { cwd: directory, env }),
            /Incompatible return value/u
        )
        await writeFile(path.join(directory, "actors.py"), source.replace("emitted(persisted(0))", "0"))
        await assert.rejects(
            run(process.execPath, [cli, "generate", "actors.py"], { cwd: directory, env }),
            /Counter.count: actor fields must declare exactly one of persisted\(\) or ephemeral\(\)/u
        )
    }
)

test("generate infers Python from a published contract without actor source", { skip: !python }, async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "actors-python-remote-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    await writeFile(path.join(directory, "actors.py"), source)
    const built = await run(python!, [
        "-m",
        "durable_actors.build",
        directory,
        "actors.py",
        path.join(directory, "build"),
        "local"
    ])
    const contract: unknown = JSON.parse(built.stdout)
    await rm(path.join(directory, "actors.py"))
    const server = createServer((_request, response) =>
        response.end(JSON.stringify({ contractHash: `sha256:${"a".repeat(64)}`, contract }))
    )
    t.after(() => server.close())
    server.listen(0, "127.0.0.1")
    await once(server, "listening")
    const origin = `http://127.0.0.1:${(server.address() as { port: number }).port}`
    await run(process.execPath, [cli, "generate", "--control-plane-url", origin], {
        cwd: directory,
        env: { ...environment, DURABLE_ACTORS_PYTHON: python }
    })
    assert.match(await readFile(path.join(directory, "generated/_counter.py"), "utf8"), /def increment/u)
})

for (const sourcePath of ["src/actors.py", "actors.py"])
    test(
        `dev discovers ${sourcePath}, checks changes before reload and preserves durable state`,
        { skip: !python || !runtime, timeout: 60000 },
        async t => {
            const directory = await mkdtemp(path.join(tmpdir(), "actors-python-dev-"))
            t.after(() => rm(directory, { recursive: true, force: true }))
            await mkdir(path.join(directory, "src"))
            const entrypoint = path.join(directory, sourcePath)
            await writeFile(entrypoint, source)
            const dev = await startDev(t, directory)
            assert.equal(await dev.invoke(), 1)
            dev.clearOutput()
            await writeFile(entrypoint, source.replace("return self.count", 'return "wrong"'))
            await dev.waitForOutput("Incompatible return value")
            assert.equal(await dev.invoke(), 2)
            assert.ok(!dev.output().includes("Updated local actors."), dev.output())
            dev.clearOutput()
            await writeFile(entrypoint, source.replace("self.count += amount", "self.count += amount * 10"))
            await dev.waitForOutput("Updated local actors.")
            assert.equal(await dev.invoke(), 12)
            await dev.stop()
            await assert.rejects(fetch(dev.origin))
        }
    )

test("dev keeps watching after the first build fails", { skip: !python || !runtime, timeout: 60000 }, async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "actors-python-dev-broken-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    const entrypoint = path.join(directory, "actors.py")
    await writeFile(entrypoint, source.replace("return self.count", 'return "wrong"'))
    const dev = await startDev(t, directory)
    assert.match(dev.output(), /Actor build failed: [\s\S]*Incompatible return value/u)
    await writeFile(entrypoint, source)
    await dev.waitForOutput("Updated local actors.")
    assert.equal(await dev.invoke(), 1)
})

test("deployment builder dispatches Python and produces a loadable artifact", { skip: !python }, async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "actors-python-deploy-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    await writeFile(path.join(directory, "actors.py"), source)
    const output = path.join(directory, "dist")
    const builder = fileURLToPath(new URL("../../../dist/compiler/deployment-build.js", import.meta.url))
    const built = await run("bun", [builder, directory, "actors.py", output], {
        env: { ...environment, DURABLE_ACTORS_PYTHON: python }
    })
    assert.equal(JSON.parse(built.stdout).actors[0].actorName, "Counter")
    await rm(path.join(directory, "actors.py"))
    const loaded = await run(
        python!,
        [
            "-c",
            `from pathlib import Path
from durable_actors.build import load_artifact
actor = load_artifact(Path("actors.pyz"))[0]()
print(actor.increment(7))`
        ],
        { cwd: output }
    )
    assert.equal(loaded.stdout.trim(), "7")
})

async function startDev(t: TestContext, directory: string) {
    const child = spawn(process.execPath, [cli, "dev", "--port", "0"], {
        cwd: directory,
        env: { ...environment, DURABLE_ACTORS_PYTHON: python, DURABLE_ACTORS_BINARY: runtime, NO_COLOR: "1" },
        stdio: ["pipe", "pipe", "pipe"]
    })
    const exited = once(child, "exit")
    const stop = async () => {
        child.kill("SIGTERM")
        await exited
    }
    t.after(stop)
    let output = ""
    child.stdout.on("data", chunk => (output += String(chunk)))
    child.stderr.on("data", chunk => (output += String(chunk)))
    const waitFor = async (matches: () => boolean) => {
        const deadline = Date.now() + 20000
        while (!matches()) {
            assert.equal(child.exitCode, null, output)
            assert.ok(Date.now() < deadline, output)
            await new Promise(resolve => setTimeout(resolve, 50))
        }
    }
    const ready = /DURABLE_ACTORS_CONTROL_PLANE_URL=(http:\/\/127.0.0.1:\d+)/u
    await waitFor(() => ready.test(output))
    const origin = output.match(ready)![1]
    return {
        origin,
        stop,
        output: () => output,
        clearOutput: () => (output = ""),
        waitForOutput: (text: string) => waitFor(() => output.includes(text)),
        invoke: async () => {
            const result = await run(
                python!,
                [
                    "-c",
                    `from durable_actors import Client
with Client(control_plane_url="${origin}") as client:
    print(client.invoke("Counter", "one", "increment", []))`
                ],
                { cwd: directory }
            )
            return Number(result.stdout.trim())
        }
    }
}
