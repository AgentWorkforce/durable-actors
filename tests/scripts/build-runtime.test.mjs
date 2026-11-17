import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { createHash } from "node:crypto"
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import test from "node:test"
import { promisify } from "node:util"

import { RuntimeBuilder } from "../../scripts/build-runtime.mjs"

const execute = promisify(execFile)

test("native builds package the runtime and embedded replication licenses with a matching checksum", async t => {
    const root = await mkdtemp(path.join(tmpdir(), "ldo-bundle-"))
    t.after(() => rm(root, { recursive: true, force: true }))
    await mkdir(path.join(root, "target/release"), { recursive: true })
    await mkdir(path.join(root, "third_party/terse-ltx"), { recursive: true })
    await writeFile(path.join(root, "third_party/terse-ltx/LICENSE"), "license")
    await mkdir(path.join(root, "third_party/terse-litestream"), { recursive: true })
    await writeFile(path.join(root, "third_party/terse-litestream/LICENSE"), "capture license")
    await writeFile(path.join(root, "third_party/terse-litestream/NOTICE"), "capture notice")
    const run = async (command, args, options) => {
        if (command === "cargo") {
            await mkdir(path.join(root, "target/release"), { recursive: true })
            await writeFile(path.join(root, "target/release/durable-actors"), "runtime", { mode: 0o755 })
        } else {
            assert.equal(command, "tar")
            return execute(command, args, options)
        }
    }
    const archive = await new RuntimeBuilder({ root, platform: "linux", arch: "arm64" }, run).build()
    assert.equal(path.basename(archive), "durable-actors-linux-arm64.tar.gz")
    const checksum = createHash("sha256")
        .update(await readFile(archive))
        .digest("hex")
    assert.equal(await readFile(`${archive}.sha256`, "utf8"), `${checksum}  ${path.basename(archive)}\n`)
    const extracted = path.join(root, "extracted")
    await mkdir(extracted)
    await execute("tar", ["-xzf", archive, "-C", extracted])
    assert.equal(await readFile(path.join(extracted, "durable-actors"), "utf8"), "runtime")
    await execute("test", ["-x", path.join(extracted, "durable-actors")])
    assert.equal(await readFile(path.join(extracted, "LICENSE.terse-litestream"), "utf8"), "capture license")
    assert.equal(await readFile(path.join(extracted, "NOTICE.terse-litestream"), "utf8"), "capture notice")
    assert.equal(await readFile(path.join(extracted, "LICENSE.terse-ltx"), "utf8"), "license")
})

test("a compiler failure does not publish a native bundle", async t => {
    const root = await mkdtemp(path.join(tmpdir(), "ldo-bundle-failure-"))
    t.after(() => rm(root, { recursive: true, force: true }))
    const builder = new RuntimeBuilder({ root }, async () => {
        throw new Error("compiler failed")
    })
    await assert.rejects(builder.build(), /compiler failed/)
    await assert.rejects(readFile(path.join(root, `dist-runtime/durable-actors-${process.platform}-${process.arch}.tar.gz`)), { code: "ENOENT" })
})

test("a prebuilt Python runtime is packaged as the native CLI without compiling again", async t => {
    const root = await mkdtemp(path.join(tmpdir(), "ldo-prebuilt-"))
    t.after(() => rm(root, { recursive: true, force: true }))
    const binary = "target/release/durable-actors-native"
    await mkdir(path.dirname(path.join(root, binary)), { recursive: true })
    await writeFile(path.join(root, binary), "#!/bin/sh\necho prebuilt-runtime\n", { mode: 0o755 })
    for (const name of ["terse-ltx/LICENSE", "terse-litestream/LICENSE", "terse-litestream/NOTICE"]) {
        const file = path.join(root, "third_party", name)
        await mkdir(path.dirname(file), { recursive: true })
        await writeFile(file, name)
    }
    const run = (command, args, options) => {
        assert.equal(command, "tar", "packaging a prebuilt runtime must not invoke Cargo")
        return execute(command, args, options)
    }
    const archive = await new RuntimeBuilder({ root, binary }, run).build()
    const extracted = path.join(root, "extracted")
    await mkdir(extracted)
    await execute("tar", ["-xzf", archive, "-C", extracted])
    assert.equal((await execute(path.join(extracted, "durable-actors"), ["--version"])).stdout, "prebuilt-runtime\n")
    assert.deepEqual(await readFile(path.join(extracted, "durable-actors")), await readFile(path.join(root, binary)))
    const checksum = createHash("sha256")
        .update(await readFile(archive))
        .digest("hex")
    assert.equal(await readFile(`${archive}.sha256`, "utf8"), `${checksum}  ${path.basename(archive)}\n`)
})

test("a missing prebuilt runtime fails instead of silently compiling a replacement", async t => {
    const root = await mkdtemp(path.join(tmpdir(), "ldo-prebuilt-missing-"))
    t.after(() => rm(root, { recursive: true, force: true }))
    const builder = new RuntimeBuilder({ root, binary: "missing-runtime" }, async () => {
        assert.fail("a missing prebuilt runtime must not run any commands")
    })
    await assert.rejects(builder.build(), { code: "ENOENT" })
    await assert.rejects(readFile(path.join(root, `dist-runtime/durable-actors-${process.platform}-${process.arch}.tar.gz`)), { code: "ENOENT" })
})
