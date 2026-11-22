import { build } from "esbuild"
import { copyFile, mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises"
import path from "node:path"
import { fileURLToPath } from "node:url"

const sdk = fileURLToPath(new URL("../", import.meta.url))

export async function buildHostRuntime(output) {
    await rm(output, { recursive: true, force: true })
    const entries = {
        index: "src/index.ts",
        backend: "src/backend.ts",
        proxy: "src/proxy.ts",
        host: "src/host.ts"
    }
    const result = await build({
        absWorkingDir: sdk,
        entryPoints: entries,
        outdir: output,
        bundle: true,
        splitting: true,
        chunkNames: "chunk-[hash]",
        platform: "node",
        format: "esm",
        target: "es2022",
        external: ["bun:*", "durable-actors"],
        keepNames: true,
        metafile: true,
        legalComments: "linked"
    })
    const { version } = JSON.parse(await readFile(path.join(sdk, "package.json"), "utf8"))
    await writeFile(
        path.join(output, "package.json"),
        JSON.stringify(
            {
                name: "durable-actors",
                version,
                type: "module",
                exports: {
                    ".": "./index.js",
                    "./backend": "./backend.js",
                    "./proxy": "./proxy.js",
                    "./host": "./host.js"
                }
            },
            null,
            2
        ) + "\n"
    )
    await copyFile(path.join(sdk, "LICENSE.md"), path.join(output, "LICENSE.md"))
    const packages = new Set()
    for (const input of Object.keys(result.metafile.inputs)) {
        if (!input.includes("node_modules/")) continue
        let directory = path.dirname(path.resolve(sdk, input))
        while (directory !== path.dirname(directory)) {
            const manifest = await readFile(path.join(directory, "package.json"), "utf8").catch(() => undefined)
            if (manifest && JSON.parse(manifest).name && JSON.parse(manifest).version) {
                packages.add(directory)
                break
            }
            directory = path.dirname(directory)
        }
    }
    for (const directory of packages) {
        const { name, version } = JSON.parse(await readFile(path.join(directory, "package.json"), "utf8"))
        const destination = path.join(output, "licenses", `${name.replaceAll("/", "-")}-${version}`)
        await mkdir(destination, { recursive: true })
        for (const file of await readdir(directory, { withFileTypes: true }))
            if (file.isFile() && /^(license|licence|notice|copying)([.-]|$)/iu.test(file.name))
                await copyFile(path.join(directory, file.name), path.join(destination, file.name))
    }
    return result.metafile
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url))
    await buildHostRuntime(path.resolve(process.argv[2] ?? path.join(sdk, "dist-host")))
