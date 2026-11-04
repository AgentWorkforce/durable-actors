import { build } from "esbuild"
import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import { test } from "node:test"

test("the public package bundles into a hosted browser app without a second React or Node runtime", async () => {
    const result = await build({
        stdin: { contents: 'export { DataProvider, ObserverRoutes } from "durable-actors-observer"; import "durable-actors-observer/theme.css"', resolveDir: process.cwd() },
        outfile: "consumer.js",
        bundle: true,
        platform: "browser",
        format: "esm",
        external: ["react", "react/*", "react-dom", "react-dom/*", "react-router", "react-router/*"],
        loader: { ".woff2": "file" },
        write: false,
        metafile: true
    })
    const inputs = Object.keys(result.metafile.inputs)
    assert.ok(inputs.includes("dist/index.js"))
    assert.ok(inputs.includes("dist/theme.css"))
    assert.ok(inputs.every(input => !input.includes("node_modules/react/") && !input.includes("node_modules/react-dom/") && !input.startsWith("src/")))
    assert.ok(
        Object.values(result.metafile.outputs)
            .flatMap(output => output.imports)
            .some(imported => imported.path === "react" && imported.external)
    )
    assert.match(await readFile("dist/index.d.ts", "utf8"), /ObserverRoutes/u)
})

test("the standalone package contains its browser entry and styles", async () => {
    const html = await readFile("dist/standalone/index.html", "utf8")
    for (const [, file] of html.matchAll(/(?:src|href)="\.\/([^"]+)"/gu)) {
        assert.ok((await readFile(`dist/standalone/${file}`)).length > 0)
    }
    assert.match(await readFile("dist/standalone/THIRD-PARTY-LICENSES.md", "utf8"), /react|Copyright/u)
    const application = await readFile("dist/standalone/app.js", "utf8")
    assert.doesNotMatch(application, /Keep the observe command running/u)
})
