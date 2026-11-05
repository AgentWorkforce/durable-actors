import { readFileSync, writeFileSync } from "node:fs"
import { resolve } from "node:path"
import { fileURLToPath } from "node:url"

export function pinChartImages(values, digests) {
    const parsed = Bun.YAML.parse(values)
    for (const name of ["controlPlane", "typescript", "python", "collector"]) {
        const digest = digests[name]
        if (!/^sha256:[a-f0-9]{64}$/u.test(digest ?? "")) throw new Error(`Expected an immutable ${name} image digest`)
        if (parsed?.images?.[name]?.digest !== "") throw new Error(`Chart values must contain an empty ${name} image digest`)
        parsed.images[name].digest = digest.slice(7)
    }
    return Bun.YAML.stringify(parsed)
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
    const values = new URL("../charts/durable-actors/values.yaml", import.meta.url)
    writeFileSync(
        values,
        pinChartImages(readFileSync(values, "utf8"), {
            controlPlane: process.argv[2],
            typescript: process.argv[3],
            python: process.argv[4],
            collector: process.argv[5]
        })
    )
}
