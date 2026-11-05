import assert from "node:assert/strict"
import test from "node:test"

import { pinChartImages } from "../../scripts/pin-chart-image.mjs"

const images = Object.fromEntries(["controlPlane", "typescript", "python", "collector"].map(name => [name, { repository: `example.com/${name}`, digest: "" }]))
const values = Bun.YAML.stringify({ images, replicaCount: 2 })
const digests = { controlPlane: `sha256:${"a".repeat(64)}`, typescript: `sha256:${"b".repeat(64)}`, python: `sha256:${"c".repeat(64)}`, collector: `sha256:${"d".repeat(64)}` }

test("published chart pins each image to its own release digest and preserves other settings", () => {
    const pinned = Bun.YAML.parse(pinChartImages(values, digests))
    assert.equal(pinned.replicaCount, 2)
    for (const name of Object.keys(images)) assert.deepEqual(pinned.images[name], { ...images[name], digest: digests[name].slice(7) })
})

test("chart packaging rejects missing, mutable, or already pinned images", () => {
    for (const name of Object.keys(images)) {
        assert.throws(() => pinChartImages(values, { ...digests, [name]: "latest" }), /digest/)
        assert.throws(() => pinChartImages(values, { ...digests, [name]: undefined }), /digest/)
        assert.throws(() => pinChartImages(Bun.YAML.stringify({ images: { ...images, [name]: {} } }), digests), /empty .* digest/)
    }
    assert.throws(() => pinChartImages("", digests), /empty .* digest/)
    assert.throws(() => pinChartImages(pinChartImages(values, digests), digests), /empty .* digest/)
})
