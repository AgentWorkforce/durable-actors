import assert from "node:assert/strict"
import { test } from "node:test"
import { z } from "zod"

import { ActorSerializationError } from "../src/errors.js"
import { cloneJson, copyJsonValue, jsonValueSchema } from "../src/json.js"

declare const Bun: { gc(synchronous: boolean): void }

test("JSON clones preserve serialized values without sharing nested state", () => {
    const value = {
        nested: { count: 1 },
        omitted: undefined,
        values: [undefined, NaN],
        date: new Date("2026-01-01T00:00:00Z")
    }
    const cloned = cloneJson(value, "test value")
    value.nested.count = 2
    assert.deepEqual(cloned, { nested: { count: 1 }, values: [null, null], date: "2026-01-01T00:00:00.000Z" })
})

test("JSON clones reject unsupported roots and cycles with a labeled error", () => {
    const circular = { self: {} }
    circular.self = circular
    for (const value of [undefined, () => {}, Symbol("value"), 1n, circular]) {
        assert.throws(
            () => cloneJson(value, "actor result"),
            error => error instanceof ActorSerializationError && error.message.includes("actor result")
        )
    }
})

test("a JSON value is copied as it stands, without sharing nested state", () => {
    for (const copy of [copyJsonValue, (value: unknown) => jsonValueSchema.parse(value)]) {
        const value = nestedValue()
        const copied = copy(value)
        value.nested.twice.text = "changed"

        assert.deepStrictEqual(copied, nestedValue())
    }
})

function nestedValue() {
    const twice = { text: "caf\u00e9" }
    return { list: [1.5, -0, true, null, "", [twice]], nested: { twice, empty: {} }, constructor: "kept" }
}

test("members a JSON text would not carry are left out of the copy", () => {
    const value = Object.defineProperty(JSON.parse('{"__proto__":{"polluted":true},"kept":1}'), "hidden", { value: 2 })
    const copy = copyJsonValue([value, Object.assign([1], { note: "left out" })])

    assert.deepStrictEqual(copy, [{ kept: 1 }, [1]])
    assert.equal(Object.getPrototypeOf((copy as object[])[0]), Object.prototype)
})

test("a value that is not JSON as it stands is refused, wherever it is nested", () => {
    const circular: Record<string, unknown> = {}
    circular.self = [circular]
    const refused = [
        undefined,
        NaN,
        -Infinity,
        1n,
        Symbol("value"),
        () => {},
        new Date(0),
        new Map(),
        new Uint8Array(1),
        new (class Point {})(),
        { [Symbol("key")]: 1 },
        [1, , 3],
        circular
    ]
    for (const value of refused.flatMap(value => [value, [value], { nested: { value } }])) {
        assert.throws(() => copyJsonValue(value), TypeError)
        assert.equal(jsonValueSchema.safeParse(value).success, false)
        assert.equal(z.record(z.string(), jsonValueSchema).safeParse({ value }).success, false)
    }
})

test("a value the schema parsed is not kept once the caller lets go of it", async () => {
    const parsed = parsedValues()
    await new Promise(resolve => setTimeout(resolve))
    Bun.gc(true)

    assert.deepEqual(
        parsed.map(value => value.deref()),
        [undefined, undefined]
    )
})

function parsedValues(): WeakRef<object>[] {
    const value = { frames: [{ html: "x".repeat(1000) }] }
    const args = z.array(jsonValueSchema).parse([value])
    return [new WeakRef(value), new WeakRef(args[0] as object)]
}
