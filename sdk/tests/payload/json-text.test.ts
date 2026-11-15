import assert from "node:assert/strict"
import { test } from "node:test"

import type { JsonValue } from "../../src/json.js"
import { encodeJson } from "../../src/payload/json-text.js"
import type { JsonText } from "../../src/payload/json-text.js"

const CHUNK_BYTES = 64 * 1024

test("the text is byte for byte what JSON.stringify gives", () => {
    const values: JsonValue[] = [
        null,
        true,
        false,
        0,
        -0,
        -1.5,
        1e21,
        5e-324,
        Number.MAX_SAFE_INTEGER,
        "",
        "plain",
        'quote " backslash \\ slash / \b\t\n\f\r \u0000 \u001f \u007f',
        "caf\u00e9 \u07ff \u0800 \uffff \u2028 \u2029",
        "\u{1f600} a \u{10ffff} \u{10000}",
        "\ud800",
        "\udc00",
        "a\ud83d",
        "\ude00\ud83d",
        "\ud83d\ud83d\ude00",
        [],
        {},
        [[], {}, [null]],
        { "": "", 'k"\n': [1, "two", { three: [true, null] }], "caf\u00e9": -0 },
        everyCodeUnit()
    ]
    for (const value of values) assertText(encodeJson(value), JSON.stringify(value))
})

test("a value that is not JSON as it stands is written the way JSON.stringify writes it", () => {
    for (const value of convertedValues()) assertText(encodeJson(value), JSON.stringify(value))
})

test("a value JSON.stringify gives no text for has none", () => {
    for (const value of [undefined, () => 1, Symbol("s"), { toJSON: () => undefined }])
        assert.equal(encodeJson(value), JSON.stringify(value))
})

test("a value JSON.stringify refuses is refused", () => {
    const loop: { self?: unknown } = {}
    loop.self = [{ loop }]
    const list: unknown[] = []
    list.push(list)
    const failure = new Error("no")
    const refused = [
        loop,
        list,
        { again: { toJSON: () => loop } },
        1n,
        [{ count: Object(1n) }],
        { toJSON: () => 1n },
        {
            get member(): never {
                throw failure
            }
        },
        {
            toJSON(): never {
                throw failure
            }
        }
    ]
    for (const value of refused) {
        const thrown = thrownBy(() => JSON.stringify(value))

        assert.ok(thrown === failure || thrown instanceof TypeError)
        assert.throws(
            () => encodeJson(value),
            error => (thrown === failure ? error === failure : error instanceof TypeError)
        )
    }
})

test("a text encoded by the value being written leaves that one whole", () => {
    const long = "caf\u00e9 ".repeat(CHUNK_BYTES)
    const inner: JsonText[] = []
    const value = {
        before: long,
        get nested() {
            inner.push(encodeJson({ long, short: "inner" }), encodeJson("inner"))
            return "outer"
        },
        after: long
    }

    assertText(encodeJson(value), JSON.stringify({ before: long, nested: "outer", after: long }))
    assertText(inner[0]!, JSON.stringify({ long, short: "inner" }))
    assertText(inner[1]!, '"inner"')
})

test("a large text is held in chunks of at most 64 KB", () => {
    const texts = ["plain ", 'q"\n', "caf\u00e9 ", "\u65e5\u672c\u8a9e", "\u{1f600}", "\ud800"].map(text =>
        text.repeat(70_000)
    )
    for (const value of [...texts, texts, { texts, count: texts.length }]) {
        const text = encodeJson(value)

        assert.ok(text.chunks.length > 1)
        assert.ok(text.chunks.every(chunk => chunk.byteLength <= CHUNK_BYTES))
        assertText(text, JSON.stringify(value))
    }
})

test("a text keeps its bytes while later ones are encoded", () => {
    const large = "x".repeat(3 * CHUNK_BYTES)
    const first = encodeJson("first")
    const second = encodeJson(large)
    const third = encodeJson(["third"])

    assertText(first, '"first"')
    assertText(second, `"${large}"`)
    assertText(third, '["third"]')
})

function everyCodeUnit(): string {
    return Array.from({ length: 0x10000 }, (_, unit) => String.fromCharCode(unit)).join("")
}

// Values `JSON.stringify` converts, or writes only part of.
function convertedValues(): unknown[] {
    class Row {
        readonly id = 7
        readonly #hidden = 1
        get computed(): number {
            return this.#hidden
        }
    }
    const sparse: unknown[] = []
    sparse[2] = "last"
    return [
        new Date(0),
        new Date(NaN),
        { at: new Date(86_400_000), list: [new Date(0)] },
        Buffer.from("hi"),
        new Row(),
        [new Row(), { rows: [new Row()] }],
        { skipped: undefined, method() {}, sign: Symbol("s"), kept: null },
        [undefined, () => 1, Symbol("s"), null],
        sparse,
        Object.assign([1, 2], { extra: "left out" }),
        [NaN, Infinity, -Infinity, -0, 1e21, 1e-7],
        new Number(3.5),
        new String("boxed \u00e9"),
        new Boolean(false),
        Object(Symbol("s")),
        { boxed: [new Number(NaN), new String(""), new Boolean(true), Object(Symbol("s"))] },
        { toJSON: () => ({ toJSON: () => "not asked again" }) },
        { toJSON: "not a function", kept: 1 },
        { toJSON: (key: string) => `asked as ${JSON.stringify(key)}` },
        { held: { toJSON: (key: string) => key }, list: [{ toJSON: (key: string) => key }] },
        new Map([[1, 2]]),
        new Set([1]),
        /x/g,
        new Error("e"),
        new Uint8Array([1, 2, 255]),
        new Proxy([1, [2]], {}),
        new Proxy({ a: [1] }, {}),
        new Proxy({ b: 1, 1: 2 }, { ownKeys: () => ["b", "1"] }),
        JSON.parse('{"__proto__":{"a":1},"b":2}'),
        Object.assign(Object.create(null), { z: 1, 10: "ten", 2: "two", a: [undefined] }),
        Object.create({ inherited: 1 }, { own: { value: 1, enumerable: true }, hidden: { value: 2 } }),
        {
            get computed() {
                return [1, undefined]
            }
        },
        { 10: "ten", 2: "two", b: 1, a: 2, "-1": 3, "01": 4 },
        ...rawJson()
    ]
}

function rawJson(): unknown[] {
    const raw = (JSON as { rawJSON?: (text: string) => unknown }).rawJSON
    if (raw === undefined) return []
    return [raw("1e1000"), { big: raw("12345678901234567890"), list: [raw('"caf\u00e9"'), raw("null")] }]
}

function thrownBy(run: () => unknown): unknown {
    try {
        run()
    } catch (error) {
        return error
    }
    return undefined
}

function assertText(text: JsonText | undefined, expected: string): void {
    assert.ok(text !== undefined, `a text for ${expected.slice(0, 60)}`)
    const bytes = Buffer.concat(text.chunks)
    assert.equal(text.byteLength, bytes.byteLength)
    assert.ok(bytes.equals(Buffer.from(expected)), `the text of ${expected.slice(0, 60)}`)
}
