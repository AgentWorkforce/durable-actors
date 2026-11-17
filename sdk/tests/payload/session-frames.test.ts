import assert from "node:assert/strict"
import { test } from "node:test"

import { ActorProtocolError } from "../../src/errors.js"
import { GarbageCollection } from "../../src/payload/garbage-collection.js"
import { SessionFrames } from "../../src/payload/session-frames.js"
import type { SessionFrame } from "../../src/payload/session-frames.js"

const session = Buffer.concat([
    Buffer.from('{"type":"command","message_id":1}\n'),
    Buffer.from('{"type":"command","message_id":2,"parts":[9]}\n'),
    Buffer.from("line\none\n"),
    Buffer.from('{"parts":[0,3,0,5]}\n'),
    Buffer.from("caf\u00e9\u2028"),
    Buffer.from('{"text":"line\\none caf\u00e9 \u2028 \u{1f600}","parts":[]}\n')
])
const expected: SessionFrame[] = [
    { head: { type: "command", message_id: 1 }, parts: [] },
    { head: { type: "command", message_id: 2, parts: [9] }, parts: ["line\none\n"] },
    { head: { parts: [0, 3, 0, 5] }, parts: ["", "caf", "", "\u00e9\u2028"] },
    { head: { text: "line\none caf\u00e9 \u2028 \u{1f600}", parts: [] }, parts: [] }
]

test("reads the same messages however the session is cut into chunks", () => {
    for (const size of [1, 2, 7, 50, session.length]) {
        const frames = new SessionFrames(new GarbageCollection(() => {}))
        const read: SessionFrame[] = []
        for (let start = 0; start < session.length; start += size)
            read.push(...frames.accept(session.subarray(start, start + size)))
        assert.deepEqual(read, expected, `chunks of ${size} bytes`)
    }
})

test("collects as a large part arrives and once it is text, before it is parsed", () => {
    const events: string[] = []
    const frames = new SessionFrames(new GarbageCollection(() => events.push("collected")))
    const part = Buffer.alloc(9 * 1024 * 1024, "x")
    frames.accept(Buffer.from(`{"parts":[${part.length}]}\n`))
    frames.accept(part.subarray(0, 8 * 1024 * 1024))
    events.push("arrived")
    for (const frame of frames.accept(part.subarray(8 * 1024 * 1024))) events.push(`read ${frame.parts[0]!.length}`)
    assert.deepEqual(events, ["collected", "arrived", "collected", `read ${part.length}`])
})

test("rejects a line that is not JSON or announces parts that are not sizes", () => {
    for (const line of ["not json\n", '{"parts":[-1]}\n', '{"parts":[1.5]}\n', '{"parts":"3"}\n', "[1]\n"])
        assert.throws(
            () => new SessionFrames(new GarbageCollection(() => {})).accept(Buffer.from(line)),
            ActorProtocolError,
            line
        )
})
