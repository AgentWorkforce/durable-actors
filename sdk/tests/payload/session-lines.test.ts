import assert from "node:assert/strict"
import { test } from "node:test"

import { ActorProtocolError } from "../../src/errors.js"
import { SessionLines } from "../../src/payload/session-lines.js"

const expected = [{ type: "command", message_id: 1 }, { text: "line\none caf\u00e9 \u2028 \u{1f600}" }, {}]
const session = Buffer.from(expected.map(message => `${JSON.stringify(message)}\n`).join(""))

test("reads the same messages however the session is cut into chunks", () => {
    for (const size of [1, 2, 7, 50, session.length]) {
        const lines = new SessionLines()
        const read: unknown[] = []
        for (let start = 0; start < session.length; start += size)
            read.push(...lines.accept(session.subarray(start, start + size)))
        assert.deepEqual(read, expected, `chunks of ${size} bytes`)
    }
})

test("rejects a line that is not JSON", () => {
    assert.throws(() => new SessionLines().accept(Buffer.from("not json\n")), ActorProtocolError)
})
