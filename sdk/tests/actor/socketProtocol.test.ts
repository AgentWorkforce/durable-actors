import assert from "node:assert/strict"
import { test } from "node:test"

import { receivedMessage } from "../../src/actor/socketValidation.js"

test("accepts automatic state control messages", () => {
    assert.deepEqual(receivedMessage({ type: "state", state: { count: 1 }, version: 3 }), {
        type: "state",
        state: { count: 1 },
        version: 3
    })
    const update = { type: "state_update", changes: { count: 2 }, removed: [], version: 4 }
    assert.deepEqual(receivedMessage(update), update)
})
