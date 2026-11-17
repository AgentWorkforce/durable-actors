import assert from "node:assert/strict"
import { test } from "node:test"

import type { HostSocketEffect } from "../../src/actor/socketProtocol.js"
import { ActorProtocolError } from "../../src/errors.js"
import { detachReply, parseActorSessionServerMessage } from "../../src/host/protocol.js"
import type { JsonValue } from "../../src/json.js"
import { encodeJson } from "../../src/payload/json-text.js"
import type { JsonText } from "../../src/payload/json-text.js"

const messages = [{ text: "caf\u00e9 \\ \u2028" }, "", "last"] as const
const encoded = (value: JsonValue) => encodeJson(value)
const effects = socketEffects(messages.map(encoded))

test("rejects malformed actor session messages", () => {
    assert.throws(
        () =>
            parseActorSessionServerMessage({
                head: { type: "command", message_id: 1, command: { type: "invoke", request_id: false } },
                parts: ["[]"]
            }),
        ActorProtocolError
    )
})

test("an invocation's arguments are the part after its line, without prototype members", () => {
    const parts = ['[{"__proto__":{"polluted":true},"name":"ada"},[1]]']
    const message = parseActorSessionServerMessage({ head: invoke({ args: ["ignored"] }), parts })
    assert.ok(message.type === "command" && message.command.type === "invoke")
    assert.deepEqual(message.command.args, [{ name: "ada" }, [1]])
    assert.deepEqual(parts, [])
})

test("rejects an invocation whose arguments are missing, not an array, or followed by another part", () => {
    for (const parts of [[], ["{}"], ["[1"], ["[]", "[]"]])
        assert.throws(
            () => parseActorSessionServerMessage({ head: invoke(), parts }),
            ActorProtocolError,
            JSON.stringify(parts)
        )
    assert.throws(
        () =>
            parseActorSessionServerMessage({
                head: { type: "command", message_id: 1, command: { type: "hydrate" } },
                parts: ["[]"]
            }),
        ActorProtocolError
    )
})

test("a reply leaves its result, its effects and the text of each message as parts of their own", () => {
    const result = encodeJson({ list: [1, "two"] })
    const { parts, ...head } = detachReply({ type: "invoked", sequence: 3, result, effects })

    assert.deepEqual(head, { type: "invoked", sequence: 3 })
    assert.deepEqual(parts, [
        result,
        JSON.stringify([
            { type: "send", connection_id: "socket-1", message: { type: "text" } },
            { type: "send", connection_id: "socket-1", message: { type: "binary", data: "AAH/" } },
            { type: "set_tags", connection_id: "socket-1", tags: ["room"] },
            { type: "broadcast", message: { type: "text" }, except_connection_ids: [], tags: [] },
            { type: "broadcast", message: { type: "text" }, except_connection_ids: ["socket-1"], tags: [] }
        ]),
        ...messages.map(encoded)
    ])
})

test("only replies that carry a result or effects have parts", () => {
    assert.equal(detachReply({ type: "invoked", result: encodeJson(null) }).parts?.length, 2)
    assert.equal(detachReply({ type: "websocket_handled", effects: [] }).parts?.length, 1)
    for (const reply of [
        { type: "failed", code: "c", message: "m" },
        { type: "evicted" },
        { type: "hydrated" }
    ] as const)
        assert.equal(detachReply(reply).parts, undefined)
})

function invoke(fields: object = {}): object {
    return { type: "command", message_id: 1, command: { type: "invoke", request_id: "r", method: "m", ...fields } }
}

function socketEffects([first, empty, last]: JsonText[]): HostSocketEffect[] {
    return [
        { type: "send", connection_id: "socket-1", message: { type: "text", data: first! } },
        { type: "send", connection_id: "socket-1", message: { type: "binary", data: "AAH/" } },
        { type: "set_tags", connection_id: "socket-1", tags: ["room"] },
        { type: "broadcast", message: { type: "text", data: empty! }, except_connection_ids: [], tags: [] },
        { type: "broadcast", message: { type: "text", data: last! }, except_connection_ids: ["socket-1"], tags: [] }
    ]
}
