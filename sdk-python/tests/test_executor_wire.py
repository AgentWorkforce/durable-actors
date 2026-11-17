import asyncio
import copy
import json

import pytest

from durable_actors.executor_wire import Channel, serialize

TEXT = '{"html":"caf\u00e9 \\\\ \u2028"}\nsecond line'
EFFECTS = [
    {"type": "send", "connection_id": "socket-1", "message": {"type": "text", "data": TEXT}},
    {"type": "send", "connection_id": "socket-1", "message": {"type": "binary", "data": "AAH/"}},
    {"type": "set_tags", "connection_id": "socket-1", "tags": ["room"]},
    {
        "type": "broadcast",
        "message": {"type": "text", "data": ""},
        "except_connection_ids": [],
        "tags": [],
    },
]
MESSAGES = [
    {"type": "reply", "message_id": 1, "reply": {"type": "invoked", "result": {"count": 1}}},
    {
        "type": "reply",
        "message_id": 2,
        "reply": {"type": "invoked", "result": None, "sequence": 3, "effects": EFFECTS},
    },
    {"type": "reply", "message_id": 3, "reply": {"type": "websocket_handled", "effects": EFFECTS}},
    {"type": "reply", "message_id": 4, "reply": {"type": "websocket_handled", "effects": []}},
    {"type": "reply", "message_id": 5, "reply": {"type": "failed", "code": "c", "message": "m"}},
    {"type": "reply", "message_id": 6, "reply": {"type": "evicted"}},
    {"type": "socket_effects", "message_id": 7, "effects": EFFECTS},
    {"type": "command", "message_id": 8, "command": {"type": "evict"}},
    {
        "type": "command",
        "message_id": 9,
        "command": {"type": "invoke", "request_id": "r", "method": "m", "args": [TEXT, 1]},
    },
]


def test_a_reply_is_a_line_followed_by_its_result_effects_and_message_texts():
    effects = (
        b'[{"type":"send","connection_id":"socket-1","message":{"type":"text"}},'
        b'{"type":"send","connection_id":"socket-1","message":{"type":"binary","data":"AAH/"}},'
        b'{"type":"set_tags","connection_id":"socket-1","tags":["room"]},'
        b'{"type":"broadcast","message":{"type":"text"},"except_connection_ids":[],"tags":[]}]'
    )
    text = TEXT.encode()
    line = (
        b'{"type":"reply","message_id":2,"reply":{"type":"invoked","sequence":3},'
        b'"parts":[4,%d,%d,0]}\n' % (len(effects), len(text))
    )
    assert serialize(MESSAGES[1]) == line + b"null" + effects + text


def test_an_invocation_is_a_line_followed_by_its_arguments():
    args = json.dumps([TEXT, 1], separators=(",", ":")).encode()
    line = (
        b'{"type":"command","message_id":9,"command":{"type":"invoke","request_id":"r","method":"m"},'
        b'"parts":[%d]}\n' % len(args)
    )
    assert serialize(MESSAGES[8]) == line + args


def test_messages_without_a_result_or_effects_are_a_single_line():
    assert serialize(MESSAGES[5]) == b'{"type":"reply","message_id":6,"reply":{"type":"evicted"}}\n'
    assert serialize(MESSAGES[7]) == (
        b'{"type":"command","message_id":8,"command":{"type":"evict"}}\n'
    )


@pytest.mark.parametrize("chunk", [1, 7, 1 << 20])
async def test_messages_read_back_whole_however_the_stream_is_cut(chunk):
    stream = b"".join(serialize(message) for message in MESSAGES)
    expected = copy.deepcopy(MESSAGES)
    reader = asyncio.StreamReader()
    channel = Channel(reader, None)  # type: ignore[arg-type]

    async def feed():
        for start in range(0, len(stream), chunk):
            reader.feed_data(stream[start : start + chunk])
            await asyncio.sleep(0)
        reader.feed_eof()

    feeding = asyncio.create_task(feed())
    assert [await channel.read() for _ in MESSAGES] == expected
    with pytest.raises(EOFError):
        await channel.read()
    await feeding


async def test_a_message_with_a_missing_part_is_rejected():
    reader = asyncio.StreamReader()
    reader.feed_data(
        b'{"type":"reply","message_id":1,"reply":{"type":"invoked"},"parts":[4]}\nnull'
    )
    reader.feed_eof()
    with pytest.raises(ValueError):
        await Channel(reader, None).read()  # type: ignore[arg-type]
