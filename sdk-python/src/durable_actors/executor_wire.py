"""Internal executor messages shared by the host and its worker.

A message is a line of JSON followed by the parts whose sizes the line lists under ``parts``.
The arguments and result of an invocation, socket effects and the text of each socket message
travel as parts, so none of them is nested inside the line.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

from .contract import Document
from .guards import is_document

REPLIES_WITH_EFFECTS = {"invoked", "websocket_handled"}


class Channel:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.reader = reader
        self.writer = writer

    async def send(self, message: Document) -> None:
        self.writer.write(serialize(message))
        await self.writer.drain()

    async def read(self) -> Document:
        line = await self.reader.readline()
        if not line:
            raise EOFError("executor disconnected")
        head = json.loads(line)
        if not is_document(head):
            raise ValueError("executor messages must be objects")
        parts = [await self.reader.readexactly(size) for size in head.pop("parts", [])]
        return attach(head, parts)


def serialize(message: Document) -> bytes:
    head, parts = detach(message)
    if parts:
        head = {**head, "parts": [len(part) for part in parts]}
    return b"".join([encode(head), b"\n", *parts])


def detach(message: Document) -> tuple[Document, list[bytes]]:
    kind = message["type"]
    if is_invocation(message):
        command = message["command"]
        return {**message, "command": without(command, "args")}, [encode(command["args"])]
    if kind == "socket_effects":
        return without(message, "effects"), detach_effects(message["effects"])
    if kind == "reply" and message["reply"]["type"] in REPLIES_WITH_EFFECTS:
        reply = message["reply"]
        result = [encode(reply["result"])] if reply["type"] == "invoked" else []
        head = {**message, "reply": without(reply, "result", "effects")}
        return head, [*result, *detach_effects(reply.get("effects", []))]
    return message, []


def detach_effects(effects: list[Document]) -> list[bytes]:
    texts: list[bytes] = []
    return [encode([detach_text(effect, texts) for effect in effects]), *texts]


def detach_text(effect: Document, texts: list[bytes]) -> Document:
    message = effect.get("message")
    if message is None or message["type"] != "text":
        return effect
    texts.append(message["data"].encode())
    return {**effect, "message": {"type": "text"}}


def attach(head: Document, parts: list[bytes]) -> Document:
    kind = head["type"]
    if is_invocation(head):
        head["command"]["args"] = json.loads(take(parts))
    elif kind == "socket_effects":
        head["effects"] = attach_effects(parts)
    elif kind == "reply" and head["reply"]["type"] in REPLIES_WITH_EFFECTS:
        attach_reply(head["reply"], parts)
    if parts:
        raise ValueError("executor message has unexpected parts")
    return head


def attach_reply(reply: Document, parts: list[bytes]) -> None:
    if reply["type"] == "invoked":
        reply["result"] = json.loads(take(parts))
    effects = attach_effects(parts)
    if effects or reply["type"] == "websocket_handled":
        reply["effects"] = effects


def attach_effects(parts: list[bytes]) -> list[Document]:
    effects: list[Document] = json.loads(take(parts))
    for effect in effects:
        message = effect.get("message")
        if message is not None and message["type"] == "text":
            message["data"] = take(parts).decode()
    return effects


def is_invocation(message: Document) -> bool:
    return bool(message["type"] == "command" and message["command"]["type"] == "invoke")


def take(parts: list[bytes]) -> bytes:
    if not parts:
        raise ValueError("executor message is missing a part")
    return parts.pop(0)


def without(document: Document, *names: str) -> Document:
    return {name: value for name, value in document.items() if name not in names}


def encode(value: Any) -> bytes:
    return json.dumps(value, separators=(",", ":"), allow_nan=False).encode()
