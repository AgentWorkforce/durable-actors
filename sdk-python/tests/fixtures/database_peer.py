"""Plays the Rust host's end of a database channel."""

import io
import json
import struct

COLUMNS, ROW, RESULT, FAILED = 0x10, 0x11, 0x12, 0x13


class DatabasePeer:
    """A channel's stream whose other end answers on the thread that asks."""

    def __init__(self, database):
        self.database = database
        self.requests = []
        self.asked = bytearray()
        self.answer = io.BytesIO()

    def writelines(self, chunks):
        for chunk in chunks:
            self.asked += chunk

    def flush(self):
        request, _ = take_request(self.asked)
        self.asked.clear()
        self.requests.append(request)
        self.answer = io.BytesIO(answer_to(request, lambda invocation: self.database))

    def read(self, size):
        return self.answer.read(size)


async def serve_database(reader, writer, resolve):
    """Answers the calls a worker process makes on one connection, until it closes.

    `resolve` gives the database that the command a call names reaches, or raises to refuse it.
    """
    asked = bytearray()
    try:
        while chunk := await reader.read(1 << 16):
            asked += chunk
            while taken := take_request(asked):
                request, length = taken
                del asked[:length]
                writer.write(answer_to(request, resolve))
    finally:
        writer.close()


def answer_to(request, resolve):
    try:
        result = resolve(request["invocation"])(request["call"])
    except Exception as error:
        return ending(FAILED, str(error).encode())
    if result["type"] == "fields":
        result = stored_rows(result["fields"])
    if result["type"] != "rows":
        return ending(RESULT, json.dumps(result).encode())
    names = [sized(name.encode()) for name in result["columns"]]
    rows = [bytes([ROW]) + b"".join(map(value, row)) for row in result["rows"]]
    return b"".join(
        [
            struct.pack("<BI", COLUMNS, len(names)),
            *names,
            *rows,
            ending(RESULT, b'{"type":"rows"}'),
        ]
    )


def stored_rows(fields):
    """The host answers a read of the fields with a row of each one's name and its JSON."""
    rows = [[name, json.dumps(value, ensure_ascii=False)] for name, value in fields.items()]
    return {"type": "rows", "columns": ["name", "value"], "rows": rows}


def ending(record, body):
    return bytes([record]) + sized(body)


def value(value):
    if value is None:
        return b"\x00"
    if isinstance(value, int):
        return struct.pack("<Bq", 1, value)
    if isinstance(value, float):
        return struct.pack("<Bd", 2, value)
    if isinstance(value, str):
        return b"\x03" + sized(value.encode())
    return b"\x04" + sized(value)


def sized(body):
    return struct.pack("<I", len(body)) + body


def take_request(asked):
    """The first request in `asked` and its length, or None while part of it is yet to arrive."""
    cursor = Cursor(asked)
    try:
        head = json.loads(cursor.sized())
        bindings = [cursor.value() for _ in range(head["bindings"])]
    except Incomplete:
        return None
    call = head["call"]
    if call["op"] == "exec":
        call["bindings"] = bindings
    if call["op"] == "persist_field":
        call["value"] = json.loads(*bindings)
    return {"invocation": head.get("message_id"), "call": call}, cursor.at


class Incomplete(Exception):
    pass


class Cursor:
    def __init__(self, data):
        self.data = data
        self.at = 0

    def value(self):
        kind = self.take(1)[0]
        if kind == 0:
            return None
        if kind == 1:
            return struct.unpack("<q", self.take(8))[0]
        if kind == 2:
            return struct.unpack("<d", self.take(8))[0]
        return self.sized().decode() if kind == 3 else self.sized()

    def sized(self):
        return self.take(struct.unpack("<I", self.take(4))[0])

    def take(self, size):
        if self.at + size > len(self.data):
            raise Incomplete
        self.at += size
        return bytes(self.data[self.at - size : self.at])
