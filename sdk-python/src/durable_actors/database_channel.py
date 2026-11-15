"""The connection an actor's database calls travel on, apart from its commands.

One call is answered at a time, and the thread that makes it waits for the answer. A request is
a ``u32`` size, that much JSON, then the values the JSON counts under ``bindings``: the values a
statement binds, or the JSON of the field a call writes. The answer is ``COLUMNS`` and a ``ROW``
per row while a statement runs or fields are read, then ``RESULT`` or ``FAILED``. Integers are
little-endian, and a value is a tag followed by its eight bytes, or by a ``u32`` size and that
many bytes.
"""

from __future__ import annotations

import json
import socket
import struct
import threading
from typing import Any, BinaryIO, cast

from .contract import Document
from .database import SqliteValue
from .executor_wire import encode
from .guards import is_document

NULL, INTEGER, REAL, TEXT, BLOB = range(5)
COLUMNS, ROW, RESULT, FAILED = 0x10, 0x11, 0x12, 0x13

# SQLite's own limit on the values one statement binds. The Rust host ends the channel beyond it.
MAX_BINDINGS = 32_766
MAX_SIZE = 0xFFFF_FFFF
# The parts of a call that follow its JSON instead of being inside it.
CARRIED = ("bindings", "value")


class DatabaseChannelError(RuntimeError):
    """The channel is unusable: an exchange stopped part way, so the next would be misread."""


class DatabaseChannel:
    def __init__(self, stream: BinaryIO) -> None:
        self.stream = stream
        self.exchange = threading.Lock()
        self.failure: DatabaseChannelError | None = None

    @classmethod
    def connect(cls, path: str) -> DatabaseChannel:
        connection = socket.socket(socket.AF_UNIX)
        connection.connect(path)
        return cls(cast(BinaryIO, connection.makefile("rwb")))

    def call(self, call: Document, invocation: int | None = None) -> Document:
        """Holds the calling thread until the Rust host has answered.

        ``invocation`` names the command whose database the call reaches. Without it, the
        activation's.
        """
        # A call that cannot be sent is refused before anything is written, which leaves the
        # channel usable.
        request = encode_request(call, invocation)
        with self.exchange:
            if self.failure is not None:
                raise self.failure
            try:
                self.stream.writelines(request)
                self.stream.flush()
                answer = self.answer(call["op"] == "fields")
            except Exception as error:
                self.failure = DatabaseChannelError(f"actor database channel failed: {error}")
                raise self.failure from error
        if answer["type"] == "failed":
            raise RuntimeError(answer["message"])
        return answer

    # Rows written before a statement fails are dropped with it.
    def answer(self, fields: bool) -> Document:
        columns: list[str] = []
        rows: list[list[SqliteValue]] = []
        stored: Document = {}
        while True:
            record = self.take(1)[0]
            if record == COLUMNS:
                columns = [self.text() for _ in range(self.u32())]
            elif record == ROW and fields:
                name, value = self.field()
                stored[name] = value
            elif record == ROW:
                rows.append([self.value() for _ in columns])
            elif record == RESULT:
                document = result(self.sized())
                if document.get("type") != "rows":
                    return document
                if fields:
                    return {"type": "fields", "fields": stored}
                return {"type": "rows", "columns": columns, "rows": rows}
            elif record == FAILED:
                return {"type": "failed", "message": self.text()}
            else:
                raise ValueError(f"unknown record {record}")

    def field(self) -> tuple[str, Any]:
        """A stored field's row: its name, then its JSON."""
        name, value = self.stored_text(), self.stored_text()
        return name.decode(), json.loads(value)

    def stored_text(self) -> bytes:
        if self.take(1)[0] != TEXT:
            raise ValueError("a field is its name and its JSON")
        return self.sized()

    def value(self) -> SqliteValue:
        kind = self.take(1)[0]
        if kind == NULL:
            return None
        if kind == INTEGER:
            return int.from_bytes(self.take(8), "little", signed=True)
        if kind == REAL:
            return float(struct.unpack("<d", self.take(8))[0])
        if kind == TEXT:
            return self.text()
        if kind == BLOB:
            return self.sized()
        raise ValueError(f"unknown value type {kind}")

    def text(self) -> str:
        return self.sized().decode(errors="replace")

    def sized(self) -> bytes:
        return self.take(self.u32())

    def u32(self) -> int:
        return int.from_bytes(self.take(4), "little")

    def take(self, size: int) -> bytes:
        data = self.stream.read(size)
        if len(data) != size:
            raise EOFError("the stream closed inside a value")
        return data


def encode_request(call: Document, invocation: int | None) -> list[bytes]:
    values = carried(call)
    head: Document = {
        "call": {name: value for name, value in call.items() if name not in CARRIED},
        "bindings": len(values),
    }
    if invocation is not None:
        head["message_id"] = invocation
    return [*sized(encode(head)), *(chunk for value in values for chunk in value)]


def carried(call: Document) -> list[list[bytes]]:
    """The values that follow a call: what a statement binds, or the JSON of the field it writes."""
    if call["op"] == "persist_field":
        return [[bytes([TEXT]), *sized(encode_field(call["value"]))]]
    bindings: list[object] = call["bindings"] if call["op"] == "exec" else []
    if len(bindings) > MAX_BINDINGS:
        raise ValueError(f"a statement binds at most {MAX_BINDINGS} values, not {len(bindings)}")
    return [encode_value(value, position) for position, value in enumerate(bindings, 1)]


# The host stores this text as it is sent, so characters outside ASCII are written as themselves.
def encode_field(value: object) -> bytes:
    return json.dumps(value, separators=(",", ":"), allow_nan=False, ensure_ascii=False).encode()


def encode_value(value: object, position: int) -> list[bytes]:
    if value is None:
        return [bytes([NULL])]
    if isinstance(value, int):
        if not -(2**63) <= value < 2**63:
            raise OverflowError(f"binding {position} does not fit a 64-bit SQLite integer")
        return [struct.pack("<Bq", INTEGER, value)]
    if isinstance(value, float):
        return [struct.pack("<Bd", REAL, value)]
    if isinstance(value, str):
        return [bytes([TEXT]), *sized(value.encode())]
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, (bytes, bytearray)):
        return [bytes([BLOB]), *sized(bytes(value))]
    raise TypeError(
        f"binding {position} is {type(value).__name__}; "
        "SQLite binds None, int, float, str and bytes"
    )


def sized(body: bytes) -> list[bytes]:
    if len(body) > MAX_SIZE:
        raise OverflowError("a database value is larger than 4 GiB")
    return [struct.pack("<I", len(body)), body]


def result(body: bytes) -> Document:
    document = json.loads(body)
    if not is_document(document):
        raise ValueError("database results must be objects")
    return document
