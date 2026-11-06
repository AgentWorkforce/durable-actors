from __future__ import annotations

import asyncio
import base64
from collections.abc import Callable, Coroutine
from typing import Any, cast

from .contract import Document
from .database import SqliteValue

DatabaseTransport = Callable[[Document], Coroutine[Any, Any, Document]]


class DatabaseCommitError(RuntimeError):
    pass


class DatabaseProxy:
    """Forwards an actor's SQLite work to the Rust host that owns the connection."""

    def __init__(self, call: DatabaseTransport, loop: asyncio.AbstractEventLoop) -> None:
        self.call = call
        self.loop = loop

    def exec(self, sql: str, *bindings: SqliteValue) -> list[dict[str, SqliteValue]]:
        if type(sql) is not str:
            raise ValueError("SQL must be a string")
        result = expect(
            self.request(
                {
                    "op": "exec",
                    "sql": sql,
                    "bindings": [encode(value) for value in bindings],
                    "immediate": False,
                }
            ),
            "rows",
        )
        columns = result["columns"]
        return [
            dict(zip(columns, (decode(value) for value in row), strict=True))
            for row in result["rows"]
        ]

    def get_alarm(self) -> int | None:
        result = expect(self.request({"op": "alarm"}), "alarm")
        timestamp_ms = result["timestamp_ms"]
        if timestamp_ms is not None and type(timestamp_ms) is not int:
            raise ValueError("invalid actor alarm timestamp")
        return timestamp_ms

    def set_alarm(self, timestamp_ms: int) -> None:
        expect(
            self.request({"op": "set_alarm", "timestamp_ms": timestamp_ms, "immediate": False}),
            "done",
        )

    def delete_alarm(self) -> None:
        expect(self.request({"op": "delete_alarm", "immediate": False}), "done")

    async def fields(self) -> Document:
        return cast(Document, expect(await self.call({"op": "fields"}), "fields")["fields"])

    async def update_fields(self, stored: Document, fields: Document) -> None:
        """Writes the fields that differ from `stored` and deletes the ones `fields` dropped."""
        for name, value in fields.items():
            if name not in stored or stored[name] != value:
                await self.call(
                    {"op": "persist_field", "name": name, "value": value, "immediate": False}
                )
        for name in stored.keys() - fields.keys():
            await self.call({"op": "delete_field", "name": name, "immediate": False})

    async def begin(self) -> None:
        await self.call({"op": "exec", "sql": "BEGIN", "bindings": [], "immediate": False})

    async def commit(self) -> None:
        try:
            await self.call({"op": "commit"})
        except Exception as error:
            raise DatabaseCommitError(
                f"failed to replicate actor SQLite commit: {error}"
            ) from error

    async def rollback(self) -> None:
        await self.call({"op": "rollback"})

    def request(self, call: Document) -> Document:
        if running_loop() is self.loop:
            raise RuntimeError(
                "synchronous actor database calls would block the event loop; "
                "use AsyncActor.db from coroutines"
            )
        return asyncio.run_coroutine_threadsafe(self.call(call), self.loop).result()


def running_loop() -> asyncio.AbstractEventLoop | None:
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        return None


def expect(result: Document, kind: str) -> Document:
    if result.get("type") != kind:
        raise ValueError(f"unexpected actor database result {result.get('type')}")
    return result


def encode(value: SqliteValue) -> Document:
    if value is None:
        return {"type": "null"}
    if isinstance(value, int):
        return {"type": "integer", "value": str(int(value))}
    if isinstance(value, float):
        return {"type": "real", "value": value}
    if isinstance(value, str):
        return {"type": "text", "value": value}
    return {"type": "blob", "value": base64.b64encode(value).decode()}


def decode(value: Any) -> SqliteValue:
    kind = value["type"]
    if kind == "null":
        return None
    if kind == "integer":
        return int(value["value"])
    if kind == "blob":
        return base64.b64decode(value["value"], validate=True)
    if kind in {"real", "text"}:
        return cast(SqliteValue, value["value"])
    raise ValueError(f"unexpected SQLite value type {kind}")
