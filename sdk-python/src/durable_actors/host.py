from __future__ import annotations

import asyncio
import os
import signal
import socket
import sys
import traceback
from collections.abc import Collection
from contextlib import suppress
from contextvars import ContextVar
from pathlib import Path
from typing import Any, cast

from .build import load_artifact
from .client import component
from .contract import Document
from .executor_wire import Channel, serialize
from .host_transport import route_through_host
from .runtime import ActorRuntime
from .sqlite import DatabaseProxy

message_id: ContextVar[int] = ContextVar("actor_message_id")


class Session(Channel):
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        super().__init__(reader, writer)
        self.pending: dict[tuple[str, int], asyncio.Future[Any]] = {}
        self.tasks: set[asyncio.Task[None]] = set()
        self.residency: asyncio.Task[None] | None = None
        self.database_calls = asyncio.Lock()
        self.actor_calls: dict[int, asyncio.Future[Document]] = {}
        self.next_actor_call = 0

    async def run(self, entrypoint: str | None, generic: bool) -> None:
        try:
            if generic:
                await self.send({"type": "warm", "protocol": 31})
                assignment = await self.read()
                if (
                    assignment.get("type") != "load"
                    or not Path(assignment["entrypoint"]).is_absolute()
                ):
                    raise ValueError("invalid code assignment")
                entrypoint = assignment["entrypoint"]
            if not entrypoint:
                raise ValueError("DURABLE_ACTORS_ENTRYPOINT is required")
            path = Path(entrypoint)
            async with asyncio.timeout(60):
                while not path.is_file():
                    await asyncio.sleep(0.01)
            actors = {actor.__name__: actor for actor in load_artifact(path)}
            await self.send({"type": "attach", "protocol": 31, "actor_names": sorted(actors)})
            identity = attached_actor(await self.read(), actors)
            database = DatabaseProxy(self.call_database, asyncio.get_running_loop())
            runtime = ActorRuntime(actors[identity["actor_name"]], identity, self, database)
            route_through_host(self.call_actor, asyncio.get_running_loop())
            self.residency = asyncio.create_task(self.report_residency(runtime))
            while True:
                message = await self.read()
                if message["type"] == "command":
                    validate_command(message)
                    task = asyncio.create_task(self.execute(runtime, message))
                    self.tasks.add(task)
                    task.add_done_callback(self.completed)
                elif message["type"] == "actor_result":
                    call = self.actor_calls.pop(message["call_id"], None)
                    if call is not None and not call.done():
                        call.set_result(message["outcome"])
                elif message["type"] in {"socket_connections", "database_result"}:
                    pending = self.pending.pop((message["type"], reply_id(message)))
                    if pending.cancelled():
                        continue
                    if message.get("error"):
                        pending.set_exception(RuntimeError(message["error"]))
                    else:
                        pending.set_result(
                            message.get("result")
                            if message["type"] == "database_result"
                            else message.get("connections")
                        )
                else:
                    raise ValueError("unexpected executor message")
        finally:
            await self.close()

    async def execute(self, runtime: ActorRuntime, message: Document) -> None:
        token = message_id.set(message["message_id"])
        try:
            reply = await runtime.handle(message["command"])
            response = {"type": "reply", "message_id": message["message_id"], "reply": reply}
            await self.send(response)
        finally:
            message_id.reset(token)

    def completed(self, task: asyncio.Task[None]) -> None:
        self.tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            self.writer.close()

    async def call_database(self, call: Document) -> Document:
        # The host accepts one outstanding database call per invocation.
        async with self.database_calls:
            return cast(
                Document,
                await self.exchange(
                    "database_call", "database_result", call_id=message_id.get(), call=call
                ),
            )

    async def call_actor(self, call: Document) -> Document:
        self.next_actor_call += 1
        call_id = self.next_actor_call
        future: asyncio.Future[Document] = asyncio.get_running_loop().create_future()
        self.actor_calls[call_id] = future
        try:
            await self.send({"type": "actor_call", "call_id": call_id, "call": call})
            return await future
        finally:
            self.actor_calls.pop(call_id, None)

    def publish(self, effects: list[Document]) -> None:
        message = {"type": "socket_effects", "message_id": message_id.get(), "effects": effects}
        self.writer.write(serialize(message))

    async def get_connections(
        self, tag: str | None = None, count_only: bool = False
    ) -> list[Document] | int:
        return cast(
            list[Document] | int,
            await self.exchange(
                "get_connections", "socket_connections", query={"tag": tag, "countOnly": count_only}
            ),
        )

    def admit(self) -> None:
        data = serialize({"type": "ready_for_invocation", "message_id": message_id.get()})
        self.writer.write(data)

    async def exchange(self, kind: str, response: str, **fields: Any) -> Any:
        key = response, message_id.get()
        if key in self.pending:
            raise ValueError("duplicate executor request")
        future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        self.pending[key] = future
        try:
            await self.send({"type": kind, "message_id": key[1], **fields})
            return await future
        finally:
            # Canceled exchanges still receive acknowledgments from the host.
            if not future.cancelled():
                self.pending.pop(key, None)

    async def report_residency(self, runtime: ActorRuntime) -> None:
        while True:
            await self.send({"type": "residency", "resident": runtime.instance is not None})
            await asyncio.sleep(1)

    async def close(self) -> None:
        if self.residency:
            self.residency.cancel()
        for future in [*self.pending.values(), *self.actor_calls.values()]:
            if not future.done():
                future.cancel()
        tasks = list(self.tasks)
        for task in tasks:
            task.cancel()
        self.writer.close()
        await self.writer.wait_closed()


def reply_id(message: Document) -> int:
    # Python reuses each command's message ID as the ID of its database call.
    return cast(int, message["call_id" if message["type"] == "database_result" else "message_id"])


def attached_actor(message: Document, actor_names: Collection[str]) -> Document:
    if message.get("type") != "attached" or message.get("protocol") != 31:
        raise ValueError("unsupported executor protocol")
    actor = cast(Document, message["actor"])
    for field, size in (("project_id", 64), ("actor_name", 255), ("actor_id", 128)):
        component(actor[field], size)
    if actor["actor_name"] not in actor_names:
        raise ValueError("attached actor is not loaded")
    return actor


def validate_command(message: Document) -> None:
    if type(message.get("message_id")) is not int or message["message_id"] < 0:
        raise ValueError("invalid executor message ID")
    command = message["command"]
    if command["type"] not in {"invoke", "hydrate", "evict", "websocket_event"}:
        raise ValueError("unsupported executor command")
    if command["type"] == "invoke":
        component(command["method"], 255)
        if not isinstance(command["args"], list):
            raise ValueError("RPC arguments must be an array")


async def main() -> None:
    if "--worker" in sys.argv:
        channel = socket.socket(fileno=int(sys.argv[sys.argv.index("--worker") + 1]))
        reader, writer = await asyncio.open_connection(sock=channel, limit=sys.maxsize)
        status = 0
        try:
            await Session(reader, writer).run(os.environ.get("DURABLE_ACTORS_ENTRYPOINT"), False)
        except EOFError:
            pass
        except BaseException:
            traceback.print_exc()
            status = 1
        finally:
            # Only the child enters this branch; its supervisor has disconnected.
            os._exit(status)
    from .supervisor import Supervisor, Worker

    task = asyncio.current_task()
    assert task is not None
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, task.cancel)
    reader, writer = await asyncio.open_unix_connection(
        os.environ["DURABLE_ACTORS_EXECUTOR_SOCKET"], limit=sys.maxsize
    )
    with suppress(asyncio.CancelledError, EOFError):
        await Supervisor(reader, writer, Worker.start).run(
            os.environ.get("DURABLE_ACTORS_ENTRYPOINT"), "--generic" in sys.argv
        )


if __name__ == "__main__":
    asyncio.run(main())
