"""Actor RPCs that the Rust host forwards within the calling actor's project."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

from .async_client import AsyncActorTransport, default_async_client, set_default_async_client
from .client import (
    ActorTransport,
    SocketGrant,
    component,
    invocation_outcome,
    set_default_client,
    shared_client,
)
from .contract import Document

ActorCaller = Callable[[Document], Awaitable[Document]]


def route_through_host(call: ActorCaller, loop: asyncio.AbstractEventLoop) -> None:
    """Make default actor references in this process call through the Rust host."""
    transport = AsyncHostTransport(call, default_async_client())
    set_default_async_client(transport)
    set_default_client(HostTransport(transport, loop, shared_client))


class AsyncHostTransport:
    def __init__(self, call: ActorCaller, remote: AsyncActorTransport) -> None:
        self.call = call
        self.remote = remote

    async def invoke(self, actor_name: str, actor_id: str, method: str, args: list[Any]) -> Any:
        request_id = str(uuid.uuid4())
        outcome = await self.call(
            {
                "request_id": request_id,
                "actor_name": component(actor_name, 255),
                "actor_id": component(actor_id, 128),
                "method": method,
                "args": args,
            }
        )
        return invocation_outcome(outcome, request_id, 1)[1]

    async def prepare_websocket(
        self, actor_name: str, actor_id: str, metadata: Any, *, home_region: str | None = None
    ) -> SocketGrant:
        return await self.remote.prepare_websocket(
            actor_name, actor_id, metadata, home_region=home_region
        )

    async def broadcast(self, actor_name: str, actor_id: str, message: Any) -> None:
        await self.remote.broadcast(actor_name, actor_id, message)


class HostTransport:
    """Blocks a synchronous actor's thread while the event loop forwards its call."""

    def __init__(
        self,
        transport: AsyncHostTransport,
        loop: asyncio.AbstractEventLoop,
        remote: Callable[[], ActorTransport],
    ) -> None:
        self.transport = transport
        self.loop = loop
        self.remote = remote

    def invoke(self, actor_name: str, actor_id: str, method: str, args: list[Any]) -> Any:
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is self.loop:
            # Waiting here would block the loop that forwards the call.
            raise RuntimeError("async actor code must call actors through async references")
        call = self.transport.invoke(actor_name, actor_id, method, args)
        return asyncio.run_coroutine_threadsafe(call, self.loop).result()

    def prepare_websocket(
        self, actor_name: str, actor_id: str, metadata: Any, *, home_region: str | None = None
    ) -> SocketGrant:
        return self.remote().prepare_websocket(
            actor_name, actor_id, metadata, home_region=home_region
        )

    def broadcast(self, actor_name: str, actor_id: str, message: Any) -> None:
        self.remote().broadcast(actor_name, actor_id, message)
