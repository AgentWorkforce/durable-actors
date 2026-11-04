"""Native asyncio transport for actor invocations and connection grants."""

from __future__ import annotations

import json
import re
import time
import uuid
from collections.abc import Callable
from types import TracebackType
from typing import Any, Protocol, cast
from urllib.parse import urlsplit

import httpx

from .client import (
    NEVER_SENT,
    ActorInvocationError,
    ActorProtocolError,
    ClientConfiguration,
    SocketGrant,
    component,
    document,
    invocation_outcome,
    invocation_response,
)
from .guards import is_document


class AsyncRpcTransport(Protocol):
    async def invoke(self, actor_name: str, actor_id: str, method: str, args: list[Any]) -> Any: ...


class AsyncActorTransport(AsyncRpcTransport, Protocol):
    async def prepare_websocket(
        self, actor_name: str, actor_id: str, metadata: Any, *, home_region: str | None = None
    ) -> SocketGrant: ...
    async def broadcast(self, actor_name: str, actor_id: str, message: Any) -> None: ...


class AsyncClient(ClientConfiguration):
    """Async HTTP transport for actor RPCs, contracts, and socket grants.

    Use async with or aclose() to release owned connections. Inject a shared
    instance into generated handles for application-lifetime connection pooling.
    Cancelling a call stops waiting; it does not guarantee remote cancellation.
    """

    def __init__(
        self,
        control_plane_url: str | None = None,
        *,
        project_id: str | None = None,
        api_key: str | None = None,
        home_region: str | None = None,
        http: httpx.AsyncClient | None = None,
        telemetry: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        """Configure the same options as Client, with an optional caller-owned httpx.AsyncClient."""
        super().__init__(
            control_plane_url,
            project_id=project_id,
            api_key=api_key,
            home_region=home_region,
            telemetry=telemetry,
        )
        self._http = (
            http if http is not None else httpx.AsyncClient(timeout=180, follow_redirects=False)
        )
        self._owns_http = http is None

    async def __aenter__(self) -> AsyncClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        """Close this transport and its owned HTTP client; injected HTTP clients stay open."""
        self._closed = True
        if self._owns_http:
            await self._http.aclose()

    async def invoke(self, actor_name: str, actor_id: str, method: str, args: list[Any]) -> Any:
        """Await a typed RPC outcome. Only failures known to precede execution are retried."""
        request_id = str(uuid.uuid4())
        started = time.perf_counter()
        outcome = "failed"
        try:
            result = await self._invoke(actor_name, actor_id, method, args, request_id)
            outcome = "completed"
            return result
        finally:
            self._telemetry(
                {
                    "type": "actor_invocation",
                    "request_id": request_id,
                    "actor_name": actor_name,
                    "actor_id": actor_id,
                    "method": method,
                    "outcome": outcome,
                    "completed_at_ms": (time.perf_counter() - started) * 1000,
                }
            )

    async def prepare_websocket(
        self,
        actor_name: str,
        actor_id: str,
        metadata: Any,
        *,
        home_region: str | None = None,
    ) -> SocketGrant:
        """Authorize a WebSocket with JSON metadata, limited to 16 KiB."""
        self._ensure_open()
        placement = home_region if home_region is not None else self.home_region
        if placement is not None:
            component(placement, 255)
        if len(json.dumps(metadata, allow_nan=False).encode()) > 16384:
            raise ValueError("socket metadata exceeds 16 KiB")
        response = await self._http.post(
            self.origin + self.actor_path(actor_name, actor_id) + "/find-websocket",
            headers=self.headers,
            json={
                "metadata": metadata,
                **({"homeRegion": placement} if placement is not None else {}),
            },
            follow_redirects=False,
        )
        response.raise_for_status()
        grant = SocketGrant.model_validate(response.json(), strict=True)
        if urlsplit(grant.websocket_url).scheme not in {"ws", "wss"}:
            raise ActorProtocolError("invalid websocket URL")
        return grant

    async def broadcast(self, actor_name: str, actor_id: str, message: Any) -> None:
        """Broadcast without persistence. Uncertain delivery is never replayed."""
        self._ensure_open()
        path = self.actor_path(actor_name, actor_id)
        if is_document(message) and message.get("type") in {"state", "state_update"}:
            raise ValueError("state and state_update messages are reserved")
        data = json.dumps(message, separators=(",", ":"), allow_nan=False)
        request_id = str(uuid.uuid4())
        payload: dict[str, Any] = {
            "effects": [
                {
                    "type": "broadcast",
                    "message": {"type": "text", "data": data},
                    "except_connection_ids": [],
                    "tags": [],
                }
            ],
        }
        try:
            response = await self._http.post(
                self.origin + path + "/socket-effects",
                headers={**self.headers, "x-request-id": request_id},
                json=payload,
                follow_redirects=False,
            )
            response.raise_for_status()
        except httpx.HTTPError as error:
            if isinstance(error, httpx.HTTPStatusError) and error.response.status_code in {
                401,
                403,
            }:
                raise ActorInvocationError(
                    "unauthenticated", request_id, "application credential rejected"
                ) from error
            raise ActorInvocationError(
                "outcome_unknown", request_id, "broadcast delivery could not be confirmed"
            ) from error

    async def get_contract(self) -> dict[str, Any]:
        self._ensure_open()
        response = await self._http.get(
            f"{self.origin}/v1/projects/{self.project_id}/deployment/contract",
            headers=self.headers,
            follow_redirects=False,
        )
        response.raise_for_status()
        value = document(response)
        if not re.fullmatch(
            r"sha256:[a-f0-9]{64}", value.get("contractHash", "")
        ) or not isinstance(value.get("contract"), dict):
            raise ActorProtocolError("invalid contract publication")
        return cast(dict[str, Any], value["contract"])

    async def _invoke(
        self, actor_name: str, actor_id: str, method: str, args: list[Any], request_id: str
    ) -> Any:
        if self._closed:
            raise RuntimeError("client is closed")
        path = self.actor_path(actor_name, actor_id)
        component(method, 255)
        json.dumps(args, allow_nan=False)
        for attempt in range(2):
            try:
                reply = await self._invoke_attempt(path, request_id, method, args)
            except httpx.TransportError as error:
                if not isinstance(error, NEVER_SENT):
                    raise ActorInvocationError(
                        "outcome_unknown",
                        request_id,
                        "invocation response was lost; execution may have occurred",
                    ) from error
                raise ActorInvocationError(
                    "unavailable", request_id, f"could not connect to {self.origin}: {error}"
                ) from error
            completed, result = invocation_outcome(reply, request_id, attempt)
            if completed:
                return result
        raise AssertionError("unreachable")

    async def _invoke_attempt(
        self,
        path: str,
        request_id: str,
        method: str,
        args: list[Any],
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"requestId": request_id, "method": method, "args": args}
        if self.home_region:
            body["homeRegion"] = self.home_region
        response = await self._http.post(
            self.origin + path + "/invoke",
            headers={**self.headers, "x-request-id": request_id},
            json=body,
            follow_redirects=False,
        )
        return invocation_response(response, request_id)


class _DefaultTransport:
    async def invoke(self, actor_name: str, actor_id: str, method: str, args: list[Any]) -> Any:
        async with AsyncClient() as client:
            return await client.invoke(actor_name, actor_id, method, args)

    async def prepare_websocket(
        self, actor_name: str, actor_id: str, metadata: Any, *, home_region: str | None = None
    ) -> SocketGrant:
        async with AsyncClient() as client:
            return await client.prepare_websocket(
                actor_name, actor_id, metadata, home_region=home_region
            )

    async def broadcast(self, actor_name: str, actor_id: str, message: Any) -> None:
        async with AsyncClient() as client:
            await client.broadcast(actor_name, actor_id, message)


_default_transport: AsyncActorTransport | None = None


def default_async_client() -> AsyncActorTransport:
    """Use a request-scoped transport; inject AsyncClient to reuse a connection pool."""
    return _default_transport if _default_transport is not None else _DefaultTransport()


def set_default_async_client(transport: AsyncActorTransport) -> None:
    global _default_transport
    _default_transport = transport
