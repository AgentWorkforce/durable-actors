"""Serve the prebuilt observer UI and its authenticated, read-only API proxy."""

from __future__ import annotations

import asyncio
import socket
import webbrowser
from collections.abc import AsyncIterator
from importlib.resources import files
from pathlib import Path

import anyio
import httpx
import uvicorn
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Mount, Route
from starlette.staticfiles import StaticFiles
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from .client import Client

RESOURCES = {
    "actors",
    "residency",
    "events",
    "events/history",
    "state",
    "state/history",
    "requests",
    "metrics",
    "queue-waits",
    "websockets",
    "connection",
}
SECURITY_HEADERS = {
    "cache-control": "no-store",
    "x-content-type-options": "nosniff",
    "content-security-policy": "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'",
}


class LocalRequests:
    def __init__(self, app: ASGIApp, host: str) -> None:
        self.app, self.host = app, host

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        request = Request(scope)
        if (
            request.headers.get("host") != self.host
            or request.headers.get("origin", f"http://{self.host}") != f"http://{self.host}"
            or request.headers.get("sec-fetch-site") == "cross-site"
        ):
            await Response(status_code=403)(scope, receive, send)
            return
        if request.method not in {"GET", "HEAD"}:
            await Response(status_code=405, headers={"allow": "GET, HEAD"})(scope, receive, send)
            return

        async def secure_send(message: Message) -> None:
            if message["type"] == "http.response.start":
                message["headers"] = [
                    *message.get("headers", []),
                    *[(k.encode(), v.encode()) for k, v in SECURITY_HEADERS.items()],
                ]
            await send(message)

        await self.app(scope, receive, secure_send)


def observer_app(client: Client, assets: Path, host: str) -> Starlette:
    async def proxy(request: Request) -> Response:
        resource = str(request.path_params["resource"])
        if resource not in RESOURCES:
            return Response(status_code=404)
        upstream_resource = "actors" if resource == "connection" else resource
        url = f"{client.origin}/v1/projects/{client.project_id}/observe/{upstream_resource}"
        if request.url.query:
            url += f"?{request.url.query}"
        streaming = resource == "events"
        if streaming and request.method == "HEAD":
            return Response(media_type="text/event-stream")
        upstream = httpx.AsyncClient(
            timeout=httpx.Timeout(30, read=60 if streaming else 30), follow_redirects=False
        )
        try:
            response = await upstream.send(
                upstream.build_request("GET", url, headers=client.headers), stream=streaming
            )
            response.raise_for_status()
        except httpx.HTTPError:
            await upstream.aclose()
            return JSONResponse({"error": "Control plane unavailable"}, status_code=503)
        if streaming:

            async def events() -> AsyncIterator[bytes]:
                try:
                    async for chunk in response.aiter_bytes():
                        yield chunk
                except httpx.HTTPError:
                    yield b"event: error\ndata: Inventory unavailable\n\n"
                finally:
                    with anyio.CancelScope(shield=True):
                        await response.aclose()
                        await upstream.aclose()

            return StreamingResponse(
                events(), media_type="text/event-stream", headers={"x-accel-buffering": "no"}
            )
        try:
            content = b'{"connected":true}' if resource == "connection" else response.content
            return Response(
                content if request.method != "HEAD" else b"", media_type="application/json"
            )
        finally:
            await upstream.aclose()

    return Starlette(
        routes=[
            Route("/api/observe/{resource:path}", proxy, methods=["GET", "HEAD"]),
            Mount("/", StaticFiles(directory=assets, html=True)),
        ],
        middleware=[Middleware(LocalRequests, host=host)],
    )


def observe(origin: str | None, *, open_browser: bool) -> None:
    assets = Path(str(files("durable_actors_runtime").joinpath("observer")))
    if not (assets / "index.html").is_file():
        raise RuntimeError("Observer assets are missing. Reinstall durable-actors[cli].")
    with Client(origin) as client:
        # Verify credentials before exposing the local UI.
        with httpx.Client(timeout=30, follow_redirects=False) as http:
            response = http.get(
                f"{client.origin}/v1/projects/{client.project_id}/observe/actors",
                headers=client.headers,
            )
            response.raise_for_status()
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            host = f"127.0.0.1:{listener.getsockname()[1]}"
            url = f"http://{host}"

            class ObserverServer(uvicorn.Server):
                async def startup(self, sockets: list[socket.socket] | None = None) -> None:
                    await super().startup(sockets)
                    if not self.started:
                        return
                    print(
                        f"Connected to the control plane.\nObserve: {url}\nPress Ctrl+C to stop.",
                        flush=True,
                    )
                    if open_browser:
                        await asyncio.to_thread(webbrowser.open, url)

            server = ObserverServer(
                uvicorn.Config(
                    observer_app(client, assets, host), log_level="warning", lifespan="off"
                )
            )
            server.run(sockets=[listener])
