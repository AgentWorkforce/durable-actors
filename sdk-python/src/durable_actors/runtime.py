from __future__ import annotations

import asyncio
import inspect
import json
import traceback
from collections import OrderedDict
from collections.abc import Callable
from functools import partial
from typing import Any

from .actor_base import ActorBase
from .contract import Document, Method, decode, describe_actor, encode
from .database import RuntimeActorContext, actor_database_invocation
from .socket import Effects, SocketScope, scope_context
from .sqlite import DatabaseCommitError, DatabaseProxy
from .threads import run_in_thread


class ActorRuntime:
    def __init__(
        self,
        actor: type[ActorBase[Any, Any, Any, Any]],
        identity: Document,
        effects: Effects,
        database: DatabaseProxy,
    ) -> None:
        self.identity = identity
        self.effects = effects
        self.database = database
        self.completion = asyncio.Lock()
        self.fatal: Document | None = None
        self.definition = describe_actor(actor)
        self.instance: ActorBase[Any, Any, Any, Any] | None = None
        self.sequence = 0
        self.last_state: Document = {}
        # The fields the database holds, so a commit sends only what changed.
        self.stored: Document = {}

    async def handle(self, command: Document) -> Document:
        if command["type"] == "evict":
            return self.evict()
        name = command.get("method") or "on_" + command.get("event", {}).get("type", "")
        if name == "onAlarm":
            name = "on_alarm"

        def admit() -> None:
            if name in self.definition.reentrant_methods:
                self.effects.admit()

        try:
            if self.fatal is not None:
                return self.fatal
            return await self.execute(command, admit)
        except Exception as error:
            traceback.print_exc()
            self.instance = None
            failure = failed("invalid_actor_state", str(error))
            if self.definition.reentrant_methods:
                self.fatal = failure
            return failure

    def evict(self) -> Document:
        self.instance = None
        self.fatal = None
        return {"type": "evicted"}

    async def execute(self, command: Document, admit: Any) -> Document:
        if self.instance is None:
            try:
                stored = await self.database.fields()
                self.restore(stored)
                self.stored = stored
            except Exception as error:
                traceback.print_exc()
                return failed("invalid_actor_state", str(error))
        if command["type"] == "hydrate":
            return {"type": "hydrated"}
        return await self.invoke(command, admit)

    async def invoke(self, command: Document, admit: Any) -> Document:
        instance = self.instance
        assert instance is not None
        socket_event = command["type"] == "websocket_event"
        name = "on_" + command["event"]["type"] if socket_event else command["method"]
        alarm_event = not socket_event and name == "onAlarm"
        if alarm_event:
            name = "on_alarm"
        if not socket_event and not alarm_event and name not in self.definition.methods:
            return failed("method_not_found", name)
        before = self.snapshot()
        connecting = socket_event and command["event"]["type"] == "connect"
        scope = SocketScope(
            instance,
            self.identity["actor_id"],
            self.definition.socket_types,
            self.effects,
            command.get("connections"),
            not connecting,
        )
        token = scope_context.set(scope)
        try:
            if not self.definition.reentrant_methods:
                await self.database.begin()
            if socket_event:
                args = await socket_arguments(command["event"], scope)
                admit()
                if hasattr(instance, name):
                    await invoke_handler(scope, getattr(instance, name), *args)
                result = None
            elif alarm_event:
                admit()
                await invoke_handler(scope, getattr(instance, name))
                result = None
            else:
                method = self.definition.methods[name]
                arguments = bind_arguments(method, command["args"])
                admit()
                value = await invoke_handler(
                    scope, getattr(instance, name), *arguments.args, **arguments.kwargs
                )
                result = encode(method.result, value)
            task = asyncio.current_task()
            if task is not None and task.cancelling():
                raise asyncio.CancelledError
            effects = scope.finish()
            async with self.completion:
                if self.fatal is not None:
                    return self.fatal
                state = self.snapshot()
                previous = self.last_state if self.definition.reentrant_methods else before
                effects.extend(self.state_updates(previous, state, command))
                if self.definition.reentrant_methods:
                    await self.database.begin()
                try:
                    await self.database.update_fields(self.stored, state)
                    await self.database.commit()
                except DatabaseCommitError as error:
                    traceback.print_exc()
                    self.fatal = failed("actor_database_failed", str(error))
                    return self.fatal
                except BaseException:
                    await self.database.rollback()
                    raise
                self.last_state = state
                self.stored = state
                reply: Document = {"type": "websocket_handled" if socket_event else "invoked"}
                if not socket_event:
                    reply["result"] = result
                if socket_event or effects:
                    reply["effects"] = effects
                if self.definition.reentrant_methods:
                    self.sequence += 1
                    reply["sequence"] = self.sequence
                return reply
        except asyncio.CancelledError:
            if not self.definition.reentrant_methods:
                self.instance = None
                await self.database.rollback()
            raise
        except Exception as error:
            traceback.print_exc()
            if not self.definition.reentrant_methods:
                await self.database.rollback()
                self.restore(before)
            return failed(
                "actor_socket_failed" if socket_event else "actor_method_failed", str(error)
            )
        finally:
            scope.active = False
            scope_context.reset(token)

    def state_updates(self, before: Document, after: Document, command: Document) -> list[Document]:
        fields = self.definition.fields
        changed = {
            name: value
            for name, value in after.items()
            if fields[name].emittable and before.get(name) != value
        }
        effects: list[Document] = []
        event = command.get("event", {})
        connecting = event.get("type") == "connect"
        if changed:
            effect: Document = {"type": "state_update", "changes": changed, "removed": []}
            if connecting:
                effect["except_connection_ids"] = [event["connection"]["id"]]
            effects.append(effect)
        if connecting:
            scope = scope_context.get()
            socket = scope.socket(event["connection"], "connecting")
            if socket.state != "closed" and any(field.emittable for field in fields.values()):
                effects.append(
                    {
                        "type": "state_snapshot",
                        "connection_id": socket.id,
                        "state": {
                            name: value for name, value in after.items() if fields[name].emittable
                        },
                    }
                )
        return effects

    def restore(self, state: Any) -> None:
        instance = self.definition.actor(RuntimeActorContext(self.database))
        if state is not None:
            if not isinstance(state, dict):
                raise ValueError("persisted state must be an object")
            for name, field in self.definition.fields.items():
                if field.persisted and name in state:
                    setattr(instance, name, decode(field.adapter, state[name]))
        self.instance = instance
        self.last_state = self.snapshot()

    def snapshot(self) -> Document:
        assert self.instance is not None
        unknown = set(vars(self.instance)) - self.definition.fields.keys()
        if unknown:
            raise ValueError(f"undeclared actor fields: {sorted(unknown)}")
        return {
            name: encode(field.adapter, getattr(self.instance, name))
            for name, field in self.definition.fields.items()
            if field.persisted
        }


async def invoke_handler(
    scope: SocketScope, handler: Callable[..., Any], *args: Any, **kwargs: Any
) -> Any:
    with actor_database_invocation(getattr(handler, "__self__", None)):
        if inspect.iscoroutinefunction(handler):
            try:
                return await handler(*args, **kwargs)
            except asyncio.CancelledError:
                scope.cancel()
                raise
        return await run_in_thread(partial(handler, *args, **kwargs), scope.cancel)


def bind_arguments(method: Method, values: list[Any]) -> inspect.BoundArguments:

    remaining = list(values)
    arguments: dict[str, Any] = {}
    for name, parameter in method.signature.parameters.items():
        if parameter.kind is inspect.Parameter.VAR_POSITIONAL:
            arguments[name] = tuple(decode(method.parameters[name], remaining))
            remaining = []
        elif remaining:
            arguments[name] = decode(method.parameters[name], remaining.pop(0))
        elif parameter.default is inspect.Parameter.empty:
            raise ValueError(f"missing required argument: {name}")
    if remaining:
        raise ValueError("too many RPC arguments")
    bound = inspect.BoundArguments(method.signature, OrderedDict(arguments))
    bound.apply_defaults()
    return bound


def failed(code: str, message: str) -> Document:
    return {"type": "failed", "code": code, "message": message}


async def socket_arguments(event: Document, scope: SocketScope) -> list[Any]:
    kind = event["type"]
    if kind == "connect":
        return [scope.socket(event["connection"], "connecting")]
    if kind == "disconnect":
        return [
            scope.socket(event["connection"], "closed"),
            event["code"],
            event["reason"],
            event["was_clean"],
        ]
    socket = scope.sockets.get(event["connection_id"])
    if socket is None or event["message"]["type"] != "text":
        raise ValueError("socket messages require an active connection and JSON text")
    return [socket, decode(scope.incoming, json.loads(event["message"]["data"]))]
