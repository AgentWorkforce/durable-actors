import asyncio
import gc
import weakref
from threading import Event

import pytest
from fixtures.effects import Effects
from fixtures.host_database import HostDatabase
from fixtures.runtime import ActorRuntime
from pydantic import BaseModel

from durable_actors import Actor, AsyncActor, ephemeral, interleave, persisted


class Value(BaseModel):
    count: int


class Counter(Actor):
    value: Value = persisted(Value(count=0))
    calls: int = ephemeral(0)

    def increment(self, amount: int = 1) -> Value:
        self.value.count += amount
        self.calls += 1
        return self.value

    def fail(self) -> None:
        self.value.count = 99
        raise ValueError("failed")


async def test_socket_handlers_load_connections_only_on_demand():
    from durable_actors import ActorSocket

    class LookupEffects(Effects):
        lookups = 0

        async def get_connections(self, tag=None, count_only=False):
            self.lookups += 1
            return await super().get_connections(tag, count_only)

    class Room(Actor[None, bool, int]):
        def on_message(self, socket: ActorSocket[None, int], enumerate: bool) -> None:
            socket.send(len(self.get_connections()) if enumerate else 0)

    effects = LookupEffects()
    effects.connections = [
        {"id": name, "metadata": None, "tags": []} for name in ["sender", "other"]
    ]
    runtime = ActorRuntime(Room, effects)
    for enumerate in [False, True]:
        reply = await runtime.handle(
            {
                "type": "websocket_event",
                "request_id": "lookup",
                "connections": effects.connections[:1],
                "event": {
                    "type": "message",
                    "connection_id": "sender",
                    "message": {"type": "text", "data": "true" if enumerate else "false"},
                },
            }
        )
        assert reply["type"] == "websocket_handled"
        assert effects.published[-1]["message"]["data"] == ("2" if enumerate else "0")
        assert effects.lookups == int(enumerate)


def command(method="increment", args=None, **extra):
    return {
        "type": "invoke",
        "request_id": "r1",
        "method": method,
        "args": args or [],
        **extra,
    }


async def test_hydrates_validates_and_snapshots_typed_state():
    runtime = ActorRuntime(Counter, Effects(), HostDatabase.seeded({"value": {"count": 3}}))
    reply = await runtime.handle(command(args=[2]))
    assert reply["result"] == {"count": 5}
    assert runtime.host.fields() == {"value": {"count": 5}}
    assert (await runtime.handle(command()))["result"] == {"count": 6}


async def test_failure_rolls_back_persisted_state():
    runtime = ActorRuntime(Counter, Effects())
    await runtime.handle(command())
    failure = await runtime.handle(command("fail"))
    assert failure["type"] == "failed"
    assert (await runtime.handle(command()))["result"] == {"count": 2}


async def test_alarm_lifecycle_uses_the_normal_invocation_path():
    class Reminder(Actor):
        fired: bool = persisted(False)

        def schedule(self, timestamp_ms: int) -> int | None:
            self.alarm.set(timestamp_ms)
            return self.alarm.get()

        def on_alarm(self) -> None:
            self.alarm.delete()
            self.fired = True

        def state(self) -> tuple[bool, int | None]:
            return self.fired, self.alarm.get()

    runtime = ActorRuntime(Reminder, Effects())
    assert (await runtime.handle(command("schedule", [1_000])))["result"] == 1_000

    delivered = await runtime.handle(command("onAlarm"))
    assert delivered == {"type": "invoked", "result": None}
    assert (await runtime.handle(command("state")))["result"] == [True, None]


async def test_async_actors_can_schedule_and_handle_alarms():
    class Reminder(AsyncActor):
        fired: bool = persisted(False)

        async def schedule(self, timestamp_ms: int) -> None:
            await self.alarm.set(timestamp_ms)

        async def on_alarm(self) -> None:
            await self.alarm.delete()
            self.fired = True

        async def fired_at(self) -> tuple[bool, int | None]:
            return self.fired, await self.alarm.get()

    runtime = ActorRuntime(Reminder, Effects())
    await runtime.handle(command("schedule", [1_000]))
    await runtime.handle(command("onAlarm"))
    assert (await runtime.handle(command("fired_at")))["result"] == [True, None]


async def test_replaced_and_evicted_instances_are_released():
    runtime = ActorRuntime(Counter, Effects())
    instances = []
    for _ in range(3):
        await asyncio.create_task(runtime.handle(command("fail")))
        instances.append(weakref.ref(runtime.instance))
    await runtime.handle({"type": "evict"})
    gc.collect()
    assert [instance() for instance in instances] == [None, None, None]


async def test_argument_validation_precedes_actor_execution():
    runtime = ActorRuntime(Counter, Effects())
    assert (await runtime.handle(command(args=["2"])))["type"] == "failed"
    assert (await runtime.handle(command()))["result"] == {"count": 1}


async def test_reentrant_failure_does_not_erase_overlapping_success():
    entered = asyncio.Event()
    resume = Event()
    loop = asyncio.get_running_loop()

    class Shared(Actor):
        count: int = persisted(0)

        @interleave
        def wait_and_fail(self) -> None:
            loop.call_soon_threadsafe(entered.set)
            if not resume.wait(3):
                raise TimeoutError("test did not release the handler")
            raise ValueError("failed")

        def increment(self) -> int:
            self.count += 1
            return self.count

    runtime = ActorRuntime(Shared, Effects())
    pending = asyncio.create_task(runtime.handle(command("wait_and_fail")))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        reply = await asyncio.wait_for(runtime.handle(command()), 1)
        resume.set()
        assert (await pending)["type"] == "failed"
        assert reply["result"] == 1
        assert reply["sequence"] == 1
        assert (await runtime.handle(command()))["result"] == 2
    finally:
        resume.set()
        await asyncio.gather(pending, return_exceptions=True)


async def test_a_pending_sql_statement_does_not_block_other_completions():
    entered, persisting, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

    class ContendedDatabase(HostDatabase):
        async def call(self, call):
            if call["op"] == "exec" and call["sql"].startswith("CREATE TABLE"):
                entered.set()
                await release.wait()
            if call["op"] == "persist_field":
                persisting.set()
            return await super().call(call)

    class Ledger(Actor):
        count: int = persisted(0)

        @interleave
        def record(self) -> None:
            self.db.exec("CREATE TABLE IF NOT EXISTS entries (id INTEGER)")

        def increment(self) -> int:
            self.count += 1
            return self.count

    runtime = ActorRuntime(Ledger, Effects(), ContendedDatabase())
    tasks = [asyncio.create_task(runtime.handle(command("record")))]
    try:
        await asyncio.wait_for(entered.wait(), 1)
        tasks.append(asyncio.create_task(runtime.handle(command())))
        await asyncio.wait_for(persisting.wait(), 1)
        release.set()
        replies = await asyncio.wait_for(asyncio.gather(*tasks), 5)
        assert [reply["type"] for reply in replies] == ["invoked", "invoked"]
        assert replies[1]["result"] == 1
    finally:
        release.set()
        await asyncio.gather(*tasks, return_exceptions=True)


async def test_socket_messages_are_typed_and_emit_persisted_changes():
    from fixtures.effects import Effects

    from durable_actors import ActorSocket, emitted

    class Room(Actor[Value, Value, Value]):
        value: Value = emitted(persisted(Value(count=0)))

        def on_message(self, socket: ActorSocket[Value, Value], message: Value) -> None:
            self.value = message
            socket.send(Value(count=message.count + socket.metadata.count))

    effects = Effects()
    runtime = ActorRuntime(Room, effects)
    reply = await runtime.handle(
        {
            "type": "websocket_event",
            "request_id": "s1",
            "connections": [{"id": "s1", "metadata": {"count": 2}, "tags": []}],
            "event": {
                "type": "message",
                "connection_id": "s1",
                "message": {"type": "text", "data": '{"count":3}'},
            },
        }
    )
    assert reply["type"] == "websocket_handled"
    assert effects.published[0]["message"]["data"] == '{"count":5}'
    assert reply["effects"][0]["changes"] == {"value": {"count": 3}}


async def test_sql_commits_with_fields_and_rolls_back_when_the_call_fails():
    captured: list[object] = []

    class Account(Actor):
        _balance: int = persisted(0)

        def deposit(self, amount: int) -> int:
            self.db.exec("CREATE TABLE IF NOT EXISTS ledger (amount INTEGER)")
            self._balance += amount
            self.db.exec("INSERT INTO ledger (amount) VALUES (?)", amount)
            return self._balance

        def fail(self, amount: int) -> None:
            self._balance += amount
            self.db.exec("INSERT INTO ledger (amount) VALUES (?)", amount)
            raise ValueError("rollback both stores")

        def balance(self) -> int:
            return self._balance

        def ledger(self) -> list[dict[str, int]]:
            captured.append(self.db)
            return self.db.exec("SELECT amount FROM ledger ORDER BY rowid")

    runtime = ActorRuntime(Account, Effects())

    def invoke(method: str, args: list[int] | None = None):
        return {
            "type": "invoke",
            "request_id": method,
            "method": method,
            "args": args or [],
        }

    assert (await runtime.handle(invoke("deposit", [5])))["result"] == 5
    assert (await runtime.handle(invoke("deposit", [7])))["result"] == 12
    assert (await runtime.handle(invoke("balance")))["result"] == 12
    assert (await runtime.handle(invoke("ledger")))["result"] == [
        {"amount": 5},
        {"amount": 7},
    ]
    failure = await runtime.handle(invoke("fail", [3]))
    assert failure["type"] == "failed"
    assert failure["message"] == "rollback both stores"
    assert (await runtime.handle(invoke("balance")))["result"] == 12
    assert (await runtime.handle(invoke("ledger")))["result"] == [
        {"amount": 5},
        {"amount": 7},
    ]
    with pytest.raises(RuntimeError, match="outside its invocation"):
        captured[0].exec("SELECT amount FROM ledger")  # type: ignore[attr-defined]


async def test_async_actor_persists_results_across_awaits():
    class Counter(AsyncActor):
        count: int = persisted(0)

        async def increment(self, amount: int = 1) -> int:
            before = self.count
            await asyncio.sleep(0)
            self.count = before + amount
            return self.count

    runtime = ActorRuntime(Counter, Effects(), HostDatabase.seeded({"count": 3}))
    reply = await runtime.handle(command(args=[2]))
    assert reply["result"] == 5
    for expected in [6, 7, 8]:
        assert (await runtime.handle(command()))["result"] == expected
    assert runtime.host.fields() == {"count": 8}
