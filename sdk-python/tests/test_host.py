import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path

import pytest
from fixtures.executor import ExecutorHost, build, warm_executor
from fixtures.host_database import HostDatabase

from durable_actors.build import build_actor


async def test_warm_executor_keeps_its_assigned_environment_across_worker_restarts(tmp_path):
    entrypoint = build(
        tmp_path,
        "environment_actors",
        """import os
from durable_actors import Actor
class Settings(Actor):
    def read(self, name: str) -> str | None:
        return os.environ.get(name)
""",
    )

    async def send(message_id, command):
        host.send({"type": "command", "message_id": message_id, "command": command})
        return (await host.receive())["reply"]

    read = {
        "type": "invoke",
        "request_id": "read",
        "method": "read",
        "args": ["PAYMENTS_API_KEY"],
    }
    async with warm_executor(entrypoint, {"PAYMENTS_API_KEY": "secret"}) as host:
        assert (await send(1, read))["result"] == "secret"
        assert await send(2, {"type": "evict"}) == {"type": "evicted"}
        assert (await send(3, read))["result"] == "secret"


async def test_actors_call_other_actors_through_the_host(tmp_path):
    entrypoint = build(
        tmp_path,
        "calling_actors",
        """from durable_actors import Actor
class Document(Actor):
    def publish(self) -> list[str]:
        return Index.get("main").add("doc-1")
class Index(Actor):
    def add(self, document: str) -> list[str]:
        return [document]
""",
    )
    publish = {"type": "invoke", "request_id": "publish", "method": "publish", "args": []}
    async with warm_executor(entrypoint, {}) as host:
        async with asyncio.timeout(5):
            host.send({"type": "command", "message_id": 1, "command": publish})
            call = await host.receive()
            assert call["type"] == "actor_call"
            assert {key: call["call"][key] for key in ("actor_name", "actor_id", "method")} == {
                "actor_name": "Index",
                "actor_id": "main",
                "method": "add",
            }
            assert call["call"]["args"] == ["doc-1"]
            host.send(
                {
                    "type": "actor_result",
                    "call_id": call["call_id"],
                    "outcome": {"type": "completed", "result": ["doc-1"]},
                }
            )
            assert (await host.receive())["reply"] == {"type": "invoked", "result": ["doc-1"]}


async def test_executor_reports_whether_its_attached_actor_is_resident(tmp_path):
    entrypoint = build(
        tmp_path,
        "residency_actors",
        """from durable_actors import Actor
class Counter(Actor):
    def ping(self) -> None:
        return None
""",
    )
    invoke = {"type": "invoke", "request_id": "ping", "method": "ping", "args": []}
    async with warm_executor(entrypoint, {}) as host:
        async with asyncio.timeout(5):
            await host.residency(False)
            host.send({"type": "command", "message_id": 1, "command": invoke})
            assert (await host.receive())["reply"]["type"] == "invoked"
            await host.residency(True)
            host.send({"type": "command", "message_id": 2, "command": {"type": "evict"}})
            await host.residency(False)


@pytest.mark.parametrize("generic", [False, True])
async def test_artifact_attaches_invokes_and_rehydrates_in_a_fresh_python_process(
    tmp_path, generic
):
    project = tmp_path / "project"
    project.mkdir()
    (project / "actors.py").write_text("""from durable_actors import Actor, persisted
class Counter(Actor):
    count: int = persisted(0)
    def increment(self, amount: int = 1) -> int:
        self.count += amount
        return self.count
""")
    contract = build_actor(project, "actors.py", tmp_path / "build")
    assert contract["actors"][0]["actorName"] == "Counter"
    actor = {"project_id": "local", "actor_name": "Counter", "actor_id": "one"}
    database = HostDatabase()
    with tempfile.TemporaryDirectory(dir="/tmp") as short:
        for expected in (2, 4):
            done = asyncio.get_running_loop().create_future()

            async def accepted(reader, writer):
                host = ExecutorHost(reader, writer, database)
                try:
                    if generic:
                        assert await host.receive() == {"type": "warm", "protocol": 31}
                        host.send(
                            {
                                "type": "load",
                                "entrypoint": str(tmp_path / "build/actors.pyz"),
                                "environment": {},
                            }
                        )
                    assert await host.receive() == {
                        "type": "attach",
                        "protocol": 31,
                        "actor_names": ["Counter"],
                    }
                    host.send({"type": "attached", "protocol": 31, "actor": actor})
                    host.send(
                        {
                            "type": "command",
                            "message_id": 1,
                            "command": {
                                "type": "invoke",
                                "request_id": "r1",
                                "method": "increment",
                                "args": [2],
                            },
                        }
                    )
                    reply = await host.receive()
                    assert reply["reply"] == {"type": "invoked", "result": expected}
                    assert database.fields() == {"count": expected}
                    done.set_result(None)
                except BaseException as error:
                    done.set_exception(error)
                finally:
                    writer.close()

            socket = str(Path(short) / "executor.sock")
            server = await asyncio.start_unix_server(accepted, socket)
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                "-m",
                "durable_actors.host",
                *(["--generic"] if generic else []),
                env={
                    **os.environ,
                    "DURABLE_ACTORS_EXECUTOR_SOCKET": socket,
                    "DURABLE_ACTORS_ENTRYPOINT": str(tmp_path / "build/actors.pyz"),
                },
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                await asyncio.wait_for(done, 10)
            finally:
                if process.returncode is None:
                    process.terminate()
                await process.communicate()
                server.close()
                await server.wait_closed()


@pytest.mark.parametrize("action", ["evict", "kill_host"])
async def test_worker_stops_blocked_handlers_on_eviction_and_host_death(tmp_path, action):
    project = tmp_path / "project"
    project.mkdir()
    (project / "eviction_actors.py").write_text("""import os
from threading import Event
from durable_actors import Actor, interleave, persisted
class Counter(Actor):
    count: int = persisted(0)
    @interleave
    def hold(self) -> None:
        self.broadcast(os.getpid())
        Event().wait()
    def increment(self) -> int:
        self.count += 1
        return self.count
""")
    build_actor(project, "eviction_actors.py", tmp_path / "build")
    connected = asyncio.get_running_loop().create_future()

    async def accepted(reader, writer):
        connected.set_result((reader, writer))

    with tempfile.TemporaryDirectory(dir="/tmp") as short:
        socket = str(Path(short) / "executor.sock")
        server = await asyncio.start_unix_server(accepted, socket)
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "durable_actors.host",
            env={
                **os.environ,
                "DURABLE_ACTORS_EXECUTOR_SOCKET": socket,
                "DURABLE_ACTORS_ENTRYPOINT": str(tmp_path / "build/actors.pyz"),
            },
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        child_pid = None
        try:
            async with asyncio.timeout(5):
                reader, writer = await connected
                host = ExecutorHost(reader, writer, HostDatabase())
                receive, send = host.receive, host.send
                assert (await receive())["type"] == "attach"
                actor = {"project_id": "local", "actor_name": "Counter", "actor_id": "one"}
                send({"type": "attached", "protocol": 31, "actor": actor})
                invocation = {
                    "type": "invoke",
                    "request_id": "r1",
                    "method": "hold",
                    "args": [],
                }
                send(
                    {
                        "type": "command",
                        "message_id": 0,
                        "command": {**invocation, "method": "increment"},
                    }
                )
                initial = await receive()
                assert initial["reply"]["sequence"] == 1
                send({"type": "command", "message_id": 1, "command": invocation})
                assert (await receive())["type"] == "ready_for_invocation"
                effect = await receive()
                assert effect["type"] == "socket_effects"
                child_pid = json.loads(effect["effects"][0]["message"]["data"])
                if action == "kill_host":
                    process.kill()
                    await process.wait()
                    for _ in range(100):
                        try:
                            os.kill(child_pid, 0)
                        except ProcessLookupError:
                            break
                        await asyncio.sleep(0.01)
                    else:
                        pytest.fail("actor worker survived its supervisor")
                    return
                send(
                    {
                        "type": "command",
                        "message_id": 2,
                        "command": {"type": "evict"},
                    }
                )
                replies = [await receive(), await receive()]
                outcomes = {reply["message_id"]: reply["reply"] for reply in replies}
                assert outcomes[1]["code"] == "actor_evicted"
                assert outcomes[2] == {"type": "evicted"}
                send(
                    {
                        "type": "command",
                        "message_id": 3,
                        "command": {**invocation, "method": "increment"},
                    }
                )
                restored = (await receive())["reply"]
                assert restored["result"] == 2
                assert restored["sequence"] == 2
        finally:
            if child_pid is not None:
                try:
                    os.kill(child_pid, 9)
                except ProcessLookupError:
                    pass
            if process.returncode is None:
                process.terminate()
            await asyncio.wait_for(process.communicate(), 2)
            server.close()
            if connected.done():
                _, writer = connected.result()
                writer.transport.abort()
                await asyncio.wait_for(writer.wait_closed(), 1)
            await server.wait_closed()
