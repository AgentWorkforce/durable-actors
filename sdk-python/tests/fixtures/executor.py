import asyncio
import os
import sys
import tempfile
from contextlib import asynccontextmanager, suppress
from pathlib import Path

from fixtures.database_peer import serve_database
from fixtures.host_database import HostDatabase

from durable_actors.build import build_actor
from durable_actors.executor_wire import Channel, serialize


def build(tmp_path, module, source):
    project = tmp_path / "project"
    project.mkdir(exist_ok=True)
    (project / f"{module}.py").write_text(source)
    build_actor(project, f"{module}.py", tmp_path / "build")
    return str(tmp_path / "build/actors.pyz")


class ExecutorHost:
    """Plays the Rust host on the connection that carries an executor's commands."""

    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer
        self.logs = []
        self.unanswered = set()

    def send(self, message):
        if message["type"] == "command":
            self.unanswered.add(message["message_id"])
        self.writer.write(serialize(message))

    async def receive(self):
        while True:
            message = await self.read()
            if message["type"] == "log":
                self.logs.append(message)
            elif message["type"] != "residency":
                return message

    async def residency(self, resident):
        """Waits for a residency report, discarding everything sent before it."""
        while True:
            message = await self.read()
            if message["type"] == "residency" and message["resident"] is resident:
                return

    async def read(self):
        message = await Channel(self.reader, self.writer).read()
        if message["type"] == "reply":
            self.unanswered.discard(message["message_id"])
        return message

    async def close(self):
        self.writer.transport.abort()
        with suppress(ConnectionError):
            await asyncio.wait_for(self.writer.wait_closed(), 1)


class ExecutorListener:
    """Accepts an executor as the Rust host does.

    Its first connection carries commands. Each later one is a worker's database channel, whose
    calls reach `database` while the command they name is unanswered.
    """

    def __init__(self, database):
        self.database = database
        self.host = asyncio.get_running_loop().create_future()

    async def accept(self, reader, writer):
        if self.host.done():
            await serve_database(reader, writer, self.resolve)
        else:
            self.host.set_result(ExecutorHost(reader, writer))

    def resolve(self, invocation):
        if invocation not in self.host.result().unanswered:
            raise RuntimeError("the command this database call belongs to has ended")
        return self.database.call


@asynccontextmanager
async def executor(database, *arguments, **environment):
    """Starts an executor process and yields the host its first connection reaches."""
    with tempfile.TemporaryDirectory(dir="/tmp") as short:
        socket = str(Path(short) / "executor.sock")
        listener = ExecutorListener(database)
        server = await asyncio.start_unix_server(listener.accept, socket)
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "durable_actors.host",
            *arguments,
            env={**os.environ, **environment, "DURABLE_ACTORS_EXECUTOR_SOCKET": socket},
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            async with asyncio.timeout(10):
                host = await listener.host
            yield host, process
        finally:
            if process.returncode is None:
                process.terminate()
            await asyncio.wait_for(process.communicate(), 5)
            server.close()
            if listener.host.done():
                await listener.host.result().close()
            await server.wait_closed()


@asynccontextmanager
async def warm_executor(entrypoint, environment):
    async with executor(HostDatabase(), "--generic") as (host, _):
        async with asyncio.timeout(10):
            assert await host.receive() == {"type": "warm", "protocol": 33}
            host.send({"type": "load", "entrypoint": entrypoint, "environment": environment})
            attach = await host.receive()
            assert attach["type"] == "attach"
            actor_name = attach["actor_names"][0]
            host.send(
                {
                    "type": "attached",
                    "protocol": 33,
                    "actor": {"project_id": "local", "actor_name": actor_name, "actor_id": "one"},
                }
            )
        yield host
