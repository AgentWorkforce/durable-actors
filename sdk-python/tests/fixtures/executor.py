import asyncio
import json
import os
import sys
import tempfile
from contextlib import asynccontextmanager, suppress
from pathlib import Path

from fixtures.host_database import HostDatabase

from durable_actors.build import build_actor


def build(tmp_path, module, source):
    project = tmp_path / "project"
    project.mkdir(exist_ok=True)
    (project / f"{module}.py").write_text(source)
    build_actor(project, f"{module}.py", tmp_path / "build")
    return str(tmp_path / "build/actors.pyz")


class ExecutorHost:
    """Plays the Rust host, answering database calls from its own SQLite database."""

    def __init__(self, reader, writer, database):
        self.reader = reader
        self.writer = writer
        self.database = database
        self.logs = []

    def send(self, message):
        self.writer.write((json.dumps(message) + "\n").encode())

    async def receive(self):
        while True:
            message = await self.read()
            if message["type"] == "database_call":
                await self.answer(message)
            elif message["type"] == "log":
                self.logs.append(message)
            elif message["type"] != "residency":
                return message

    async def residency(self, resident):
        """Waits for a residency report, discarding everything sent before it."""
        while True:
            message = await self.read()
            if message["type"] == "database_call":
                await self.answer(message)
            elif message["type"] == "residency" and message["resident"] is resident:
                return

    async def read(self):
        line = await self.reader.readline()
        if not line:
            raise EOFError("executor disconnected")
        return json.loads(line)

    async def answer(self, message):
        reply = {"type": "database_result", "call_id": message["call_id"]}
        try:
            reply["result"] = await self.database.call(message["call"])
        except Exception as error:
            reply["error"] = str(error)
        self.send(reply)


@asynccontextmanager
async def warm_executor(entrypoint, environment):
    connected = asyncio.get_running_loop().create_future()

    async def accepted(reader, writer):
        connected.set_result(ExecutorHost(reader, writer, HostDatabase()))

    with tempfile.TemporaryDirectory(dir="/tmp") as short:
        socket = str(Path(short) / "executor.sock")
        server = await asyncio.start_unix_server(accepted, socket)
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "durable_actors.host",
            "--generic",
            env={**os.environ, "DURABLE_ACTORS_EXECUTOR_SOCKET": socket},
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            async with asyncio.timeout(10):
                host = await connected
                assert await host.receive() == {"type": "warm", "protocol": 32}
                host.send({"type": "load", "entrypoint": entrypoint, "environment": environment})
                attach = await host.receive()
                assert attach["type"] == "attach"
                actor_name = attach["actor_names"][0]
                host.send(
                    {
                        "type": "attached",
                        "protocol": 32,
                        "actor": {
                            "project_id": "local",
                            "actor_name": actor_name,
                            "actor_id": "one",
                        },
                    }
                )
            yield host
        finally:
            if process.returncode is None:
                process.terminate()
            await asyncio.wait_for(process.communicate(), 5)
            server.close()
            if connected.done():
                connected.result().writer.transport.abort()
                with suppress(ConnectionError):
                    await asyncio.wait_for(connected.result().writer.wait_closed(), 1)
            await server.wait_closed()
