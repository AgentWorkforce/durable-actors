"""Exercise the production executor without installing development tools."""

import json
import os
import socket
import struct
import subprocess
import sys
import tempfile
import threading
from pathlib import Path

from durable_actors.build import build_actor
from durable_actors.executor_wire import attach, serialize


def main() -> None:
    with tempfile.TemporaryDirectory(dir="/tmp") as directory:
        project = Path(directory)
        (project / "actors.py").write_text("""from durable_actors import Actor, persisted
class Counter(Actor):
    count: int = persisted(0)
    def increment(self) -> int:
        self.count += 1
        return self.count
""")
        # Package a fixture artifact; production executors receive compiled artifacts.
        build_actor(project, "actors.py", project / "build")
        database = FieldStore()
        for expected in (1, 2):
            with socket.socket(socket.AF_UNIX) as listener:
                address = str(project / f"executor-{expected}.sock")
                listener.bind(address)
                listener.listen(1)
                listener.settimeout(10)
                executor = subprocess.Popen(
                    [sys.executable, "-m", "durable_actors.host", "--generic"],
                    env={**os.environ, "DURABLE_ACTORS_EXECUTOR_SOCKET": address},
                )
                try:
                    connection, _ = listener.accept()
                    threading.Thread(
                        target=serve_database, args=(listener, database), daemon=True
                    ).start()
                    with connection, connection.makefile("rb") as messages:
                        connection.settimeout(10)

                        def send(message):
                            connection.sendall(serialize(message))

                        def receive():
                            while True:
                                head = json.loads(messages.readline())
                                parts = [messages.read(size) for size in head.pop("parts", [])]
                                message = attach(head, parts)
                                if message["type"] != "residency":
                                    return message

                        assert receive() == {"type": "warm", "protocol": 33}
                        send(
                            {
                                "type": "load",
                                "entrypoint": str(project / "build/actors.pyz"),
                                "environment": {},
                            }
                        )
                        assert receive() == {
                            "type": "attach",
                            "protocol": 33,
                            "actor_names": ["Counter"],
                        }
                        actor = {"project_id": "local", "actor_name": "Counter", "actor_id": "one"}
                        send({"type": "attached", "protocol": 33, "actor": actor})
                        send(
                            {
                                "type": "command",
                                "message_id": expected,
                                "command": {
                                    "type": "invoke",
                                    "request_id": f"request-{expected}",
                                    "method": "increment",
                                    "args": [],
                                },
                            }
                        )
                        reply = receive()["reply"]
                        assert reply["type"] == "invoked", reply
                        assert reply["result"] == expected
                        send(
                            {
                                "type": "command",
                                "message_id": 10 + expected,
                                "command": {"type": "evict"},
                            }
                        )
                        assert receive()["reply"]["type"] == "evicted"
                finally:
                    executor.terminate()
                    try:
                        executor.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        executor.kill()
                        executor.wait()
                        raise
    print("Python image: generic warmup, invocation, and database recovery after restart passed")


def serve_database(listener, database):
    """Answers the worker's database calls, made on the connection after the executor's.

    A call is a size and that much JSON, then the JSON of the field it writes. Its answer is a row
    for each field it reads, then how the call ended.
    """
    connection, _ = listener.accept()
    with connection, connection.makefile("rwb") as channel:
        while size := channel.read(4):
            head = json.loads(channel.read(struct.unpack("<I", size)[0]))
            values = [json.loads(read_text(channel)) for _ in range(head["bindings"])]
            channel.write(answer(database.call(head["call"], values)))
            channel.flush()


def read_text(channel):
    tag, size = struct.unpack("<BI", channel.read(5))
    assert tag == 3, "these calls only carry text"
    return channel.read(size)


def answer(result):
    if result["type"] != "fields":
        return b"\x12" + sized(json.dumps(result))
    rows = [
        b"\x11\x03" + sized(name) + b"\x03" + sized(json.dumps(value))
        for name, value in result["fields"].items()
    ]
    columns = struct.pack("<BI", 0x10, 2) + sized("name") + sized("value")
    return b"".join([columns, *rows, b"\x12", sized('{"type":"rows"}')])


def sized(text):
    return struct.pack("<I", len(text.encode())) + text.encode()


class FieldStore:
    """Stands in for the host database; the actor only touches its fields."""

    def __init__(self):
        self.committed = {}
        self.pending = {}

    def call(self, call, values):
        op = call["op"]
        if op == "fields":
            return {"type": "fields", "fields": self.committed}
        if op == "exec" and call["sql"] == "BEGIN":
            self.pending = self.committed.copy()
            return {"type": "rows", "columns": [], "rows": []}
        if op == "persist_field":
            self.pending[call["name"]] = values[0]
        elif op == "delete_field":
            self.pending.pop(call["name"], None)
        elif op == "commit":
            self.committed = self.pending
        elif op == "rollback":
            self.pending = self.committed
        else:
            raise ValueError(f"unexpected database call {op}")
        return {"type": "done"}


main()
