"""Exercise the production executor without installing development tools."""

import json
import os
import socket
import subprocess
import sys
import tempfile
from pathlib import Path

from durable_actors.build import build_actor


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
                    with connection, connection.makefile("rb") as messages:
                        connection.settimeout(10)

                        def send(message):
                            connection.sendall((json.dumps(message) + "\n").encode())

                        def receive():
                            while True:
                                message = json.loads(messages.readline())
                                if message["type"] == "residency":
                                    continue
                                if message["type"] != "database_call":
                                    return message
                                send(
                                    {
                                        "type": "database_result",
                                        "call_id": message["call_id"],
                                        "result": database.call(message["call"]),
                                    }
                                )

                        assert receive() == {"type": "warm", "protocol": 32}
                        send(
                            {
                                "type": "load",
                                "entrypoint": str(project / "build/actors.pyz"),
                                "environment": {},
                            }
                        )
                        assert receive() == {
                            "type": "attach",
                            "protocol": 32,
                            "actor_names": ["Counter"],
                        }
                        actor = {"project_id": "local", "actor_name": "Counter", "actor_id": "one"}
                        send({"type": "attached", "protocol": 32, "actor": actor})
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


class FieldStore:
    """Stands in for the host database; the actor only touches its fields."""

    def __init__(self):
        self.committed = {}
        self.pending = {}

    def call(self, call):
        op = call["op"]
        if op == "fields":
            return {"type": "fields", "fields": self.committed}
        if op == "exec" and call["sql"] == "BEGIN":
            self.pending = self.committed.copy()
            return {"type": "rows", "columns": [], "rows": []}
        if op == "persist_field":
            self.pending[call["name"]] = call["value"]
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
