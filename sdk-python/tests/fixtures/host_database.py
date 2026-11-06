import base64
import json
import sqlite3
import tempfile
from pathlib import Path

_directories = []


class HostDatabase:
    """Stands in for the Rust host's SQLite engine at the protocol boundary."""

    def __init__(self):
        directory = tempfile.TemporaryDirectory(dir="/tmp")
        _directories.append(directory)
        path = Path(directory.name) / "actor.sqlite"
        self.calls = []
        self.commits = 0
        self.connection = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS __terse_fields"
            "(name TEXT PRIMARY KEY, value TEXT NOT NULL CHECK(json_valid(value)))"
        )
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS __terse_alarm"
            "(singleton INTEGER PRIMARY KEY CHECK(singleton = 1), timestamp INTEGER)"
        )
        self.synced = self.change_token()

    @classmethod
    def seeded(cls, fields):
        database = cls()
        database.write(False, lambda: database.replace(fields))
        database.commit()
        database.commits = 0
        return database

    async def call(self, call):
        self.calls.append(call)
        op = call["op"]
        if op == "exec":
            return self.write(call["immediate"], lambda: self.exec(call["sql"], call["bindings"]))
        if op == "fields":
            return {"type": "fields", "fields": self.fields()}
        if op == "alarm":
            return {"type": "alarm", "timestamp_ms": self.alarm()}
        if op == "set_alarm":
            return self.write(call["immediate"], lambda: self.update_alarm(call["timestamp_ms"]))
        if op == "delete_alarm":
            return self.write(call["immediate"], lambda: self.update_alarm(None))
        if op == "replace_fields":
            return self.write(False, lambda: self.replace(call["fields"]))
        if op == "persist_field":
            return self.write(call["immediate"], lambda: self.persist(call["name"], call["value"]))
        if op == "delete_field":
            return self.write(call["immediate"], lambda: self.delete(call["name"]))
        if op == "commit":
            self.commit()
            return {"type": "done"}
        if op == "rollback":
            if self.connection.in_transaction:
                self.connection.rollback()
            self.synced = self.change_token()
            return {"type": "done"}
        raise ValueError(f"unexpected database call {op}")

    def fields(self):
        return {
            name: json.loads(value)
            for name, value in self.connection.execute("SELECT name, value FROM __terse_fields")
        }

    def alarm(self):
        row = self.connection.execute(
            "SELECT timestamp FROM __terse_alarm WHERE singleton = 1"
        ).fetchone()
        return None if row is None else row[0]

    def update_alarm(self, timestamp_ms):
        self.connection.execute(
            "INSERT INTO __terse_alarm VALUES (1, ?) "
            "ON CONFLICT(singleton) DO UPDATE SET timestamp=excluded.timestamp",
            (timestamp_ms,),
        )

    def close(self):
        if self.connection.in_transaction:
            self.connection.rollback()
        self.connection.close()

    def write(self, immediate, operation):
        result = operation() or {"type": "done"}
        if not self.connection.in_transaction:
            self.commit()
        return result

    def exec(self, sql, bindings):
        cursor = self.connection.execute(sql, [decode(value) for value in bindings])
        columns = [column[0] for column in cursor.description or []]
        return {
            "type": "rows",
            "columns": columns,
            "rows": [[encode(value) for value in row] for row in cursor.fetchall()],
        }

    def replace(self, fields):
        self.connection.executemany(
            "INSERT INTO __terse_fields VALUES (?, ?) "
            "ON CONFLICT(name) DO UPDATE SET value=excluded.value WHERE value<>excluded.value",
            [(name, json.dumps(value, separators=(",", ":"))) for name, value in fields.items()],
        )
        self.connection.execute(
            "DELETE FROM __terse_fields WHERE name NOT IN (SELECT value FROM json_each(?))",
            (json.dumps(list(fields)),),
        )

    def persist(self, name, value):
        self.connection.execute(
            "INSERT INTO __terse_fields VALUES (?, ?) "
            "ON CONFLICT(name) DO UPDATE SET value=excluded.value WHERE value<>excluded.value",
            (name, json.dumps(value, separators=(",", ":"))),
        )

    def delete(self, name):
        self.connection.execute("DELETE FROM __terse_fields WHERE name = ?", (name,))

    def commit(self):
        if self.connection.in_transaction:
            self.connection.commit()
        token = self.change_token()
        if token != self.synced:
            self.commits += 1
        self.synced = token

    def change_token(self):
        return (
            self.connection.total_changes,
            self.connection.execute("PRAGMA schema_version").fetchone()[0],
            self.connection.execute("PRAGMA user_version").fetchone()[0],
        )


def decode(value):
    kind = value["type"]
    if kind == "null":
        return None
    if kind == "integer":
        return int(value["value"])
    if kind == "blob":
        return base64.b64decode(value["value"])
    return value["value"]


def encode(value):
    if value is None:
        return {"type": "null"}
    if isinstance(value, int):
        return {"type": "integer", "value": str(value)}
    if isinstance(value, float):
        return {"type": "real", "value": value}
    if isinstance(value, bytes):
        return {"type": "blob", "value": base64.b64encode(value).decode()}
    return {"type": "text", "value": value}
