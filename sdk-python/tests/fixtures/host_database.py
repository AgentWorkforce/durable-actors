import json
import sqlite3
import tempfile
import threading
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
        self.answering = threading.Lock()
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
        for name, value in fields.items():
            database.persist(name, value)
        database.commit()
        database.commits = 0
        return database

    def call(self, call):
        """Answers one call at a time, as a database channel does."""
        with self.answering:
            return self.answer(call)

    def answer(self, call):
        self.calls.append(call)
        op = call["op"]
        if op == "exec":
            return self.write(lambda: self.exec(call["sql"], call["bindings"]))
        if op == "fields":
            return {"type": "fields", "fields": self.fields()}
        if op == "alarm":
            return {"type": "alarm", "timestamp_ms": self.alarm()}
        if op == "set_alarm":
            return self.write(lambda: self.update_alarm(call["timestamp_ms"]))
        if op == "delete_alarm":
            return self.write(lambda: self.update_alarm(None))
        if op == "persist_field":
            return self.write(lambda: self.persist(call["name"], call["value"]))
        if op == "delete_field":
            return self.write(lambda: self.delete(call["name"]))
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

    def write(self, operation):
        result = operation() or {"type": "done"}
        if not self.connection.in_transaction:
            self.commit()
        return result

    def exec(self, sql, bindings):
        cursor = self.connection.execute(sql, bindings)
        columns = [column[0] for column in cursor.description or []]
        return {
            "type": "rows",
            "columns": columns,
            "rows": [list(row) for row in cursor.fetchall()],
        }

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
