import asyncio

import pytest
from fixtures.host_database import HostDatabase

from durable_actors.sqlite import DatabaseCommitError, DatabaseProxy


async def test_values_keep_their_sqlite_storage_classes():
    host = HostDatabase()
    database = DatabaseProxy(host.call, asyncio.get_running_loop())
    values = [None, 2**63 - 1, 1.5, "text", b"\x00\x01\xff"]

    def write_and_read():
        database.exec("CREATE TABLE value_types(value)")
        for value in values:
            database.exec("INSERT INTO value_types VALUES (?)", value)
        return database.exec("SELECT value, typeof(value) AS type FROM value_types ORDER BY rowid")

    rows = await asyncio.to_thread(write_and_read)
    assert rows == [
        {"value": None, "type": "null"},
        {"value": 2**63 - 1, "type": "integer"},
        {"value": 1.5, "type": "real"},
        {"value": "text", "type": "text"},
        {"value": b"\x00\x01\xff", "type": "blob"},
    ]
    assert [call["bindings"] for call in host.calls[1:6]] == [
        [{"type": "null"}],
        [{"type": "integer", "value": "9223372036854775807"}],
        [{"type": "real", "value": 1.5}],
        [{"type": "text", "value": "text"}],
        [{"type": "blob", "value": "AAH/"}],
    ]
    host.close()


async def test_fields_commit_and_rollback_run_on_the_event_loop():
    host = HostDatabase()
    database = DatabaseProxy(host.call, asyncio.get_running_loop())
    await database.begin()
    await database.update_fields({}, {"count": 1})
    await database.commit()
    await database.begin()
    await database.update_fields({"count": 1}, {"count": 2})
    await database.rollback()
    assert await database.fields() == {"count": 1}
    assert host.commits == 1
    host.close()


async def test_sync_statements_cannot_block_the_event_loop():
    database = DatabaseProxy(HostDatabase().call, asyncio.get_running_loop())
    with pytest.raises(RuntimeError, match="event loop"):
        database.exec("SELECT 1")


async def test_host_errors_reach_actor_code_and_commit_failures_are_distinguishable():
    async def reject(call):
        raise RuntimeError(
            "replication failed"
            if call["op"] == "commit"
            else "SQLite runtime table names are reserved"
        )

    database = DatabaseProxy(reject, asyncio.get_running_loop())
    with pytest.raises(RuntimeError, match="reserved"):
        await asyncio.to_thread(database.exec, "SELECT * FROM __terse_fields")
    with pytest.raises(DatabaseCommitError):
        await database.commit()
