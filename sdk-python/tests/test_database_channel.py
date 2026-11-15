import io
import json
import struct

import pytest
from fixtures.database_peer import COLUMNS, FAILED, RESULT, ROW, DatabasePeer, ending, sized, value

from durable_actors.database_channel import DatabaseChannel, DatabaseChannelError


def statement(sql, *bindings):
    return {"op": "exec", "sql": sql, "bindings": list(bindings), "immediate": True}


def echo(call):
    bindings = call["bindings"]
    columns = [f"column {index}" for index in range(len(bindings))]
    return {"type": "rows", "columns": columns, "rows": [bindings, bindings]}


def columns(*names):
    return struct.pack("<BI", COLUMNS, len(names)) + b"".join(
        sized(name.encode()) for name in names
    )


def row(*cells):
    return bytes([ROW]) + b"".join(map(value, cells))


class Scripted:
    """Answers each request with the next prepared answer, then closes. Keeps what each wrote."""

    def __init__(self, *answers):
        self.answers = list(answers)
        self.answer = io.BytesIO()
        self.sent = []
        self.writing = b""

    def writelines(self, chunks):
        self.writing += b"".join(chunks)

    def flush(self):
        self.sent.append(self.writing)
        self.writing = b""
        self.answer = io.BytesIO(self.answers.pop(0) if self.answers else b"")

    def read(self, size):
        return self.answer.read(size)


def test_bound_values_come_back_as_rows_of_the_same_sqlite_storage_classes():
    bindings = [
        None,
        7,
        -(2**63),
        2**63 - 1,
        True,
        1.5,
        1e300,
        "",
        "caf\u00e9 \U0001f600",
        b"",
        b"\x00\xff",
    ]
    channel = DatabaseChannel(DatabasePeer(echo))

    result = channel.call(statement("SELECT ?", *bindings))

    assert result["columns"] == [f"column {index}" for index in range(len(bindings))]
    assert result["rows"] == [bindings, bindings]
    assert [type(cell) for cell in result["rows"][0]] == [
        type(None), int, int, int, int, float, float, str, str, bytes, bytes
    ]  # fmt: skip


def test_a_call_names_the_command_it_belongs_to_and_nothing_outside_one():
    peer = DatabasePeer(lambda call: {"type": "done"})
    channel = DatabaseChannel(peer)

    channel.call({"op": "commit"}, 17)
    channel.call({"op": "rollback"})
    channel.call(statement("DELETE FROM notes WHERE id = ?", 3), 18)

    assert peer.requests == [
        {"invocation": 17, "call": {"op": "commit"}},
        {"invocation": None, "call": {"op": "rollback"}},
        {"invocation": 18, "call": statement("DELETE FROM notes WHERE id = ?", 3)},
    ]


def test_results_that_are_not_rows_are_read_as_the_host_wrote_them():
    results = [
        {"type": "alarm", "timestamp_ms": 1_000},
        {"type": "alarm", "timestamp_ms": None},
        {"type": "done"},
    ]
    channel = DatabaseChannel(DatabasePeer(lambda call: results[0]))

    while results:
        assert channel.call({"op": "alarm"}) == results[0]
        results.pop(0)


def test_a_fields_json_follows_its_call_instead_of_being_inside_it_and_fields_are_read_from_rows():
    note = {"z": 'caf\u00e9 \U0001f600 "quoted"\n' * 20_000, "a": [1, None]}
    text = json.dumps(note, separators=(",", ":"), ensure_ascii=False)
    stored = columns("name", "value") + row("note", text) + row("count", "1")
    stream = Scripted(
        ending(RESULT, b'{"type":"done"}'), stored + ending(RESULT, b'{"type":"rows"}')
    )
    channel = DatabaseChannel(stream)

    channel.call({"op": "persist_field", "name": "note", "value": note, "immediate": False}, 7)

    head = (
        b'{"call":{"op":"persist_field","name":"note","immediate":false},'
        b'"bindings":1,"message_id":7}'
    )
    assert stream.sent == [sized(head) + value(text)]
    assert channel.call({"op": "fields"}) == {
        "type": "fields",
        "fields": {"note": note, "count": 1},
    }


def test_a_failed_statement_raises_the_hosts_message_and_leaves_the_channel_usable():
    failure = b"UNIQUE constraint failed: notes.id"
    channel = DatabaseChannel(
        Scripted(
            columns("id") + row(1) + row(2) + ending(FAILED, failure),
            columns("id") + row(3) + ending(RESULT, b'{"type":"rows"}'),
        )
    )

    with pytest.raises(RuntimeError, match="^UNIQUE constraint failed: notes.id$"):
        channel.call(statement("INSERT"))
    assert channel.call(statement("SELECT")) == {"type": "rows", "columns": ["id"], "rows": [[3]]}


def test_text_that_is_not_utf8_is_read_with_replacement_characters():
    channel = DatabaseChannel(
        Scripted(
            columns("note") + b"\x11\x03" + sized(b"a\xffb") + ending(RESULT, b'{"type":"rows"}')
        )
    )

    assert channel.call(statement("SELECT"))["rows"] == [["a\ufffdb"]]


@pytest.mark.parametrize(
    ("bindings", "refusal"),
    [
        ([1, [2]], "binding 2 is list"),
        ([2**63], "binding 1 does not fit a 64-bit SQLite integer"),
        (["\ud83d"], "surrogates not allowed"),
        ([None] * 32_767, "at most 32766 values"),
    ],
)
def test_a_value_that_sqlite_cannot_bind_is_refused_before_anything_is_sent(bindings, refusal):
    peer = DatabasePeer(echo)
    channel = DatabaseChannel(peer)

    with pytest.raises((TypeError, ValueError, OverflowError), match=refusal):
        channel.call(statement("SELECT ?", *bindings))

    assert peer.requests == []
    assert channel.call(statement("SELECT ?", "still usable"))["type"] == "rows"


def test_an_answer_cut_short_ends_the_channel_for_every_later_call():
    stream = Scripted(columns("id") + b"\x11\x03" + struct.pack("<I", 200) + b"cut")
    channel = DatabaseChannel(stream)

    for _ in range(2):
        with pytest.raises(DatabaseChannelError, match="actor database channel failed"):
            channel.call({"op": "commit"})
    assert len(stream.sent) == 1
