"""Framing and codecs of the worker wire protocol (no processes)."""

from __future__ import annotations

import math

import pytest

from dblayer.core.results import Results
from dblayer.core.schema import ColumnInfo, ForeignKey, Index, Schema, TableInfo, Trigger
from dblayer.worker.protocol import (
    FrameDecoder,
    FrameTooLarge,
    available_codecs,
    frame,
    get_codec,
    results_from_wire,
    results_to_wire,
    schema_from_wire,
    schema_to_wire,
)

CODECS = available_codecs()


@pytest.fixture(params=CODECS)
def codec(request):
    return get_codec(request.param)


def test_frame_is_length_prefixed_big_endian():
    assert frame(b"abc") == b"\x00\x00\x00\x03abc"
    assert frame(b"") == b"\x00\x00\x00\x00"


def test_decoder_handles_split_and_joined_frames():
    data = frame(b"one") + frame(b"two") + frame(b"x" * 1000)
    dec = FrameDecoder()
    out = []
    for i in range(0, len(data), 3):
        out += dec.feed(data[i : i + 3])
    assert out == [b"one", b"two", b"x" * 1000]
    assert dec.pending == 0


def test_truncated_frame_leaves_pending_bytes():
    dec = FrameDecoder()
    assert dec.feed(frame(b"hello")[:-2]) == []
    assert dec.pending == 7


def test_oversized_frame_refused_from_header_alone():
    dec = FrameDecoder(max_frame_bytes=10)
    with pytest.raises(FrameTooLarge):
        dec.feed(b"\x00\x00\x01\x00")  # 256 bytes declared, none sent
    with pytest.raises(FrameTooLarge):
        frame(b"x" * 11, 10)


def test_round_trip_values(codec):
    msg = {
        "id": 7,
        "type": "execute",
        "params": {"a": b"\x00\xff", "b": 2**62, "c": -1.5, "d": None, "e": "é☃", "f": True},
        "rows": [[1, "x", b"", 1e308, None]],
    }
    back = codec.decode(codec.encode(msg))
    assert back == {**msg, "rows": [[1, "x", b"", 1e308, None]]}
    assert isinstance(back["params"]["a"], bytes)
    assert back["params"]["f"] is True


def test_non_finite_floats(codec):
    back = codec.decode(codec.encode({"v": [math.inf, -math.inf, math.nan]}))
    assert back["v"][0] == math.inf and back["v"][1] == -math.inf and math.isnan(back["v"][2])


def test_big_integers_stay_exact(codec):
    big = 2**70
    assert codec.decode(codec.encode({"v": [big, -big]}))["v"] == [big, -big]


def test_tuples_become_lists(codec):
    assert codec.decode(codec.encode({"v": (1, (2, 3))}))["v"] == [1, [2, 3]]


def test_json_escapes_user_dicts_that_look_like_wrappers():
    c = get_codec("json")
    msg = {"type": "op", "args": {"rows": [{"$b": "not bytes"}, {"$d": 1}, {"a": 1}]}}
    assert c.decode(c.encode(msg)) == msg


def test_json_bytes_wire_form():
    c = get_codec("json")
    assert c.encode({"v": b"hi"}) == b'{"v":{"$b":"aGk="}}'


def test_results_round_trip(codec):
    r = Results.from_tuples(["a", "b"], [(1, b"x"), (2, None)], truncated=True, rowcount=3, last_pk=9)
    back = results_from_wire(codec.decode(codec.encode(results_to_wire(r))))
    assert back.columns == ("a", "b")
    assert back.rows == [(1, b"x"), (2, None)]
    assert (back.truncated, back.rowcount, back.last_pk) == (True, 3, 9)
    empty = results_from_wire(results_to_wire(Results.from_tuples([], [])))
    assert (empty.columns, empty.rows, empty.rowcount, empty.last_pk) == ((), [], -1, None)


def test_schema_round_trip(codec):
    t = TableInfo(
        name="t",
        kind="table",
        columns=(ColumnInfo("id", "INTEGER", True, None, 1), ColumnInfo("x", "", default="'a'")),
        foreign_keys=(ForeignKey(("x",), "o", ("id",), "CASCADE", None),),
        indexes=(Index("i", ("x", None), True, "c", True, "CREATE INDEX i ..."),),
        definition="CREATE TABLE t (...)",
    )
    v = TableInfo(name="v", kind="view", columns=(ColumnInfo("id", ""),), definition="CREATE VIEW v")
    s = Schema(version=12, tables={"t": t}, views={"v": v}, triggers={"tr": Trigger("tr", "t", "CREATE TRIGGER")})
    assert schema_from_wire(codec.decode(codec.encode(schema_to_wire(s)))) == s
