"""``db.export()`` on every backend: CSV, TSV, JSON shapes and NDJSON.

Each test runs with ``native`` on and off. On a backend without
``SupportsExport`` both are the Python fallback; on one with it, the native
path must give the same answers (byte-level parity is in
``tests/test_export_parity.py``)."""

from __future__ import annotations

import asyncio
import csv
import io
import json
import math

import pytest

from ..helpers import series_sql

from dblayer import QueryError, QueryInterrupted, ResourceLimitExceeded

INFINITE = "with recursive c(x) as (select 1 union all select x + 1 from c) select x from c"




@pytest.fixture(params=[True, False], ids=["native", "fallback"])
def native(request: pytest.FixtureRequest) -> bool:
    return request.param


@pytest.fixture
async def items(db):
    await db.create_table(
        "items", {"id": "integer", "name": "text", "price": "float", "note": "text"}, pk="id"
    )
    await db.insert_rows(
        "items",
        [
            {"id": 1, "name": "plain", "price": 1.5, "note": "x"},
            {"id": 2, "name": 'quote " and, comma', "price": 2.0, "note": None},
            {"id": 3, "name": "tab\there\nnewline\r\n", "price": -0.25, "note": ""},
            {"id": 4, "name": "é ünïcode ☃", "price": 1e20, "note": "back\\slash"},
            {"id": 5, "name": "ctl \x01\x1f", "price": 0.1, "note": "x"},
        ],
    )
    return db


async def body(db, sql, params=None, **kw) -> bytes:
    return await db.export(sql, params, **kw).read_all()


ITEMS = "select id, name, price, note from items order by id"
EXPECTED = [
    (1, "plain", 1.5, "x"),
    (2, 'quote " and, comma', 2.0, None),
    (3, "tab\there\nnewline\r\n", -0.25, ""),
    (4, "é ünïcode ☃", 1e20, "back\\slash"),
    (5, "ctl \x01\x1f", 0.1, "x"),
]


def csv_rows(data: bytes, delim: str = ",") -> list[list[str]]:
    csv.field_size_limit(1 << 30)
    return list(csv.reader(io.StringIO(data.decode("utf-8"), newline=""), delimiter=delim))


async def test_csv(items, native):
    data = await body(items, ITEMS, format="csv", native=native)
    assert data.startswith(b"id,name,price,note\r\n")
    assert data.endswith(b"\r\n")
    rows = csv_rows(data)
    assert rows[0] == ["id", "name", "price", "note"]
    assert [[int(r[0]), r[1], float(r[2]), r[3]] for r in rows[1:]] == [
        [i, n, p, "" if note is None else note] for i, n, p, note in EXPECTED
    ]
    # NULL is an empty field, the empty string is quoted
    assert b'2,"quote "" and, comma",2.0,\r\n' in data
    assert b'"tab\there\nnewline\r\n",-0.25,""\r\n' in data


async def test_csv_header_off(items, native):
    data = await body(items, "select id from items order by id", format="csv", header=False, native=native)
    assert data == b"1\r\n2\r\n3\r\n4\r\n5\r\n"


async def test_tsv(items, native):
    data = await body(items, ITEMS, format="tsv", native=native)
    assert data.startswith(b"id\tname\tprice\tnote\r\n")
    rows = csv_rows(data, "\t")
    assert rows[2] == ["2", 'quote " and, comma', "2.0", ""]
    # commas are not special in TSV; tabs, quotes and newlines are
    assert b'2\t"quote "" and, comma"\t2.0\t\r\n' in data
    assert b'3\t"tab\there\nnewline\r\n"\t-0.25\t""\r\n' in data


@pytest.mark.parametrize("shape", ["objects", "arrays", "array"])
async def test_json_shapes(items, native, shape):
    stream = items.export(ITEMS, format="json", json_shape=shape, native=native)
    data = await stream.read_all()
    doc = json.loads(data)
    assert stream.columns == ("id", "name", "price", "note")
    assert stream.row_count == 5
    if shape == "array":
        rows = doc
    else:
        assert doc["ok"] is True
        assert doc["columns"] == ["id", "name", "price", "note"]
        assert doc["truncated"] is False
        rows = doc["rows"]
    if shape == "arrays":
        assert [tuple(r) for r in rows] == EXPECTED
    else:
        assert [tuple(r.values()) for r in rows] == EXPECTED
        assert list(rows[0]) == ["id", "name", "price", "note"]
    # the fixed layout: a row per line, a leading comma from the second on
    lines = data.split(b"\n")
    assert lines[1].startswith(b"[" if shape == "arrays" else b"{")
    assert lines[2].startswith(b",")


async def test_ndjson(items, native):
    data = await body(items, ITEMS, format="ndjson", native=native)
    lines = data.split(b"\n")
    assert lines[-1] == b""
    assert [tuple(json.loads(line).values()) for line in lines[:-1]] == EXPECTED


@pytest.mark.parametrize("fmt", ["csv", "tsv", "json", "ndjson"])
async def test_empty_result(items, native, fmt, backend_spec):
    if backend_spec.id == "postgres-psycopg" and not (native and items.export_formats):
        pytest.xfail("psycopg's cursor.stream() reports no description for an empty result")
    stream = items.export(ITEMS.replace("order", "where id > 99 order"), format=fmt, native=native)
    data = await stream.read_all()
    assert stream.row_count == 0
    assert stream.truncated is False
    expected = {
        "csv": b"id,name,price,note\r\n",
        "tsv": b"id\tname\tprice\tnote\r\n",
        "json": b'{"ok":true,"columns":["id","name","price","note"],"rows":[\n],"truncated":false}\n',
        "ndjson": b"",
    }
    assert data == expected[fmt]


@pytest.mark.parametrize("fmt", ["csv", "json", "ndjson"])
async def test_max_rows_truncates(db, native, fmt):
    stream = db.export(series_sql(db, 50), format=fmt, max_rows=7, native=native)
    data = await stream.read_all()
    assert stream.row_count == 7
    assert stream.truncated is True
    if fmt == "json":
        doc = json.loads(data)
        assert doc["truncated"] is True
        assert [r["x"] for r in doc["rows"]] == list(range(1, 8))
    elif fmt == "csv":
        assert data == b"x\r\n" + b"".join(b"%d\r\n" % i for i in range(1, 8))
    else:
        assert len(data.splitlines()) == 7


async def test_max_rows_exact_is_not_truncated(db, native):
    stream = db.export(series_sql(db, 7), format="json", max_rows=7, native=native)
    doc = json.loads(await stream.read_all())
    assert stream.truncated is False and doc["truncated"] is False
    assert len(doc["rows"]) == 7


async def test_max_rows_zero(db, native):
    stream = db.export(series_sql(db, 3), format="csv", max_rows=0, native=native)
    assert await stream.read_all() == b"x\r\n"
    assert stream.truncated is True


async def test_max_rows_stops_an_unbounded_query(db, native, request):
    if not native:
        _no_recursive_cte_streaming(request)
    stream = db.export(INFINITE, format="csv", max_rows=10, native=native)
    data = await asyncio.wait_for(stream.read_all(), 10)
    assert data.count(b"\r\n") == 11
    assert stream.truncated is True


async def test_many_rows_and_chunks(db, native):
    chunks = []
    async with db.export(series_sql(db, 20000), format="csv", chunk_bytes=4096, native=native) as stream:
        async for chunk in stream:
            assert type(chunk) is bytes
            chunks.append(chunk)
    data = b"".join(chunks)
    assert len(chunks) > 3
    assert data == b"x\r\n" + b"".join(b"%d\r\n" % i for i in range(1, 20001))
    assert stream.row_count == 20000
    assert stream.bytes_sent == len(data)


async def test_byte_cap(db, native):
    got = []
    with pytest.raises(ResourceLimitExceeded):
        async with db.export(series_sql(db, 100000), format="csv", max_bytes=10000, chunk_bytes=1024, native=native) as s:
            async for chunk in s:
                got.append(chunk)
    assert sum(map(len, got)) <= 10000
    assert db.stats().in_use_read == 0


def _no_recursive_cte_streaming(request: pytest.FixtureRequest) -> None:
    if "duckdb-cppworker" in request.node.name:
        # Through DuckDB's C API an unbounded recursive CTE yields its first
        # rows only after seconds of work (about 3.6 s here; the duckdb
        # Python package streams them in well under a second), past these
        # tests' time limits. See duckdb-cpp-worker/README.md.
        pytest.xfail("DuckDB's C API streams a recursive CTE's first rows late")


async def test_time_limit_mid_stream(db, native, request):
    _no_recursive_cte_streaming(request)
    chunks = 0
    with pytest.raises(QueryInterrupted):
        async with db.export(INFINITE, format="csv", time_limit_ms=200, native=native) as s:
            async for _ in s:
                chunks += 1
    assert chunks > 0
    assert db.stats().in_use_read == 0
    assert (await db.execute("select 1 as a")).rows == [(1,)]


async def test_time_limit_before_first_row(db, native):
    sql = "select count(*) from (" + INFINITE + ") t"
    with pytest.raises(QueryInterrupted):
        await asyncio.wait_for(body(db, sql, format="csv", time_limit_ms=150, native=native), 20)
    assert (await db.execute("select 2 as a")).rows == [(2,)]


async def test_close_early_releases_connection(db, native):
    async with db.export(INFINITE, format="ndjson", chunk_bytes=1024, native=native) as s:
        async for _ in s:
            assert db.stats().in_use_read == 1
            break
    assert db.stats().in_use_read == 0
    assert (await db.execute("select 3 as a")).rows == [(3,)]


async def test_cancel_mid_stream_releases_connection(db, native):
    started = asyncio.Event()

    async def consume() -> None:
        async with db.export(INFINITE, format="csv", chunk_bytes=1024, native=native) as s:
            async for _ in s:
                started.set()
                await asyncio.sleep(0.01)

    task = asyncio.create_task(consume())
    await asyncio.wait_for(started.wait(), 10)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert db.stats().in_use_read == 0
    assert (await db.execute("select 4 as a")).rows == [(4,)]


async def test_error_before_any_bytes(db, native):
    with pytest.raises(QueryError):
        await body(db, "select * from no_such_table", format="csv", native=native)
    assert db.stats().in_use_read == 0


async def test_writes_are_refused(items, native):
    with pytest.raises(Exception):
        await body(items, "delete from items", format="csv", native=native)
    assert (await items.execute("select count(*) as n from items")).rows == [(5,)]


async def test_params(db, native):
    data = await body(db, "select :a as a, :b as b", {"a": 1, "b": "two"}, format="json", native=native)
    assert json.loads(data)["rows"] == [{"a": 1, "b": "two"}]


async def test_null_and_numbers(db, native):
    await db.create_table("n", {"id": "integer", "i": "integer", "f": "float", "t": "text"}, pk="id")
    await db.insert_rows(
        "n",
        [
            {"id": 1, "i": None, "f": None, "t": None},
            {"id": 2, "i": 9223372036854775807, "f": 0.1, "t": ""},
            {"id": 3, "i": -9223372036854775808, "f": 1e16, "t": "x"},
        ],
    )
    sql = "select i, f, t from n order by id"
    data = await body(db, sql, format="csv", native=native)
    assert data == (
        b'i,f,t\r\n,,\r\n9223372036854775807,0.1,""\r\n-9223372036854775808,1e+16,x\r\n'
    )
    doc = json.loads(await body(db, sql, format="json", json_shape="arrays", native=native))
    assert doc["rows"] == [[None, None, None], [9223372036854775807, 0.1, ""], [-9223372036854775808, 1e16, "x"]]


async def test_non_finite_floats(db, native):
    await db.create_table("f", {"id": "integer", "f": "float"}, pk="id")
    await db.insert_rows("f", [{"id": 1, "f": math.inf}, {"id": 2, "f": -math.inf}, {"id": 3, "f": 1.0}])
    sql = "select f from f order by id"
    assert await body(db, sql, format="csv", native=native) == b"f\r\nInfinity\r\n-Infinity\r\n1.0\r\n"
    # strict JSON: no Infinity literal
    data = await body(db, sql, format="ndjson", native=native)
    assert data == b'{"f":null}\n{"f":null}\n{"f":1.0}\n'
    json.loads(await body(db, sql, format="json", native=native))


@pytest.mark.requires("distinct_blob_type")
async def test_blob(db, native):
    await db.create_table("b", {"id": "integer", "b": "blob"}, pk="id")
    await db.insert_rows("b", [{"id": 1, "b": b"\x00\xffhi"}, {"id": 2, "b": b""}])
    sql = "select b from b order by id"
    assert await body(db, sql, format="csv", native=native) == b'b\r\nAP9oaQ==\r\n""\r\n'
    assert await body(db, sql, format="ndjson", native=native) == (
        b'{"b":{"$base64":true,"encoded":"AP9oaQ=="}}\n{"b":{"$base64":true,"encoded":""}}\n'
    )


async def test_big_value(db, native):
    big = "ab,\"c\n" * 200_000
    await db.create_table("big", {"id": "integer", "t": "text"}, pk="id")
    await db.insert_rows("big", [{"id": 1, "t": big}])
    data = await body(db, "select t from big", format="csv", chunk_bytes=4096, native=native)
    assert csv_rows(data) == [["t"], [big]]
    data = await body(db, "select t from big", format="ndjson", native=native)
    assert json.loads(data) == {"t": big}


@pytest.mark.requires("duplicate_column_names")
async def test_duplicate_column_names(db, native):
    data = await body(db, "select 1 as a, 2 as a, 3 as b", format="ndjson", native=native)
    assert data == b'{"a":1,"b":3}\n'
    data = await body(db, "select 1 as a, 2 as a", format="csv", native=native)
    assert data == b"a,a\r\n1,2\r\n"


async def test_unknown_format(db):
    with pytest.raises(ValueError):
        db.export("select 1", format="xml")
    with pytest.raises(ValueError):
        db.export("select 1", format="json", json_shape="object")
