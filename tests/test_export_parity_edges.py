"""Edge cases of native export against the Python fallback, on the three
native backends, beyond the seeded rows of ``test_export_parity.py``:
NaN and infinities (DuckDB, PostgreSQL), a single huge value, an empty
result, a zero-column result, NULL against the empty string, 64-bit
extremes, statements that cannot be wrapped, and a REAL/FLOAT column.

Each test asserts the parity class the docs table claims, so a change in
either the engine or the fallback shows up here."""

from __future__ import annotations

import csv
import io
import json
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from dblayer import Database

from .conftest import _cworker_available  # type: ignore[attr-defined]

ALL = [("csv", "objects"), ("tsv", "objects"), ("json", "objects"), ("json", "arrays"), ("json", "array"),
       ("ndjson", "objects")]
HUGE = 5 * 1024 * 1024  # one 5 MiB text value
_KINDS: dict[int, str] = {}


async def both(db: Database, sql: str, fmt: str, shape: str = "objects", **kw: object) -> tuple[bytes, bytes]:
    n = db.export(sql, format=fmt, json_shape=shape, **kw)  # type: ignore[arg-type]
    a = await n.read_all()
    assert n.native is True, "expected the backend to serialise"
    b = await db.export(sql, format=fmt, json_shape=shape, native=False, **kw).read_all()  # type: ignore[arg-type]
    return a, b


@pytest.fixture(params=["cworker", "duckdb", "postgres"])
async def ndb(request: pytest.FixtureRequest, tmp_path: Path) -> AsyncIterator[Database]:
    kind = request.param
    backend = None
    if kind == "cworker":
        reason = _cworker_available()
        if reason:
            pytest.skip(str(reason))
        from dblayer.backends.sqlite_cworker import SqliteCWorker

        db = Database(SqliteCWorker(tmp_path / "e.db"))
    elif kind == "duckdb":
        pytest.importorskip("duckdb")
        from dblayer.backends.duckdb import DuckDB

        db = Database(DuckDB(tmp_path / "e.duckdb"))
    else:
        from . import pg

        reason = pg.unavailable_psycopg()
        if reason:
            pytest.skip(reason)
        backend = pg.make("psycopg")
        db = Database(backend)
    _KINDS[id(db)] = kind
    try:
        yield db
    finally:
        await db.close()
        if backend is not None:
            from . import pg

            await pg.drop_schema(backend.schema)


def _kind(db: Database) -> str:
    return _KINDS[id(db)]


@pytest.mark.parametrize("fmt,shape", ALL)
async def test_huge_single_value_identical(ndb, fmt, shape):
    k = _kind(ndb)
    sql = {
        "cworker": f"select 1 as id, replace(hex(zeroblob({HUGE // 2})), '00', 'ab') as v",
        "duckdb": f"select 1 as id, repeat('ab', {HUGE // 2}) as v",
        "postgres": f"select 1 as id, repeat('ab', {HUGE // 2}) as v",
    }[k]
    a, b = await both(ndb, sql, fmt, shape, chunk_bytes=1 << 16)
    assert a == b
    assert len(a) > HUGE


@pytest.mark.parametrize("fmt,shape", ALL)
async def test_null_empty_string_and_64_bit_extremes_identical(ndb, fmt, shape):
    k = _kind(ndb)
    big = "9223372036854775807"
    small = "(-9223372036854775807 - 1)"
    cast = "::bigint" if k == "postgres" else ""
    sql = (
        f"select {big}{cast} as hi, {small}{cast} as lo, null as n, '' as e, ' ' as sp, '\"' as q "
        f"union all select 0{cast}, 0{cast}, null, null, '', ','"
    )
    a, b = await both(ndb, sql, fmt, shape)
    assert a == b
    if fmt == "csv":
        lines = a.split(b"\r\n")
        assert lines[1] == b'9223372036854775807,-9223372036854775808,,"", ,""""'
        assert lines[2] == b'0,0,,,"",","'


@pytest.mark.parametrize("fmt,shape", ALL)
async def test_empty_result_identical(ndb, fmt, shape):
    if _kind(ndb) == "postgres":
        # psycopg's cursor.stream() reports no columns for an empty result,
        # so the fallback writes no header (conformance xfail): native is
        # the more correct one here.
        n = ndb.export("select 1 as a, 'x' as b where false", format=fmt, json_shape=shape)
        body = await n.read_all()
        assert n.native is True
        if fmt == "csv":
            assert body == b"a,b\r\n"
        return
    a, b = await both(ndb, "select 1 as a, 'x' as b where 1 = 0", fmt, shape)
    assert a == b


async def test_not_finite_floats(ndb):
    """NaN and the infinities: CSV/TSV text ``NaN``/``Infinity``/``-Infinity``,
    JSON ``null``. SQLite has no NaN (it stores NULL); 1e999 is infinity."""
    k = _kind(ndb)
    sql = {
        "cworker": "select 1 as id, 1e999 as f union all select 2, -1e999 union all select 3, null",
        "duckdb": "select 1 as id, 'inf'::double as f union all select 2, '-inf'::double "
                  "union all select 3, 'nan'::double order by id",
        "postgres": "select 1 as id, 'Infinity'::float8 as f union all select 2, '-Infinity'::float8 "
                    "union all select 3, 'NaN'::float8 order by id",
    }[k]
    for fmt, shape in ALL:
        a, b = await both(ndb, sql, fmt, shape)
        assert a == b, (fmt, shape, a, b)
    csv_body, _ = await both(ndb, sql, "csv")
    third = "" if k == "cworker" else "NaN"
    assert csv_body == f"id,f\r\n1,Infinity\r\n2,-Infinity\r\n3,{third}\r\n".encode()
    doc = json.loads((await both(ndb, sql, "json"))[0])
    assert [r["f"] for r in doc["rows"]] == [None, None, None]


async def test_float32_column(ndb):
    """REAL / FLOAT (32-bit): identical on PostgreSQL, parse-equal on
    DuckDB (it widens differently from Python's float of the value);
    SQLite's REAL is 64-bit."""
    k = _kind(ndb)
    if k == "cworker":
        pytest.skip("SQLite has no 32-bit float")
    sql = "select 1.1::real as r union all select 3.4028235e38::real union all select 1e-7::real"
    a, b = await both(ndb, sql, "csv")
    if k == "postgres":
        assert a == b
    else:
        ra = [float(x) for x in a.decode().split("\r\n")[1:-1]]
        rb = [float(x) for x in b.decode().split("\r\n")[1:-1]]
        assert ra == pytest.approx(rb, rel=1e-6)
        if a != b:
            print("DuckDB float32 differs in text:", a, b)


async def test_statement_that_cannot_be_wrapped(ndb):
    """PRAGMA / SHOW / EXPLAIN-like statements: still exported, natively on
    the C worker, through the fallback inside the native source elsewhere."""
    k = _kind(ndb)
    sql = {"cworker": "pragma table_info('sqlite_master')", "duckdb": "pragma version",
           "postgres": "show server_version_num"}[k]
    n = ndb.export(sql, format="csv")
    body = await n.read_all()
    assert body.count(b"\r\n") >= 2
    f = await ndb.export(sql, format="csv", native=False).read_all()
    assert body == f


async def test_csv_round_trips_control_characters(ndb):
    k = _kind(ndb)
    if k == "postgres":
        text = "a\tb\nc\rd\"e,f\x01g"  # no NUL in PostgreSQL text
        sql = "select %s as t" % ("'" + text.replace("'", "''") + "'")
        sql = sql.replace("\x01", "' || chr(1) || '")
    elif k == "duckdb":
        sql = "select 'a' || chr(9) || 'b' || chr(10) || 'c' || chr(13) || 'd\"e,f' || chr(1) || 'g' || chr(0) as t"
    else:
        sql = "select 'a' || char(9) || 'b' || char(10) || 'c' || char(13) || 'd\"e,f' || char(1) || 'g' || char(0) as t"
    for fmt in ("csv", "tsv", "json"):
        a, b = await both(ndb, sql, fmt)
        assert a == b, (fmt, a, b)
    a, _ = await both(ndb, sql, "csv")
    (row,) = list(csv.reader(io.StringIO(a.decode(), newline="")))[1:]
    assert row[0].startswith("a\tb\nc\rd\"e,f\x01g")
