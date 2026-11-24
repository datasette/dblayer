"""Native export against the Python fallback, per backend (see the parity
table in ``docs/export-formats.md``).

* C SQLite worker: byte-identical in every format, on every transport and
  both codecs.
* DuckDB and PostgreSQL: see their sections below.
"""

from __future__ import annotations

import csv
import io
import json
import math
import random
import struct
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest

from dblayer import Database, QueryError

from .conftest import _cworker_available  # type: ignore[attr-defined]

FORMATS = [
    ("csv", "objects"),
    ("tsv", "objects"),
    ("json", "objects"),
    ("json", "arrays"),
    ("json", "array"),
    ("ndjson", "objects"),
]


def _floats(rng: random.Random, n: int) -> list[float]:
    special = [0.0, -0.0, 1.0, -1.0, 0.1, 1e15, 1e16, 1e-4, 1e-5, 1234567890123456.0, 12345678901234567.0,
               1.7976931348623157e308, 5e-324, 2.5e-7, 100000.0, 1 / 3, math.inf, -math.inf, 123.456]
    out = list(special)
    for _ in range(n):
        out.append(struct.unpack("<d", rng.getrandbits(64).to_bytes(8, "little"))[0])
        out.append(rng.uniform(-1e6, 1e6))
        out.append(round(rng.uniform(-1000, 1000), rng.randint(0, 6)))
    return [f for f in out if not math.isnan(f)]  # SQLite stores NaN as NULL


TEXTS = ["", " ", "plain", "a,b", 'q"q', "tab\tx", "nl\nx", "cr\rx", "crlf\r\n", "\\", "é☃𝄞", "\x00nul",
         "\x01\x1f\x7f", "\b\f", "long " * 300, " ", '"', ",", "\t"]


def rows(seed: int = 1, n: int = 400) -> list[tuple[Any, ...]]:
    rng = random.Random(seed)
    fl = _floats(rng, n)
    out = []
    for i in range(n):
        out.append((
            i,
            rng.choice([None, rng.randint(-2**63, 2**63 - 1), rng.randint(-1000, 1000), 2**53 + 1]),
            fl[i % len(fl)],
            rng.choice(TEXTS + [None]),
            rng.choice([None, b"", b"\x00\xff", bytes(rng.getrandbits(8) for _ in range(rng.randint(1, 40)))]),
            # a column whose type changes from row to row (SQLite)
            rng.choice([None, 7, 2.5, "text", b"blob", ""]),
        ))
    return out


async def fill(db: Database, data: list[tuple[Any, ...]]) -> None:
    await db.execute_write("create table t (id integer primary key, i integer, f real, s text, b blob, m)")
    async with db.transaction() as tx:
        await tx.execute_many("insert into t values (?, ?, ?, ?, ?, ?)", data)


SQL = "select id, i, f, s, b, m, s as s, i as i2 from t order by id"

needs_cworker = pytest.mark.skipif(bool(_cworker_available()), reason=str(_cworker_available()))


@pytest.fixture(params=["raw", "frames", "fifo"])
def transport(request: pytest.FixtureRequest) -> str:
    return request.param


@pytest.fixture(params=["msgpack", "json"])
def codec(request: pytest.FixtureRequest) -> str:
    return request.param


@pytest.fixture
async def cdb(tmp_path: Path, transport: str, codec: str) -> AsyncIterator[Database]:
    from dblayer.backends.sqlite_cworker import SqliteCWorker

    db = Database(SqliteCWorker(tmp_path / "p.db", export_transport=transport, codec=codec))
    await fill(db, rows())
    yield db
    await db.close()


@needs_cworker
@pytest.mark.parametrize("fmt,shape", FORMATS)
async def test_cworker_identical(cdb, fmt, shape):
    native = cdb.export(SQL, format=fmt, json_shape=shape, chunk_bytes=3000)
    a = await native.read_all()
    assert native.native is True
    fallback = cdb.export(SQL, format=fmt, json_shape=shape, native=False)
    b = await fallback.read_all()
    assert fallback.native is False
    assert a == b
    assert native.row_count == fallback.row_count == 400
    if fmt in ("json", "ndjson"):
        for line in a.split(b"\n"):
            line = line.strip(b",[]")
            if line.startswith(b"{") and line.endswith(b"}"):
                json.loads(line)
        if fmt == "json":
            json.loads(a)


@needs_cworker
@pytest.mark.parametrize("max_rows", [0, 1, 399, 400, 401])
async def test_cworker_max_rows_identical(cdb, max_rows):
    for fmt in ("csv", "json"):
        n = cdb.export(SQL, format=fmt, max_rows=max_rows)
        f = cdb.export(SQL, format=fmt, max_rows=max_rows, native=False)
        assert await n.read_all() == await f.read_all()
        assert (n.row_count, n.truncated) == (f.row_count, f.truncated) == (min(max_rows, 400), max_rows < 400)


@needs_cworker
async def test_cworker_invalid_utf8_is_an_error_in_both(cdb):
    await cdb.execute_write("create table bad (t text)")
    await cdb.execute_write("insert into bad values (cast(x'ff41' as text))")
    for native in (True, False):
        with pytest.raises(QueryError, match="Could not decode to UTF-8"):
            await cdb.export("select t from bad", format="csv", native=native).read_all()


@needs_cworker
async def test_cworker_csv_parses_back(cdb):
    data = await cdb.export(SQL, format="csv").read_all()
    csv.field_size_limit(1 << 30)
    parsed = list(csv.reader(io.StringIO(data.decode(), newline="")))
    assert len(parsed) == 401


# -- DuckDB ---------------------------------------------------------------------------


@pytest.fixture
async def ddb(tmp_path: Path) -> AsyncIterator[Database]:
    pytest.importorskip("duckdb")
    from dblayer.backends.duckdb import DuckDB

    db = Database(DuckDB(tmp_path / "p.duckdb"))
    await db.execute_write(
        "create table t (id bigint primary key, i bigint, f double, s varchar, b blob, ok boolean, h hugeint)"
    )
    data = [(r[0], r[1], r[2], r[3], r[4], [None, True, False][r[0] % 3], (r[1] or 0) * 1000) for r in rows(2)]
    async with db.transaction() as tx:
        await tx.execute_many("insert into t values (?, ?, ?, ?, ?, ?, ?)", data)
    await db.execute_write(
        "create table x (id bigint, d date, ts timestamp, tz timestamptz, dec decimal(12, 3), u uuid, "
        "fl float, l bigint[], st struct(a int, b varchar))"
    )
    await db.execute_write(
        "insert into x values "
        "(1, date '2024-01-02', timestamp '2024-01-02 03:04:05', timestamptz '2024-01-02 03:04:05+00', 1.5, "
        "'12345678-1234-1234-1234-123456789abc', 1.1, [1, 2], {'a': 1, 'b': 'x'}), "
        "(2, date '1999-12-31', timestamp '2024-01-02 03:04:05.5', timestamptz '2024-06-02 03:04:05.123456+02', "
        "-0.001, null, -0.0, [], null), "
        "(3, null, null, null, null, null, null, null, null)"
    )
    yield db
    await db.close()


DSQL = "select id, i, f, s, b, ok, h, s as s, i as i2 from t order by id"


@pytest.mark.parametrize("fmt,shape", FORMATS)
async def test_duckdb_identical_core_types(ddb, fmt, shape):
    a = ddb.export(DSQL, format=fmt, json_shape=shape, chunk_bytes=3000)
    na = await a.read_all()
    assert a.native is True
    nb = await ddb.export(DSQL, format=fmt, json_shape=shape, native=False).read_all()
    assert na == nb


@pytest.mark.parametrize("max_rows", [0, 1, 399, 400, 401])
async def test_duckdb_max_rows_identical(ddb, max_rows):
    for fmt in ("csv", "json"):
        n = ddb.export(DSQL, format=fmt, max_rows=max_rows)
        f = ddb.export(DSQL, format=fmt, max_rows=max_rows, native=False)
        assert await n.read_all() == await f.read_all()
        assert (n.row_count, n.truncated) == (f.row_count, f.truncated)


def _cells_equal(a: str, b: str) -> bool:
    """Type-aware comparison of two text forms."""
    import datetime as dt
    from decimal import Decimal, InvalidOperation

    if a == b:
        return True
    for parse in (float, Decimal):
        try:
            return parse(a) == parse(b)
        except (ValueError, InvalidOperation):
            pass
    try:
        return dt.datetime.fromisoformat(a) == dt.datetime.fromisoformat(b)
    except ValueError:
        return False


XSQL = "select id, d, ts, tz, dec, u, fl from x order by id"


@pytest.mark.parametrize("fmt", ["csv", "tsv"])
async def test_duckdb_other_types_parse_equal(ddb, fmt):
    delim = "," if fmt == "csv" else "\t"
    na = await ddb.export(XSQL, format=fmt).read_all()
    nb = await ddb.export(XSQL, format=fmt, native=False).read_all()
    ra = list(csv.reader(io.StringIO(na.decode(), newline=""), delimiter=delim))
    rb = list(csv.reader(io.StringIO(nb.decode(), newline=""), delimiter=delim))
    assert len(ra) == len(rb) == 4
    for x, y in zip(ra, rb):
        assert all(_cells_equal(p, q) for p, q in zip(x, y)), (x, y)


async def test_duckdb_other_types_json_parse_equal(ddb):
    na = json.loads(await ddb.export(XSQL, format="json").read_all())
    nb = json.loads(await ddb.export(XSQL, format="json", native=False).read_all())
    for x, y in zip(na["rows"], nb["rows"]):
        for k in x:
            p, q = x[k], y[k]
            assert p == q or _cells_equal(str(p), str(q)), (k, p, q)


@pytest.fixture
async def pgdb() -> AsyncIterator[Database]:
    from . import pg

    reason = pg.unavailable_psycopg()
    if reason:
        pytest.skip(reason)
    backend = pg.make("psycopg")
    db = Database(backend)
    try:
        await db.execute_write(
            "create table t (id bigint primary key, i bigint, f float8, s text, b bytea, ok boolean, r real)"
        )
        data = []
        for r in rows(3):
            s = r[3].replace("\x00", "") if r[3] is not None else None  # PostgreSQL text has no NUL
            data.append((r[0], r[1], r[2], s, r[4], [None, True, False][r[0] % 3], 1.1 if r[0] % 2 else None))
        async with db.transaction() as tx:
            await tx.execute_many("insert into t values (:a, :b, :c, :d, :e, :f::boolean, :g)",
                                  [dict(zip("abcdefg", d)) for d in data])
        await db.execute_write(
            "create table x (id bigint, d date, ts timestamp, tz timestamptz, dec numeric(12, 3), u uuid, "
            "l bigint[], j jsonb)"
        )
        await db.execute_write(
            "insert into x values "
            "(1, date '2024-01-02', timestamp '2024-01-02 03:04:05', timestamptz '2024-01-02 03:04:05+00', 1.5, "
            "'12345678-1234-1234-1234-123456789abc', array[1, 2], '{\"a\": 1}'), "
            "(2, date '1999-12-31', timestamp '2024-01-02 03:04:05.5', "
            "timestamptz '2024-06-02 03:04:05.123456+02', -0.001, null, '{}', '[]'), "
            "(3, null, null, null, null, null, null, null)"
        )
        yield db
    finally:
        await db.close()
        await pg.drop_schema(backend.schema)


PSQL = "select id, i, f, s, b, ok, r, s as s, i as i2 from t order by id"


@pytest.mark.parametrize("fmt,shape", FORMATS)
async def test_postgres_identical_core_types(pgdb, fmt, shape):
    a = pgdb.export(PSQL, format=fmt, json_shape=shape, chunk_bytes=3000)
    na = await a.read_all()
    assert a.native is True
    nb = await pgdb.export(PSQL, format=fmt, json_shape=shape, native=False).read_all()
    assert na == nb


@pytest.mark.parametrize("max_rows", [0, 1, 399, 400, 401])
async def test_postgres_max_rows_identical(pgdb, max_rows):
    for fmt in ("csv", "json"):
        n = pgdb.export(PSQL, format=fmt, max_rows=max_rows)
        f = pgdb.export(PSQL, format=fmt, max_rows=max_rows, native=False)
        assert await n.read_all() == await f.read_all()
        assert (n.row_count, n.truncated) == (f.row_count, f.truncated)


async def test_postgres_params_are_inlined(pgdb):
    sql = "select id, s from t where id < :n and coalesce(s, '') <> :x order by id"
    params = {"n": 50, "x": "it's '$1' -- not a param"}
    for fmt in ("csv", "json"):
        n = await pgdb.export(sql, params, format=fmt).read_all()
        f = await pgdb.export(sql, params, format=fmt, native=False).read_all()
        assert n == f


@pytest.mark.parametrize("fmt", ["csv", "tsv"])
async def test_postgres_other_types_parse_equal(pgdb, fmt):
    delim = "," if fmt == "csv" else "\t"
    sql = "select id, d, ts, tz, dec, u from x order by id"
    na = await pgdb.export(sql, format=fmt).read_all()
    nb = await pgdb.export(sql, format=fmt, native=False).read_all()
    ra = list(csv.reader(io.StringIO(na.decode(), newline=""), delimiter=delim))
    rb = list(csv.reader(io.StringIO(nb.decode(), newline=""), delimiter=delim))
    assert len(ra) == len(rb) == 4
    for x, y in zip(ra, rb):
        assert all(_cells_equal(p, q) for p, q in zip(x, y)), (x, y)


async def test_postgres_other_types_json_parse_equal(pgdb):
    sql = "select id, d, ts, tz, dec, u, l, j from x order by id"
    na = json.loads(await pgdb.export(sql, format="json").read_all())
    nb = json.loads(await pgdb.export(sql, format="json", native=False).read_all())
    for x, y in zip(na["rows"], nb["rows"]):
        for k in x:
            p, q = x[k], y[k]
            assert p == q or _cells_equal(str(p), str(q)), (k, p, q)


async def test_duckdb_nested_types_are_valid_json(ddb):
    for native in (True, False):
        doc = json.loads(await ddb.export("select l, st from x order by id", format="json", native=native).read_all())
        assert doc["rows"][0]["l"] == [1, 2]
        assert doc["rows"][0]["st"] == {"a": 1, "b": "x"}
