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
