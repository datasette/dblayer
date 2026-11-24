"""Behaviour specific to the DuckDB backend (the spec's "must demonstrate"
items and DuckDB-only rules). Not parametrised: these construct DuckDB
directly. Skipped without the duckdb extra."""

from __future__ import annotations

import asyncio
import subprocess
import sys
import time

import pytest

from dblayer import Database, IntegrityError, NotSupported, QueryError, QueryInterrupted

pytest.importorskip("duckdb")

from dblayer.backends.duckdb import DuckDB  # noqa: E402


@pytest.fixture
async def ddb(tmp_path):
    backend = DuckDB(tmp_path / "data.duckdb")
    db = Database(backend, name="data")
    yield db
    await db.close()


# -- native insert_rows ---------------------------------------------------------


async def test_insert_rows_uses_the_appender(ddb):
    backend = ddb._backend
    await ddb.create_table("t", {"id": "integer", "name": "text"}, pk="id")
    r = await ddb.insert_rows("t", [{"id": i, "name": f"n{i}"} for i in range(250)])
    assert r.rowcount == 250
    assert backend.native_calls["insert_rows"] == 3  # three batches of <= 100
    assert (await ddb.execute("select count(*), sum(id) from t")).rows[0] == (250, sum(range(250)))


async def test_native_insert_rows_creates_and_alters(ddb):
    r = await ddb.insert_rows("t", [{"id": 1, "a": "x"}], pk="id")
    assert r.table_created
    r = await ddb.insert_rows("t", [{"id": 2, "a": "y", "b": 2.5}], alter=True)
    assert r.columns_added == ("b",) and r.rowcount == 1
    assert (await ddb.execute("select * from t order by id")).rows == [(1, "x", None), (2, "y", 2.5)]


async def test_native_insert_rows_errors_and_rollback(ddb):
    await ddb.create_table("t", {"id": "integer"}, pk="id")
    with pytest.raises(IntegrityError):
        await ddb.insert_rows("t", [{"id": 1}, {"id": 1}])
    assert (await ddb.execute("select count(*) from t")).rows[0][0] == 0
    with pytest.raises(QueryError):
        await ddb.insert_rows("t", [{"id": "not a number"}])
    # The connection is usable afterwards.
    await ddb.insert_rows("t", [{"id": 5}])
    assert (await ddb.execute("select id from t")).rows == [(5,)]


async def test_native_insert_rows_is_fast(ddb):
    await ddb.create_table("t", {"id": "integer", "name": "text"}, pk="id")
    rows = [{"id": i, "name": f"n{i}"} for i in range(20_000)]
    start = time.perf_counter()
    await ddb.insert_rows("t", rows, batch_size=10_000)
    native = time.perf_counter() - start
    slow = Database(ddb._backend, native_operations=False)
    start = time.perf_counter()
    await slow.insert_rows("t", [{"id": i + 20_000, "name": "x"} for i in range(2_000)], batch_size=1000)
    default = time.perf_counter() - start
    await slow.close()
    # 10x the rows in well under the time of the executemany path.
    assert native < default, (native, default)
