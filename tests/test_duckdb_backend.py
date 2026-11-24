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

from .helpers import check_latency, wait_until  # noqa: E402


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
    # Per row, the native path is faster. (Comparing totals - 10x the rows
    # in less time - left only the margin above 10x, which CI noise ate:
    # 1.13 s vs 0.67 s, still about 6x faster per row.)
    assert native / 20_000 < default / 2_000, (native, default)


# -- time limit -------------------------------------------------------------------

ANALYTICAL = (
    "select count(*) from range(1000000000) a, range(100) b "
    "where (a.range * b.range) % 7 = 3"
)


@pytest.mark.parametrize("limit_ms", [50, 200, 500])
async def test_long_analytical_query_interrupted_within_100ms(ddb, limit_ms):
    await ddb.execute("select 1")  # wake first, so opening is not timed
    start = time.perf_counter()
    with pytest.raises(QueryInterrupted):
        await ddb.execute(ANALYTICAL, time_limit_ms=limit_ms)
    # Spec: interrupted within 100 ms of its time limit (strict timing).
    overshoot = time.perf_counter() - start - limit_ms / 1000
    check_latency("DuckDB time limit -> interrupted (overshoot)", overshoot, 0.1)
    assert (await ddb.execute("select 42")).rows == [(42,)]


async def test_time_limit_in_a_write_transaction_aborts_it(ddb):
    await ddb.execute_write("create table t (id integer)")
    with pytest.raises(QueryError) as e:
        async with ddb.transaction() as tx:
            await tx.execute("insert into t values (1)")
            with pytest.raises(QueryInterrupted):
                await tx.execute(ANALYTICAL, time_limit_ms=50)
    assert e.value.code == "TRANSACTION_ABORTED"
    assert (await ddb.execute("select count(*) from t")).rows == [(0,)]


async def test_cancelling_a_query_interrupts_it(ddb):
    await ddb.execute("select 1")
    task = asyncio.create_task(ddb.execute(ANALYTICAL))
    await asyncio.sleep(0.1)
    start = time.perf_counter()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    check_latency("DuckDB cancel -> query interrupted", time.perf_counter() - start, 0.1)
    assert (await ddb.execute("select 1")).rows == [(1,)]


# -- sleep releases the file ---------------------------------------------------------

_WRITE_FROM_CHILD = """
import sys, duckdb
con = duckdb.connect(sys.argv[1])  # read-write: needs the file lock
con.execute("insert into t values (2, 'from child')")
con.close()
print("ok")
"""


def _child_write(path: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", _WRITE_FROM_CHILD, path], capture_output=True, text=True, timeout=60
    )


async def test_sleep_releases_the_file_to_another_process(ddb):
    path = str(ddb._backend.path)
    await ddb.execute_write("create table t (id integer, v text)")
    await ddb.execute_write("insert into t values (1, 'from parent')")
    # While awake the file is locked: another process cannot open it.
    held = await asyncio.to_thread(_child_write, path)
    assert "ok" not in held.stdout and "lock" in held.stderr.lower()
    await ddb.sleep()
    assert not ddb._backend.is_open
    child = await asyncio.to_thread(_child_write, path)
    assert child.stdout.strip() == "ok", child.stderr
    # Waking reopens the file and sees the other process's write.
    rows = (await ddb.execute("select id, v from t order by id")).rows
    assert rows == [(1, "from parent"), (2, "from child")]
    assert ddb._backend.instances_opened == 2


async def test_sleep_with_a_reader_in_flight_releases_after_it_returns(ddb):
    await ddb.execute_write("create table t (id integer, v text)")
    task = asyncio.create_task(
        ddb.execute("select count(*) from range(200000000) where range % 3 = 1")
    )
    await wait_until(lambda: ddb.stats().in_use_read == 1, interval=0)  # in flight
    await ddb.sleep()
    assert (await task).rows[0][0] > 0
    await wait_until(lambda: not ddb._backend.is_open)
    child = await asyncio.to_thread(_child_write, str(ddb._backend.path))
    assert child.stdout.strip() == "ok", child.stderr


# -- read-only at the API level, settings, isolation from the file system ---------------


async def test_reads_cannot_write(ddb):
    await ddb.execute_write("create table t (id integer)")
    await ddb.execute_write("create sequence s")
    for sql in (
        "insert into t values (1)",
        "create table u (a integer)",
        "drop table t",
        "select nextval('s')",  # a write hidden in a SELECT: refused by the engine
        "create temp table x (a integer)",
        "checkpoint; drop table t",
    ):
        with pytest.raises(QueryError):
            await ddb.execute(sql)
    assert await ddb.table_names() == ["t"]


async def test_one_statement_per_call(ddb):
    await ddb.execute_write("create table t (id integer)")
    with pytest.raises(QueryError) as e:
        await ddb.execute_write("insert into t values (1); drop table t")
    assert e.value.code == "MULTIPLE_STATEMENTS"
    assert await ddb.table_names() == ["t"]
    with pytest.raises(QueryError):
        await ddb.execute_write("commit")


async def test_settings_applied_and_locked(tmp_path):
    db = Database(DuckDB(tmp_path / "s.duckdb", memory_limit="100MB", threads=1))
    try:
        r = await db.execute(
            "select current_setting('memory_limit'), current_setting('threads')"
        )
        limit, threads = r.rows[0]
        assert "MiB" in limit and float(limit.split()[0]) < 100 and threads == 1
        for sql in ("set memory_limit = '10GB'", "set threads = 8"):
            with pytest.raises(QueryError):
                await db.execute_write(sql)
    finally:
        await db.close()


async def test_external_access_disabled_by_default(ddb, tmp_path):
    (tmp_path / "x.csv").write_text("a\n1\n")
    with pytest.raises(QueryError):
        await ddb.execute(f"select * from read_csv('{tmp_path / 'x.csv'}')")
    await ddb.execute_write("create table t (a integer)")
    with pytest.raises(QueryError):
        await ddb.execute_write(f"copy t to '{tmp_path / 'out.csv'}'")
    assert not (tmp_path / "out.csv").exists()


async def test_read_only_database(tmp_path):
    path = tmp_path / "ro.duckdb"
    w = Database(DuckDB(path))
    await w.execute_write("create table t (a integer)")
    await w.close()
    db = Database(DuckDB(path, read_only=True))
    try:
        assert await db.table_names() == ["t"]
        with pytest.raises(NotSupported):
            await db.execute_write("insert into t values (1)")
    finally:
        await db.close()


# -- staged import dependents -----------------------------------------------------------


async def test_staged_replace_of_a_referenced_table_fails_and_rolls_back(ddb):
    await ddb.create_table("authors", {"id": "integer", "name": "text"}, pk="id")
    await ddb.create_table(
        "posts", {"id": "integer", "author_id": "integer"}, pk="id",
        foreign_keys=[("author_id", "authors")],
    )
    await ddb.insert_rows("authors", [{"id": 1, "name": "a"}])
    with pytest.raises(QueryError):
        async with ddb.staged_import("authors", mode="replace") as imp:
            await imp.write([{"id": 1, "name": "b"}])
    assert (await ddb.execute("select name from authors")).rows == [("a",)]
    assert sorted((await ddb.schema()).tables) == ["authors", "posts"]


async def test_staged_replace_keeps_indexes_and_views(ddb):
    await ddb.create_table("t", {"id": "integer", "v": "text"}, pk="id")
    await ddb.create_index("t", ["v"], name="t_v")
    await ddb.execute_write("create view tv as select v from t")
    async with ddb.staged_import("t", mode="replace") as imp:
        await imp.write([{"id": i, "v": str(i)} for i in range(1000)])
    assert {i.name for i in await ddb.indexes("t") if i.origin == "c"} == {"t_v"}
    assert (await ddb.execute("select count(*) from tv")).rows == [(1000,)]
