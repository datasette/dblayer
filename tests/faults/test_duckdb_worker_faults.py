"""Hard kill, memory limits and crashes for DuckDB in a worker process."""

from __future__ import annotations

import asyncio
import os
import signal
import time

import pytest

pytest.importorskip("duckdb")

from dblayer import Database, QueryInterrupted, ResourceLimitExceeded, WorkerCrashed  # noqa: E402
from dblayer.backends.duckdb_cppworker import DuckDBCppWorker, binary_unavailable  # noqa: E402
from dblayer.backends.duckdb_worker import DuckDBWorker  # noqa: E402

from ..scale.harness import current_rss  # noqa: E402
from .conftest import LINUX, linux_only  # noqa: E402

UNBOUNDED = "with recursive c(x) as (select 1 union all select x + 1 from c) select count(*) from c"


@pytest.fixture(params=["duckdb-worker", "duckdb-cppworker", "duckdb-rsworker"])
def Worker(request: pytest.FixtureRequest) -> type[DuckDBWorker]:
    """The Python DuckDB worker, and the C++ and Rust ones (skipped when not built)."""
    if request.param == "duckdb-rsworker":
        from dblayer.backends import duckdb_rsworker

        reason = duckdb_rsworker.binary_unavailable()
        if reason:
            pytest.skip(reason)
        return duckdb_rsworker.DuckDBRustWorker
    if request.param == "duckdb-cppworker":
        reason = binary_unavailable()
        if reason:
            pytest.skip(reason)
        return DuckDBCppWorker
    return DuckDBWorker


async def _setup(db: Database) -> None:
    await db.execute_write("create table t as select range i from range(1000)")


async def test_time_limit_interrupts_without_a_kill(tmp_path, Worker):
    b = Worker(tmp_path / "a.duckdb")
    db = Database(b)
    try:
        await _setup(db)
        pid = b.pid
        t0 = time.perf_counter()
        with pytest.raises(QueryInterrupted):
            await db.execute(UNBOUNDED, time_limit_ms=100)
        elapsed = time.perf_counter() - t0
        print(f"\n[duckdb-worker] 100 ms limit: QueryInterrupted after {elapsed * 1000:.0f} ms")
        assert elapsed < 5.0  # generous; the engine's own interrupt lands in ms
        assert b.pid == pid  # DuckDB's interrupt worked: no kill needed
        assert (await db.execute("select count(*) from t")).rows == [(1000,)]
    finally:
        await db.close()


async def test_worker_that_ignores_its_time_limit_is_killed_and_replaced(tmp_path, Worker):
    """A request that blocks the whole worker (the test-only ``sleep``
    debug message: nothing in the worker can interrupt it) is killed at
    its limit plus the grace; the database then works on a new process,
    which can open the file because the kill released DuckDB's lock."""
    b = Worker(tmp_path / "k.duckdb", debug=True, kill_grace_ms=200)
    db = Database(b)
    try:
        await _setup(db)
        proc = b.placement_group.process
        assert proc is not None
        pid = proc.pid
        t0 = time.perf_counter()
        with pytest.raises(QueryInterrupted):
            await proc.call({"type": "_debug", "action": "sleep", "seconds": 30}, owner=None, limit_ms=100)
        elapsed = time.perf_counter() - t0
        print(f"\n[duckdb-worker] wedged worker killed after {elapsed * 1000:.0f} ms (limit 100 + grace 200)")
        assert elapsed < 10.0
        assert not proc.alive
        assert (await db.execute("select count(*) from t")).rows == [(1000,)]
        assert b.pid not in (None, pid)
    finally:
        await db.close()


async def test_external_kill_mid_transaction_loses_only_uncommitted_rows(tmp_path, Worker):
    b = Worker(tmp_path / "x.duckdb")
    db = Database(b)
    try:
        await _setup(db)
        with pytest.raises(WorkerCrashed):
            async with db.transaction() as tx:
                await tx.execute("insert into t select range + 1000 from range(500)")
                assert b.pid is not None
                os.kill(b.pid, signal.SIGKILL)
                await asyncio.sleep(0.2)
                await tx.execute("select 1")
        # Committed rows are there, uncommitted ones are not; the WAL (if
        # any) is replayed by the new process.
        assert (await db.execute("select count(*) from t")).rows == [(1000,)]
        await db.execute_write("insert into t values (5000)")
        assert (await db.execute("select count(*) from t")).rows == [(1001,)]
    finally:
        await db.close()


async def test_idle_worker_killed_from_outside_is_replaced(tmp_path, Worker):
    b = Worker(tmp_path / "i.duckdb")
    db = Database(b, idle_timeout=None)
    try:
        await _setup(db)
        pid = b.pid
        assert pid is not None
        os.kill(pid, signal.SIGKILL)
        await asyncio.sleep(0.2)
        assert (await db.execute("select count(*) from t")).rows == [(1000,)]
        assert b.pid not in (None, pid)
    finally:
        await db.close()


async def test_duckdb_memory_limit_applies_inside_the_worker(tmp_path, Worker):
    """DuckDB's own ``memory_limit`` still works in the worker: the query
    fails with ResourceLimitExceeded and the worker carries on."""
    b = Worker(tmp_path / "m.duckdb", memory_limit="64MB")
    db = Database(b)
    try:
        await _setup(db)
        pid = b.pid
        with pytest.raises(ResourceLimitExceeded):
            await db.execute("select length(list(range)::varchar) from range(20000000)")
        assert b.pid == pid
        assert (await db.execute("select count(*) from t")).rows == [(1000,)]
    finally:
        await db.close()


def test_memory_limit_capability(tmp_path, Worker):
    caps = Worker(tmp_path / "c.duckdb").capabilities
    assert caps.hard_kill
    assert caps.memory_limit  # DuckDB's own memory_limit, everywhere
    assert Worker(tmp_path / "c.duckdb", memory_limit=None).capabilities.memory_limit is LINUX


@linux_only
async def test_rlimit_as_bounds_the_whole_worker(tmp_path, Worker):
    """With DuckDB's own limit set high, RLIMIT_AS on the worker stops an
    allocation the engine would have allowed. The worker fails the query
    (or dies and is replaced); the parent's memory is untouched."""
    b = Worker(tmp_path / "r.duckdb", memory_limit="16GB", memory_limit_mb=768)
    db = Database(b)
    try:
        await _setup(db)
        proc = b.placement_group.process
        assert proc is not None and proc.hello.get("rlimit_as") is True
        rss_before = current_rss()
        with pytest.raises((ResourceLimitExceeded, WorkerCrashed)) as e:
            await db.execute("select length(list(range)::varchar) from range(300000000)")
        print(f"\n[duckdb-worker] over RLIMIT_AS: {type(e.value).__name__}: {str(e.value)[:200]}")
        rss_after = current_rss()
        if rss_before is not None and rss_after is not None:
            assert rss_after - rss_before < 100 * 1024 * 1024
        assert (await db.execute("select count(*) from t")).rows == [(1000,)]
    finally:
        await db.close()
