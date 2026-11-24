"""``SqliteCPool`` (``dblayer.backends.sqlite_cpool``): many databases on a
small pool of multi-tenant C worker processes. Placement, what a new
database costs on a warm process, and proof that requests for different
databases really run at the same time.

Timing assertions are conditions with generous bounds; figures are printed
(``-s``)."""

from __future__ import annotations

import asyncio
import os
import time

import pytest

from dblayer import Database, ResourceLimitExceeded
from dblayer.backends.sqlite_cworker import SqliteCWorker, binary_unavailable, c_shared_workers
from dblayer.backends.sqlite_worker import WorkerProcess

UNAVAILABLE = binary_unavailable()
pytestmark = pytest.mark.skipif(UNAVAILABLE is not None, reason=UNAVAILABLE or "")

COUNTING = "with recursive c(x) as (select 1 union all select x + 1 from c where x < 2000000) select count(*) from c"


@pytest.fixture
def pool():
    from dblayer.backends.sqlite_cpool import CPool

    p = CPool(2, linger=0)
    yield p
    p.close()


def make(pool, path):
    from dblayer.backends.sqlite_cpool import SqliteCPool

    return Database(SqliteCPool(path, pool=pool))


async def test_slow_queries_on_different_databases_run_in_parallel(tmp_path, pool):
    """Eight CPU-bound queries on eight databases sharing one process take
    about as long as one; on the single-tenant shared placement (one
    request at a time per process) they take eight times as long."""
    from dblayer.backends.sqlite_cpool import CPool

    one_proc = CPool(1, linger=0)
    dbs = [make(one_proc, tmp_path / f"p{i}.db") for i in range(8)]
    group = c_shared_workers(1, linger=0)
    serial = [Database(SqliteCWorker(tmp_path / f"s{i}.db", workers=group)) for i in range(8)]
    try:
        for d in dbs + serial:
            await d.execute("select 1")
        assert len(one_proc.processes()) == 1
        t0 = time.perf_counter()
        await dbs[0].execute(COUNTING)
        one = time.perf_counter() - t0
        t0 = time.perf_counter()
        results = await asyncio.gather(*(d.execute(COUNTING) for d in dbs))
        eight = time.perf_counter() - t0
        assert all(r.rows == [(2_000_000,)] for r in results)
        t0 = time.perf_counter()
        await asyncio.gather(*(d.execute(COUNTING) for d in serial))
        eight_serial = time.perf_counter() - t0
        print(
            f"\n[cpool] one query {one * 1000:.0f} ms; eight on eight databases, one multi-tenant process "
            f"{eight * 1000:.0f} ms; eight on one single-tenant shared process {eight_serial * 1000:.0f} ms "
            f"(load {os.getloadavg()[0]:.1f}, {os.cpu_count()} CPUs)"
        )
        assert eight < 4 * one  # one at a time would be 8x
        assert eight_serial > 4 * one
    finally:
        for d in dbs + serial:
            await d.close()
        one_proc.close()
        group.close()


async def test_other_databases_are_served_while_one_runs_a_long_query(tmp_path, pool):
    a, b = make(pool, tmp_path / "a.db"), make(pool, tmp_path / "b.db")
    unbounded = "with recursive c(x) as (select 1 union all select x + 1 from c) select count(*) from c"
    try:
        await a.execute("select 1")
        await b.execute("select 1")
        slow = asyncio.create_task(a.execute(unbounded, time_limit_ms=1500))
        await asyncio.sleep(0.05)
        times = []
        for i in range(50):
            t0 = time.perf_counter()
            assert (await b.execute("select :i", {"i": i})).rows == [(i,)]
            times.append(time.perf_counter() - t0)
        assert not slow.done()
        times.sort()
        print(f"\n[cpool] round trip on b while a runs a long query: median {times[25] * 1e6:.0f} us, max {times[-1] * 1e6:.0f} us")
        assert times[25] < 0.1
        slow.cancel()
        with pytest.raises(asyncio.CancelledError):
            await slow
    finally:
        await a.close()
        await b.close()


async def test_a_new_database_on_a_warm_process_costs_one_open(tmp_path):
    from dblayer.backends.sqlite_cpool import CPool

    pool = CPool(1, linger=0)
    first = make(pool, tmp_path / "first.db")
    await first.execute("select 1")  # starts a process
    proc = pool.processes()[0]
    rec: list = []
    proc.recorder = rec
    started = pool.stats.processes_started
    second = make(pool, tmp_path / "second.db")
    t0 = time.perf_counter()
    assert (await second.execute("select 2")).rows == [(2,)]
    elapsed = time.perf_counter() - t0
    try:
        assert pool.stats.processes_started == started
        assert [m["type"] for d, m in rec if d == "->"] == ["open", "execute"]
        print(f"\n[cpool] new database on a warm pool: first result in {elapsed * 1000:.2f} ms")
    finally:
        await first.close()
        await second.close()
        pool.close()


async def test_many_databases_share_size_processes(tmp_path, pool):
    dbs = [make(pool, tmp_path / f"m{i}.db") for i in range(20)]
    try:
        await asyncio.gather(*(d.execute_write("create table t (x)") for d in dbs))
        await asyncio.gather(*(d.execute("select count(*) from t") for d in dbs))
        procs = pool.processes()
        assert len(procs) == 2
        assert sum(p.handles for p in procs) == 40  # a read and a write connection each
        assert abs(procs[0].handles - procs[1].handles) <= 2
    finally:
        for d in dbs:
            await d.close()
    assert pool.processes() == []  # linger=0: they exit with their last connection


async def test_connection_cap_per_process(tmp_path):
    from dblayer.backends.sqlite_cpool import CPool

    pool = CPool(1, linger=0, max_connections=2, max_processes=1)
    dbs = [make(pool, tmp_path / f"c{i}.db") for i in range(2)]
    try:
        await dbs[0].execute("select 1")
        await dbs[1].execute("select 1")
        third = make(pool, tmp_path / "c2.db")
        dbs.append(third)
        with pytest.raises(ResourceLimitExceeded, match="max_connections"):
            await third.execute("select 1")
        await dbs[0].sleep()
        assert (await third.execute("select 3")).rows == [(3,)]
    finally:
        for d in dbs:
            await d.close()
        pool.close()


async def test_full_processes_overflow_to_more_up_to_max_processes(tmp_path):
    """size is a target: when every process is at max_connections, the
    pool starts more (up to max_processes), even for many concurrent
    first opens, and never puts more than max_connections on one."""
    from dblayer.backends.sqlite_cpool import CPool

    pool = CPool(1, linger=0, max_connections=4, max_processes=8)
    dbs = [make(pool, tmp_path / f"o{i}.db") for i in range(12)]
    try:
        await asyncio.gather(*(d.execute("select 1") for d in dbs))
        procs = pool.processes()
        assert len(procs) == 3 and all(p.handles == 4 for p in procs)
    finally:
        for d in dbs:
            await d.close()
        pool.close()


async def test_concurrent_first_opens_start_one_process(tmp_path):
    from dblayer.backends.sqlite_cpool import CPool

    pool = CPool(1, linger=0)
    dbs = [make(pool, tmp_path / f"f{i}.db") for i in range(10)]
    try:
        await asyncio.gather(*(d.execute("select 1") for d in dbs))
        assert pool.stats.processes_started == 1
    finally:
        for d in dbs:
            await d.close()
        pool.close()


def test_default_pool_and_a_new_event_loop(tmp_path):
    from dblayer.backends.sqlite_cpool import SqliteCPool, default_pool

    pids = []

    async def use() -> None:
        db = Database(SqliteCPool(tmp_path / "d.db"))
        try:
            await db.execute("select 1")
            pids.append(db._pool._idle_read[0].conn.pid)  # type: ignore[union-attr]
        finally:
            await db.close()

    asyncio.run(use())
    asyncio.run(use())  # a new loop: the pool starts afresh
    default_pool().close()
    assert len(pids) == 2 and pids[0] != pids[1]


def test_options_that_belong_to_the_pool_are_refused(tmp_path):
    from dblayer.backends.sqlite_cpool import CPool, SqliteCPool

    pool = CPool(1)
    with pytest.raises(ValueError, match="per process"):
        SqliteCPool(tmp_path / "x.db", pool=pool, memory_limit_mb=10)
    with pytest.raises(ValueError, match="pool"):
        SqliteCPool(tmp_path / "x.db", pool=pool, workers=None)
    b = SqliteCPool(tmp_path / "x.db", pool=pool)
    assert b.pool is pool and b.capabilities.hard_kill


async def test_hello_of_a_pool_process(tmp_path, pool):
    db = make(pool, tmp_path / "h.db")
    try:
        await db.execute("select 1")
        hello = pool.processes()[0].hello
        assert hello["multi"] is True and hello["implementation"] == "c"
        assert isinstance(pool.processes()[0], WorkerProcess)
    finally:
        await db.close()
