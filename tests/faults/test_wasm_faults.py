"""Fault tests for ``SqliteWasm`` (SQLite in WebAssembly, in-process): the
queries that evade the progress handler and ``interrupt()``, a
multi-gigabyte allocation against the memory cap, an unbounded recursive
query, and cancellation at random points.

No process is involved: a statement that will not stop is trapped by
wasmtime's epoch interruption and its instance replaced. Bounds are
generous; measured figures are printed (``pytest -s``)."""

from __future__ import annotations

import asyncio
import ctypes
import os
import random
import resource
import sys
import time

import pytest

from dblayer import Database, QueryInterrupted, ResourceLimitExceeded
from dblayer.backends.sqlite_wasm import module_unavailable, runtime_stats

from .test_worker_time import EVADING, EVADING_BIG, UNBOUNDED

UNAVAILABLE = module_unavailable()
pytestmark = pytest.mark.skipif(UNAVAILABLE is not None, reason=UNAVAILABLE or "")

LIMIT_MS = 200
GRACE_MS = 50
BOUND_S = 3.0


def _wasm(path, **kwargs):
    from dblayer.backends.sqlite_wasm import SqliteWasm

    return SqliteWasm(path, **kwargs)


@pytest.fixture
async def db(tmp_path):
    d = Database(_wasm(tmp_path / "t.db", kill_grace_ms=GRACE_MS, memory_limit_mb=256), name="t")
    yield d
    await d.close()


def _maxrss_mb() -> float:
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return rss / (1024 * 1024) if sys.platform == "darwin" else rss / 1024


def _footprint_mb() -> float:
    """This process's current memory: phys_footprint on macOS, RSS on
    Linux (the measure benchmarks/duckdb_memory.py uses)."""
    if sys.platform == "darwin":
        class Info(ctypes.Structure):
            _fields_ = [("uuid", ctypes.c_uint8 * 16)] + [(f"f{i}", ctypes.c_uint64) for i in range(18)] + [
                ("spare", ctypes.c_uint8 * 512)
            ]

        lib = ctypes.CDLL("/usr/lib/libproc.dylib")
        info = Info()
        lib.proc_pid_rusage(os.getpid(), 2, ctypes.byref(info))
        return info.f7 / 2**20  # ri_phys_footprint
    with open("/proc/self/status") as f:
        return next(int(line.split()[1]) for line in f if line.startswith("VmRSS:")) / 1024


async def test_unbounded_recursive_query_stops_at_the_limit_without_a_trap(db):
    await db.execute("select 1")
    traps = runtime_stats()["traps"]
    t0 = time.perf_counter()
    with pytest.raises(QueryInterrupted):
        await db.execute(UNBOUNDED, time_limit_ms=LIMIT_MS)
    elapsed = time.perf_counter() - t0
    print(f"\n[wasm] unbounded recursive CTE, {LIMIT_MS} ms limit: stopped after {elapsed * 1000:.0f} ms (progress handler)")
    assert elapsed < BOUND_S
    assert runtime_stats()["traps"] == traps  # soft stop: same instance
    assert (await db.execute("select 2")).rows == [(2,)]


@pytest.mark.parametrize("name", sorted(EVADING) + sorted(EVADING_BIG) + ["randomblob_1e8_x30"])
async def test_query_evading_the_progress_handler_is_stopped_at_the_limit(db, name):
    """The queries only a SIGKILL stopped in a worker process: here the
    instance traps at the limit plus the grace period and is replaced.
    ``randomblob_1e8_x30`` runs fewer than 1,000 VDBE opcodes, so a progress
    handler never fires."""
    sql = {
        **EVADING,
        **EVADING_BIG,
        "randomblob_1e8_x30": "with r(i) as (select 1 union all select i + 1 from r where i < 30) "
        "select sum(length(randomblob(100000000))) from r",
    }[name]
    await db.execute("select 1")
    rss_before = _maxrss_mb()
    traps = runtime_stats()["traps"]
    t0 = time.perf_counter()
    with pytest.raises((QueryInterrupted, ResourceLimitExceeded)) as e:
        await db.execute(sql, time_limit_ms=LIMIT_MS)
    elapsed = time.perf_counter() - t0
    trapped = runtime_stats()["traps"] - traps
    print(
        f"\n[wasm] {name}: {type(e.value).__name__} after {elapsed * 1000:.0f} ms "
        f"({LIMIT_MS} ms limit + {GRACE_MS} ms grace; {'trapped' if trapped else 'no trap'}); "
        f"host max RSS {rss_before:.0f} -> {_maxrss_mb():.0f} MB"
    )
    assert elapsed < BOUND_S
    if isinstance(e.value, QueryInterrupted):
        assert elapsed * 1000 >= LIMIT_MS * 0.9
    # The next query succeeds, on the same or a replacement instance.
    assert (await db.execute("select 42")).rows == [(42,)]


async def test_multi_gigabyte_allocation_hits_the_memory_cap(tmp_path):
    db = Database(_wasm(tmp_path / "m.db", memory_limit_mb=64), name="m")
    try:
        await db.execute("select 1")
        rss_before = _footprint_mb()
        for sql in (
            "select length(randomblob(900000000))",
            "select length(zeroblob(900000000) || 'x')",
            "select length(printf('%*d', 900000000, 1))",
            "with r(i) as (select 1 union all select i + 1 from r where i < 12) "
            "select length(group_concat(randomblob(10000000))) from r",
        ):
            t0 = time.perf_counter()
            with pytest.raises(ResourceLimitExceeded) as e:
                await db.execute(sql)
            elapsed = time.perf_counter() - t0
            print(f"\n[wasm] {sql[:50]}...: {e.value} after {elapsed * 1000:.0f} ms")
            assert (await db.execute("select 7")).rows == [(7,)]
        rss_after = _footprint_mb()
        print(f"[wasm] host footprint {rss_before:.0f} -> {rss_after:.0f} MB with a 64 MB cap")
        assert rss_after - rss_before < 100
    finally:
        await db.close()


async def test_memory_cap_is_per_instance(tmp_path):
    """A query that fits under the cap succeeds; the same one fails under a
    smaller cap."""
    sql = "select length(randomblob(40000000))"
    big = Database(_wasm(tmp_path / "a.db", memory_limit_mb=128), name="a")
    small = Database(_wasm(tmp_path / "b.db", memory_limit_mb=16), name="b")
    try:
        assert (await big.execute(sql)).rows == [(40000000,)]
        with pytest.raises(ResourceLimitExceeded):
            await small.execute(sql)
    finally:
        await big.close()
        await small.close()


async def test_an_instance_that_grew_is_replaced_and_gives_its_memory_back(tmp_path):
    """Linear memory never shrinks, so an instance that held a 150 MB value
    is replaced before its next request (outside a transaction)."""
    db = Database(_wasm(tmp_path / "g.db", memory_limit_mb=512, recycle_memory_mb=64), name="g")
    try:
        await db.execute("select 1")
        before = _footprint_mb()
        assert (await db.execute("select length(randomblob(150000000))")).rows == [(150000000,)]
        grown = _footprint_mb()
        assert (await db.execute("select 2")).rows == [(2,)]
        after = _footprint_mb()
        procs = [e.conn._proc for e in db._pool._idle_read]
        print(f"\n[wasm] footprint {before:.0f} -> {grown:.0f} (150 MB value) -> {after:.0f} MB after recycling")
        assert sum(p.recycled for p in procs) == 1
        assert all(p._inst.memory_bytes() < 64 << 20 for p in procs)
        assert after < grown - 100
    finally:
        await db.close()


async def test_cancelling_an_evading_query_stops_it_after_the_grace(db):
    await db.execute("select 1")
    task = asyncio.create_task(db.execute(EVADING["instr_quadratic"]))
    await asyncio.sleep(0.2)
    t0 = time.perf_counter()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    elapsed = time.perf_counter() - t0
    print(f"\n[wasm] cancel of an evading query -> returned after {elapsed * 1000:.0f} ms (trap)")
    assert elapsed < BOUND_S
    assert (await db.execute("select 4")).rows == [(4,)]


async def test_cancellation_at_random_points_is_never_lost(db):
    """The lost-interrupt race (notes.md): an interrupt that arrives just
    before the statement steps is dropped by ``sqlite3_interrupt()``. Here
    the cancel is a flag in the instance's memory that every statement start
    and the progress handler read, so every cancel lands."""
    await db.execute("select 1")
    rng = random.Random(1234)
    n = 300
    worst = 0.0
    traps = runtime_stats()["traps"]
    for _ in range(n):
        task = asyncio.create_task(db.execute(UNBOUNDED))
        await asyncio.sleep(rng.random() * 0.004)
        t0 = time.perf_counter()
        task.cancel()
        try:
            await asyncio.wait_for(task, timeout=10)
        except asyncio.CancelledError:
            pass
        worst = max(worst, time.perf_counter() - t0)
    trapped = runtime_stats()["traps"] - traps
    print(f"\n[wasm] {n} random-point cancels all returned; worst {worst * 1000:.1f} ms; {trapped} needed a trap")
    assert worst < BOUND_S
    assert (await db.execute("select 5")).rows == [(5,)]


async def test_cancelled_write_transaction_releases_the_write_slot(db):
    await db.execute_write("create table t (x)")

    async def hold():
        async with db.transaction() as tx:
            await tx.execute("insert into t values (1)")
            await tx.execute(UNBOUNDED)

    task = asyncio.create_task(hold())
    await asyncio.sleep(0.3)
    t0 = time.perf_counter()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await db.execute_write("insert into t values (2)")
    elapsed = time.perf_counter() - t0
    print(f"\n[wasm] cancel -> next writer done in {elapsed * 1000:.1f} ms")
    assert elapsed < BOUND_S
    assert (await db.execute("select x from t")).rows == [(2,)]


async def test_trap_inside_a_write_transaction_rolls_back(db, tmp_path):
    """A write transaction whose statement is trapped is lost, as with a
    killed worker: the hot journal is rolled back by the next opener and
    the file stays intact."""
    await db.execute_write("create table t (x)")
    await db.execute_write("insert into t values (1)")
    with pytest.raises(Exception):
        async with db.transaction(time_limit_ms=LIMIT_MS) as tx:
            await tx.execute("pragma cache_size = 5")
            await tx.execute("insert into t select randomblob(2000) from (with r(i) as (select 1 union all select i + 1 from r where i < 500) select i from r)")
            await tx.execute(EVADING["instr_quadratic"], time_limit_ms=LIMIT_MS)
    assert (await db.execute("select count(*) from t")).rows == [(1,)]
    assert (await db.execute("pragma integrity_check")).rows == [("ok",)]
    await db.execute_write("insert into t values (2)")
    assert (await db.execute("select count(*) from t")).rows == [(2,)]
