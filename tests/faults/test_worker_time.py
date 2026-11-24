"""Time limits and cancellation on the worker backend: soft (progress
handler, interrupt) and hard (SIGKILL by the parent within the grace
period), and replacement of a killed worker.

Wall-clock bounds are deliberately generous (CI machines are slow and
shared); measured figures are printed (``-s``) and recorded in notes."""

from __future__ import annotations

import asyncio
import time

import pytest

from dblayer import Database, QueryInterrupted

UNBOUNDED = "with recursive c(x) as (select 1 union all select x + 1 from c) select count(*) from c"

# Single statements that spend seconds inside one C function call (one VDBE
# opcode), where neither the progress handler nor interrupt() can reach
# them. Found by trying candidates on SQLite 3.53.1; each runs ~1-5 s
# unlimited on a 2026 laptop. Only SIGKILL stops them.
EVADING = {
    "instr_quadratic": "select instr(printf('%.*c', 2000000, 'a'), printf('%.*c', 100000, 'a') || 'b')",
    "like_long_literal": "select printf('%.*c', 200000, 'a') like '%' || printf('%.*c', 40000, 'a') || 'b'",
    "glob_long_literal": "select printf('%.*c', 200000, 'a') glob '*' || printf('%.*c', 40000, 'a') || 'b'",
}
# These also evade, but each allocates about 1 GB inside the worker.
EVADING_BIG = {
    "randomblob_1e9": "select length(randomblob(1000000000))",
    "printf_width_1e9": "select length(printf('%*d', 1000000000, 1))",
}

LIMIT_MS = 200
GRACE_MS = 250
BOUND_S = 3.0  # generous: limit + grace is 0.45 s


@pytest.fixture
async def db(tmp_path, make_worker):
    d = Database(make_worker(tmp_path / "t.db", kill_grace_ms=GRACE_MS), name="t")
    yield d
    await d.close()


def _read_pids(db: Database) -> set[int]:
    pool = db._pool
    return set() if pool is None else {e.conn.pid for e in pool._idle_read}


async def test_unbounded_recursive_query_stops_at_time_limit_without_a_kill(db):
    await db.execute("select 1")
    before = _read_pids(db)
    t0 = time.perf_counter()
    with pytest.raises(QueryInterrupted):
        await db.execute(UNBOUNDED, time_limit_ms=LIMIT_MS)
    elapsed = time.perf_counter() - t0
    print(f"\n[worker] unbounded recursive CTE, {LIMIT_MS} ms limit: stopped after {elapsed * 1000:.0f} ms")
    assert elapsed < BOUND_S
    assert _read_pids(db) == before  # soft limit: same worker, still usable
    assert (await db.execute("select 2")).rows == [(2,)]


@pytest.mark.parametrize("name", sorted(EVADING) + sorted(EVADING_BIG))
async def test_query_evading_progress_handler_is_killed_within_grace(db, name):
    sql = {**EVADING, **EVADING_BIG}[name]
    await db.execute("select 1")
    before = _read_pids(db)
    t0 = time.perf_counter()
    with pytest.raises(QueryInterrupted, match="killed") as e:
        await db.execute(sql, time_limit_ms=LIMIT_MS)
    elapsed = time.perf_counter() - t0
    print(f"\n[worker] {name}: hard kill after {elapsed * 1000:.0f} ms ({LIMIT_MS} ms limit + {GRACE_MS} ms grace): {e.value}")
    assert elapsed < BOUND_S
    # The next query succeeds, on a replacement worker.
    assert (await db.execute("select 42")).rows == [(42,)]
    assert _read_pids(db).isdisjoint(before)


async def test_cancelling_the_caller_interrupts_the_worker_query(db):
    await db.execute("select 1")
    before = _read_pids(db)
    task = asyncio.create_task(db.execute(UNBOUNDED))
    await asyncio.sleep(0.2)
    t0 = time.perf_counter()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    elapsed = time.perf_counter() - t0
    print(f"\n[worker] cancel -> call returned in {elapsed * 1000:.1f} ms")
    assert elapsed < BOUND_S
    assert _read_pids(db) == before  # interrupted, not killed
    assert (await db.execute("select 3")).rows == [(3,)]


async def test_cancelling_an_evading_query_kills_the_worker_after_the_grace(db):
    await db.execute("select 1")
    task = asyncio.create_task(db.execute(EVADING["instr_quadratic"]))
    await asyncio.sleep(0.2)
    t0 = time.perf_counter()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    elapsed = time.perf_counter() - t0
    print(f"\n[worker] cancel of an evading query -> returned after {elapsed * 1000:.0f} ms (kill)")
    assert elapsed < BOUND_S
    assert (await db.execute("select 4")).rows == [(4,)]


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
    print(f"\n[worker] cancel -> next writer done in {elapsed * 1000:.1f} ms")
    assert elapsed < BOUND_S
    assert (await db.execute("select x from t")).rows == [(2,)]
