"""Faults on the PGlite worker (``PGliteWorker``): SIGKILL mid-query, mid-
transaction and after commit, then reopen with a consistency check (WAL
recovery); queries nothing inside WebAssembly can stop, killed at their
time limit; transaction deadlines; malformed and oversized frames; a reply
too large for the frame cap.

A query runs on the worker's only thread, inside WebAssembly, so the
parent's SIGKILL is the only stop; these tests show what a kill loses (the
one session: its open transaction) and that the data directory survives."""

from __future__ import annotations

import asyncio
import os
import signal
import time
from pathlib import Path
from typing import Any

import pytest

from dblayer import Database, QueryInterrupted, ResourceLimitExceeded, TransactionExpired, WorkerCrashed
from dblayer.backends.pglite_worker import PGliteWorker, unavailable

from .. import pglite_spec

UNAVAILABLE = unavailable()
pytestmark = pytest.mark.skipif(UNAVAILABLE is not None, reason=UNAVAILABLE or "")

# Seconds of pure CPU inside PostgreSQL, no I/O, nothing that yields.
ENDLESS = "select count(*) from generate_series(1, 10000000000) a"


def make(tmp_path: Path, **kwargs: Any) -> PGliteWorker:
    return PGliteWorker(tmp_path / "db", template=pglite_spec.template(), **kwargs)


async def consistent(db: Database) -> int:
    """Rows in t, checked through its primary key index and a sequential
    scan (they must agree after recovery), plus amcheck-free sanity: the
    index finds every row."""
    seq = (await db.execute("select count(*) n, coalesce(sum(id), 0) s from t")).rows[0]
    await db.execute_write("set enable_seqscan = off")
    idx = (await db.execute("select count(*) n, coalesce(sum(id), 0) s from t where id > -1")).rows[0]
    await db.execute_write("reset enable_seqscan")
    assert (seq["n"], seq["s"]) == (idx["n"], idx["s"])
    return seq["n"]


async def setup_table(db: Database, n: int = 100) -> None:
    await db.execute_write("create table t (id bigint primary key, v text)")
    await db.execute_write("insert into t select g, md5(g::text) from generate_series(1, :n) g", {"n": n})


async def test_kill_mid_query_time_limit(tmp_path):
    db = Database(make(tmp_path))
    try:
        await setup_table(db)
        pid = db._backend.pid  # type: ignore[attr-defined]
        t0 = time.perf_counter()
        with pytest.raises(QueryInterrupted, match="killed"):
            await db.execute(ENDLESS, time_limit_ms=300)
        elapsed = time.perf_counter() - t0
        print(f"\n[pglite] endless query killed after {elapsed * 1000:.0f} ms (limit 300 + grace 250)")
        assert elapsed < 5
        assert await consistent(db) == 100  # a new worker, WAL recovery, same data
        assert db._backend.pid != pid  # type: ignore[attr-defined]
    finally:
        await db.close()


async def test_pg_sleep_is_not_interruptible_either(tmp_path):
    db = Database(make(tmp_path))
    try:
        t0 = time.perf_counter()
        with pytest.raises(QueryInterrupted):
            await db.execute("select pg_sleep(30)", time_limit_ms=200)
        assert time.perf_counter() - t0 < 5
        assert (await db.execute("select 1 x")).rows[0]["x"] == 1
    finally:
        await db.close()


async def test_statement_timeout_does_not_fire(tmp_path):
    """PostgreSQL's timers are JavaScript timers in PGlite: they cannot run
    while the query blocks the event loop, so statement_timeout never
    interrupts anything. (The worker does not rely on it.)"""
    db = Database(make(tmp_path))
    try:
        await db.execute_write("set statement_timeout = 50")
        t0 = time.perf_counter()
        r = await db.execute("select count(*) n from generate_series(1, 2000000)")
        assert r.rows[0]["n"] == 2000000
        assert time.perf_counter() - t0 > 0.05
    finally:
        await db.close()


async def test_kill_mid_transaction_loses_only_the_open_transaction(tmp_path):
    backend = make(tmp_path)
    db = Database(backend)
    try:
        await setup_table(db)
        async with db.transaction() as tx:
            await tx.execute("insert into t select g, 'x' from generate_series(1000, 1999) g")
            os.kill(backend.pid, signal.SIGKILL)  # type: ignore[arg-type]
            await asyncio.sleep(0.1)
            with pytest.raises(WorkerCrashed):
                await tx.execute("select 1")
    except WorkerCrashed:
        pass
    try:
        t0 = time.perf_counter()
        assert await consistent(db) == 100  # the uncommitted 1000 rows are gone
        print(f"\n[pglite] reopen after SIGKILL mid-transaction: {(time.perf_counter() - t0) * 1000:.0f} ms")
    finally:
        await db.close()


async def test_kill_after_commit_keeps_the_commit(tmp_path):
    backend = make(tmp_path)
    db = Database(backend)
    try:
        await setup_table(db)
        for k in range(5):
            await db.execute_write("insert into t values (:id, 'c')", {"id": 10_000 + k})
        os.kill(backend.pid, signal.SIGKILL)  # type: ignore[arg-type]
        await asyncio.sleep(0.1)
        assert await consistent(db) == 105
    finally:
        await db.close()


def test_kill_during_bulk_writes_then_reopen_in_a_new_process(tmp_path):
    """Kill the worker while a large insert is running; a fresh Database (as
    after a restart of the whole service) opens the directory and sees
    exactly the committed rows."""

    async def phase1() -> None:
        db = Database(make(tmp_path))
        await setup_table(db)
        task = asyncio.ensure_future(
            db.execute_write("insert into t select g, repeat('x', 200) from generate_series(1000, 400000) g")
        )
        await asyncio.sleep(0.3)
        os.kill(db._backend.pid, signal.SIGKILL)  # type: ignore[attr-defined]
        with pytest.raises(WorkerCrashed):
            await task
        await db.close()

    async def phase2() -> float:
        db = Database(PGliteWorker(tmp_path / "db"))
        t0 = time.perf_counter()
        try:
            assert await consistent(db) == 100
            await db.execute_write("insert into t values (-5, 'after')")  # still writable
            return time.perf_counter() - t0
        finally:
            await db.close()

    asyncio.run(phase1())
    took = asyncio.run(phase2())
    print(f"\n[pglite] open + WAL recovery after a kill mid-bulk-insert: {took * 1000:.0f} ms")


async def test_transaction_deadline_rolls_back_in_the_worker(tmp_path):
    db = Database(make(tmp_path))
    try:
        await setup_table(db, 1)
        with pytest.raises(TransactionExpired):
            async with db.transaction(deadline_ms=200) as tx:
                await tx.execute("insert into t values (2, 'late')")
                await asyncio.sleep(0.5)
                await tx.execute("select 1")
        assert await consistent(db) == 1
        # the session is free again: another connection is not kept waiting
        assert (await db.execute("select 1 x", time_limit_ms=1000)).rows[0]["x"] == 1
    finally:
        await db.close()


async def test_deadline_passing_during_an_endless_statement_kills(tmp_path):
    db = Database(make(tmp_path))
    try:
        await setup_table(db, 1)
        t0 = time.perf_counter()
        with pytest.raises((TransactionExpired, QueryInterrupted, WorkerCrashed)):
            async with db.transaction(deadline_ms=300) as tx:
                await tx.execute(ENDLESS)
        assert time.perf_counter() - t0 < 5
        assert await consistent(db) == 1
    finally:
        await db.close()


async def _debug(db: Database, action: str, **kw: Any) -> Any:
    await db.execute("select 1")
    proc = db._backend.process  # type: ignore[attr-defined]
    return await proc.call({"type": "_debug", "action": action, **kw}, owner=None, limit_ms=2000)


@pytest.mark.parametrize("action,exc", [
    ("oversized_frame", ResourceLimitExceeded),
    ("garbage", WorkerCrashed),
    ("truncated_frame", WorkerCrashed),
])
async def test_bad_frames_from_the_worker(tmp_path, action, exc):
    db = Database(make(tmp_path, debug=True))
    try:
        with pytest.raises(exc):
            await _debug(db, action)
        assert (await db.execute("select 1 x")).rows[0]["x"] == 1  # replaced on the next request
    finally:
        await db.close()


async def test_reply_over_the_frame_cap_is_refused_in_the_worker(tmp_path):
    db = Database(make(tmp_path, max_frame_bytes=1_000_000))
    try:
        with pytest.raises(ResourceLimitExceeded, match="frame size cap"):
            await db.execute("select repeat('x', 2000000) s")
        assert (await db.execute("select 1 x")).rows[0]["x"] == 1  # the same worker
    finally:
        await db.close()


async def test_request_over_the_frame_cap_is_not_sent(tmp_path):
    db = Database(make(tmp_path, max_frame_bytes=1_000_000))
    try:
        await db.execute("select 1")
        pid = db._backend.pid  # type: ignore[attr-defined]
        with pytest.raises(ResourceLimitExceeded, match="too large"):
            await db.execute("select length(:s) n", {"s": "x" * 2_000_000})
        assert (await db.execute("select 1 x")).rows[0]["x"] == 1
        assert db._backend.pid == pid  # type: ignore[attr-defined]
    finally:
        await db.close()


async def test_parent_stops_reading(tmp_path):
    """A huge result while the parent is not reading: the worker blocks on
    its pipe; the parent's kill timer (time limit + grace) ends it."""
    db = Database(make(tmp_path))
    try:
        await db.execute("select 1")
        proc = db._backend.process  # type: ignore[attr-defined]
        proc.loop.remove_reader(proc._fds[1])  # stop reading the worker's stdout
        t0 = time.perf_counter()
        with pytest.raises(QueryInterrupted):
            await db.execute("select repeat('x', 1000) from generate_series(1, 200000)", time_limit_ms=500)
        assert time.perf_counter() - t0 < 5
        assert (await db.execute("select 1 x")).rows[0]["x"] == 1
    finally:
        await db.close()


async def test_stdin_eof_rolls_back_and_exits(tmp_path):
    backend = make(tmp_path)
    db = Database(backend)
    try:
        await setup_table(db, 3)
        async with db.transaction() as tx:
            await tx.execute("insert into t values (99, 'never')")
            proc = backend.process
            assert proc is not None and proc.popen is not None
            proc.popen.stdin.close()  # the parent "goes away"
            proc.popen.wait(10)
            assert proc.popen.returncode == 0
            with pytest.raises(WorkerCrashed):
                await tx.execute("select 1")
    except WorkerCrashed:
        pass
    try:
        assert await consistent(db) == 3
    finally:
        await db.close()
