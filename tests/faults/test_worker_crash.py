"""Killing workers: mid-query, mid-transaction, and when the parent stops
reading. A killed worker must never corrupt the database or leave a lock
behind, and the pool must replace it on the next request."""

from __future__ import annotations

import asyncio
import os
import signal
import sqlite3
import time

import pytest

from dblayer import Database, QueryInterrupted, TransactionExpired, WorkerCrashed
from dblayer.backends.sqlite import SqliteInProcess
from dblayer.backends.sqlite_worker import WorkerConnection, reap_workers

UNBOUNDED = "with recursive c(x) as (select 1 union all select x + 1 from c) select count(*) from c"
EVADING = "select instr(printf('%.*c', 2000000, 'a'), printf('%.*c', 100000, 'a') || 'b')"


def _in_use(db: Database, write: bool) -> WorkerConnection:
    pool = db._pool
    assert pool is not None
    conns = [e.conn for e in pool._in_use.values() if e.write == write]
    assert len(conns) == 1
    return conns[0]  # type: ignore[return-value]


async def _wait_in_use(db: Database, write: bool) -> WorkerConnection:
    for _ in range(500):
        pool = db._pool
        if pool is not None and any(e.write == write for e in pool._in_use.values()):
            c = _in_use(db, write)
            if c.process.inflight is not None:
                return c
        await asyncio.sleep(0.01)
    raise AssertionError("no connection in use")


def _check_file(path) -> None:
    """Independent of dblayer: the file passes integrity_check and no lock
    is held (an immediate write transaction starts at once). Called once
    every worker has exited (closing is asynchronous)."""
    assert not reap_workers(timeout=5.0)
    conn = sqlite3.connect(path, timeout=0)
    try:
        assert conn.execute("pragma integrity_check").fetchall() == [("ok",)]
        conn.execute("begin immediate")
        conn.execute("rollback")
    finally:
        conn.close()


@pytest.mark.parametrize("journal", ["delete", "wal"])
async def test_kill_read_worker_mid_query(tmp_path, journal, make_worker):
    path = tmp_path / "r.db"
    db = Database(make_worker(path))
    try:
        await db.execute_write(f"pragma journal_mode = {journal}")
        await db.execute_write("create table t (x)")
        task = asyncio.create_task(db.execute(UNBOUNDED))
        conn = await _wait_in_use(db, write=False)
        os.kill(conn.pid, signal.SIGKILL)  # type: ignore[arg-type]
        with pytest.raises(WorkerCrashed):
            await task
        assert (await db.execute("select 1")).rows == [(1,)]  # a new worker
        await db.execute_write("insert into t values (1)")
    finally:
        await db.close()
    _check_file(path)


@pytest.mark.parametrize("journal", ["delete", "wal"])
async def test_kill_write_worker_mid_transaction(tmp_path, journal, make_worker):
    path = tmp_path / "w.db"
    db = Database(make_worker(path))
    try:
        await db.execute_write(f"pragma journal_mode = {journal}")
        await db.execute_write("create table t (x)")
        await db.execute_write("insert into t values (0)")
        killed = asyncio.Event()

        async def writer():
            async with db.transaction() as tx:
                # Enough data to spill pages to the journal / WAL.
                await tx.execute(
                    "insert into t select randomblob(1000) from "
                    "(with recursive c(i) as (select 1 union all select i + 1 from c where i < 5000) select i from c)"
                )
                conn = tx._conn
                assert isinstance(conn, WorkerConnection)
                os.kill(conn.pid, signal.SIGKILL)  # type: ignore[arg-type]
                killed.set()
                await tx.execute("insert into t values (2)")

        with pytest.raises(WorkerCrashed):
            await writer()
        assert killed.is_set()
        # No partial write visible; the next writer proceeds at once.
        assert (await db.execute("select count(*) from t")).rows == [(1,)]
        t0 = time.perf_counter()
        await db.execute_write("insert into t values (3)")
        assert time.perf_counter() - t0 < 5  # no lock left behind (busy_timeout is 5 s)
        assert (await db.execute("select count(*) from t")).rows == [(2,)]
    finally:
        await db.close()
    _check_file(path)


@pytest.mark.parametrize("kind", ["worker", "in-process"])
async def test_idle_readers_recover_a_hot_journal_left_by_a_killed_writer(tmp_path, kind, make_worker):
    """Found while writing these tests: in rollback-journal mode a writer
    killed mid-transaction leaves a hot journal that a ``mode=ro`` reader
    cannot roll back (``SQLITE_READONLY_ROLLBACK``) - every read fails
    until a writer opens the file. Readers now recover it through a
    read-write connection, including readers that were already open."""
    import subprocess
    import sys

    path = tmp_path / "h.db"
    setup = sqlite3.connect(path)
    setup.execute("create table t (x)")
    setup.execute("insert into t values (0)")
    setup.commit()
    setup.close()
    backend = make_worker(path) if kind == "worker" else SqliteInProcess(path)
    db = Database(backend, idle_timeout=None)
    try:
        assert (await db.execute("select count(*) from t")).rows == [(1,)]  # an idle reader now
        child = subprocess.Popen(
            [sys.executable, "-c",
             "import sqlite3, sys, time\n"
             f"c = sqlite3.connect({str(path)!r}, isolation_level=None)\n"
             "c.execute('begin immediate')\n"
             "c.execute('insert into t select randomblob(1000) from (with recursive c(i) as "
             "(select 1 union all select i + 1 from c where i < 5000) select i from c)')\n"
             "print('ready', flush=True)\n"
             "time.sleep(60)\n"],
            stdout=subprocess.PIPE, text=True,
        )
        assert child.stdout is not None and child.stdout.readline().strip() == "ready"
        os.kill(child.pid, signal.SIGKILL)
        child.wait()
        child.stdout.close()
        assert os.path.getsize(str(path) + "-journal") > 0
        assert (await db.execute("select count(*) from t")).rows == [(1,)]
    finally:
        await db.close()
    _check_file(path)


async def test_kill_write_worker_while_statement_runs(tmp_path, make_worker):
    path = tmp_path / "s.db"
    db = Database(make_worker(path))
    try:
        await db.execute_write("create table t (x)")

        async def writer():
            async with db.transaction() as tx:
                await tx.execute("insert into t values (1)")
                await tx.execute(
                    "insert into t select randomblob(100) from "
                    "(with recursive c(i) as (select 1 union all select i + 1 from c) select i from c)"
                )

        task = asyncio.create_task(writer())
        conn = await _wait_in_use(db, write=True)
        await asyncio.sleep(0.2)  # mid-statement, journal being written
        os.kill(conn.pid, signal.SIGKILL)  # type: ignore[arg-type]
        with pytest.raises(WorkerCrashed):
            await task
        assert (await db.execute("select count(*) from t")).rows == [(0,)]
        await db.execute_write("insert into t values (9)")
    finally:
        await db.close()
    _check_file(path)


async def test_parent_stops_reading_worker_is_killed_by_timer(tmp_path, make_worker):
    db = Database(make_worker(tmp_path / "p.db", kill_grace_ms=250))
    try:
        await db.execute("select 1")
        pool = db._pool
        assert pool is not None
        conn = pool._idle_read[0].conn
        assert isinstance(conn, WorkerConnection)
        proc = conn.process
        assert proc._fds is not None
        proc.loop.remove_reader(proc._fds[1])  # the parent stops reading
        # ~8 MB of rows: far more than a pipe holds, so the worker blocks
        # writing its reply.
        sql = (
            "select randomblob(1000) from (with recursive c(i) as "
            "(select 1 union all select i + 1 from c where i < 8000) select i from c)"
        )
        t0 = time.perf_counter()
        with pytest.raises(QueryInterrupted, match="killed"):
            await db.execute(sql, time_limit_ms=300)
        elapsed = time.perf_counter() - t0
        print(f"\n[worker] parent not reading: worker killed after {elapsed * 1000:.0f} ms")
        assert elapsed < 5
        assert not proc.alive
        assert (await db.execute("select 5")).rows == [(5,)]
    finally:
        await db.close()


async def test_deadline_kill_when_worker_cannot_confirm_rollback(tmp_path, make_worker):
    """A statement that ignores interrupts is running when the deadline
    passes: the worker cannot roll back, so the parent kills it shortly
    after; the caller gets TransactionExpired and other writers proceed."""
    path = tmp_path / "d.db"
    db = Database(make_worker(path, kill_grace_ms=250))
    try:
        await db.execute_write("create table t (x)")
        t0 = time.perf_counter()
        with pytest.raises(TransactionExpired):
            async with db.transaction(deadline_ms=300) as tx:
                await tx.execute("insert into t values (1)")
                await tx.execute(EVADING)
        elapsed = time.perf_counter() - t0
        print(f"\n[worker] deadline 300 ms, evading statement: TransactionExpired after {elapsed * 1000:.0f} ms")
        assert elapsed < 5
        await db.execute_write("insert into t values (2)")
        assert (await db.execute("select x from t")).rows == [(2,)]
    finally:
        await db.close()
    _check_file(path)


async def test_worker_rolls_back_at_deadline_even_if_parent_is_unresponsive(tmp_path, make_worker):
    """The worker's own deadline: with the parent's event loop blocked, the
    transaction is still rolled back on time (another process can write)."""
    path = tmp_path / "u.db"
    db = Database(make_worker(path))
    try:
        await db.execute_write("create table t (x)")
        async with db.transaction(deadline_ms=200) as tx:
            await tx.execute("insert into t values (1)")
            time.sleep(0.6)  # blocks the parent's loop: no timers, no reads
            # The worker has already rolled back and released the lock.
            other = sqlite3.connect(path, timeout=0)
            other.execute("begin immediate")
            other.execute("rollback")
            other.close()
            with pytest.raises(TransactionExpired):
                await tx.execute("insert into t values (2)")
    except TransactionExpired:
        pass
    finally:
        await db.close()
    assert sqlite3.connect(path).execute("select count(*) from t").fetchone() == (0,)
