"""The multi-tenant C pool's blast radius: one tenant runs a statement no
interrupt can stop, the parent SIGKILLs the process it shares with other
tenants, and the others must carry on. Reads in flight are retried on a
replacement process, writes and transactions fail loudly (never silently
lost, never applied twice), idle connections reconnect, the culprit is
quarantined, and every database passes ``integrity_check``.

Timing is reported (``-s``), with generous functional bounds only."""

from __future__ import annotations

import asyncio
import os
import sqlite3
import statistics
import time

import pytest

from dblayer import Database, QueryInterrupted, WorkerCrashed
from dblayer.backends.sqlite_cworker import binary_unavailable

# Every test runs on the C worker and on its Rust port (sqlite-rs-worker/),
# each skipped when not built.
from ..native_impl import autouse_native_impl, native_impl  # noqa: E402,F401

EVADING = "select instr(printf('%.*c', 2000000, 'a'), printf('%.*c', 100000, 'a') || 'b')"
READ = "select count(*), (with recursive c(x) as (select 1 union all select x + 1 from c where x < 100000) select count(*) from c) from t"
LIMIT_MS = 200
GRACE_MS = 250


def _integrity(path) -> None:
    conn = sqlite3.connect(path, timeout=0)
    try:
        assert conn.execute("pragma integrity_check").fetchall() == [("ok",)]
        conn.execute("begin immediate")
        conn.execute("rollback")
    finally:
        conn.close()


class Load:
    """Readers and a writer on other tenants' databases, running until
    stopped; every outcome recorded."""

    def __init__(self, readers: list[Database], writer: Database, first_id: int = 1) -> None:
        self.first_id = first_id
        self.readers = readers
        self.writer = writer
        self.read_times: list[tuple[float, float]] = []  # (start, seconds)
        self.read_errors: list[BaseException] = []
        self.written: list[int] = []
        self.write_failed: list[int] = []
        self.write_errors: list[BaseException] = []
        self.stop = asyncio.Event()

    async def read_loop(self, db: Database) -> None:
        while not self.stop.is_set():
            t0 = time.perf_counter()
            try:
                r = await db.execute(READ)
                assert r.rows[0][1] == 100000
            except BaseException as e:  # noqa: BLE001 - recorded and asserted on
                self.read_errors.append(e)
                return
            self.read_times.append((t0, time.perf_counter() - t0))

    async def write_loop(self) -> None:
        i = self.first_id - 1
        while not self.stop.is_set():
            i += 1
            try:
                await self.writer.execute_write("insert into t (id, v) values (:id, randomblob(2000))", {"id": i})
                self.written.append(i)
            except WorkerCrashed:
                self.write_failed.append(i)  # in flight at the kill: may or may not have committed
            except BaseException as e:  # noqa: BLE001
                self.write_errors.append(e)
                return

    def tasks(self) -> list[asyncio.Task[None]]:
        return [asyncio.create_task(self.read_loop(d)) for d in self.readers] + [
            asyncio.create_task(self.write_loop())
        ]


async def test_kill_for_one_tenant_retries_others_reads_and_fails_writes_loudly(tmp_path):
    from dblayer.backends.sqlite_cpool import CPool, SqliteCPool

    pool = CPool(1, linger=0, kill_grace_ms=GRACE_MS, quarantine_s=60)
    paths = {name: tmp_path / f"{name}.db" for name in ("culprit", "r1", "r2", "r3", "r4", "w", "tx")}
    dbs = {name: Database(SqliteCPool(p, pool=pool), name=name) for name, p in paths.items()}
    started: set[int | None] = set()
    try:
        for d in dbs.values():
            await d.execute_write("create table t (id integer primary key, v)")
            await d.execute_write("insert into t (id, v) values (0, 'seed')")
            await d.execute("select 1")
        assert len(pool.processes()) == 1
        victim = pool.processes()[0]
        started.add(victim.pid)

        load = Load([dbs[n] for n in ("r1", "r2", "r3", "r4")], dbs["w"])
        tasks = load.tasks()
        # A tenant with an open transaction across the kill.
        tx_ready, tx_go = asyncio.Event(), asyncio.Event()

        async def held_transaction() -> None:
            async with dbs["tx"].transaction() as tx:
                await tx.execute("insert into t (id, v) values (1, 'uncommitted')")
                tx_ready.set()
                await tx_go.wait()
                await tx.execute("insert into t (id, v) values (2, 'after the kill')")

        held = asyncio.create_task(held_transaction())
        await tx_ready.wait()
        await asyncio.sleep(0.3)  # the load is running
        before = len(load.read_times)

        t0 = time.perf_counter()
        with pytest.raises(QueryInterrupted, match="killed"):
            await dbs["culprit"].execute(EVADING, time_limit_ms=LIMIT_MS)
        kill_s = time.perf_counter() - t0
        killed_at = pool.stats.last_kill_at
        assert killed_at is not None and not victim.alive
        await asyncio.sleep(0.5)  # the load carries on, on a new process
        load.stop.set()
        tx_go.set()
        await asyncio.gather(*tasks)
        with pytest.raises(WorkerCrashed):
            await held

        # Nothing failed but the writes that were in flight at the kill.
        assert load.read_errors == [] and load.write_errors == [], (load.read_errors, load.write_errors)
        assert len(load.read_times) > before + 4  # reads went on after the kill
        assert len(load.write_failed) <= 1
        assert pool.stats.kills == 1

        # No write lost silently or applied twice: every acknowledged
        # insert is there, once; an unacknowledged one may or may not be.
        ids = [r[0] for r in (await dbs["w"].execute("select id from t where id > 0 order by id")).rows]
        assert len(ids) == len(set(ids))
        assert set(load.written) <= set(ids) <= set(load.written) | set(load.write_failed)
        # The transaction open at the kill left nothing behind.
        assert (await dbs["tx"].execute("select id from t order by id")).rows == [(0,)]

        # How long the others stalled: the slowest read that spanned the kill.
        normal = [s for t, s in load.read_times if t + s < killed_at - 0.05]
        spanning = [s for t, s in load.read_times if t <= killed_at <= t + s]
        after = [t + s - killed_at for t, s in load.read_times if t <= killed_at <= t + s]
        print(
            f"\n[cpool] kill after {kill_s * 1000:.0f} ms ({LIMIT_MS} ms limit + {GRACE_MS} ms grace); "
            f"normal read {statistics.median(normal) * 1000:.1f} ms median, max {max(normal) * 1000:.1f} ms; "
            f"{len(spanning)} reads spanned the kill (retried {pool.stats.reads_retried}), slowest "
            f"{max(spanning, default=0) * 1000:.1f} ms, done {max(after, default=0) * 1000:.1f} ms after the kill; "
            f"retry stalls {[round(s * 1000, 1) for s in pool.stats.stalls]} ms; reconnects {pool.stats.reconnects}; "
            f"writes acknowledged {len(load.written)}, failed loudly {len(load.write_failed)}; "
            f"load {os.getloadavg()[0]:.1f}"
        )
        assert pool.stats.reads_retried == pool.stats.retried_ok
        assert max(after, default=0) < 3.0

        # The culprit is quarantined: its next connection is on a process
        # of its own, so a second offence kills only that process.
        assert pool.is_quarantined(str(paths["culprit"].resolve()))
        await dbs["culprit"].execute("select 1")
        ordinary = [p for p in pool.processes() if not p.quarantine]
        assert pool.quarantine_proc is not None and pool.quarantine_proc.alive
        started.add(pool.quarantine_proc.pid)
        started.update(p.pid for p in ordinary)
        assert len(ordinary) == 1
        retried = pool.stats.reads_retried
        load2 = Load([dbs[n] for n in ("r1", "r2", "r3", "r4")], dbs["w"], first_id=1_000_000)
        tasks = load2.tasks()
        await asyncio.sleep(0.1)
        with pytest.raises(QueryInterrupted, match="killed"):
            await dbs["culprit"].execute(EVADING, time_limit_ms=LIMIT_MS)
        await asyncio.sleep(0.2)
        load2.stop.set()
        await asyncio.gather(*tasks)
        assert load2.read_errors == [] and load2.write_errors == [] and load2.write_failed == []
        assert pool.stats.kills == 2 and pool.stats.reads_retried == retried  # nobody else was on it
        assert ordinary[0].alive
    finally:
        ours = started | {p.pid for p in pool.processes()}
        for d in dbs.values():
            await d.close()
        pool.close()
    from dblayer.backends.sqlite_worker import live_worker_pids

    # Every process this pool started has exited. (Not reap_workers():
    # that waits for every worker in this interpreter, other tests' too.)
    deadline = time.monotonic() + 5.0
    while ours & set(live_worker_pids()) and time.monotonic() < deadline:
        await asyncio.sleep(0.05)
    assert not ours & set(live_worker_pids())
    for p in paths.values():
        _integrity(p)


async def test_idle_connections_reconnect_after_a_kill(tmp_path):
    """Databases that were idle (connections pooled, nothing in flight)
    when their process was killed reconnect on next use: one ``open``."""
    from dblayer.backends.sqlite_cpool import CPool, SqliteCPool

    pool = CPool(1, linger=0, kill_grace_ms=GRACE_MS)
    dbs = [Database(SqliteCPool(tmp_path / f"i{i}.db", pool=pool)) for i in range(5)]
    try:
        for d in dbs:
            await d.execute_write("create table t (x)")
            await d.execute("select 1")
        old = pool.processes()[0]
        with pytest.raises(QueryInterrupted):
            await dbs[0].execute(EVADING, time_limit_ms=LIMIT_MS)
        assert not old.alive
        for d in dbs[1:]:
            t0 = time.perf_counter()
            assert (await d.execute("select count(*) from t")).rows == [(0,)]
            await d.execute_write("insert into t values (1)")
            print(f"\n[cpool] reconnect after a kill: first read {(time.perf_counter() - t0) * 1000:.2f} ms", end="")
        assert pool.stats.reconnects >= 4 * 2  # a read and a write connection each
    finally:
        for d in dbs:
            await d.close()
        pool.close()


async def test_a_stream_open_at_a_kill_fails_loudly_and_its_database_carries_on(tmp_path):
    """Another tenant's stream paused mid-way when the process is killed
    for one tenant's sake is not retried (rows were already handed out):
    its next chunk raises WorkerCrashed, and the database's next request
    reconnects on a new process."""
    from dblayer.backends.sqlite_cpool import CPool, InnocentCrash, SqliteCPool

    pool = CPool(1, linger=0, kill_grace_ms=GRACE_MS)
    culprit = Database(SqliteCPool(tmp_path / "c.db", pool=pool))
    other = Database(SqliteCPool(tmp_path / "s.db", pool=pool))
    try:
        await culprit.execute("select 1")
        series = "with recursive c(x) as (select 1 union all select x + 1 from c where x < 5000) select x from c"
        seen = 0
        with pytest.raises(WorkerCrashed) as e:
            async for chunk in other.stream(series, chunk_rows=100):
                seen += len(chunk)
                if seen == 100:
                    with pytest.raises(QueryInterrupted):
                        await culprit.execute(EVADING, time_limit_ms=LIMIT_MS)
        assert not isinstance(e.value, InnocentCrash)
        assert 100 <= seen < 5000
        assert pool.stats.kills == 1
        assert (await other.execute("select count(*) from (" + series + ")")).rows == [(5000,)]
    finally:
        await culprit.close()
        await other.close()
        pool.close()


async def test_external_kill_retries_nothing(tmp_path):
    """A process killed from outside has no known culprit: every in-flight
    request fails with WorkerCrashed, reads included (one of them may be
    what crashed it)."""
    import signal

    from dblayer.backends.sqlite_cpool import CPool, InnocentCrash, SqliteCPool

    pool = CPool(1, linger=0)
    dbs = [Database(SqliteCPool(tmp_path / f"x{i}.db", pool=pool)) for i in range(3)]
    try:
        for d in dbs:
            await d.execute("select 1")
        unbounded = "with recursive c(x) as (select 1 union all select x + 1 from c) select count(*) from c"
        tasks = [asyncio.create_task(d.execute(unbounded)) for d in dbs]
        await asyncio.sleep(0.2)
        os.kill(pool.processes()[0].pid, signal.SIGKILL)  # type: ignore[arg-type]
        for t in tasks:
            with pytest.raises(WorkerCrashed) as e:
                await t
            assert not isinstance(e.value, InnocentCrash)
        assert pool.stats.reads_retried == 0
        assert (await dbs[0].execute("select 2")).rows == [(2,)]
    finally:
        for d in dbs:
            await d.close()
        pool.close()
