"""Placement policies: ``dedicated`` (a worker per pooled connection) and
``shared`` (a fixed group of workers serving many databases, one request
at a time per worker)."""

from __future__ import annotations

import asyncio
import os
import signal

import pytest

from dblayer import Database, WorkerCrashed
from dblayer.backends.sqlite_worker import SharedWorkers, SqliteWorker, live_worker_pids


@pytest.fixture
async def group():
    g = SharedWorkers(size=2, linger=0.2)
    yield g
    g.close()


async def test_shared_group_serves_many_databases_with_few_processes(tmp_path, group):
    dbs = [Database(SqliteWorker(tmp_path / f"s{i}.db", workers=group)) for i in range(6)]
    try:
        for i, db in enumerate(dbs):
            await db.execute_write("create table t (x)")
            await db.execute_write("insert into t values (:i)", {"i": i})
        results = await asyncio.gather(*(db.execute("select x from t") for db in dbs))
        assert [r.rows for r in results] == [[(i,)] for i in range(6)]
        assert len(group.procs) <= 2 and group.started <= 2
        assert len(live_worker_pids()) <= 2
    finally:
        for db in dbs:
            await db.close()


async def test_shared_transaction_pins_its_connection_while_others_interleave(tmp_path, group):
    a = Database(SqliteWorker(tmp_path / "a.db", workers=group))
    b = Database(SqliteWorker(tmp_path / "b.db", workers=group))
    try:
        await a.execute_write("create table t (x)")
        await b.execute_write("create table t (x)")
        async with a.transaction() as tx:
            await tx.execute("insert into t values (1)")
            # b works meanwhile, possibly on the same process.
            await b.execute_write("insert into t values (2)")
            assert (await b.execute("select count(*) from t")).rows == [(1,)]
            await tx.execute("insert into t values (3)")
        assert (await a.execute("select count(*) from t")).rows == [(2,)]
    finally:
        await a.close()
        await b.close()


async def test_shared_processes_exit_after_linger(tmp_path, group):
    db = Database(SqliteWorker(tmp_path / "l.db", workers=group))
    try:
        await db.execute("select 1")
        assert live_worker_pids()
    finally:
        await db.close()
    for _ in range(100):
        if not group.procs:
            break
        await asyncio.sleep(0.05)
    assert not group.procs


async def test_kill_on_a_shared_process_takes_down_its_other_connections(tmp_path):
    group = SharedWorkers(size=1)
    a = Database(SqliteWorker(tmp_path / "ka.db", workers=group))
    b = Database(SqliteWorker(tmp_path / "kb.db", workers=group))
    try:
        await a.execute("select 1")
        await b.execute("select 1")
        (proc,) = group.procs
        assert proc.handles == 2
        with pytest.raises(WorkerCrashed):
            async with b.transaction() as tx:
                await tx.execute("create table t (x)")
                os.kill(proc.pid, signal.SIGKILL)  # type: ignore[arg-type]
                await asyncio.sleep(0.2)
                await tx.execute("insert into t values (1)")
        assert not proc.alive
    finally:
        # Both databases recover on a new shared process.
        assert (await a.execute("select 2")).rows == [(2,)]
        assert (await b.execute("select 3")).rows == [(3,)]
        await a.close()
        await b.close()
        group.close()


async def test_dedicated_is_the_default_and_one_process_per_connection(tmp_path):
    b = SqliteWorker(tmp_path / "d.db")
    assert b.placement == "dedicated" and b.workers is None
    db = Database(b)
    try:
        await db.execute_write("create table t (x)")
        await asyncio.gather(*(db.execute("select count(*) from t, (select 1), (select 2)") for _ in range(6)))
        assert b.processes_started <= 1 + db.max_read_connections
    finally:
        await db.close()


def test_shared_placement_needs_a_group(tmp_path):
    with pytest.raises(ValueError):
        SqliteWorker(tmp_path / "x.db", placement="shared")
