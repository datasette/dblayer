"""DuckDB in a worker process that the database owns (``DuckDBWorker``).

The placement and sleep tests also run against the C++ and Rust workers
(``Worker`` fixture; skipped when their binaries are not built)."""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys

import pytest

pytest.importorskip("duckdb")

from dblayer import Database  # noqa: E402
from dblayer.backends.duckdb_worker import DuckDBWorker  # noqa: E402
from dblayer.backends.sqlite_worker import live_worker_pids  # noqa: E402


@pytest.fixture(params=["duckdb-worker", "duckdb-cppworker", "duckdb-rsworker"])
def Worker(request: pytest.FixtureRequest) -> type[DuckDBWorker]:
    if request.param == "duckdb-worker":
        return DuckDBWorker
    if request.param == "duckdb-cppworker":
        from dblayer.backends import duckdb_cppworker as mod
    else:
        from dblayer.backends import duckdb_rsworker as mod  # type: ignore[no-redef]
    reason = mod.binary_unavailable()
    if reason:
        pytest.skip(reason)
    return mod.DuckDBCppWorker if request.param == "duckdb-cppworker" else mod.DuckDBRustWorker  # type: ignore[attr-defined]


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    # A zombie still answers kill(0); the placement reaps its children.
    return True


async def test_every_connection_of_a_database_shares_one_process(tmp_path, Worker):
    b = Worker(tmp_path / "a.duckdb")
    db = Database(b)
    try:
        await db.execute_write("create table t (i bigint)")
        # Concurrent reads open several read connections at once: all go to
        # the one process (a second would fail on DuckDB's file lock).
        results = await asyncio.gather(*[db.execute("select count(*) from t") for _ in range(6)])
        assert all(r.rows == [(0,)] for r in results)
        assert b.placement_group.started == 1
        assert len(live_worker_pids()) == 1
        assert b.pid == live_worker_pids()[0]
    finally:
        await db.close()
    assert live_worker_pids() == []


async def test_concurrent_first_opens_start_one_process(tmp_path, Worker):
    b = Worker(tmp_path / "c.duckdb")
    db = Database(b)
    try:
        await asyncio.gather(db.execute("select 1"), db.execute("select 2"), db.execute_write("create table t (i int)"))
        assert b.placement_group.started == 1
    finally:
        await db.close()


async def test_process_exits_when_the_database_sleeps(tmp_path, Worker):
    path = tmp_path / "s.duckdb"
    b = Worker(path)
    db = Database(b)
    try:
        await db.execute_write("create table t as select range i from range(1000)")
        pid = b.pid
        assert pid is not None and pid != os.getpid()
        await db.sleep()
        # sleep() returns once the process has exited and been reaped.
        assert b.pid is None
        assert live_worker_pids() == []
        assert not _alive(pid)
        # The file lock is free: another process can open the file.
        out = subprocess.run(
            [sys.executable, "-c", f"import duckdb; print(duckdb.connect({str(path)!r}).execute('select count(*) from t').fetchone()[0])"],
            capture_output=True, text=True, check=True,
        )
        assert out.stdout.strip() == "1000"
        # Waking starts a new process.
        assert (await db.execute("select count(*) from t")).rows == [(1000,)]
        assert b.pid not in (None, pid)
        assert b.placement_group.started == 2
    finally:
        await db.close()
    assert live_worker_pids() == []


async def test_sleep_then_immediate_wake_waits_for_the_old_process(tmp_path, Worker):
    b = Worker(tmp_path / "w.duckdb")
    db = Database(b)
    try:
        await db.execute_write("create table t (i int)")
        for i in range(5):
            await db.execute_write("insert into t values (:i)", {"i": i})
            await db.sleep()
        assert (await db.execute("select count(*) from t")).rows == [(5,)]
        assert b.placement_group.started == 6
    finally:
        await db.close()


def test_parent_does_not_import_duckdb(tmp_path):
    code = (
        "import asyncio, sys\n"
        "from dblayer import Database\n"
        "from dblayer.backends.duckdb_worker import DuckDBWorker\n"
        "async def main():\n"
        f"    db = Database(DuckDBWorker({str(tmp_path / 'p.duckdb')!r}))\n"
        "    try:\n"
        "        assert (await db.execute('select 42')).rows == [(42,)]\n"
        "        try:\n"
        "            await db.execute('select * from nope')\n"
        "        except Exception as e:\n"
        "            print(type(e).__name__)\n"
        "    finally:\n"
        "        await db.close()\n"
        "asyncio.run(main())\n"
        "print('duckdb' in sys.modules)\n"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert out.stdout.split() == ["QueryError", "False"], out.stdout


def test_options_reach_the_engine(tmp_path):
    b = DuckDBWorker(tmp_path / "o.duckdb", memory_limit="64MB", threads=1, read_only=True)
    assert b.engine_config["memory_limit"] == "64MB"
    assert b.engine_config["threads"] == 1
    assert b.engine_config["read_only"] is True
    assert b.dialect.name == "duckdb"
    assert b.capabilities.hard_kill
    assert repr(b).startswith("DuckDBWorker(")


async def test_memory_limit_and_threads_apply_inside_the_worker(tmp_path, Worker):
    db = Database(Worker(tmp_path / "m.duckdb", memory_limit="64MB", threads=1))
    try:
        r = await db.execute("select current_setting('memory_limit'), current_setting('threads')")
        limit, threads = r.rows[0]
        assert "MiB" in limit or "MB" in limit
        assert threads == 1
    finally:
        await db.close()


# -- experimental: several databases per process ---------------------------------


async def test_pool_packs_databases_and_exits_when_all_sleep(tmp_path):
    from dblayer.backends.duckdb_worker import DuckDBWorkerPool

    pool = DuckDBWorkerPool(databases_per_process=2)
    backends = [DuckDBWorker(tmp_path / f"p{i}.duckdb", pool=pool) for i in range(5)]
    dbs = [Database(b) for b in backends]
    try:
        for i, db in enumerate(dbs):
            await db.execute_write(f"create table t as select {i} v")
        # All five awake: three processes (2 + 2 + 1).
        assert pool.processes == 3
        results = await asyncio.gather(*[db.execute("select v from t") for db in dbs])
        assert [r.rows[0][0] for r in results] == list(range(5))
        pids = [b.pid for b in backends]
        assert len(set(pids)) == 3
        for db in dbs:
            await db.sleep()
        assert pool.processes == 0
        await asyncio.sleep(0)
        from dblayer.backends.sqlite_worker import reap_workers

        assert reap_workers(timeout=5.0) == []
        # Wake one again: it may land on any process; its data is intact.
        assert (await dbs[3].execute("select v from t")).rows == [(3,)]
    finally:
        for db in dbs:
            await db.close()
        pool.close()


async def test_pool_recycles_a_process_after_its_wake_budget(tmp_path):
    from dblayer.backends.duckdb_worker import DuckDBWorkerPool

    pool = DuckDBWorkerPool(databases_per_process=4, wakes_per_process=2)
    a, b, c = (Database(DuckDBWorker(tmp_path / f"{n}.duckdb", pool=pool)) for n in "abc")
    try:
        await a.execute("select 1")
        first = a._backend.pid  # type: ignore[attr-defined]
        await b.execute("select 1")
        assert b._backend.pid == first  # type: ignore[attr-defined]
        await b.sleep()
        # The first process has used its two wakes: c goes to a new one,
        # and the first exits once a sleeps.
        await c.execute("select 1")
        assert c._backend.pid != first  # type: ignore[attr-defined]
        await a.sleep()
        assert pool.processes == 1
    finally:
        for db in (a, b, c):
            await db.close()
        pool.close()


async def test_spare_worker_is_taken_on_wake_and_replaced(tmp_path):
    from dblayer.backends.duckdb_worker import SpareWorkers

    spares = SpareWorkers(1)
    b = DuckDBWorker(tmp_path / "s.duckdb", spares=spares)
    db = Database(b)
    try:
        spares.fill(b.placement_group.config)
        deadline = asyncio.get_running_loop().time() + 30
        while spares.ready < 1:
            assert asyncio.get_running_loop().time() < deadline
            await asyncio.sleep(0.01)
        spare_pid = spares._spares[0].pid
        assert (await db.execute("select 42")).rows == [(42,)]
        assert b.pid == spare_pid  # the wake took the pre-started process
        assert spares.taken == 1
        await db.sleep()
        while spares.ready < 1:
            assert asyncio.get_running_loop().time() < deadline
            await asyncio.sleep(0.01)
        assert (await db.execute("select 43")).rows == [(43,)]
        assert spares.taken == 2 and b.pid != spare_pid
        print(f"\n[duckdb-worker] wake on a spare: {b.placement_group.last_start_ms:.1f} ms to take it")
    finally:
        await db.close()
        spares.close()
