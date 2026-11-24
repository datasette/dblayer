"""DuckDB in a worker process that the database owns (``DuckDBWorker``)."""

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


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    # A zombie still answers kill(0); the placement reaps its children.
    return True


async def test_every_connection_of_a_database_shares_one_process(tmp_path):
    b = DuckDBWorker(tmp_path / "a.duckdb")
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


async def test_concurrent_first_opens_start_one_process(tmp_path):
    b = DuckDBWorker(tmp_path / "c.duckdb")
    db = Database(b)
    try:
        await asyncio.gather(db.execute("select 1"), db.execute("select 2"), db.execute_write("create table t (i int)"))
        assert b.placement_group.started == 1
    finally:
        await db.close()


async def test_process_exits_when_the_database_sleeps(tmp_path):
    path = tmp_path / "s.duckdb"
    b = DuckDBWorker(path)
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


async def test_sleep_then_immediate_wake_waits_for_the_old_process(tmp_path):
    b = DuckDBWorker(tmp_path / "w.duckdb")
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


async def test_memory_limit_and_threads_apply_inside_the_worker(tmp_path):
    db = Database(DuckDBWorker(tmp_path / "m.duckdb", memory_limit="64MB", threads=1))
    try:
        r = await db.execute("select current_setting('memory_limit'), current_setting('threads')")
        limit, threads = r.rows[0]
        assert "MiB" in limit or "MB" in limit
        assert threads == 1
    finally:
        await db.close()
