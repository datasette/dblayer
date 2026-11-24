"""File locking and durability of the Node worker (SQLite-WASM, ``nodeworker/``).

Node has no API for POSIX advisory locks, so the worker's VFS keeps SQLite's
lock states in shared memory inside its process. These tests pin down what
that means:

* inside the process (every connection of a database: one Node process,
  a thread each) locking is correct: writers exclude each other, readers
  and a writer coexist, a commit waits for readers;
* across processes there is no locking at all, in either direction. A Node
  worker writing next to native SQLite can corrupt the database: it takes a
  live native writer's journal for a hot journal and rolls it back. Hence
  the backend's rule that nothing else opens the file while the worker
  does;
* durability does not depend on locks: after a SIGKILL mid-transaction
  native SQLite (or the next Node worker) rolls the hot journal back and
  the file passes ``integrity_check``;
* WAL mode is unavailable (no shared memory in the VFS).
"""

from __future__ import annotations

import asyncio
import os
import signal
import sqlite3
import subprocess
import sys
import textwrap
import time

import pytest

from dblayer import Database, QueryError
from dblayer.backends.sqlite_nodeworker import SqliteNodeWorker, unavailable

UNAVAILABLE = unavailable()
pytestmark = pytest.mark.skipif(UNAVAILABLE is not None, reason=UNAVAILABLE or "")


def _native(path, **kw):
    return sqlite3.connect(path, isolation_level=None, **kw)


async def test_locking_inside_the_process(tmp_path):
    """Two backends on one file share one Node process; its connections
    lock each other as SQLite's own in-process locking does."""
    path = tmp_path / "in.db"
    a = Database(SqliteNodeWorker(path))
    b = Database(SqliteNodeWorker(path))
    try:
        await a.execute_write("create table t (x)")
        async with a.transaction() as tx:
            await tx.execute("insert into t values (1)")
            # a reader on another connection sees the committed state
            assert (await b.execute("select count(*) from t")).rows == [(0,)]
            # a second writer waits for the first (here: the pool's write
            # slot is per Database, so b's write connection meets the lock)
            task = asyncio.create_task(b.execute_write("insert into t values (2)"))
            await asyncio.sleep(0.2)
            assert not task.done()
        await asyncio.wait_for(task, 5)
        assert (await a.execute("select x from t order by x")).rows == [(1,), (2,)]
    finally:
        await a.close()
        await b.close()


async def test_node_locks_are_invisible_to_native_sqlite(tmp_path):
    path = tmp_path / "a.db"
    db = Database(SqliteNodeWorker(path))
    try:
        await db.execute_write("create table t (x)")
        async with db.transaction() as tx:  # BEGIN IMMEDIATE: RESERVED, in the worker
            await tx.execute("insert into t values (1)")
            other = _native(path, timeout=0)
            other.execute("begin immediate")  # succeeds: nothing is locked as far as the OS knows
            other.execute("rollback")
            other.close()
    finally:
        await db.close()


async def test_native_locks_are_invisible_to_the_node_worker(tmp_path):
    path = tmp_path / "b.db"
    n = _native(path)
    n.execute("create table t (x)")
    n.execute("begin exclusive")  # the strongest native lock
    db = Database(SqliteNodeWorker(path))
    try:
        r = await db.execute("select count(*) from t")  # a native reader would get SQLITE_BUSY
        assert r.rows == [(0,)]
    finally:
        await db.close()
        n.execute("rollback")
        n.close()


async def test_writing_next_to_a_native_writer_corrupts_the_database(tmp_path):
    """The hazard the one-owner rule prevents. A native connection is
    mid-transaction with its journal on disk; the Node worker cannot see
    its RESERVED lock, so it treats the live journal as hot, rolls it back
    and writes; the native commit then fails, and the file is damaged."""
    path = tmp_path / "c.db"
    n = _native(path)
    n.execute("create table t (x)")
    n.execute("pragma cache_size = 10")  # spill to the file mid-transaction
    n.execute("begin immediate")
    n.executemany("insert into t values (?)", [[i] for i in range(100, 5000)])
    assert (tmp_path / "c.db-journal").exists()
    db = Database(SqliteNodeWorker(path))
    try:
        await db.execute_write("insert into t values (2)")  # no SQLITE_BUSY
    finally:
        await db.close()
    assert not (tmp_path / "c.db-journal").exists()  # "recovered" under the live writer
    with pytest.raises(sqlite3.Error) as commit_error:
        n.execute("commit")
    n.close()
    try:
        check = sqlite3.connect(path).execute("pragma integrity_check").fetchall()
        damaged = check != [("ok",)]
    except sqlite3.DatabaseError as e:
        check, damaged = str(e), True
    print(f"\n[nodeworker] native commit after the Node write: {commit_error.value}; integrity_check: {check!r}"[:400])
    assert damaged


def _spill_writer(path: str) -> str:
    """A script: open a Node-worker transaction big enough to spill pages
    into the database file, print the worker pid, then wait."""
    return textwrap.dedent(f"""
        import asyncio, sys
        from dblayer import Database
        from dblayer.backends.sqlite_nodeworker import SqliteNodeWorker

        async def main():
            db = Database(SqliteNodeWorker({path!r}))
            await db.execute_write("create table if not exists t (x)")
            await db.execute_write("insert into t values (-1)")
            async with db.transaction() as tx:
                await tx.execute("pragma cache_size = 10")
                await tx.execute(
                    "with recursive c(i) as (select 1 union all select i + 1 from c where i < 20000) "
                    "insert into t select i from c"
                )
                print(tx._conn.process.pid, flush=True)
                await asyncio.sleep(60)

        asyncio.run(main())
    """)


def _kill_mid_transaction(tmp_path, name: str) -> str:
    path = str(tmp_path / name)
    p = subprocess.Popen([sys.executable, "-c", _spill_writer(path)], stdout=subprocess.PIPE, text=True)
    try:
        pid = int(p.stdout.readline())  # type: ignore[union-attr]
        size_mid = os.path.getsize(path)
        assert os.path.exists(path + "-journal")
        os.kill(pid, signal.SIGKILL)  # the Node worker, mid-transaction
        for _ in range(500):
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                break
            time.sleep(0.01)
    finally:
        p.kill()
        p.wait()
    assert os.path.exists(path + "-journal")  # hot
    assert size_mid > 8192  # the transaction had reached the file
    return path


def test_sigkill_mid_transaction_then_native_sqlite_recovers(tmp_path):
    path = _kill_mid_transaction(tmp_path, "k.db")
    c = sqlite3.connect(path)
    assert c.execute("pragma integrity_check").fetchall() == [("ok",)]
    assert c.execute("select count(*), min(x) from t").fetchall() == [(1, -1)]  # only the committed row
    c.close()
    assert not os.path.exists(path + "-journal")


async def test_sigkill_mid_transaction_then_a_node_reader_recovers(tmp_path):
    path = _kill_mid_transaction(tmp_path, "r.db")
    db = Database(SqliteNodeWorker(path))
    try:
        # a read connection (mode=ro) meets the hot journal and recovers
        # through a read-write connection, as the in-process backend does
        assert (await db.execute("select count(*) from t")).rows == [(1,)]
    finally:
        await db.close()
    assert not os.path.exists(path + "-journal")
    assert sqlite3.connect(path).execute("pragma integrity_check").fetchall() == [("ok",)]


WRITER = """
import sqlite3, sys, time
c = sqlite3.connect(sys.argv[1], isolation_level=None, timeout=5)
end = time.time() + float(sys.argv[2])
k = 0
while time.time() < end:
    k += 1
    c.execute("begin immediate")
    c.execute("insert into t values (?)", [k])
    c.execute("insert into t values (?)", [-k])
    c.execute("commit")
print(k)
"""


async def test_native_writer_in_another_process_while_the_node_worker_reads(tmp_path):
    """Reads with no lock against a writer with one: each native commit
    keeps sum(x) at 0, so a consistent read always sees 0. Even a reader is
    unsafe: meeting the writer's live journal, it "recovers" it through a
    read-write connection (as it must after a kill of its own writer), and
    the native commit fails. Reported, not asserted: the outcome depends on
    timing."""
    path = tmp_path / "w.db"
    n = _native(path)
    n.execute("create table t (x)")
    n.close()
    writer = subprocess.Popen(
        [sys.executable, "-c", WRITER, str(path), "2"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    db = Database(SqliteNodeWorker(path))
    reads = torn = errors = 0
    kinds: set[str] = set()
    try:
        while writer.poll() is None:
            try:
                r = await db.execute("select count(*), coalesce(sum(x), 0) from t")
                reads += 1
                if r.rows[0][1] != 0 or r.rows[0][0] % 2:
                    torn += 1
            except QueryError as e:
                errors += 1
                kinds.add(str(e)[:60])
    finally:
        await db.close()
        out, err = writer.communicate()
    commits = int(out or 0)
    failure = err.strip().splitlines()[-1] if err.strip() else None
    try:
        integrity = sqlite3.connect(path).execute("pragma integrity_check").fetchall()
    except sqlite3.DatabaseError as e:
        integrity = str(e)
    print(
        f"\n[nodeworker] native writer: {commits} commits, failure {failure!r}; Node reader: {reads} reads, "
        f"{torn} inconsistent, {errors} errors {sorted(kinds)}; integrity afterwards {integrity}"
    )
    assert reads + errors > 0


async def test_wal_mode_is_unavailable(tmp_path):
    db = Database(SqliteNodeWorker(tmp_path / "wal.db"))
    try:
        r = await db.execute_write("pragma journal_mode = wal")
        mode = (await db.execute("pragma journal_mode")).rows[0][0]
        print(f"\n[nodeworker] journal_mode=wal -> {r.rows if hasattr(r, 'rows') else r}; now {mode!r}")
        assert mode == "delete"
    finally:
        await db.close()
    # a database already in WAL mode (made by native SQLite)
    path = tmp_path / "native-wal.db"
    n = _native(path)
    n.execute("pragma journal_mode = wal")
    n.execute("create table t (x)")
    n.execute("insert into t values (1)")
    n.close()  # checkpointed; the header still says WAL
    db = Database(SqliteNodeWorker(path))
    try:
        with pytest.raises(QueryError) as e:
            await db.execute("select count(*) from t")
        print(f"[nodeworker] reading a WAL-mode database: {type(e.value).__name__}: {e.value}")
    finally:
        await db.close()
