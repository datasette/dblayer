"""File locking and durability of the Node worker (SQLite-WASM, ``nodeworker/``).

Node has no API for POSIX advisory locks, so the worker's VFS keeps SQLite's
lock states in shared memory inside its process. These tests pin down what
that means:

* inside the process (every connection of a database: one Node process,
  a thread each) locking is correct: writers exclude each other, readers
  and a writer coexist, a commit waits for readers;
* across processes, the VFS's locks are invisible, so the worker holds a
  cross-process guard (``nodeworker/src/guard.mjs``): a ``node:sqlite``
  connection in BEGIN EXCLUSIVE, an EXCLUSIVE fcntl lock, for as long as a
  connection is open. Native SQLite is locked out (SQLITE_BUSY) instead of
  meeting a writer it cannot see; a native lock makes the worker's open
  wait, then refuse. Without the guard (``DBLAYER_NODEWORKER_GUARD=0``) a
  Node worker writing next to native SQLite corrupts the database;
* durability does not depend on locks: after a SIGKILL mid-transaction
  native SQLite (or the next Node worker) rolls the hot journal back and
  the file passes ``integrity_check``;
* WAL mode is unavailable (no shared memory in the VFS).
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import sqlite3
import struct
import subprocess
import sys
import textwrap
import time

import pytest

from dblayer import Database, QueryError
from dblayer.backends.sqlite_nodeworker import SqliteNodeWorker, find_node, find_script, unavailable

from ..helpers import wait_until

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


async def test_the_guard_locks_native_sqlite_out(tmp_path):
    """While the Node worker has the file open, its cross-process guard (a
    ``node:sqlite`` connection holding BEGIN EXCLUSIVE, guard.mjs) is an
    EXCLUSIVE fcntl lock: native SQLite can neither write nor read."""
    path = tmp_path / "a.db"
    db = Database(SqliteNodeWorker(path))
    try:
        await db.execute_write("create table t (x)")
        async with db.transaction() as tx:
            await tx.execute("insert into t values (1)")
            other = _native(path, timeout=0)
            with pytest.raises(sqlite3.OperationalError, match="locked"):
                other.execute("begin immediate")
            with pytest.raises(sqlite3.OperationalError, match="locked"):
                other.execute("select count(*) from t")
            other.close()
        # between transactions too: the guard is held while a connection is open
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            _native(path, timeout=0).execute("select count(*) from t")
        # the worker's own connections are not affected by its guard
        assert (await db.execute("select count(*) from t")).rows == [(1,)]
        proc = db._backend.process  # type: ignore[attr-defined]
    finally:
        await db.close()
    # The worker gone, the file is free. close() does not wait for the
    # process to exit, and the guard goes only with it (as documented in
    # sqlite_nodeworker), so wait for the exit: asserting straight after
    # close() failed once on Linux CI with "database is locked".
    await wait_until(lambda: proc.popen.poll() is not None)
    assert _native(path, timeout=0).execute("select count(*) from t").fetchall() == [(1,)]


async def test_a_native_lock_refuses_the_node_worker(tmp_path):
    """A native connection holds a lock: the Node worker waits for its busy
    timeout, then refuses to open the file (it would not see the lock)."""
    path = tmp_path / "b.db"
    n = _native(path)
    n.execute("create table t (x)")
    n.execute("begin exclusive")
    db = Database(SqliteNodeWorker(path, busy_timeout=0.3))
    try:
        t0 = time.monotonic()
        with pytest.raises(QueryError, match="database is locked") as e:
            await db.execute("select count(*) from t")
        waited = time.monotonic() - t0
        print(f"\n[nodeworker] open next to a native EXCLUSIVE lock: refused after {waited * 1000:.0f} ms: {e.value}")
        assert waited >= 0.25
        n.execute("rollback")
        assert (await db.execute("select count(*) from t")).rows == [(0,)]
    finally:
        await db.close()
        n.close()


async def test_a_reader_waits_for_a_native_writer_and_takes_over(tmp_path):
    """A native writer mid-transaction (RESERVED, its journal on disk): the
    Node worker's open waits; once the writer commits it opens, and the
    native commit stands."""
    path = tmp_path / "c.db"
    n = _native(path)
    n.execute("create table t (x)")
    n.execute("pragma cache_size = 10")
    n.execute("begin immediate")
    n.executemany("insert into t values (?)", [[i] for i in range(100, 5000)])
    assert (tmp_path / "c.db-journal").exists()
    db = Database(SqliteNodeWorker(path))
    try:
        task = asyncio.create_task(db.execute_write("insert into t values (2)"))
        await asyncio.sleep(0.3)
        assert not task.done()
        n.execute("commit")
        await asyncio.wait_for(task, 10)
        assert (await db.execute("select count(*) from t")).rows == [(4901,)]
    finally:
        await db.close()
        n.close()
    assert sqlite3.connect(path).execute("pragma integrity_check").fetchall() == [("ok",)]


async def test_without_the_guard_writing_next_to_a_native_writer_corrupts(tmp_path, monkeypatch):
    """The hazard the guard prevents (DBLAYER_NODEWORKER_GUARD=0). A native
    connection is mid-transaction with its journal on disk; the Node
    worker cannot see its RESERVED lock, so it treats the live journal as
    hot, rolls it back and writes; the native commit then fails, and the
    file is damaged."""
    monkeypatch.setenv("DBLAYER_NODEWORKER_GUARD", "0")
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
    print(f"\n[nodeworker, no guard] native commit after the Node write: {commit_error.value}; integrity_check: {check!r}"[:400])
    assert damaged


async def test_two_node_workers_in_two_processes(tmp_path):
    """Another Python process with its own Node worker on the same file is
    refused while this one has it open."""
    path = tmp_path / "two.db"
    db = Database(SqliteNodeWorker(path))
    try:
        await db.execute_write("create table t (x)")
        script = textwrap.dedent(f"""
            import asyncio
            from dblayer import Database, QueryError
            from dblayer.backends.sqlite_nodeworker import SqliteNodeWorker

            async def main():
                db = Database(SqliteNodeWorker({str(path)!r}, busy_timeout=0.2))
                try:
                    await db.execute("select count(*) from t")
                    print("opened")
                except QueryError as e:
                    print("refused:", e)
                finally:
                    await db.close()

            asyncio.run(main())
        """)
        out = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, timeout=60).stdout
        assert out.startswith("refused: database is locked"), out
    finally:
        await db.close()


def test_closing_a_connection_keeps_the_guard(tmp_path):
    """POSIX drops every fcntl lock of a process when it closes any
    descriptor of the file, and Node closes a worker thread's descriptors
    when the thread exits; so while the guard is held the VFS keeps a closed
    connection's descriptor and the main thread keeps its thread for the
    next connection. Raw protocol: two connections,
    close one, the guard still holds; close the other, the file is free."""
    path = str(tmp_path / "g.db")
    p = subprocess.Popen([find_node(), find_script()], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    ids = iter(range(1, 100))

    def call(m: dict) -> dict:
        m["id"] = next(ids)
        b = json.dumps(m).encode()
        p.stdin.write(struct.pack(">I", len(b)) + b)  # type: ignore[union-attr]
        p.stdin.flush()  # type: ignore[union-attr]
        n = struct.unpack(">I", p.stdout.read(4))[0]  # type: ignore[union-attr]
        return json.loads(p.stdout.read(n))  # type: ignore[union-attr]

    def native_locked() -> bool:
        try:
            sqlite3.connect(path, timeout=0).execute("select count(*) from sqlite_master").fetchall()
            return False
        except sqlite3.OperationalError as e:
            assert "locked" in str(e)
            return True

    try:
        call({"type": "hello", "protocol": 1, "codecs": ["json"]})
        w = call({"type": "open", "path": path, "mode": "write"})["conn"]
        r = call({"type": "open", "path": path, "mode": "read"})["conn"]
        assert call({"type": "execute", "conn": r, "sql": "select 1"})["result"]["rows"] == [[1]]
        assert native_locked()
        assert call({"type": "close", "conn": r}).get("ok")
        time.sleep(0.3)  # long enough for a thread to exit, had it been let go
        assert native_locked()  # its descriptor and its thread were kept
        for _ in range(3):  # a new connection reuses the kept thread and descriptor
            r = call({"type": "open", "path": path, "mode": "read"})["conn"]
            assert call({"type": "execute", "conn": r, "sql": "select 2"})["result"]["rows"] == [[2]]
            assert call({"type": "close", "conn": r}).get("ok")
            assert native_locked()
        assert call({"type": "close", "conn": w}).get("ok")
        for _ in range(100):  # the main thread releases it once the executor is gone
            if not native_locked():
                break
            time.sleep(0.01)
        assert not native_locked()
    finally:
        p.stdin.close()  # type: ignore[union-attr]
        p.wait(10)


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
c = sqlite3.connect(sys.argv[1], isolation_level=None, timeout=0.5)
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
    """The Node worker reads in a loop; a native writer in another process
    starts meanwhile. The guard locks the writer out (it fails with
    "database is locked" after its 0.5 s busy timeout); every read is
    consistent and the file stays intact."""
    path = tmp_path / "w.db"
    n = _native(path)
    n.execute("create table t (x)")
    n.close()
    db = Database(SqliteNodeWorker(path))
    reads = torn = 0
    try:
        await db.execute("select count(*) from t")  # the worker has the file
        writer = subprocess.Popen(
            [sys.executable, "-c", WRITER, str(path), "1"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        while writer.poll() is None:
            r = await db.execute("select count(*), coalesce(sum(x), 0) from t")
            reads += 1
            if r.rows[0][1] != 0 or r.rows[0][0] % 2:
                torn += 1
        out, err = writer.communicate()
    finally:
        await db.close()
    failure = err.strip().splitlines()[-1] if err.strip() else None
    integrity = sqlite3.connect(path).execute("pragma integrity_check").fetchall()
    print(f"\n[nodeworker] native writer: {out.strip() or 0} commits, failure {failure!r}; Node reader: {reads} reads, {torn} inconsistent")
    assert failure is not None and "database is locked" in failure
    assert torn == 0 and reads > 0
    assert integrity == [("ok",)]


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
