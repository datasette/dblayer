"""Locking for ``SqliteWasm``: WASI has no ``fcntl`` locks, so the module's
VFS calls host functions that take real POSIX record locks on the bytes
the native unix VFS uses. These tests prove that ordinary SQLite in another
process and the WebAssembly instances exclude each other: no torn reads
while another process writes, no lost updates with writers on both sides,
locks visible both ways, and a writer killed mid-transaction leaves a hot
journal that the next opener rolls back. Plus the sandbox: no other files."""

from __future__ import annotations

import asyncio
import os
import signal
import sqlite3
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

from dblayer import Database, QueryError
from dblayer.backends.sqlite_wasm import module_unavailable

UNAVAILABLE = module_unavailable()
pytestmark = pytest.mark.skipif(UNAVAILABLE is not None, reason=UNAVAILABLE or "")

ROOT = Path(__file__).resolve().parent.parent.parent


def _wasm(path, **kwargs):
    from dblayer.backends.sqlite_wasm import SqliteWasm

    return SqliteWasm(path, **kwargs)


def _setup(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        create table a (id integer primary key, v integer, pad blob);
        create table b (id integer primary key, v integer, pad blob);
        create table counter (n integer);
        insert into counter values (0);
        """
    )
    # Several pages per table, so a commit rewrites pages of both tables
    # and a reader without locks could see one table updated and not the other.
    for t, v in (("a", 600), ("b", 400)):
        conn.execute(f"insert into {t} values (1, {v}, randomblob(100))")
        conn.executemany(f"insert into {t} (v, pad) values (0, randomblob(3000))", [()] * 20)
    conn.commit()
    conn.close()


def _spawn(script: str, *args: str) -> subprocess.Popen[str]:
    return subprocess.Popen(
        [sys.executable, "-c", textwrap.dedent(script), *args],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=ROOT,
    )


NATIVE_WRITER = """
import sqlite3, sys, time
conn = sqlite3.connect(sys.argv[1], timeout=30, isolation_level=None)
print("ready", flush=True)
for i in range(int(sys.argv[2])):
    time.sleep(float(sys.argv[3]) if len(sys.argv) > 3 else 0)
    conn.execute("begin immediate")
    # Rewrite every page of both tables, so that the commit writes many
    # pages and a reader without locks would catch some old, some new.
    conn.execute("update a set v = v + 1 where id = 1")
    conn.execute("update a set pad = randomblob(length(pad))")
    conn.execute("update b set pad = randomblob(length(pad))")
    conn.execute("update b set v = v - 1 where id = 1")
    conn.execute("update counter set n = n + 1")
    conn.execute("commit")
print("done", flush=True)
"""


async def test_no_torn_reads_while_another_process_writes(tmp_path):
    path = tmp_path / "torn.db"
    _setup(path)
    db = Database(_wasm(path), name="torn")
    try:
        p = _spawn(NATIVE_WRITER, str(path), "400", "0.002")
        assert p.stdout.readline().strip() == "ready"
        sums: list[int] = []

        async def reader() -> None:
            while p.poll() is None:
                r = await db.execute("select (select sum(v) from a) + (select sum(v) from b) as s")
                sums.append(r.rows[0]["s"])

        await asyncio.gather(*(reader() for _ in range(3)))
        out, err = p.communicate(timeout=60)
        assert p.returncode == 0, err
        print(f"\n[wasm] {len(sums)} reads while another process committed 400 transactions")
        assert len(sums) > 50 and set(sums) == {1000}
        assert (await db.execute("select n from counter")).rows == [(400,)]
    finally:
        await db.close()


async def test_the_torn_read_test_detects_missing_locks(tmp_path, monkeypatch):
    """Negative control for the test above: with the host lock functions
    turned into no-ops, the same readers do see half-committed states."""
    import dblayer.backends.sqlite_wasm as sw

    monkeypatch.setattr(sw, "_lock", lambda node, h, level: setattr(h, "lock", level) or 0)
    monkeypatch.setattr(sw, "_unlock", lambda node, h, level: setattr(h, "lock", level) or 0)
    monkeypatch.setattr(sw, "_check_reserved", lambda node: 0)
    path = tmp_path / "nolock.db"
    _setup(path)
    db = Database(_wasm(path), name="nolock")
    try:
        p = _spawn(NATIVE_WRITER, str(path), "400", "0.002")
        assert p.stdout.readline().strip() == "ready"
        good = bad = 0

        async def reader() -> None:
            nonlocal good, bad
            while p.poll() is None:
                try:
                    r = await db.execute("select (select sum(v) from a) + (select sum(v) from b) as s")
                except QueryError:
                    bad += 1
                    continue
                if r.rows[0]["s"] == 1000:
                    good += 1
                else:
                    bad += 1

        await asyncio.gather(*(reader() for _ in range(3)))
        p.communicate(timeout=60)
        print(f"\n[wasm, locks off] {bad} torn or failed reads of {good + bad}")
        assert bad > 0
    finally:
        await db.close()


async def test_no_lost_updates_with_writers_in_both_processes(tmp_path):
    path = tmp_path / "lost.db"
    _setup(path)
    db = Database(_wasm(path), name="lost")
    try:
        p = _spawn(NATIVE_WRITER, str(path), "300")
        assert p.stdout.readline().strip() == "ready"
        for _ in range(300):
            async with db.transaction() as tx:
                await tx.execute("update a set v = v + 1 where id = 1")
                await tx.execute("update b set v = v - 1 where id = 1")
                await tx.execute("update counter set n = n + 1")
        out, err = p.communicate(timeout=60)
        assert p.returncode == 0, err
        assert (await db.execute("select n from counter")).rows == [(600,)]
        assert (await db.execute("select (select v from a where id=1) + (select v from b where id=1)")).rows == [(1000,)]
        assert (await db.execute("pragma integrity_check")).rows == [("ok",)]
    finally:
        await db.close()


HOLD_EXCLUSIVE = """
import sqlite3, sys
conn = sqlite3.connect(sys.argv[1], isolation_level=None)
conn.execute("begin exclusive")
conn.execute("update counter set n = n + 1")
print("locked", flush=True)
sys.stdin.readline()
conn.execute("commit")
print("released", flush=True)
"""

TRY_WRITE = """
import sqlite3, sys
conn = sqlite3.connect(sys.argv[1], timeout=0, isolation_level=None)
try:
    conn.execute("begin immediate")
    conn.execute("rollback")
    print("got it")
except sqlite3.OperationalError as e:
    print(e)
"""


async def test_locks_are_visible_across_processes_both_ways(tmp_path):
    path = tmp_path / "vis.db"
    _setup(path)
    db = Database(_wasm(path, busy_timeout=0.2), name="vis")
    try:
        # A transaction in a WebAssembly instance holds RESERVED: another
        # process cannot start writing.
        async with db.transaction() as tx:
            await tx.execute("update counter set n = n + 1")
            p = _spawn(TRY_WRITE, str(path))
            assert p.communicate(timeout=30)[0].strip() == "database is locked"
        p = _spawn(TRY_WRITE, str(path))
        assert p.communicate(timeout=30)[0].strip() == "got it"
        # Another process holds EXCLUSIVE: instances cannot read.
        holder = _spawn(HOLD_EXCLUSIVE, str(path))
        assert holder.stdout.readline().strip() == "locked"
        with pytest.raises(QueryError) as e:
            await db.execute("select n from counter")
        assert e.value.code == "SQLITE_BUSY"
        holder.stdin.write("\n")
        holder.stdin.flush()
        assert holder.stdout.readline().strip() == "released"
        holder.wait(timeout=30)
        assert (await db.execute("select n from counter")).rows == [(2,)]
    finally:
        await db.close()


WASM_WRITER_KILLED = """
import asyncio, sys
from dblayer import Database
from dblayer.backends.sqlite_wasm import SqliteWasm

async def main():
    db = Database(SqliteWasm(sys.argv[1]))
    await db.execute_write("create table t (id integer primary key, pad blob)")
    await db.execute_write("insert into t (pad) select randomblob(500) from (with r(i) as (select 1 union all select i + 1 from r where i < 1000) select i from r)")
    async with db.transaction() as tx:
        await tx.execute("pragma cache_size = 5")  # spill: pages reach the file before commit
        await tx.execute("update t set pad = randomblob(600)")
        await tx.execute("insert into t (pad) select randomblob(4000) from (with r(i) as (select 1 union all select i + 1 from r where i < 2000) select i from r)")
        print("mid-transaction", flush=True)
        await asyncio.sleep(60)

asyncio.run(main())
"""


async def test_killed_mid_transaction_then_integrity_check(tmp_path):
    """A process using SqliteWasm is SIGKILLed with a write transaction
    half done (pages already in the file, a hot journal beside it). The
    next opener, WebAssembly or native, rolls it back."""
    path = tmp_path / "killed.db"
    p = _spawn(WASM_WRITER_KILLED, str(path))
    line = p.stdout.readline().strip()
    assert line == "mid-transaction", p.stderr.read()
    assert os.path.exists(str(path) + "-journal")
    os.kill(p.pid, signal.SIGKILL)
    p.wait(timeout=30)
    db = Database(_wasm(path), name="killed")
    try:
        assert (await db.execute("select count(*), max(length(pad)) from t")).rows == [(1000, 500)]
        assert (await db.execute("pragma integrity_check")).rows == [("ok",)]
    finally:
        await db.close()
    conn = sqlite3.connect(path)
    assert conn.execute("pragma integrity_check").fetchone() == ("ok",)
    assert conn.execute("select count(*) from t").fetchone() == (1000,)
    conn.execute("begin immediate")  # no lock left behind
    conn.rollback()
    conn.close()


async def test_a_reader_in_this_process_holds_off_a_writer_until_it_ends(tmp_path):
    """Two instances in one process: the per-inode lock table makes a
    read transaction's SHARED lock hold off the writer's commit, as two
    native connections would."""
    path = tmp_path / "inproc.db"
    _setup(path)
    db = Database(_wasm(path), name="inproc")
    try:
        await db.execute("select 1")
        async with db.transaction(mode="read") as rtx:
            before = (await rtx.execute("select n from counter")).rows
            write = asyncio.create_task(db.execute_write("update counter set n = n + 1"))
            await asyncio.sleep(0.3)
            assert not write.done()  # its commit waits for our SHARED lock
            assert (await rtx.execute("select n from counter")).rows == before  # snapshot
        await asyncio.wait_for(write, 10)
        assert (await db.execute("select n from counter")).rows == [(1,)]
    finally:
        await db.close()


async def test_other_files_are_out_of_reach(tmp_path):
    path = tmp_path / "box.db"
    other = tmp_path / "other.db"
    sqlite3.connect(other).execute("create table secret (x)").connection.close()
    db = Database(_wasm(path), name="box")
    try:
        await db.execute_write("create table t (x)")
        with pytest.raises(QueryError, match="unable to open"):
            await db.execute_write(f"attach database '{other}' as o")
        with pytest.raises(QueryError):
            await db.execute_write(f"vacuum into '{tmp_path / 'copy.db'}'")
        assert not (tmp_path / "copy.db").exists()
        # In-memory attachments and temporary tables still work.
        await db.execute_write("attach database ':memory:' as m")
        assert (await db.execute("with r(i) as (select 1 union all select i + 1 from r where i < 100000) "
                                 "select count(distinct i) from r")).rows == [(100000,)]
    finally:
        await db.close()


async def test_wal_is_not_available(tmp_path):
    path = tmp_path / "wal.db"
    db = Database(_wasm(path), name="wal")
    try:
        await db.execute_write("create table t (x)")
        r = await db.execute_write("pragma journal_mode = wal")
        assert r.rows[0][0] == "delete"
    finally:
        await db.close()
    # A file another program put in WAL mode cannot be opened.
    walpath = tmp_path / "native-wal.db"
    conn = sqlite3.connect(walpath)
    conn.execute("pragma journal_mode = wal")
    conn.execute("create table t (x)")
    conn.commit()
    conn.close()
    db = Database(_wasm(walpath), name="native-wal")
    try:
        with pytest.raises(QueryError) as e:
            await db.execute("select count(*) from t")
        print(f"\n[wasm] WAL-mode file: {e.value.code}: {e.value}")
    finally:
        await db.close()
