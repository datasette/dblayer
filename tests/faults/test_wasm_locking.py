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
import shutil
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


async def _read_sum(db: Database) -> int:
    """Both halves of the invariant in one read transaction, with a pause
    between them: with locks, the transaction's SHARED lock keeps the other
    process's commit out until it ends; without, the commit lands between."""
    async with db.transaction(mode="read") as tx:
        a = (await tx.execute("select sum(v) as s from a")).rows[0]["s"]
        await asyncio.sleep(0.002)
        b = (await tx.execute("select sum(v) as s from b")).rows[0]["s"]
    return a + b


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
                sums.append(await _read_sum(db))

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
                    total = await _read_sum(db)
                except QueryError:
                    bad += 1
                    continue
                if total == 1000:
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


def _ofd() -> bool:
    from dblayer.backends.sqlite_wasm import OFD_LOCKS

    return OFD_LOCKS


needs_ofd = pytest.mark.skipif(
    UNAVAILABLE is not None or not _ofd(),
    reason="no open file description locks here (Linux and macOS have them)",
)


def test_open_file_description_locks_are_used_on_linux_and_macos():
    if sys.platform.startswith("linux") or sys.platform == "darwin":
        assert _ofd() or os.environ.get("DBLAYER_WASM_POSIX_LOCKS") == "1"


@needs_ofd
async def test_ofd_locks_exclude_sqlite3_in_this_same_process(tmp_path):
    """With F_OFD_SETLK the instances' locks conflict with the classic
    POSIX locks of ordinary SQLite even in this process, and a ``sqlite3``
    connection closing its descriptor does not drop them."""
    path = tmp_path / "ofd.db"
    _setup(path)
    db = Database(_wasm(path, busy_timeout=0.2), name="ofd")
    try:
        async with db.transaction() as tx:
            await tx.execute("update counter set n = n + 1")
            native = sqlite3.connect(path, timeout=0, isolation_level=None)
            with pytest.raises(sqlite3.OperationalError, match="locked"):
                native.execute("begin immediate")
            native.close()  # closes a descriptor of the file in this process
            p = _spawn(TRY_WRITE, str(path))
            assert p.communicate(timeout=30)[0].strip() == "database is locked"
        native = sqlite3.connect(path, isolation_level=None)
        native.execute("begin exclusive")
        with pytest.raises(QueryError) as e:
            await db.execute("select n from counter")
        assert e.value.code == "SQLITE_BUSY"
        native.execute("commit")
        native.close()
        assert (await db.execute("select n from counter")).rows == [(1,)]
    finally:
        await db.close()


@needs_ofd
async def test_opening_and_closing_instances_never_drops_sqlite3_locks_in_this_process(tmp_path):
    """Closing any descriptor of a file drops every classic lock this
    process holds on it, including ordinary SQLite's. So while ``sqlite3``
    here holds EXCLUSIVE, new instances opening the file (and closing it
    again) must neither open-and-close a descriptor nor close the last one:
    the lock survives, instances keep getting SQLITE_BUSY, and the
    descriptor parked meanwhile is closed once the lock is gone."""
    import dblayer.backends.sqlite_wasm as sw

    path = tmp_path / "keep.db"
    _setup(path)
    native = sqlite3.connect(path, isolation_level=None)
    native.execute("begin exclusive")
    try:
        for i in range(3):
            db = Database(_wasm(path, busy_timeout=0.05), name=f"keep{i}")
            try:
                with pytest.raises(QueryError) as e:
                    await db.execute("select n from counter")
                assert e.value.code == "SQLITE_BUSY"
            finally:
                await db.close()
            p = _spawn(TRY_WRITE, str(path))
            assert p.communicate(timeout=30)[0].strip() == "database is locked", i
        assert sum(len(v) for v in sw._parked.values()) <= 1
    finally:
        native.execute("rollback")
        native.close()
    db = Database(_wasm(path), name="keep")
    try:
        assert (await db.execute("select n from counter")).rows == [(0,)]
    finally:
        await db.close()
    assert not sw._parked


def _native_writer_thread(path: Path, n: int, stop: list[bool]) -> None:
    """NATIVE_WRITER, on a thread of this process (ordinary SQLite: classic
    POSIX locks owned by this very process)."""
    conn = sqlite3.connect(path, timeout=30, isolation_level=None, check_same_thread=False)
    try:
        for _ in range(n):
            time.sleep(0.002)
            conn.execute("begin immediate")
            conn.execute("update a set v = v + 1 where id = 1")
            conn.execute("update a set pad = randomblob(length(pad))")
            conn.execute("update b set pad = randomblob(length(pad))")
            conn.execute("update b set v = v - 1 where id = 1")
            conn.execute("update counter set n = n + 1")
            conn.execute("commit")
    finally:
        conn.close()
        stop[0] = True


@needs_ofd
async def test_no_torn_reads_while_sqlite3_in_this_process_writes(tmp_path):
    """The in-process backend's library (``sqlite3``) writing the same file
    from a thread of this process while instances read: safe only because
    open file description locks conflict with this process's own POSIX
    locks. With classic locks this is the documented unsafe case."""
    import threading

    path = tmp_path / "same.db"
    _setup(path)
    db = Database(_wasm(path), name="same")
    stop = [False]
    t = threading.Thread(target=_native_writer_thread, args=(path, 300, stop))
    try:
        t.start()
        sums: list[int] = []

        async def reader() -> None:
            while not stop[0]:
                sums.append(await _read_sum(db))

        await asyncio.gather(*(reader() for _ in range(3)))
        t.join(60)
        print(f"\n[wasm] {len(sums)} reads while sqlite3 in this process committed 300 transactions")
        assert len(sums) > 30 and set(sums) == {1000}
        assert (await db.execute("select n from counter")).rows == [(300,)]
    finally:
        t.join(60)
        await db.close()


@needs_ofd
async def test_in_process_backend_and_wasm_writers_in_one_process(tmp_path):
    """``SqliteInProcess`` and ``SqliteWasm`` on one file in one process,
    both writing: no lost update, integrity ok."""
    from dblayer.backends.sqlite import SqliteInProcess

    path = tmp_path / "both.db"
    _setup(path)
    local = Database(SqliteInProcess(path), name="local")
    wasm = Database(_wasm(path, busy_timeout=10), name="wasm")

    async def bump(db: Database, n: int) -> None:
        for _ in range(n):
            async with db.transaction() as tx:
                await tx.execute("update a set v = v + 1 where id = 1")
                await tx.execute("update b set v = v - 1 where id = 1")
                await tx.execute("update counter set n = n + 1")

    try:
        await asyncio.gather(bump(local, 200), bump(wasm, 200))
        for db in (local, wasm):
            assert (await db.execute("select n from counter")).rows == [(400,)]
            assert (await db.execute("select (select v from a where id=1) + (select v from b where id=1)")).rows == [
                (1000,)
            ]
        assert (await wasm.execute("pragma integrity_check")).rows == [("ok",)]
    finally:
        await local.close()
        await wasm.close()


async def test_no_torn_reads_while_a_c_worker_writes(tmp_path):
    """Another backend's worker process (the C worker) as the writer."""
    from dblayer.backends.sqlite_cworker import SqliteCWorker, binary_unavailable

    if binary_unavailable():
        pytest.skip(binary_unavailable() or "")
    path = tmp_path / "cw.db"
    _setup(path)
    cw = Database(SqliteCWorker(path), name="cw")
    db = Database(_wasm(path), name="wasm-cw")
    done = False
    try:

        async def writer() -> None:
            nonlocal done
            try:
                for _ in range(150):
                    async with cw.transaction() as tx:
                        await tx.execute("update a set v = v + 1 where id = 1")
                        await tx.execute("update a set pad = randomblob(length(pad))")
                        await tx.execute("update b set pad = randomblob(length(pad))")
                        await tx.execute("update b set v = v - 1 where id = 1")
                        await tx.execute("update counter set n = n + 1")
            finally:
                done = True

        sums: list[int] = []

        async def reader() -> None:
            while not done:
                sums.append(await _read_sum(db))

        await asyncio.gather(writer(), reader(), reader())
        print(f"\n[wasm] {len(sums)} reads while a C worker committed 150 transactions")
        assert len(sums) > 20 and set(sums) == {1000}
    finally:
        await cw.close()
        await db.close()


@pytest.mark.skipif(shutil.which("sqlite3") is None, reason="no sqlite3 command-line shell")
async def test_no_torn_reads_while_the_sqlite3_shell_writes(tmp_path):
    path = tmp_path / "cli.db"
    _setup(path)
    script = "".join(
        "begin immediate; update a set v = v + 1 where id = 1; update a set pad = randomblob(length(pad));"
        " update b set pad = randomblob(length(pad)); update b set v = v - 1 where id = 1;"
        " update counter set n = n + 1; commit;\n"
        for _ in range(300)
    )
    db = Database(_wasm(path), name="cli")
    try:
        p = subprocess.Popen(
            ["sqlite3", "-cmd", ".timeout 30000", str(path)], stdin=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        assert p.stdin is not None
        p.stdin.write(script)
        p.stdin.close()
        sums: list[int] = []

        async def reader() -> None:
            while p.poll() is None:
                sums.append(await _read_sum(db))

        await asyncio.gather(reader(), reader())
        assert p.wait(60) == 0, p.stderr.read() if p.stderr else ""
        print(f"\n[wasm] {len(sums)} reads while the sqlite3 shell committed 300 transactions")
        assert set(sums) <= {1000} and (await db.execute("select n from counter")).rows == [(300,)]
    finally:
        await db.close()


async def test_switched_to_wal_by_another_process_while_open(tmp_path):
    """Another process turns an open database to WAL: the instance's next
    statement fails cleanly (no shared memory, so it cannot follow), the
    file is intact, and the other process carries on in WAL."""
    path = tmp_path / "towal.db"
    _setup(path)
    db = Database(_wasm(path), name="towal")
    try:
        assert (await db.execute("select n from counter")).rows == [(0,)]
        conn = sqlite3.connect(path, isolation_level=None)
        assert conn.execute("pragma journal_mode = wal").fetchone() == ("wal",)
        conn.execute("update counter set n = 7")
        with pytest.raises(QueryError) as e:
            await db.execute("select n from counter")
        print(f"\n[wasm] after another process switched to WAL: {e.value.code}: {e.value}")
        with pytest.raises(QueryError):
            await db.execute_write("update counter set n = 8")
        assert conn.execute("select n from counter").fetchone() == (7,)
        assert conn.execute("pragma integrity_check").fetchone() == ("ok",)
        conn.close()
    finally:
        await db.close()
