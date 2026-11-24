"""Behaviour specific to the in-process SQLite backend. Not parametrised by
backend: these tests construct SqliteInProcess directly."""

import asyncio
import contextvars
import sqlite3
import threading

import pytest

from dblayer import Database, NotSupported, QueryError
from dblayer.backends.sqlite import SqliteInProcess

from ..helpers import EVENTUALLY_S, count_open_fds


@pytest.fixture
async def file_db(tmp_path):
    db = Database(SqliteInProcess(tmp_path / "data.db"), name="data")
    yield db
    await db.close()


async def test_read_connections_are_read_only_uri(file_db):
    await file_db.execute_write("create table t (x)")
    with pytest.raises(QueryError) as e:
        await file_db.execute("insert into t values (1)")
    assert e.value.code == "SQLITE_READONLY"


async def test_creating_database_does_not_create_file(tmp_path):
    path = tmp_path / "lazy.db"
    db = Database(SqliteInProcess(path))
    assert not path.exists()
    await db.execute("select 1")
    assert path.exists()
    await db.close()


async def test_create_false_requires_existing_file(tmp_path):
    db = Database(SqliteInProcess(tmp_path / "missing.db", create=False))
    with pytest.raises(QueryError) as e:
        await db.execute("select 1")
    assert e.value.code == "SQLITE_CANTOPEN"
    with pytest.raises(QueryError):
        await db.execute_write("create table t (x)")
    await db.close()


async def test_immutable(tmp_path):
    path = tmp_path / "imm.db"
    conn = sqlite3.connect(path)
    conn.execute("create table t (x)")
    conn.execute("insert into t values (42)")
    conn.commit()
    conn.close()
    db = Database(SqliteInProcess(path, immutable=True))
    assert (await db.execute("select x from t")).rows == [(42,)]
    with pytest.raises(NotSupported):
        await db.execute_write("insert into t values (1)")
    await db.close()


async def test_init_hook_runs_on_every_connection(tmp_path):
    seen = []

    def init(conn: sqlite3.Connection) -> None:
        seen.append(conn)
        conn.create_function("double", 1, lambda v: v * 2)

    db = Database(SqliteInProcess(tmp_path / "f.db", init=init))
    assert (await db.execute("select double(21)")).rows == [(42,)]
    assert (await db.execute_write("select double(2)")).rows == [(4,)]
    assert len(seen) == 2
    await db.close()


async def test_context_variables_reach_the_thread(tmp_path):
    var: contextvars.ContextVar[str] = contextvars.ContextVar("var", default="unset")

    def init(conn: sqlite3.Connection) -> None:
        conn.create_function("ctx", 0, lambda: var.get())

    db = Database(SqliteInProcess(tmp_path / "c.db", init=init))
    var.set("request-1")
    assert (await db.execute("select ctx()")).rows == [("request-1",)]

    async def other():
        var.set("request-2")
        return (await db.execute("select ctx()")).rows[0][0]

    assert await asyncio.create_task(other()) == "request-2"
    await db.close()


async def test_blocking_work_runs_off_the_event_loop_thread(tmp_path):
    loop_thread = threading.get_ident()

    def init(conn: sqlite3.Connection) -> None:
        conn.create_function("tid", 0, threading.get_ident)

    db = Database(SqliteInProcess(tmp_path / "t.db", init=init))
    assert (await db.execute("select tid()")).rows[0][0] != loop_thread
    await db.close()


async def test_memory_database_is_shared_between_connections():
    db = Database(SqliteInProcess.memory())
    await db.execute_write("create table t (x)")
    await db.execute_write("insert into t values (1)")
    # Several read connections all see the writer's data.
    results = await asyncio.gather(*(db.execute("select x from t") for _ in range(5)))
    assert all(r.rows == [(1,)] for r in results)
    with pytest.raises(QueryError):
        await db.execute("insert into t values (2)")  # query_only reader
    await db.close()


async def test_memory_database_survives_sleep():
    db = Database(SqliteInProcess.memory("survivor"))
    await db.execute_write("create table t (x)")
    await db.execute_write("insert into t values (1)")
    await db.sleep()
    assert db.stats().open_total == 0
    assert (await db.execute("select x from t")).rows == [(1,)]
    await db.close()
    # Closing for good releases the anchor; a new backend with the same
    # name starts empty.
    db2 = Database(SqliteInProcess.memory("survivor"))
    assert await db2.table_names() == []
    await db2.close()


async def test_two_memory_backends_with_one_name_share_data():
    a = Database(SqliteInProcess.memory("shared-name"))
    b = Database(SqliteInProcess.memory("shared-name"))
    await a.execute_write("create table t (x)")
    await a.execute_write("insert into t values (7)")
    assert (await b.execute("select x from t")).rows == [(7,)]
    await a.close()
    await b.close()


async def test_unnamed_memory_databases_are_private():
    a = Database(SqliteInProcess.memory())
    b = Database(SqliteInProcess.memory())
    await a.execute_write("create table t (x)")
    assert await b.table_names() == []
    await a.close()
    await b.close()


async def test_change_hint_is_a_stat_and_handles_missing_file(tmp_path):
    backend = SqliteInProcess(tmp_path / "h.db")
    assert backend.change_hint() == (None, None)
    db = Database(backend)
    await db.execute_write("create table t (x)")
    hint = db.change_hint()
    assert hint[0] is not None
    await db.close()


async def test_change_hint_sees_wal_writes(tmp_path):
    def init(conn):
        conn.execute("pragma journal_mode = wal")

    db = Database(SqliteInProcess(tmp_path / "w.db", init=init))
    await db.execute_write("create table t (x)")
    h = db.change_hint()
    await db.execute_write("create table u (x)")
    assert db.change_hint() != h
    assert h[1] is not None  # the -wal file exists and is part of the hint
    await db.close()


async def test_schema_cache_uses_change_hint(file_db):
    await file_db.execute_write("create table t (x)")
    await file_db.table_names()
    opened = file_db.stats().opened_total
    await file_db.sleep()
    # Cache was dropped by sleep, so this wakes the database once ...
    await file_db.table_names()
    assert file_db.stats().opened_total == opened + 1
    # ... and repeated introspection does no further work.
    for _ in range(5):
        await file_db.table_columns("t")
    assert file_db.stats().opened_total == opened + 1


async def test_schema_cache_sees_external_change(tmp_path):
    path = tmp_path / "ext.db"
    db = Database(SqliteInProcess(path))
    await db.execute_write("create table t (x)")
    assert await db.table_names() == ["t"]
    conn = sqlite3.connect(path)
    conn.execute("create table external (y)")
    conn.commit()
    conn.close()
    assert sorted(await db.table_names()) == ["external", "t"]
    await db.close()


async def test_last_pk_is_rowid_for_tables_without_integer_pk(file_db):
    await file_db.execute_write("create table t (code text primary key, v)")
    r = await file_db.execute_write("insert into t (code, v) values ('a', 1)")
    assert r.last_pk == 1  # the rowid, not 'a'


async def test_last_pk_none_when_insert_ignored(file_db):
    await file_db.execute_write("create table t (id integer primary key)")
    await file_db.execute_write("insert into t (id) values (5)")
    r = await file_db.execute_write("insert or ignore into t (id) values (5)")
    assert r.rowcount == 0 and r.last_pk is None


async def test_insert_with_cte_last_pk(file_db):
    await file_db.execute_write("create table t (id integer primary key, v)")
    r = await file_db.execute_write("with x(v) as (select 9) insert into t (v) select v from x")
    assert r.last_pk == 1 and r.rowcount == 1


async def test_introspection_details(file_db):
    for sql in [
        "create table a (id integer primary key, g integer generated always as (id * 2))",
        "create table b (aid integer references a, x)",
        "create trigger trg after insert on a begin select 1; end",
    ]:
        await file_db.execute_write(sql)
    cols = {c.name: c for c in await file_db.table_columns("a")}
    assert cols["g"].generated and not cols["g"].hidden
    # Foreign key to an implicit primary key resolves the column name.
    fk = (await file_db.foreign_keys("b"))[0]
    assert fk.other_columns == ("id",)
    assert "create trigger" in (await file_db.definition("trg")).lower()
    assert (await file_db.schema()).triggers["trg"].table == "a"


@pytest.mark.skipif(
    not SqliteInProcess(":memory:").capabilities.full_text_search, reason="no FTS"
)
async def test_fts_shadow_tables_are_marked_hidden(file_db):
    await file_db.execute_write("create virtual table docs using fts5 (body)")
    schema = await file_db.schema()
    assert schema.tables["docs"].virtual
    shadows = [t for t in schema.tables.values() if t.hidden]
    if sqlite3.sqlite_version_info >= (3, 37):
        assert shadows and all(t.name.startswith("docs_") for t in shadows)


async def test_internal_sqlite_tables_are_not_listed(file_db):
    await file_db.execute_write("create table t (id integer primary key autoincrement)")
    await file_db.execute_write("insert into t default values")
    assert await file_db.table_names() == ["t"]  # no sqlite_sequence


async def test_cancelled_write_releases_write_slot(file_db):
    await file_db.execute_write("create table t (x)")
    task = asyncio.create_task(
        file_db.execute_write(
            "insert into t with recursive c(x) as (select 1 union all select x + 1 from c) "
            "select x from c"
        )
    )
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.wait_for(file_db.execute_write("insert into t values (1)"), EVENTUALLY_S)
    assert (await file_db.execute("select count(*) from t")).rows == [(1,)]


async def test_many_databases_do_not_add_threads(tmp_path):
    # Warm the shared executor to its maximum size first. Queries alone do
    # not guarantee that on a loaded machine (a finished thread is reused
    # before the next submit), so jobs that wait for one another start
    # every thread.
    from dblayer.core.executor import DEFAULT_MAX_THREADS, get_default_executor

    warm = [Database(SqliteInProcess(tmp_path / f"warm{i}.db")) for i in range(16)]
    await asyncio.gather(*(d.execute_write("create table t (x)") for d in warm))
    for d in warm:
        await d.close()
    executor = get_default_executor()
    size = getattr(executor, "_max_workers", DEFAULT_MAX_THREADS)
    barrier = threading.Barrier(size)
    loop = asyncio.get_running_loop()
    await asyncio.gather(*(loop.run_in_executor(executor, barrier.wait, 30) for _ in range(size)))
    threads = threading.active_count()
    dbs = [Database(SqliteInProcess(tmp_path / f"m{i}.db")) for i in range(200)]
    await asyncio.gather(*(d.execute_write("create table t (x)") for d in dbs))
    assert threading.active_count() <= threads
    fds = count_open_fds()
    for d in dbs:
        await d.sleep()
    assert count_open_fds() < fds
    for d in dbs:
        await d.close()


async def test_capabilities():
    caps = SqliteInProcess(":memory:").capabilities
    assert caps.implicit_rowid and caps.cheap_change_hint and caps.sleep_releases_everything
    assert not caps.concurrent_writers and not caps.hard_kill and not caps.memory_limit
    mem = SqliteInProcess.memory().capabilities
    assert not mem.sleep_releases_everything and not mem.cheap_change_hint


async def test_last_pk_none_for_without_rowid_tables(file_db):
    await file_db.execute_write("create table r (id integer primary key, v)")
    await file_db.execute_write("insert into r (v) values ('x')")
    await file_db.execute_write("create table w (k text primary key, v) without rowid")
    r = await file_db.execute_write("insert into w values ('a', 1)")
    assert r.rowcount == 1 and r.last_pk is None
