"""The apsw in-process SQLite backend (``SqliteApsw``): behaviour specific
to it. The conformance suite covers the rest (backend id ``sqlite-apsw``).
Skipped when apsw is not installed."""

from __future__ import annotations

import asyncio

import pytest

from dblayer import Database, NotSupported, QueryError
from dblayer.backends.sqlite_apsw import apsw_unavailable

if apsw_unavailable():
    pytest.skip(apsw_unavailable() or "", allow_module_level=True)

import apsw  # noqa: E402

from dblayer.backends.sqlite_apsw import SqliteApsw  # noqa: E402


@pytest.fixture
async def file_db(tmp_path):
    db = Database(SqliteApsw(tmp_path / "data.db"), name="data")
    yield db
    await db.close()


# -- open / execute / params -------------------------------------------------


async def test_execute_reads_and_writes(file_db):
    await file_db.execute_write("create table t (id integer primary key, name text)")
    r = await file_db.execute_write("insert into t (name) values (:n)", {"n": "a"})
    assert (r.rowcount, r.last_pk) == (1, 1)
    r = await file_db.execute("select id, name from t where id = ?", [1])
    assert r.rows == [(1, "a")]


async def test_read_connections_are_read_only(file_db):
    await file_db.execute_write("create table t (x)")
    with pytest.raises(QueryError) as e:
        await file_db.execute("insert into t values (1)")
    assert e.value.code == "SQLITE_READONLY"


async def test_one_statement_per_call(file_db):
    await file_db.execute_write("create table t (x)")
    with pytest.raises(QueryError, match="one statement"):
        await file_db.execute_write("insert into t values (1); insert into t values (2)")
    # Nothing ran: the check happens before the first statement.
    assert (await file_db.execute("select count(*) from t")).rows == [(0,)]
    # A trailing semicolon or comment is still one statement.
    await file_db.execute_write("insert into t values (1); -- done")
    assert (await file_db.execute("select count(*) from t")).rows == [(1,)]


async def test_immutable(tmp_path):
    path = tmp_path / "imm.db"
    conn = apsw.Connection(str(path))
    conn.execute("create table t (x); insert into t values (42)")
    conn.close()
    db = Database(SqliteApsw(path, immutable=True))
    assert (await db.execute("select x from t")).rows == [(42,)]
    with pytest.raises(NotSupported):
        await db.execute_write("insert into t values (1)")
    await db.close()


async def test_init_hook_receives_an_apsw_connection(tmp_path):
    seen = []

    def init(conn: apsw.Connection) -> None:
        seen.append(type(conn))
        conn.create_scalar_function("double", lambda v: v * 2)

    db = Database(SqliteApsw(tmp_path / "f.db", init=init))
    assert (await db.execute("select double(21)")).rows == [(42,)]
    assert (await db.execute_write("select double(2)")).rows == [(4,)]
    assert seen == [apsw.Connection, apsw.Connection]
    await db.close()


async def test_memory_databases_use_memdb_and_are_shared():
    a = Database(SqliteApsw.memory("apsw-shared"))
    b = Database(SqliteApsw.memory("apsw-shared"))
    await a.execute_write("create table t (x)")
    await a.execute_write("insert into t values (7)")
    assert (await b.execute("select x from t")).rows == [(7,)]
    with pytest.raises(QueryError):
        await b.execute("insert into t values (8)")  # read-only reader
    await a.close()
    await b.close()


async def test_unnamed_memory_databases_are_private():
    a = Database(SqliteApsw.memory())
    b = Database(SqliteApsw.memory())
    await a.execute_write("create table t (x)")
    assert await b.table_names() == []
    await a.close()
    await b.close()


async def test_memory_database_survives_sleep():
    db = Database(SqliteApsw.memory("apsw-survivor"))
    await db.execute_write("create table t (x)")
    await db.execute_write("insert into t values (1)")
    await db.sleep()
    assert (await db.execute("select x from t")).rows == [(1,)]
    await db.close()
    db2 = Database(SqliteApsw.memory("apsw-survivor"))
    assert await db2.table_names() == []
    await db2.close()


async def test_bundled_sqlite_version():
    assert SqliteApsw.sqlite_version() == apsw.sqlite_lib_version()
