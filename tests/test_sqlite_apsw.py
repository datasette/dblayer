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


async def test_memdb_readers_wait_for_an_open_write_transaction():
    """The known limitation of memory databases here (memdb's RESERVED lock
    shuts readers out), pinned so a change in SQLite shows up."""
    db = Database(SqliteApsw.memory(busy_timeout=0.05))
    await db.execute_write("create table t (x)")
    async with db.transaction() as tx:
        await tx.execute("insert into t values (1)")
        with pytest.raises(QueryError) as e:
            await db.execute("select count(*) from t")
        assert e.value.code == "SQLITE_BUSY"
    assert (await db.execute("select count(*) from t")).rows == [(1,)]
    await db.close()


# -- exact counts and rowids without extra statements --------------------------


async def test_cte_write_rowcount_and_last_pk(file_db):
    await file_db.execute_write("create table t (id integer primary key, v)")
    r = await file_db.execute_write(
        "with x(v) as (values (1), (2), (3)) insert into t (v) select v from x"
    )
    assert (r.rowcount, r.last_pk) == (3, 3)
    r = await file_db.execute_write(
        "with x(n) as (select 2) delete from t where id >= (select n from x)"
    )
    assert r.rowcount == 2 and r.last_pk is None


async def test_last_pk_when_an_insert_reuses_the_previous_rowid(file_db):
    """SqliteInProcess compares last_insert_rowid() before and after, so it
    reports None here; apsw sets a sentinel first and gets it right."""
    await file_db.execute_write("create table t (id integer primary key)")
    await file_db.execute_write("insert into t (id) values (5)")
    await file_db.execute_write("delete from t")
    r = await file_db.execute_write("insert into t (id) values (5)")
    assert (r.rowcount, r.last_pk) == (1, 5)


async def test_skipped_insert_leaves_last_insert_rowid_alone(file_db):
    await file_db.execute_write("create table t (id integer primary key)")
    await file_db.execute_write("insert into t (id) values (7)")
    r = await file_db.execute_write("insert into t (id) values (7) on conflict do nothing")
    assert (r.rowcount, r.last_pk) == (0, None)
    # The sentinel was restored: SQL still sees the last real insert.
    assert (await file_db.execute_write("select last_insert_rowid()")).rows == [(7,)]


async def test_upsert_update_path_has_no_last_pk(file_db):
    await file_db.execute_write("create table t (id integer primary key, v)")
    await file_db.execute_write("insert into t values (1, 'a')")
    r = await file_db.execute_write(
        "insert into t values (1, 'b') on conflict (id) do update set v = excluded.v"
    )
    assert (r.rowcount, r.last_pk) == (1, None)


async def test_execute_many_rowcount(file_db):
    await file_db.execute_write("create table t (id integer primary key, v)")
    async with file_db.transaction() as tx:
        r = await tx.execute_many("insert into t (v) values (:v)", [{"v": i} for i in range(5)])
        assert r.rowcount == 5
        r = await tx.execute_many("update t set v = 0 where id <= :n", [{"n": 2}, {"n": 3}])
        assert r.rowcount == 5  # 2 + 3, per statement, as sqlite3 counts


# -- declared column types ----------------------------------------------------


async def test_result_columns_carry_declared_types(tmp_path):
    backend = SqliteApsw(tmp_path / "types.db")
    file_db = Database(backend)
    await file_db.execute_write(
        "create table t (id integer primary key, name text, score real, b blob, n)"
    )
    await file_db.execute_write("create view v as select id as ident, name from t")
    r = await file_db.execute("select id, name, score, b, n, id + 1 as expr from t")
    assert [(c.name, c.type) for c in r.columns] == [
        ("id", "INTEGER"), ("name", "TEXT"), ("score", "REAL"), ("b", "BLOB"),
        ("n", None), ("expr", None),
    ]
    r = await file_db.execute("select * from v")
    assert [(c.name, c.type) for c in r.columns] == [("ident", "INTEGER"), ("name", "TEXT")]
    await file_db.close()
    conn = await backend.open(write=False)
    stream = conn.stream("select name from t", params={}, chunk_rows=10)
    async for _ in stream:
        pass
    assert [c.type for c in stream.columns] == ["TEXT"]
    await conn.close()
    await backend.close()


# -- statement-level control --------------------------------------------------


async def test_expanded_sql_is_kept_when_tracing(tmp_path):
    db = Database(SqliteApsw(tmp_path / "x.db", trace_sql=True))
    await db.execute("select :a, :b", {"a": 1, "b": "it's"})
    conn = db._pool._idle_read[0].conn  # type: ignore[union-attr]
    assert conn.last_expanded_sql == "select 1, 'it''s'"
    await db.close()
