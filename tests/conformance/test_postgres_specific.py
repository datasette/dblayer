"""Behaviour specific to the PostgreSQL backend, against the real server.

Not parametrised by backend: these construct ``Postgres`` directly, each on
its own schema in the ``dblayer_test`` database (see ``tests/pg.py``).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from typing import Any

import pytest

from dblayer import Database, IntegrityError, QueryError

from .. import pg

pytestmark = pytest.mark.skipif(pg.unavailable() is not None, reason=pg.unavailable() or "")


@pytest.fixture
async def make_pg() -> AsyncIterator[Callable[..., Database]]:
    """``make_pg(backend_kwargs=None, **database_kwargs)``: a Database on a
    fresh schema. Several calls with ``schema=`` share one schema."""
    made: list[Database] = []
    schemas: set[str] = set()

    def make(*, schema: str | None = None, backend: dict[str, Any] | None = None, **kw: Any) -> Database:
        b = pg.make(schema=schema, **(backend or {}))
        schemas.add(b.schema)
        db = Database(b, **kw)
        made.append(db)
        return db

    yield make
    for db in made:
        await db.close()
    for schema in schemas:
        await pg.drop_schema(schema)


@pytest.fixture
async def db(make_pg: Callable[..., Database]) -> Database:
    return make_pg()


async def test_one_database_is_one_schema(make_pg):
    a = make_pg()
    b = make_pg()
    await a.execute_write("create table t (x integer)")
    await a.execute_write("insert into t values (1)")
    await b.execute_write("create table t (x integer)")
    assert (await b.execute("select count(*) from t")).rows == [(0,)]
    assert await a.table_names() == ["t"] == await b.table_names()
    schema = a._backend.schema
    r = await a.execute("select current_schema(), current_setting('search_path')")
    assert r.rows[0][0] == schema


async def test_read_connections_are_read_only(db):
    await db.execute_write("create table t (x integer)")
    with pytest.raises(QueryError) as e:
        await db.execute("insert into t values (1)")
    assert e.value.code == "25006"  # read_only_sql_transaction


async def test_cached_statements_survive_ddl(make_pg):
    # asyncpg caches prepared statements per connection; DDL makes them
    # stale ("cached plan must not change result type"). Own DDL drops the
    # cache; other connections' DDL is retried, inside a transaction too.
    db = make_pg(max_read_connections=1)
    await db.execute_write("create table t (id bigint primary key, name text)")
    await db.execute_write("insert into t values (1, 'a')")
    assert (await db.execute("select * from t")).columns == ("id", "name")
    async with db.transaction() as tx:
        await tx.execute("select * from t")
        await tx.execute("alter table t add column c text")
        assert (await tx.execute("select * from t")).columns == ("id", "name", "c")
    await db.execute_write("alter table t add column d text")
    assert (await db.execute("select * from t")).columns == ("id", "name", "c", "d")
    # One read connection: its cached "select * from t" is now stale.
    async with db.transaction("read") as tx:
        # (Not "select * from t" here: its lock would block the ALTER.)
        await tx.execute("select 1")
        await db.execute_write("alter table t add column e text")
        assert len((await tx.execute("select * from t")).columns) == 5


async def test_untyped_parameters(db):
    # PostgreSQL types an uninferable parameter as text; the backend casts
    # it from the Python value instead, as SQLite would.
    r = await db.execute("select :i as i, :f as f, :b as b, :s as s", {"i": 2**40, "f": 1.5, "b": b"\x00", "s": "x"})
    assert r.rows[0] == (2**40, 1.5, b"\x00", "x")
    assert r.columns[0].type == "int8"
    # A string for a typed parameter is converted by the server.
    assert (await db.execute("select cast(:x as integer) + 1", {"x": "41"})).rows == [(42,)]
    await db.execute_write("create table t (id integer primary key, name text)")
    await db.execute_write("insert into t values (:id, :name)", {"id": "7", "name": 7})
    assert (await db.execute("select id, name from t")).rows == [(7, "7")]


async def test_execute_many_counts_rows(db):
    await db.execute_write("create table t (id integer primary key, v text)")
    async with db.transaction() as tx:
        r = await tx.execute_many("insert into t values (?, ?)", [(i, str(i)) for i in range(5)])
        assert r.rowcount == 5
        r = await tx.execute_many(
            "insert into t values (?, ?) on conflict do nothing", [(1, "x"), (9, "y")]
        )
        assert r.rowcount == 1
        r = await tx.execute_many("update t set v = 'u' where id = ? returning id", [(1,), (2,), (99,)])
        assert r.rowcount == 2
        r = await tx.execute_many("delete from t where id = ?;", [(3,), (4,)])
        assert r.rowcount == 2


async def test_commit_of_a_failed_transaction_raises(make_pg):
    db = make_pg(backend={"statement_savepoints": False})
    await db.execute_write("create table t (id integer primary key)")
    assert db.capabilities.statement_errors_abort_transaction
    with pytest.raises(QueryError) as e:
        async with db.transaction() as tx:
            await tx.execute("insert into t values (1)")
            with pytest.raises(IntegrityError):
                await tx.execute("insert into t values (1)")
    assert e.value.code == "25P02"
    assert (await db.execute("select count(*) from t")).rows == [(0,)]


async def test_definition_not_supported(db):
    from dblayer import NotSupported

    await db.execute_write("create table t (x integer)")
    with pytest.raises(NotSupported):
        await db.definition("t")


async def test_introspection_details(db):
    await db.execute_write("create table p (id integer primary key)")
    await db.execute_write(
        "create table c (id bigint, k text, pid integer references p (id) on delete cascade, "
        "n numeric(10, 2) not null default 0, ts timestamptz, g integer generated always as (id * 2) stored, "
        "v varchar(20) check (length(v) > 1), primary key (k, id), unique (pid, v))"
    )
    await db.execute_write("create index c_lower on c (lower(k))")
    await db.execute_write("create view cv as select id, k from c")
    info = (await db.schema()).tables["c"]
    cols = {c.name: c for c in info.columns}
    assert [c.name for c in info.columns] == ["id", "k", "pid", "n", "ts", "g", "v"]
    assert cols["id"].type == "BIGINT" and cols["ts"].type == "TIMESTAMPTZ"
    assert cols["n"].type == "NUMERIC(10,2)" and cols["n"].not_null and cols["n"].default == "0"
    assert cols["v"].type == "VARCHAR(20)"
    assert cols["g"].generated and cols["g"].default is None
    assert info.primary_keys == ("k", "id")
    assert info.foreign_keys[0].other_table == "p"
    assert info.foreign_keys[0].on_delete == "CASCADE"
    assert info.checks and "length" in info.checks[0]
    by_name = {i.name: i for i in info.indexes}
    assert by_name["c_pkey"].origin == "pk"
    assert by_name["c_pid_v_key"].origin == "u" and by_name["c_pid_v_key"].columns == ("pid", "v")
    assert by_name["c_lower"].origin == "c" and by_name["c_lower"].columns == (None,)
    assert await db.view_names() == ["cv"]
    assert [c.name for c in await db.table_columns("cv")] == ["id", "k"]


@pytest.fixture
def spy(monkeypatch):
    """Counts COPYs, and native operations that ran (did not decline)."""
    import asyncpg

    from dblayer.backends.postgres import PostgresConnection

    counts = {"copy": 0, "insert_rows": 0, "upsert_rows": 0}
    copy = asyncpg.Connection.copy_records_to_table

    async def counting_copy(self, *a, **kw):
        counts["copy"] += 1
        return await copy(self, *a, **kw)

    monkeypatch.setattr(asyncpg.Connection, "copy_records_to_table", counting_copy)
    for name in ("insert_rows", "upsert_rows"):
        original = getattr(PostgresConnection, f"_op_{name}")

        def wrap(original=original, name=name):
            async def op(self, **kw):
                r = await original(self, **kw)
                if r is not NotImplemented:
                    counts[name] += 1
                return r

            return op

        monkeypatch.setattr(PostgresConnection, f"_op_{name}", wrap())
    return counts


async def test_insert_rows_uses_copy(db, spy):
    r = await db.insert_rows("t", [{"id": i, "v": f"v{i}"} for i in range(250)], pk="id")
    assert (r.rowcount, r.table_created) == (250, True)
    assert spy["copy"] == 3 and spy["insert_rows"] == 3  # batch_size=100
    # Conflict handling is not COPY's: the default path takes those.
    r = await db.insert_rows("t", [{"id": 1, "v": "x"}, {"id": 999, "v": "y"}], ignore=True)
    assert r.rowcount == 1 and spy["copy"] == 3
    # Values COPY's binary codecs refuse go through INSERT, as by default.
    r = await db.insert_rows("t", [{"id": 1000, "v": 5}])
    assert r.rowcount == 1 and spy["insert_rows"] == 4 and spy["copy"] == 3
    assert (await db.execute("select v from t where id = 1000")).rows == [("5",)]


async def test_upsert_rows_is_one_statement_per_group(db, spy):
    await db.create_table("t", {"id": "integer", "a": "text", "b": "text"}, pk="id")
    await db.insert_rows("t", [{"id": 1, "a": "a1", "b": "b1"}])
    r = await db.upsert_rows("t", [{"id": 1, "a": "A"}, {"id": 2, "a": "B", "b": "x"}])
    assert r.rowcount == 2 and spy["upsert_rows"] == 1
    assert (await db.execute("select * from t order by id")).rows == [(1, "A", "b1"), (2, "B", "x")]
    # The same key twice cannot be one INSERT ... ON CONFLICT: declined.
    r = await db.upsert_rows("t", [{"id": 3, "a": "x"}, {"id": 3, "a": "y"}])
    assert r.rowcount == 2 and spy["upsert_rows"] == 1
    assert (await db.execute("select a from t where id = 3")).rows == [("y",)]
