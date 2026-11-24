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


# -- alter_table and staged import -------------------------------------------------


async def test_alter_table_is_native(db):
    # Every change is a native ALTER: the table keeps its identity, so
    # views and other tables' foreign keys stay attached.
    await db.create_table("t", {"id": "integer", "a": "text", "b": "text"}, pk="id")
    await db.insert_rows("t", [{"id": 1, "a": "10", "b": "x"}, {"id": 2, "a": "20", "b": "y"}])
    oid = (await db.execute("select 't'::regclass::oid")).rows[0][0]
    await db.alter_table("t", rename={"a": "n"}, types={"a": "integer"})  # text -> BIGINT
    # (PostgreSQL refuses a type change on a column a view uses.)
    await db.execute_write("create view v as select id, n from t")
    await db.alter_table("t", pk=["id", "n"])
    await db.alter_table("t", drop=["b"])
    assert (await db.execute("select 't'::regclass::oid")).rows[0][0] == oid
    assert await db.primary_keys("t") == ["id", "n"]
    cols = {c.name: c.type for c in await db.table_columns("t")}
    assert cols == {"id": "BIGINT", "n": "BIGINT"}
    assert (await db.execute("select * from v order by id")).rows == [(1, 10), (2, 20)]
    assert [i.name for i in await db.indexes("t") if i.origin == "pk"] == ["t_pkey"]
    with pytest.raises(ValueError):
        await db.alter_table("t", drop=["id"])  # a pk column, with no new pk
    await db.alter_table("t", pk=None)
    assert await db.primary_keys("t") == []


async def test_staged_replace_renames_constraints(db):
    await db.create_table("posts", {"id": "integer", "slug": "text"}, pk="id")
    await db.execute_write("alter table posts add constraint posts_slug_key unique (slug)")
    await db.create_index("posts", ["slug"], name="posts_slug_idx")
    for n in range(2):  # twice: names from the first swap must not collide
        async with db.staged_import("posts", mode="replace") as imp:
            await imp.write([{"id": i, "slug": f"s{n}-{i}"} for i in range(5)])
    names = sorted(i.name for i in await db.indexes("posts"))
    assert names == ["posts_pkey", "posts_slug_idx", "posts_slug_key"]
    assert (await db.execute("select count(*) from posts")).rows == [(5,)]


async def test_staged_replace_refuses_check_constraints(db):
    from dblayer import NotSupported

    await db.execute_write("create table t (id integer primary key, n integer check (n > 0))")
    with pytest.raises(NotSupported):
        async with db.staged_import("t", mode="replace") as imp:
            await imp.write([{"id": 1, "n": 1}])


async def test_staged_replace_of_a_referenced_table_fails_cleanly(db):
    await db.create_table("p", {"id": "integer"}, pk="id")
    await db.insert_rows("p", [{"id": 1}])
    await db.execute_write("create table c (pid integer references p (id))")
    with pytest.raises(QueryError) as e:
        async with db.staged_import("p", mode="replace") as imp:
            await imp.write([{"id": 2}])
    assert e.value.code == "2BP01"  # dependent_objects_still_exist
    assert (await db.execute("select id from p")).rows == [(1,)]
    assert not [t for t in (await db.schema()).tables if t.startswith("_dblayer_")]


# -- the spec's "what this backend must demonstrate" ---------------------------
# 1. The conformance suite passes: the "postgres" entry in conftest.BACKENDS.


async def test_two_write_transactions_progress_at_the_same_time(db):
    # Each transaction waits inside its block until the other has written.
    # With a single write slot the second could not even begin: deadlock,
    # caught by the timeout.
    await db.execute_write("create table t (id integer primary key, who text)")
    barrier = asyncio.Barrier(2)
    pids = []

    async def writer(who: str, id: int) -> None:
        async with db.transaction() as tx:
            await tx.execute("insert into t values (:id, :who)", {"id": id, "who": who})
            pids.append((await tx.execute("select pg_backend_pid()")).rows[0][0])
            await asyncio.wait_for(barrier.wait(), 5)
            # Both are open and have written; neither sees the other's row.
            assert (await tx.execute("select who from t")).rows == [(who,)]

    await asyncio.wait_for(asyncio.gather(writer("a", 1), writer("b", 2)), 10)
    assert len(set(pids)) == 2
    assert (await db.execute("select who from t order by id")).rows == [("a",), ("b",)]
    assert db.stats().open_write == 2


async def test_concurrent_conflicting_writers_raise_write_conflict(db):
    # A deadlock between two write transactions surfaces as WriteConflict.
    from dblayer import WriteConflict

    await db.execute_write("create table t (id integer primary key, n integer)")
    await db.execute_write("insert into t values (1, 0), (2, 0)")
    barrier = asyncio.Barrier(2)

    async def writer(first: int, second: int) -> str:
        try:
            async with db.transaction() as tx:
                await tx.execute("update t set n = n + 1 where id = :id", {"id": first})
                await barrier.wait()
                await tx.execute("update t set n = n + 1 where id = :id", {"id": second})
        except WriteConflict:
            return "conflict"
        return "ok"

    results = await asyncio.wait_for(asyncio.gather(writer(1, 2), writer(2, 1)), 10)
    assert sorted(results) == ["conflict", "ok"]


async def test_server_rolls_back_a_transaction_whose_client_is_suspended(make_pg):
    # The event loop is blocked (time.sleep), so the client's own deadline
    # timer cannot run. The server's idle_in_transaction_session_timeout
    # (deadline + grace) ends the session, which releases the row lock; a
    # second client, on another thread, sees that while this one is still
    # frozen. When the loop resumes, the transaction is expired.
    import threading
    import time

    from dblayer import TransactionExpired

    db = make_pg(deadline_ms=300, backend={"server_deadline_grace_ms": 200})
    await db.execute_write("create table t (id integer primary key, n integer)")
    await db.execute_write("insert into t values (1, 0)")
    released_after: list[float] = []

    async def other_client(start: float) -> None:
        conn = await pg.connect()
        try:
            await conn.execute(f'set search_path = "{db._backend.schema}"')
            await conn.execute("set lock_timeout = '3s'")
            await conn.execute("update t set n = 100 where id = 1")
            released_after.append(time.monotonic() - start)
        finally:
            await conn.close()

    with pytest.raises(TransactionExpired):
        async with db.transaction() as tx:
            await tx.execute("update t set n = n + 1 where id = 1")  # row lock
            start = time.monotonic()
            thread = threading.Thread(target=lambda: pg.run_sync(other_client(start)))
            thread.start()
            time.sleep(1.5)  # the whole client is suspended
            frozen_for = time.monotonic() - start
            thread.join(5)
            await tx.execute("select 1")
    assert released_after, "the other client never got the row lock"
    # Released by the server around 500 ms (300 + 200), while frozen.
    assert 0.3 < released_after[0] < frozen_for
    assert (await db.execute("select n from t")).rows == [(100,)]
    # The dead session was not put back in the pool.
    async with db.transaction() as tx:
        await tx.execute("update t set n = 0 where id = 1")


async def test_sleep_closes_every_server_connection(make_pg):
    db = make_pg(max_read_connections=3)
    schema = db._backend.schema
    await db.execute_write("create table t (x integer)")
    await asyncio.gather(*(db.execute("select pg_sleep(0.05)") for _ in range(3)))

    async def write() -> None:
        async with db.transaction() as tx:
            await tx.execute("select pg_sleep(0.05)")

    await asyncio.gather(write(), write())  # two writers at once
    assert db.stats().open_total == 5
    assert await pg.sessions(schema) == 5
    await db.sleep()
    assert db.stats().open_total == 0
    assert await pg.sessions(schema) == 0
    # A connection checked out during sleep is closed when it comes back.
    async with db.transaction() as tx:
        await tx.execute("select 1")
        await db.sleep()
        assert await pg.sessions(schema) == 1
    assert await pg.sessions(schema) == 0
    # And the database wakes as usual.
    assert (await db.execute("select count(*) from t")).rows == [(0,)]


async def test_schema_change_by_another_client_seen_after_refresh_and_ttl(make_pg):
    db = make_pg(backend={"schema_ttl": 0.6})
    schema = db._backend.schema
    other = make_pg(schema=schema)  # another client: own backend, same schema
    await db.execute_write("create table a (x integer)")
    assert await db.table_names() == ["a"]
    hint = db.change_hint()

    await other.execute_write("create table b (x integer)")
    assert await db.table_names() == ["a"]  # cached: no cheap way to know
    assert db.change_hint() == hint
    await db.refresh_schema()
    assert await db.table_names() == ["a", "b"]
    assert db.change_hint() != hint

    await other.execute_write("create table c (x integer)")
    assert await db.table_names() == ["a", "b"]
    await asyncio.sleep(0.7)  # past the TTL: the hint has moved on
    assert await db.table_names() == ["a", "b", "c"]
    # Own writes are seen at once, as on every backend.
    await db.execute_write("create table d (x integer)")
    assert await db.table_names() == ["a", "b", "c", "d"]


async def test_schema_version_tracks_ddl_from_any_client(make_pg):
    db = make_pg()
    other = make_pg(schema=db._backend.schema)
    v0 = await db.schema_version()
    await other.execute_write("create table t (x integer)")
    v1 = await db.schema_version()
    await other.execute_write("insert into t values (1)")
    assert await db.schema_version() == v1  # data is not schema
    for ddl in (
        "alter table t add column y text",
        "alter table t alter column y set default 'z'",
        "create index t_x on t (x)",
        "create view v as select x from t",
        "create or replace view v as select x, y from t",
        "alter table t rename column y to z",
        "alter table t add constraint t_pos check (x > 0)",
    ):
        before = await db.schema_version()
        await other.execute_write(ddl)
        assert await db.schema_version() != before, ddl
    assert len({v0, v1}) == 2


# -- the parameter rewriter against the real server -----------------------------


@pytest.mark.parametrize(
    "sql, params, expected",
    [
        ("select :x::int + 1", {"x": 41}, (42,)),
        ("select cast(:x as text) || '::' || :y::text", {"x": "a", "y": 1}, ("a::1",)),
        ("select $$ :nope ? $$ as s, :x as x", {"x": 1}, (" :nope ? ", 1)),
        ("select $tag$ it's :nope $tag$, :x", {"x": "y"}, (" it's :nope ", "y")),
        ("select E'it\\'s :nope', :x", {"x": 2}, ("it's :nope", 2)),
        ("select /* /* :nope */ ? */ :x", {"x": 3}, (3,)),
        ("select '{\"a\": 1}'::jsonb ? 'a', :k", {"k": "v"}, (True, "v")),
        ("select :doc::jsonb ? :key", {"doc": '{"a": 1}', "key": "a"}, (True,)),
        ("select :doc::jsonb ?| array['b', :key]", {"doc": '{"a": 1}', "key": "a"}, (True,)),
        # [2:3] is a slice; [:n] is an index by parameter (the rewriter's
        # documented rule); a slice to a parameter needs a space: [1: :n].
        ("select (array[10, 20, 30])[2:3], (array[10, 20, 30])[:n]", {"n": 1}, ([20, 30], 10)),
        ("select (array[10, 20, 30])[1: :n]", {"n": 2}, ([10, 20],)),
        ("select ? || ?, ?::int * 2", ["a", "b", "21"], ("ab", 42)),
        ('select 1 as ":x"', None, (1,)),
    ],
)
async def test_rewriter_against_server(db, sql, params, expected):
    r = await db.execute(sql, params)
    assert tuple(r.rows[0]) == expected
