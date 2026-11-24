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


# -- memory: limits and status --------------------------------------------------


@pytest.fixture
def heap_limit_reset():
    from dblayer.backends.sqlite_apsw import set_process_heap_limit

    previous = apsw.hard_heap_limit(-1)
    yield
    set_process_heap_limit(previous // (1024 * 1024))


async def test_memory_limit_stops_a_huge_value(tmp_path, heap_limit_reset):
    import sqlite3

    from dblayer import ResourceLimitExceeded

    backend = SqliteApsw(tmp_path / "m.db", memory_limit_mb=64)
    assert backend.capabilities.memory_limit
    assert not SqliteApsw(tmp_path / "n.db").capabilities.memory_limit
    db = Database(backend)
    with pytest.raises(ResourceLimitExceeded):
        await db.execute("select length(randomblob(200 * 1024 * 1024))")
    assert apsw.hard_heap_limit(-1) == 64 * 1024 * 1024
    # Process-wide for apsw's SQLite: another database is limited too ...
    other = Database(SqliteApsw(tmp_path / "o.db"))
    with pytest.raises(ResourceLimitExceeded):
        await other.execute("select length(randomblob(200 * 1024 * 1024))")
    # ... but not the standard library's SQLite, a separate copy.
    conn = sqlite3.connect(":memory:")
    assert conn.execute("select length(randomblob(100 * 1024 * 1024))").fetchone() == (
        100 * 1024 * 1024,
    )
    conn.close()
    assert (await db.execute("select 1")).rows == [(1,)]  # still usable
    await db.close()
    await other.close()


async def test_max_value_bytes_is_per_connection(tmp_path):
    from dblayer import ResourceLimitExceeded

    db = Database(SqliteApsw(tmp_path / "v.db", max_value_bytes=1000))
    assert (await db.execute("select length(randomblob(1000))")).rows == [(1000,)]
    with pytest.raises(ResourceLimitExceeded):
        await db.execute("select randomblob(1001)")
    other = Database(SqliteApsw(tmp_path / "w.db"))
    assert (await other.execute("select length(randomblob(5000))")).rows == [(5000,)]
    await db.close()
    await other.close()


async def test_memory_status_per_backend_and_process(tmp_path):
    from dblayer.backends.sqlite_apsw import process_memory_status

    backend = SqliteApsw(tmp_path / "s.db", cache_size_kb=512)
    assert backend.memory_status()["connections"] == 0
    db = Database(backend)
    await db.execute_write("create table t (id integer primary key, b blob)")
    await db.execute_write(
        "with recursive c(i) as (select 1 union all select i + 1 from c where i < 300) "
        "insert into t (b) select randomblob(1000) from c"
    )
    await db.execute("select count(*), sum(length(b)) from t")
    s = backend.memory_status()
    assert s["connections"] == 2  # one reader, one writer
    # Pages read into each connection's cache (overflow pages bypass it:
    # SQLITE_DIRECT_OVERFLOW_READ), parsed schema, prepared statements.
    assert s["cache_used"] > 8192 and s["schema_used"] > 0 and s["stmt_used"] > 0
    p = process_memory_status()
    assert p["memory_used"] >= s["cache_used"]
    assert p["memory_high_water"] >= p["memory_used"]
    await db.close()
    assert backend.memory_status()["connections"] == 0


# -- untrusted SQL --------------------------------------------------------------


@pytest.fixture
async def untrusted_db(tmp_path):
    db = Database(SqliteApsw(tmp_path / "u.db", untrusted=True))
    await db.execute_write("create table t (id integer primary key, v)")
    await db.execute_write("insert into t (v) values ('a')")
    yield db
    await db.close()


@pytest.mark.parametrize(
    "sql",
    [
        "attach database ':memory:' as other",
        "attach database 'file:/tmp/elsewhere.db' as other",
        "pragma cache_size = 100000",
        "pragma query_only = 0",
        "pragma writable_schema = 1",
        "pragma journal_mode = delete",
        "select load_extension('/tmp/evil')",
        "create temp table x (y)",  # writes to temp; not stmt_readonly
        "create temp view x as select 1",
    ],
)
async def test_untrusted_blocks(untrusted_db, sql):
    with pytest.raises(QueryError):
        await untrusted_db.execute(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "select * from t",
        "pragma table_info(t)",
        "select * from pragma_table_xinfo('t')",
        "pragma cache_size",
        "explain select * from t",
        "explain query plan select * from t",
        "with recursive c(x) as (select 1 union all select x + 1 from c where x < 5) select * from c",
    ],
)
async def test_untrusted_allows_reads(untrusted_db, sql):
    assert (await untrusted_db.execute(sql)).rows


async def test_untrusted_introspection_and_writes_still_work(untrusted_db):
    assert await untrusted_db.table_names() == ["t"]
    assert [c.name for c in await untrusted_db.table_columns("t")] == ["id", "v"]
    # The write connection is not locked down: dblayer's own operations
    # need pragmas there.
    await untrusted_db.execute_write("pragma user_version = 3")
    assert (await untrusted_db.execute("pragma user_version")).rows == [(3,)]


async def test_trusted_read_connections_allow_temp_writes(file_db):
    # As on SqliteInProcess: mode=ro does not cover the temp schema.
    await file_db.execute("create temp table x (y)")


# -- cancellation: no lost-interrupt window ---------------------------------------


UNBOUNDED = "with recursive c(x) as (select 1 union all select x + 1 from c) select count(*) from c"


async def test_cancel_at_random_points_always_returns(file_db):
    """A cancel can land before the statement starts, as it starts, or while
    it runs. Every one must stop the statement: the progress handler reads
    the cancel flag, so an interrupt that arrives early is not lost."""
    import random

    rng = random.Random(1)
    for _ in range(200):
        task = asyncio.create_task(file_db.execute(UNBOUNDED))
        for _ in range(rng.randrange(4)):
            await asyncio.sleep(0)
        if rng.random() < 0.5:
            await asyncio.sleep(rng.random() / 1000)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 10)
    assert (await file_db.execute("select 1")).rows == [(1,)]


async def test_cancel_flag_does_not_leak_into_the_next_call(tmp_path):
    backend = SqliteApsw(tmp_path / "c.db")
    conn = await backend.open(write=False)
    conn._on_cancel()  # as if a previous call had been cancelled
    r = await conn.execute("select 1", {}, time_limit_ms=None, max_rows=None)
    assert r.rows == [(1,)]
    await conn.close()


# -- inline reads (no executor hop for cheap reads) -----------------------------


def _tid_init(conn: apsw.Connection) -> None:
    import threading

    conn.create_scalar_function("tid", threading.get_ident)


@pytest.fixture
async def inline(tmp_path):
    backend = SqliteApsw(tmp_path / "i.db", inline_reads=True, init=_tid_init)
    db = Database(backend, max_read_connections=1)
    await db.execute_write("create table t (id integer primary key, v)")
    await db.execute_write(
        "with recursive c(i) as (select 1 union all select i + 1 from c where i < 20000) "
        "insert into t (v) select 'value ' || i from c"
    )
    yield db
    await db.close()


def _reader(db: Database):
    return db._pool._idle_read[0].conn  # type: ignore[union-attr]


class _Ticker:
    """Counts event-loop iterations and the longest gap between them while
    something else runs."""

    def __init__(self) -> None:
        self.ticks = 0
        self.max_gap = 0.0
        self._stop = False

    async def run(self) -> None:
        import time

        last = time.perf_counter()
        while not self._stop:
            await asyncio.sleep(0)
            now = time.perf_counter()
            self.max_gap = max(self.max_gap, now - last)
            last = now
            self.ticks += 1

    async def during(self, coro):
        task = asyncio.create_task(self.run())
        await asyncio.sleep(0)
        try:
            return await coro
        finally:
            self._stop = True
            await task


async def test_cheap_read_runs_on_the_loop_thread(inline):
    import threading

    r = await inline.execute("select tid(), v from t where id = 7")
    assert r.rows == [(threading.get_ident(), "value 7")]
    conn = _reader(inline)
    assert (conn.inline_runs, conn.inline_continued, conn.inline_reruns) == (1, 0, 0)


async def test_default_is_the_executor(tmp_path):
    import threading

    db = Database(SqliteApsw(tmp_path / "d.db", init=_tid_init))
    assert (await db.execute("select tid()")).rows[0][0] != threading.get_ident()
    await db.close()


async def test_slow_query_goes_to_the_executor_and_the_loop_keeps_running(inline):
    """A query that does not finish within the budget is stopped on the loop
    thread and re-run on the executor; the loop keeps turning meanwhile."""
    import threading

    ticker = _Ticker()
    sql = (
        "select tid(), count(*) from t a join t b on a.id % 1000 = b.id % 1000 "
        "where a.id < 3000"
    )
    r = await ticker.during(inline.execute(sql))
    assert r.rows[0][0] != threading.get_ident() and r.rows[0][1] == 2999 * 20
    conn = _reader(inline)
    assert conn.inline_reruns == 1
    assert ticker.ticks > 10  # the loop ran while the executor worked
    # The budget is 10,000 opcodes or 2 ms; allow a lot for a loaded machine.
    assert ticker.max_gap < 0.5, ticker.max_gap
    print(f"\n[inline] longest loop gap during a slow query: {ticker.max_gap * 1000:.2f} ms")


async def test_long_result_is_continued_on_the_executor(inline):
    """Rows come out inline until the budget is half used, then the same
    statement is stepped on to the end on an executor thread."""
    import threading

    r = await inline.execute("select tid(), id from t order by id")
    assert len(r.rows) == 20000 and [row[1] for row in r.rows[:3]] == [1, 2, 3]
    tids = {row[0] for row in r.rows}
    assert threading.get_ident() in tids and len(tids) == 2  # both threads stepped it
    conn = _reader(inline)
    assert conn.inline_continued == 1 and conn.inline_reruns == 0


async def test_non_read_only_statement_is_not_run_inline(inline):
    await inline.execute("create temp table x (y)")
    conn = _reader(inline)
    assert conn.inline_reruns == 1 and conn.inline_runs == 0


async def test_inline_time_limit_still_applies(inline):
    from dblayer import QueryInterrupted

    with pytest.raises(QueryInterrupted):
        await inline.execute(UNBOUNDED, time_limit_ms=50)
    assert (await inline.execute("select count(*) from t")).rows == [(20000,)]


async def test_inline_read_never_waits_for_a_lock_on_the_loop(tmp_path):
    """Busy timeout is 0 inline: a locked database sends the read to the
    executor, where it waits (here for 0.3 s) without blocking the loop."""
    path = tmp_path / "locked.db"
    db = Database(SqliteApsw(path, inline_reads=True, busy_timeout=0.3))
    await db.execute_write("create table t (x)")
    other = apsw.Connection(str(path))
    other.execute("begin exclusive")
    ticker = _Ticker()
    with pytest.raises(QueryError) as e:
        await ticker.during(db.execute("select count(*) from t"))
    assert e.value.code == "SQLITE_BUSY"
    assert ticker.ticks > 10 and ticker.max_gap < 0.25
    other.execute("rollback")
    other.close()
    assert (await db.execute("select count(*) from t")).rows == [(0,)]
    await db.close()
