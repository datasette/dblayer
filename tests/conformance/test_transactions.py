"""Interactive transactions: commit, rollback, cancellation, use after exit,
nesting, read mode, write ordering."""

import asyncio
import time

import pytest

from dblayer import (
    IntegrityError,
    NestedWriteError,
    QueryError,
    Results,
    Transaction,
    TransactionClosed,
    WriteResult,
)


@pytest.fixture
async def t(db):
    await db.execute_write("create table t (id integer primary key, v text)")
    return db


async def count(db, table="t"):
    return (await db.execute(f"select count(*) from {table}")).rows[0][0]


async def test_commit_on_clean_exit(t):
    async with t.transaction() as tx:
        assert isinstance(tx, Transaction) and tx.active and tx.mode == "write"
        r = await tx.execute("insert into t (id, v) values (:id, :v)", {"id": 1, "v": "a"})
        assert isinstance(r, Results) and r.rowcount == 1
        # Own writes are visible inside the transaction...
        assert (await tx.execute("select v from t")).rows == [("a",)]
    assert tx.state == "committed" and not tx.active
    assert (await t.execute("select v from t")).rows == [("a",)]


async def test_uncommitted_writes_invisible_to_other_connections(t):
    async with t.transaction() as tx:
        await tx.execute("insert into t (id, v) values (1, 'a')")
        if t.capabilities.snapshot_reads:
            assert await count(t) == 0
    assert await count(t) == 1


async def test_rollback_on_exception(t):
    with pytest.raises(ZeroDivisionError):
        async with t.transaction() as tx:
            await tx.execute("insert into t (id, v) values (1, 'a')")
            1 / 0
    assert tx.state == "rolled back"
    assert await count(t) == 0


async def test_rollback_on_query_error_propagates(t):
    with pytest.raises(IntegrityError):
        async with t.transaction() as tx:
            await tx.execute("insert into t (id, v) values (1, 'a')")
            await tx.execute("insert into t (id, v) values (1, 'b')")
    assert await count(t) == 0


async def test_failed_statement_does_not_end_transaction(t):
    async with t.transaction() as tx:
        await tx.execute("insert into t (id, v) values (1, 'a')")
        with pytest.raises(QueryError):
            await tx.execute("insert into nope values (1)")
        if t._backend.dialect.name == "sqlite":
            # SQLite rolls back only the failed statement.
            await tx.execute("insert into t (id, v) values (2, 'b')")
    if t._backend.dialect.name == "sqlite":
        assert await count(t) == 2


async def test_execute_many(t):
    async with t.transaction() as tx:
        r = await tx.execute_many(
            "insert into t (id, v) values (:id, :v)", [{"id": i, "v": str(i)} for i in range(5)]
        )
        assert isinstance(r, WriteResult) and r.rowcount == 5
        r = await tx.execute_many("insert into t (id, v) values (?, ?)", [[10, "x"], [11, "y"]])
        assert r.rowcount == 2
        assert (await tx.execute_many("delete from t where id = ?", [])).rowcount == 0
    assert await count(t) == 7


async def test_use_after_exit_raises(t):
    async with t.transaction() as tx:
        pass
    with pytest.raises(TransactionClosed):
        await tx.execute("select 1")
    with pytest.raises(TransactionClosed):
        await tx.table_names()
    with pytest.raises(RuntimeError):
        async with tx:
            pass


async def test_use_without_entering_raises(t):
    tx = t.transaction()
    with pytest.raises(TransactionClosed):
        await tx.execute("select 1")


async def test_bad_mode(t):
    with pytest.raises(ValueError):
        t.transaction("sideways")


async def test_calls_on_one_tx_are_serialised(t):
    async with t.transaction() as tx:
        await asyncio.gather(
            *(tx.execute("insert into t (id, v) values (?, ?)", [i, str(i)]) for i in range(20))
        )
    assert await count(t) == 20


async def test_cancel_rolls_back_and_releases_write_slot(t):
    started = asyncio.Event()

    async def holder():
        async with t.transaction(deadline_ms=None) as tx:
            await tx.execute("insert into t (id, v) values (1, 'a')")
            started.set()
            await asyncio.sleep(10)

    task = asyncio.create_task(holder())
    await started.wait()
    t0 = time.perf_counter()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    # Write slot free: another writer proceeds at once.
    await asyncio.wait_for(t.execute_write("insert into t (id, v) values (2, 'b')"), 1)
    assert time.perf_counter() - t0 < 0.05
    assert (await t.execute("select id from t")).rows == [(2,)]
    assert t.stats().in_use_write == 0


async def test_cancel_during_running_statement(t):
    started = asyncio.Event()
    infinite = (
        "with recursive c(x) as (select 1 union all select x + 1 from c) "
        "select count(*) from c"
    )

    async def holder():
        async with t.transaction(deadline_ms=None) as tx:
            await tx.execute("insert into t (id, v) values (1, 'a')")
            started.set()
            await tx.execute(infinite, time_limit_ms=None)

    task = asyncio.create_task(holder())
    await started.wait()
    await asyncio.sleep(0.05)
    t0 = time.perf_counter()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.wait_for(t.execute_write("insert into t (id, v) values (2, 'b')"), 1)
    assert time.perf_counter() - t0 < 0.5
    assert (await t.execute("select id from t")).rows == [(2,)]


async def test_double_cancel_still_rolls_back(t):
    started = asyncio.Event()

    async def holder():
        async with t.transaction(deadline_ms=None) as tx:
            await tx.execute("insert into t (id, v) values (1, 'a')")
            started.set()
            await asyncio.sleep(10)

    task = asyncio.create_task(holder())
    await started.wait()
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.wait_for(t.execute_write("insert into t (id, v) values (2, 'b')"), 1)
    assert (await t.execute("select id from t")).rows == [(2,)]


# -- nesting -------------------------------------------------------------------


async def test_nested_transaction_raises(t):
    async with t.transaction() as tx:
        with pytest.raises(NestedWriteError):
            async with t.transaction():
                pass
        with pytest.raises(NestedWriteError):
            async with t.transaction("read"):
                pass
        await tx.execute("insert into t (id, v) values (1, 'a')")
    assert await count(t) == 1


async def test_execute_write_inside_write_transaction_raises(t):
    async with t.transaction():
        with pytest.raises(NestedWriteError):
            await asyncio.wait_for(t.execute_write("insert into t (id, v) values (1, 'a')"), 1)


async def test_named_operation_on_db_inside_transaction_raises(t):
    async with t.transaction():
        with pytest.raises(NestedWriteError):
            await asyncio.wait_for(t.insert_rows("t", [{"id": 1}]), 1)


async def test_child_task_inherits_nesting_check(t):
    async with t.transaction():
        task = asyncio.create_task(t.execute_write("insert into t (id, v) values (1, 'a')"))
        with pytest.raises(NestedWriteError):
            await asyncio.wait_for(task, 1)


async def test_reads_on_db_inside_write_transaction(t):
    await t.execute_write("insert into t (id, v) values (1, 'a')")
    async with t.transaction() as tx:
        await tx.execute("insert into t (id, v) values (2, 'b')")
        r = await asyncio.wait_for(t.execute("select count(*) from t"), 2)
        assert r.rows[0][0] in (1, 2)
        assert await asyncio.wait_for(t.table_names(), 2) == ["t"]


async def test_other_database_not_affected(make_db):
    a = make_db(name="a")
    b = make_db(name="b")
    await a.execute_write("create table t (x)")
    await b.execute_write("create table t (x)")
    async with a.transaction() as tx:
        await tx.execute("insert into t values (1)")
        await asyncio.wait_for(b.execute_write("insert into t values (2)"), 1)
        async with b.transaction() as tx2:
            await tx2.execute("insert into t values (3)")
    assert await count(a) == 1 and await count(b) == 2


async def test_sequential_transactions_after_one_ends(t):
    async with t.transaction() as tx:
        await tx.execute("insert into t (id, v) values (1, 'a')")
    # The context variable is reset: these are not "nested".
    await t.execute_write("insert into t (id, v) values (2, 'b')")
    async with t.transaction() as tx:
        await tx.execute("insert into t (id, v) values (3, 'c')")
    assert await count(t) == 3


async def test_write_inside_read_transaction_is_allowed(t):
    async with t.transaction("read") as tx:
        await tx.execute("select count(*) from t")
        # A read transaction holds no write slot, so this cannot deadlock.
        task = asyncio.create_task(t.execute_write("insert into t (id, v) values (1, 'a')"))
        await asyncio.sleep(0.05)
    await asyncio.wait_for(task, 2)
    assert await count(t) == 1


# -- read mode -------------------------------------------------------------------


async def test_read_transaction_rejects_writes(t):
    async with t.transaction("read") as tx:
        with pytest.raises(QueryError):
            await tx.execute("insert into t (id, v) values (1, 'a')")
    assert await count(t) == 0


async def test_read_transaction_never_takes_write_slot(t):
    go = asyncio.Event()
    inserted = asyncio.Event()

    async def writer():
        # Created outside the read block, so not "nested" in it.
        await go.wait()
        async with t.transaction() as w:
            await w.execute("insert into t (id, v) values (1, 'a')")
            inserted.set()

    task = asyncio.create_task(writer())
    async with t.transaction("read") as tx:
        await tx.execute("select count(*) from t")
        assert t.stats().in_use_write == 0
        go.set()
        # The writer gets the slot and writes while the read tx is open
        # (its commit may wait for the reader on rollback-journal SQLite).
        await asyncio.wait_for(inserted.wait(), 2)
    await asyncio.wait_for(task, 6)
    assert await count(t) == 1


@pytest.mark.requires("snapshot_reads")
async def test_read_transaction_snapshot(t):
    await t.execute_write("insert into t (id, v) values (1, 'a')")
    async with t.transaction("read") as tx:
        assert (await tx.execute("select count(*) from t")).rows[0][0] == 1
        task = asyncio.create_task(t.execute_write("insert into t (id, v) values (2, 'b')"))
        await asyncio.sleep(0.1)
        assert (await tx.execute("select count(*) from t")).rows[0][0] == 1
    await asyncio.wait_for(task, 6)
    assert await count(t) == 2


async def test_read_transaction_introspection(t):
    async with t.transaction("read") as tx:
        assert await tx.table_names() == ["t"]
        assert [c.name for c in await tx.table_columns("t")] == ["id", "v"]


# -- write ordering ------------------------------------------------------------------


@pytest.mark.requires_not("concurrent_writers")
async def test_two_writers_are_serialised(t):
    events = []

    async def writer(name):
        async with t.transaction() as tx:
            events.append(f"{name} begin")
            await tx.execute("insert into t (v) values (?)", [name])
            await asyncio.sleep(0.05)
            events.append(f"{name} end")

    await asyncio.gather(writer("a"), writer("b"))
    assert events in (
        ["a begin", "a end", "b begin", "b end"],
        ["b begin", "b end", "a begin", "a end"],
    )
    assert await count(t) == 2


@pytest.mark.requires_not("concurrent_writers")
async def test_writers_queue_fifo(t):
    order = []

    async def writer(i):
        async with t.transaction() as tx:
            order.append(i)
            await tx.execute("insert into t (v) values (?)", [str(i)])

    holder_ready = asyncio.Event()
    release = asyncio.Event()

    async def holder():
        async with t.transaction():
            holder_ready.set()
            await release.wait()

    h = asyncio.create_task(holder())
    await holder_ready.wait()
    tasks = []
    for i in range(5):
        tasks.append(asyncio.create_task(writer(i)))
        await asyncio.sleep(0.005)
    release.set()
    await asyncio.gather(h, *tasks)
    assert order == [0, 1, 2, 3, 4]


# -- introspection inside a transaction -------------------------------------------------


async def test_introspection_sees_own_ddl(t):
    async with t.transaction() as tx:
        await tx.execute("create table u (a integer, b text)")
        assert sorted(await tx.table_names()) == ["t", "u"]
        assert [c.name for c in await tx.table_columns("u")] == ["a", "b"]
        await tx.execute("alter table u add column c text")
        assert [c.name for c in await tx.table_columns("u")] == ["a", "b", "c"]
        v = await tx.schema_version()
        assert v == (await tx.schema()).version
    assert sorted(await t.table_names()) == ["t", "u"]


async def test_schema_cache_invalidated_by_transaction_ddl(t):
    assert await t.table_names() == ["t"]  # fill the cache
    async with t.transaction() as tx:
        await tx.execute("create table u (a)")
    # Invalidated before the caller hears back, even within one mtime tick.
    assert sorted(await t.table_names()) == ["t", "u"]


async def test_schema_cache_not_poisoned_by_rolled_back_ddl(t):
    assert await t.table_names() == ["t"]
    with pytest.raises(RuntimeError):
        async with t.transaction() as tx:
            await tx.execute("create table u (a)")
            raise RuntimeError
    assert await t.table_names() == ["t"]


async def test_transaction_on_closed_database(make_db):
    db = make_db()
    await db.close()
    from dblayer import DatabaseClosed

    with pytest.raises(DatabaseClosed):
        db.transaction()
