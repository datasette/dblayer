"""Transaction deadlines (spec fault table: caller sleeps past the deadline)."""

import asyncio
import time

import pytest

from dblayer import TransactionExpired

from ..helpers import EVENTUALLY_S, check_latency, wait_until

INFINITE = (
    "with recursive c(x) as (select 1 union all select x + 1 from c) select count(*) from c"
)


@pytest.fixture
async def t(db):
    await db.execute_write("create table t (id integer primary key, v text)")
    return db


async def count(db):
    return (await db.execute("select count(*) from t")).rows[0][0]


async def test_sleeping_caller_expires_and_other_writers_proceed(t):
    other_done = asyncio.Event()
    other_done_at = None
    holding = asyncio.Event()

    async def other_writer():
        nonlocal other_done_at
        await holding.wait()
        await t.execute_write("insert into t (id, v) values (2, 'other')")
        other_done_at = time.perf_counter()
        other_done.set()

    serialised = not t.capabilities.concurrent_writers
    other = asyncio.create_task(other_writer())
    with pytest.raises(TransactionExpired):
        async with t.transaction(deadline_ms=100) as tx:
            t0 = time.perf_counter()
            await tx.execute("insert into t (id, v) values (1, 'mine')")
            holding.set()
            # The caller "sleeps" until the other writer has written. With a
            # write slot, that only happens if the deadline took the slot
            # back while this block was still inside the transaction.
            await asyncio.wait_for(other_done.wait(), EVENTUALLY_S)
            if serialised:
                assert tx.expired
            else:
                # Writers are not queued (PostgreSQL): the other one never
                # waited; the deadline still expires this transaction.
                await wait_until(lambda: tx.expired)
            await tx.execute("select 1")
    await other
    if serialised:
        check_latency("deadline -> other writer proceeds", other_done_at - t0 - 0.1, 0.2)
    assert (await t.execute("select v from t")).rows == [("other",)]


async def test_clean_exit_after_expiry_raises(t):
    with pytest.raises(TransactionExpired):
        async with t.transaction(deadline_ms=50) as tx:
            await tx.execute("insert into t (id, v) values (1, 'a')")
            await asyncio.sleep(0.2)
    assert tx.state == "expired"
    assert await count(t) == 0


async def test_exception_after_expiry_propagates_unchanged(t):
    with pytest.raises(KeyError):
        async with t.transaction(deadline_ms=50):
            await asyncio.sleep(0.2)
            raise KeyError("x")


async def test_every_call_after_expiry_raises(t):
    with pytest.raises(TransactionExpired):
        async with t.transaction(deadline_ms=50) as tx:
            await asyncio.sleep(0.15)
            for call in (
                tx.execute("select 1"),
                tx.execute_many("insert into t (v) values (?)", [["a"]]),
                tx.table_names(),
                tx.insert_rows("t", [{"v": "a"}]),
            ):
                with pytest.raises(TransactionExpired):
                    await call
    assert t.stats().in_use_write == 0


async def test_deadline_interrupts_running_statement(t):
    with pytest.raises(TransactionExpired):
        async with t.transaction(deadline_ms=150) as tx:
            t0 = time.perf_counter()
            await tx.execute("insert into t (id, v) values (1, 'a')")
            await tx.execute(INFINITE, time_limit_ms=None)
    check_latency("deadline -> running statement interrupted", time.perf_counter() - t0 - 0.15, 0.85)
    assert await count(t) == 0
    # The connection was not left in use, and works.
    await asyncio.wait_for(t.execute_write("insert into t (id, v) values (2, 'b')"), EVENTUALLY_S)
    assert await count(t) == 1


async def test_deadline_releases_slot_while_statement_holder_never_returns(t):
    """The holder task is blocked forever on something unrelated after its
    statement; the slot must still come free."""
    forever = asyncio.Event()
    holding = asyncio.Event()

    async def holder():
        async with t.transaction(deadline_ms=100) as tx:
            await tx.execute("insert into t (id, v) values (1, 'a')")
            holding.set()
            await forever.wait()

    task = asyncio.create_task(holder())
    await holding.wait()
    await asyncio.wait_for(t.execute_write("insert into t (id, v) values (2, 'b')"), EVENTUALLY_S)
    assert (await t.execute("select id from t")).rows == [(2,)]
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_default_deadline_comes_from_database(make_db):
    db = make_db(deadline_ms=50)
    await db.execute_write("create table t (x integer)")
    with pytest.raises(TransactionExpired):
        async with db.transaction() as tx:
            await asyncio.sleep(0.15)
            await tx.execute("select 1")


async def test_no_deadline(make_db):
    db = make_db(deadline_ms=None)
    async with db.transaction() as tx:
        await asyncio.sleep(0.1)
        await tx.execute("select 1")
    assert tx.deadline_ms is None


async def test_read_transaction_deadline(t):
    with pytest.raises(TransactionExpired):
        async with t.transaction("read", deadline_ms=50) as tx:
            await tx.execute("select 1")
            await asyncio.sleep(0.15)
            await tx.execute("select 1")
    assert t.stats().in_use_read == 0


async def test_finish_well_inside_deadline_commits(t):
    # A deadline far longer than the block can take, even on a loaded
    # machine; the timer it armed must be cancelled, not left to fire later.
    async with t.transaction(deadline_ms=int(EVENTUALLY_S * 1000)) as tx:
        timer = tx._timer
        assert timer is not None
        await tx.execute("insert into t (id, v) values (1, 'a')")
    assert timer.cancelled()
    assert await count(t) == 1
    assert tx.state == "committed"
