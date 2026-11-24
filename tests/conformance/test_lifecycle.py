"""Creation opens nothing, pool bounds, sleep and wake, close is permanent."""

import asyncio

import pytest

from dblayer import DatabaseClosed

from ..helpers import count_open_fds

SLOW = (
    "with recursive c(x) as (select 1 union all select x + 1 from c where x < 200000) "
    "select count(*) from c"
)


async def test_creating_a_database_opens_nothing(make_db):
    db = make_db()
    s = db.stats()
    assert s.open_total == 0 and s.opened_total == 0


async def test_capabilities_without_opening(make_db):
    db = make_db()
    assert db.capabilities.has("sleep_releases_everything") in (True, False)
    assert db.stats().opened_total == 0


async def test_read_pool_is_bounded(make_db):
    db = make_db(max_read_connections=2)
    peak = 0

    async def read():
        nonlocal peak
        r = await db.execute(SLOW)
        peak = max(peak, db.stats().open_read)
        return r.rows[0][0]

    assert await asyncio.gather(*(read() for _ in range(8))) == [200000] * 8
    assert db.stats().open_read <= 2
    assert peak <= 2


async def test_connections_are_reused(db):
    for _ in range(20):
        await db.execute("select 1")
    assert db.stats().opened_total == 1


async def test_sleep_then_wake(db):
    await db.execute_write("create table t (x integer)")
    await db.execute_write("insert into t (x) values (1)")
    await db.execute("select * from t")
    await db.sleep()
    s = db.stats()
    assert s.open_read == 0 and s.open_write == 0
    # Waking is invisible: data is still there.
    assert (await db.execute("select x from t")).rows == [(1,)]
    await db.execute_write("insert into t (x) values (2)")
    assert (await db.execute("select count(*) from t")).rows[0][0] == 2


async def test_sleep_is_repeatable(db):
    await db.execute_write("create table t (x integer)")
    for i in range(5):
        await db.execute_write("insert into t (x) values (:i)", {"i": i})
        await db.sleep()
        await db.sleep()
    assert (await db.execute("select count(*) from t")).rows[0][0] == 5


async def test_sleep_during_in_flight_read(db):
    await db.execute_write("create table t (x integer)")
    task = asyncio.create_task(db.execute(SLOW))
    await asyncio.sleep(0.01)
    await db.sleep()
    assert (await task).rows[0][0] == 200000  # finished on the connection it held
    assert db.stats().open_read == 0  # and that connection was closed on return


@pytest.mark.requires("sleep_releases_everything")
async def test_sleep_leaks_no_file_descriptors(make_db):
    db = make_db()
    await db.execute_write("create table t (x integer)")
    await db.sleep()
    baseline = count_open_fds()
    for _ in range(3):
        # Force several read connections plus the writer open at once.
        await asyncio.gather(*(db.execute(SLOW) for _ in range(3)))
        await db.execute_write("insert into t (x) values (1)")
        await db.table_names()
        assert count_open_fds() > baseline
        await db.sleep()
        assert count_open_fds() == baseline
    assert db.stats().open_total == 0


async def test_close_is_permanent(db):
    await db.execute_write("create table t (x integer)")
    await db.close()
    assert db.closed
    with pytest.raises(DatabaseClosed):
        await db.execute("select 1")
    with pytest.raises(DatabaseClosed):
        await db.execute_write("insert into t (x) values (1)")
    await db.close()  # idempotent
    assert db.stats().open_total == 0


async def test_close_waits_for_nothing_but_closes_on_return(db):
    task = asyncio.create_task(db.execute(SLOW))
    await asyncio.sleep(0.01)
    await db.close()
    # The in-flight read finishes on the connection it holds.
    assert (await task).rows[0][0] == 200000
    assert db.stats().open_total == 0


async def test_cancelled_read_returns_connection(db):
    task = asyncio.create_task(
        db.execute(
            "with recursive c(x) as (select 1 union all select x + 1 from c) "
            "select count(*) from c"
        )
    )
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    s = db.stats()
    assert s.in_use_read == 0
    assert (await asyncio.wait_for(db.execute("select 1"), 2)).rows == [(1,)]
