"""Sleep releases the backend even when connections were checked out at
the time (they finish, come back, are closed, and then the backend is
closed); an asleep database holds no pool."""

import asyncio

import pytest

from dblayer import Database

from ..helpers import CountingBackend

SLOW = (
    "with recursive c(x) as (select 1 union all select x + 1 from c where x < 300000) "
    "select count(*) from c"
)


@pytest.fixture
async def counted(make_backend):
    backend = CountingBackend(make_backend())
    db = Database(backend)  # type: ignore[arg-type]
    yield db, backend
    await db.close()


async def _settle(db) -> None:
    """Wait for the sleep that follows the last connection coming back: it
    runs in the background, and closing a backend can take a thread hop or
    more (DuckDB checkpoints its WAL on close)."""
    for _ in range(1000):
        await asyncio.sleep(0.005)
        if db._transition is None and not db._awake:
            return


@pytest.mark.requires("sleep_releases_everything")
async def test_sleep_with_nothing_in_use_closes_backend(counted):
    db, backend = counted
    await db.execute("select 1")
    assert not backend.released
    await db.sleep()
    assert backend.released
    assert db._pool is None
    assert db.state == "asleep"


@pytest.mark.requires("sleep_releases_everything")
async def test_sleep_while_reading_closes_backend_after_return(counted):
    db, backend = counted
    task = asyncio.create_task(db.execute(SLOW))
    await asyncio.sleep(0.01)
    await db.sleep()
    assert not backend.released  # a connection is still in use
    assert (await task).rows[0][0] == 300000
    await _settle(db)
    assert backend.released
    assert db.stats().open_total == 0
    assert db._pool is None


@pytest.mark.requires("sleep_releases_everything")
async def test_sleep_inside_transaction_closes_backend_after_commit(counted):
    db, backend = counted
    await db.execute_write("create table t (x integer)")
    async with db.transaction() as tx:
        await tx.execute("insert into t (x) values (1)")
        await db.sleep()
        assert not backend.released
        await tx.execute("insert into t (x) values (2)")
    await _settle(db)
    assert backend.released
    assert (await db.execute("select count(*) from t")).rows[0][0] == 2


@pytest.mark.requires("sleep_releases_everything")
async def test_stream_open_across_sleep(counted):
    db, backend = counted
    sql = "with recursive c(x) as (select 1 union all select x + 1 from c where x < 50) select x from c"
    async with db.stream(sql, chunk_rows=10) as chunks:
        first = await chunks.__anext__()
        await db.sleep()
        rest = [row async for chunk in chunks for row in chunk]
    assert len(first) + len(rest) == 50
    await _settle(db)
    assert backend.released


async def test_close_while_in_use_closes_backend_after_return(counted):
    db, backend = counted
    task = asyncio.create_task(db.execute(SLOW))
    await asyncio.sleep(0.01)
    closes = backend.closes
    await db.close()
    assert (await task).rows[0][0] == 300000
    await _settle(db)
    assert backend.closes > closes
    assert backend.released
    assert db.state == "closed"


async def test_wake_during_sleep_waits_for_backend_close(counted):
    db, backend = counted
    await db.execute("select 1")
    sleeping = asyncio.create_task(db.sleep())
    await asyncio.sleep(0)
    assert (await db.execute("select 2")).rows == [(2,)]
    await sleeping
    assert db.stats().opened_total == 2
