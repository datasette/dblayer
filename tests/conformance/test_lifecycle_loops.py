"""A Database can be used from one event loop after another (as Datasette's
tests do, with a fresh loop per test). Its pool is per loop: it is replaced
when the database is used from a new loop with nothing checked out on the
old one. Moving while the old loop still holds a connection is an error."""

import asyncio
import threading

import pytest

from dblayer import Database

from ..helpers import CountingBackend, count_open_fds

# These tests drive their own event loops, so they are plain functions.


def test_database_works_across_successive_loops(make_backend):
    db = Database(make_backend())

    async def first():
        await db.execute_write("create table t (x integer)")
        await db.execute_write("insert into t (x) values (1)")
        return db.stats().open_total

    async def second():
        await db.execute_write("insert into t (x) values (2)")
        return (await db.execute("select count(*) from t")).rows[0][0]

    assert asyncio.run(first()) >= 1
    assert asyncio.run(second()) == 2
    assert asyncio.run(second()) == 3
    asyncio.run(db.close())


def test_moving_loops_closes_connections_left_on_the_old_one(make_backend):
    backend = CountingBackend(make_backend())
    db = Database(backend)  # type: ignore[arg-type]
    asyncio.run(db.execute("select 1"))  # leaves one idle connection open
    left_open = db.stats().open_total
    assert left_open == 1
    closed_before = db.stats().closed_total

    async def again():
        await db.execute("select 1")
        return db.stats()

    s = asyncio.run(again())
    assert s.closed_total == closed_before + 1
    assert s.open_total == 1
    if db.capabilities.sleep_releases_everything:
        assert backend.closes >= 1  # it went to sleep on the way
    asyncio.run(db.close())


@pytest.mark.requires("sleep_releases_everything")
def test_moving_loops_leaks_no_file_descriptors(make_backend):
    # PostgreSQL: connections left on a closed loop have their socket closed
    # directly (PostgresConnection.close), since asyncpg's close() and
    # terminate() both need the old loop.
    db = Database(make_backend())
    asyncio.run(db.execute_write("create table t (x integer)"))
    asyncio.run(db.sleep())
    baseline = count_open_fds()

    async def use():
        await asyncio.gather(*(db.execute("select * from t") for _ in range(3)))
        await db.execute_write("insert into t (x) values (1)")

    for _ in range(4):
        asyncio.run(use())  # each run leaves idle connections behind
    asyncio.run(db.sleep())
    assert count_open_fds() == baseline
    asyncio.run(db.close())


def test_abandoned_stream_on_a_closed_loop(make_backend):
    db = Database(make_backend())

    async def abandon():
        s = db.stream(
            "with recursive c(x) as (select 1 union all select x + 1 from c where x < 100) "
            "select x from c",
            chunk_rows=10,
        )
        await s.__anext__()
        return s  # never closed; the loop ends with the connection checked out

    stream = asyncio.run(abandon())

    async def after():
        return (await db.execute("select 42")).rows

    assert asyncio.run(after()) == [(42,)]
    del stream
    asyncio.run(db.close())


def test_moving_while_in_use_on_a_live_loop_raises(make_backend):
    db = Database(make_backend())
    asyncio.run(db.execute_write("create table t (x integer)"))
    holding = threading.Event()
    release = threading.Event()
    errors: list[BaseException] = []

    async def hold():
        async with db.transaction() as tx:
            await tx.execute("insert into t (x) values (1)")
            holding.set()
            while not release.is_set():
                await asyncio.sleep(0.01)

    def run_other_loop():
        try:
            asyncio.run(hold())
        except BaseException as e:  # pragma: no cover - reported below
            errors.append(e)

    thread = threading.Thread(target=run_other_loop)
    thread.start()
    try:
        assert holding.wait(5)
        with pytest.raises(RuntimeError, match="another event loop"):
            asyncio.run(db.execute("select 1"))
    finally:
        release.set()
        thread.join(5)
    assert not errors
    # Once the other loop is done, this one can take over.
    assert asyncio.run(db.execute("select count(*) from t")).rows[0][0] == 1
    asyncio.run(db.close())
