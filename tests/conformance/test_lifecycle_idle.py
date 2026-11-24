"""Idle timeout: a connection unused for ``idle_timeout`` seconds is closed;
when the last one closes the database is asleep. One timer per hub, none
per database; only checked-in connections are ever closed.

Tests that say "connection X has timed out but Y has not yet" use
``IdleClock`` (time moved by hand, long timeouts), since on a loaded machine
any real-time gap between two steps can stretch past a short timeout. The
others use the real timer and poll for the outcome."""

import asyncio

import pytest

from dblayer import Database
from dblayer.core.hub import Hub

from ..helpers import CountingBackend, IdleClock, wait_until

def asleep(db: Database) -> bool:
    """Asleep and the sleep finished: ``state`` says "asleep" as soon as the
    last connection closes, while the backend is still being released."""
    return db.state == "asleep" and db._transition is None


SLOW = (
    "with recursive c(x) as (select 1 union all select x + 1 from c where x < 400000) "
    "select count(*) from c"
)


async def test_idle_connection_is_closed_and_database_sleeps(make_backend):
    backend = CountingBackend(make_backend())
    db = Database(backend, idle_timeout=0.05)  # type: ignore[arg-type]
    try:
        await db.execute("select 1")
        assert db.stats().open_total == 1 and db.state == "awake"
        await wait_until(lambda: asleep(db))
        assert db.stats().open_total == 0
        assert db.state == "asleep"
        assert db._pool is None
        if db.capabilities.sleep_releases_everything:
            assert backend.released
        # And it wakes transparently.
        assert (await db.execute("select 2")).rows == [(2,)]
    finally:
        await db.close()


async def test_use_before_the_timeout_keeps_the_connection(make_db, monkeypatch):
    clock = IdleClock(monkeypatch)
    db = make_db(idle_timeout=100)
    for _ in range(8):
        await db.execute("select 1")
        clock.advance(40, db._get_hub())  # 320 s in all, never 100 s unused
    assert db.stats().opened_total == 1
    assert db.state == "awake"
    clock.advance(70, db._get_hub())
    await wait_until(lambda: asleep(db))


async def test_connection_in_use_is_never_closed_by_the_timeout(make_db):
    db = make_db(idle_timeout=0.01)
    assert (await db.execute(SLOW)).rows[0][0] == 400000
    # Then it was checked in, and times out.
    await wait_until(lambda: db.stats().open_total == 0)


async def test_transaction_survives_idle_timeout(make_db):
    db = make_db(idle_timeout=0.02)
    await db.execute_write("create table t (x integer)")
    async with db.transaction() as tx:
        await tx.execute("insert into t (x) values (1)")
        await asyncio.sleep(0.1)  # the write connection is checked out
        await tx.execute("insert into t (x) values (2)")
    assert (await db.execute("select count(*) from t")).rows[0][0] == 2


async def test_idle_timeout_none_never_closes(make_db):
    db = make_db(idle_timeout=None)
    await db.execute("select 1")
    await asyncio.sleep(0.1)
    assert db.stats().open_total == 1


async def test_write_and_read_connections_time_out_separately(make_db, monkeypatch):
    clock = IdleClock(monkeypatch)
    db = make_db(idle_timeout=100)
    hub = db._get_hub()
    await db.execute_write("create table t (x integer)")
    clock.advance(60, hub)
    await db.execute("select 1")  # the reader is fresh, the writer is not
    clock.advance(50, hub)  # writer unused for 110 s, reader for 50 s
    await wait_until(lambda: db.stats().open_write == 0)
    assert db.stats().open_read == 1
    clock.advance(60, hub)
    await wait_until(lambda: db.stats().open_total == 0)


async def test_many_databases_share_one_timer(make_backend, monkeypatch):
    loop = asyncio.get_running_loop()
    scheduled = getattr(loop, "_scheduled", None)
    if scheduled is None:
        pytest.skip("event loop does not expose its timer heap")
    # A timeout the timer cannot reach (and be re-armed or dropped) while 40
    # databases open on a loaded machine; then time is moved past it.
    clock = IdleClock(monkeypatch)
    hub = Hub(idle_timeout=100)
    dbs = [Database(make_backend()) for _ in range(40)]
    for d in dbs:
        d._hub = hub
    try:
        await asyncio.gather(*(d.execute("select 1") for d in dbs))
        # Heap entries include cancelled handles, and drivers keep timers of
        # their own (asyncpg's statement cache); count live hub timers.
        # Read loop._scheduled afresh: asyncio replaces the list when it
        # purges cancelled handles (drivers such as psycopg cancel many
        # timers), so a reference taken earlier can be stale and empty.
        def hub_timers():
            return [
                h
                for h in loop._scheduled  # type: ignore[attr-defined]
                if not h.cancelled() and getattr(h._callback, "__self__", None) is hub
            ]

        assert len(hub_timers()) == 1
        clock.advance(100, hub)
        await wait_until(lambda: all(asleep(d) for d in dbs))
        assert all(d._pool is None for d in dbs)
        assert hub.idle_count == 0
    finally:
        for d in dbs:
            await d.close()


async def test_asleep_databases_leave_nothing_in_the_hub(make_backend):
    hub = Hub(idle_timeout=0.02)
    dbs = [Database(make_backend()) for _ in range(20)]
    for d in dbs:
        d._hub = hub
    try:
        for d in dbs:
            await d.execute("select 1")
        await wait_until(lambda: all(asleep(d) for d in dbs))
        assert hub.idle_count == 0
        assert hub._timer is None
    finally:
        for d in dbs:
            await d.close()
