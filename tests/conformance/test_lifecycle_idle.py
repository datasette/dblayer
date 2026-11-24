"""Idle timeout: a connection unused for ``idle_timeout`` seconds is closed;
when the last one closes the database is asleep. One timer per hub, none
per database; only checked-in connections are ever closed."""

import asyncio

import pytest

from dblayer import Database
from dblayer.core.hub import Hub

from ..helpers import CountingBackend

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
        await asyncio.sleep(0.2)
        assert db.stats().open_total == 0
        assert db.state == "asleep"
        assert db._pool is None
        if db.capabilities.sleep_releases_everything:
            assert backend.released
        # And it wakes transparently.
        assert (await db.execute("select 2")).rows == [(2,)]
    finally:
        await db.close()


async def test_use_before_the_timeout_keeps_the_connection(make_db):
    db = make_db(idle_timeout=0.15)
    for _ in range(8):
        await db.execute("select 1")
        await asyncio.sleep(0.04)
    assert db.stats().opened_total == 1
    assert db.state == "awake"


async def test_connection_in_use_is_never_closed_by_the_timeout(make_db):
    db = make_db(idle_timeout=0.01)
    assert (await db.execute(SLOW)).rows[0][0] == 400000
    # Then it was checked in, and times out.
    await asyncio.sleep(0.1)
    assert db.stats().open_total == 0


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


async def test_write_and_read_connections_time_out_separately(make_db):
    db = make_db(idle_timeout=0.12)
    await db.execute_write("create table t (x integer)")
    await asyncio.sleep(0.08)
    await db.execute("select 1")  # the reader is fresh, the writer is not
    await asyncio.sleep(0.08)
    s = db.stats()
    assert s.open_write == 0 and s.open_read == 1
    await asyncio.sleep(0.1)
    assert db.stats().open_total == 0


async def test_many_databases_share_one_timer(make_backend):
    loop = asyncio.get_running_loop()
    scheduled = getattr(loop, "_scheduled", None)
    if scheduled is None:
        pytest.skip("event loop does not expose its timer heap")
    hub = Hub(idle_timeout=0.2)
    dbs = [Database(make_backend()) for _ in range(40)]
    for d in dbs:
        d._hub = hub
    try:
        before = len(scheduled)
        await asyncio.gather(*(d.execute("select 1") for d in dbs))
        # Heap entries include cancelled handles; live ones are what count.
        live = [h for h in scheduled if not h.cancelled()]
        assert len(live) <= before + 1
        await asyncio.sleep(0.4)
        assert all(d.state == "asleep" for d in dbs)
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
        await asyncio.sleep(0.1)
        assert hub.idle_count == 0
        assert hub._timer is None
    finally:
        for d in dbs:
            await d.close()
