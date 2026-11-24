"""Global connection cap: at ``max_open_connections`` the least recently
used idle connection in any database is closed; with none idle the caller
waits."""

import asyncio
import random

import pytest

from dblayer import Database, Registry

SLOW = (
    "with recursive c(x) as (select 1 union all select x + 1 from c where x < 50000) "
    "select count(*) from c"
)


def _open(dbs):
    return sum(d.stats().open_total for d in dbs)


async def test_cap_closes_least_recently_used_idle_connection(make_backend):
    async with Registry(max_open_connections=3) as registry:
        dbs = [registry.add(f"d{i}", Database(make_backend())) for i in range(10)]
        for d in dbs:
            await d.execute("select 1")
            assert _open(dbs) <= 3
        assert [d.state for d in dbs[-3:]] == ["awake"] * 3
        assert all(d.state == "asleep" for d in dbs[:-3])
        assert registry._hub.cap_evictions == 7
        assert registry._hub.open == 3


async def test_lru_is_by_last_use_not_by_open_order(make_backend):
    async with Registry(max_open_connections=3) as registry:
        a, b, c, d = (registry.add(n, Database(make_backend())) for n in "abcd")
        await a.execute("select 1")
        await b.execute("select 1")
        await c.execute("select 1")
        await a.execute("select 1")  # a is now the most recently used
        await d.execute("select 1")  # evicts b
        assert b.state == "asleep"
        assert a.state == c.state == d.state == "awake"


async def test_caller_waits_when_nothing_is_idle(make_backend):
    async with Registry(max_open_connections=2) as registry:
        a, b, c = (registry.add(n, Database(make_backend())) for n in "abc")
        await a.execute_write("create table t (x integer)")
        await b.execute_write("create table t (x integer)")
        release = asyncio.Event()

        async def hold(db):
            async with db.transaction() as tx:
                await tx.execute("insert into t (x) values (1)")
                await release.wait()

        holders = [asyncio.create_task(hold(a)), asyncio.create_task(hold(b))]
        await asyncio.sleep(0.05)
        reader = asyncio.create_task(c.execute("select 42"))
        await asyncio.sleep(0.05)
        assert not reader.done()
        assert registry._hub.waiting == 1
        assert registry._hub.open == 2
        release.set()
        assert (await asyncio.wait_for(reader, 2)).rows == [(42,)]
        await asyncio.gather(*holders)
        assert registry._hub.open <= 2


async def test_cancelled_waiter_does_not_leak_a_place(make_backend):
    async with Registry(max_open_connections=1) as registry:
        a, c, d = (registry.add(n, Database(make_backend())) for n in "acd")
        await a.execute_write("create table t (x integer)")
        release = asyncio.Event()

        async def hold():
            async with a.transaction() as tx:
                await tx.execute("insert into t (x) values (1)")
                await release.wait()

        holder = asyncio.create_task(hold())
        await asyncio.sleep(0.02)
        waiter = asyncio.create_task(c.execute("select 1"))
        await asyncio.sleep(0.02)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        release.set()
        await holder
        assert (await asyncio.wait_for(d.execute("select 7"), 2)).rows == [(7,)]
        assert registry._hub.open == 1
        assert registry._hub.waiting == 0


async def test_checked_in_connection_goes_to_a_waiter(make_backend):
    async with Registry(max_open_connections=1) as registry:
        a, b = (registry.add(n, Database(make_backend())) for n in "ab")
        first = asyncio.create_task(a.execute(SLOW))
        await asyncio.sleep(0.005)
        second = asyncio.create_task(b.execute("select 1"))
        await asyncio.gather(first, second)
        # a's connection was closed on check-in to make room for b.
        assert a.state == "asleep" and b.state == "awake"
        assert registry._hub.open == 1


@pytest.mark.parametrize("seed", [1, 2, 3])
async def test_cap_holds_under_concurrent_load(make_backend, seed):
    cap = 5
    rng = random.Random(seed)
    async with Registry(max_open_connections=cap) as registry:
        dbs = [registry.add(f"d{i}", Database(make_backend())) for i in range(12)]
        for d in dbs:
            await d.execute_write("create table t (x integer)")
        peak = 0

        async def op(db, write):
            nonlocal peak
            if write:
                await db.execute_write("insert into t (x) values (1)")
            else:
                await db.execute("select count(*) from t")
            peak = max(peak, registry._hub.open)

        plan = [(rng.choice(dbs), rng.random() < 0.3) for _ in range(300)]
        ops = [op(d, w) for d, w in plan]
        await asyncio.wait_for(asyncio.gather(*ops), 30)
        assert peak <= cap
        assert registry._hub.open <= cap
        total = 0
        for d in dbs:
            total += (await d.execute("select count(*) from t")).rows[0][0]
        assert total == sum(1 for _, w in plan if w)
