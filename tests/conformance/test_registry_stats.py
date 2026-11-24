"""registry.stats(): awake databases, open connections, wakes, sleeps,
evictions."""

import asyncio

from dblayer import Database, Registry, RegistryStats

from ..helpers import IdleClock, wait_until


async def test_stats_follow_the_lifecycle(make_backend, monkeypatch):
    # The idle timeout must not fire between the steps below, however slow
    # the machine: time is moved by hand.
    clock = IdleClock(monkeypatch)
    async with Registry(max_open_connections=2, max_threads=3, idle_timeout=100) as registry:
        s = registry.stats()
        assert isinstance(s, RegistryStats)
        assert (s.databases, s.awake, s.open_connections, s.wakes, s.sleeps) == (0, 0, 0, 0, 0)
        assert s.max_open_connections == 2 and s.max_threads == 3
        dbs = [registry.add(f"d{i}", Database(make_backend())) for i in range(3)]
        assert registry.stats().databases == 3
        await dbs[0].execute("select 1")
        await dbs[1].execute("select 1")
        s = registry.stats()
        assert (s.awake, s.open_connections, s.idle_connections, s.wakes) == (2, 2, 2, 2)
        await dbs[2].execute("select 1")  # at the cap: evicts d0's connection
        await asyncio.sleep(0)
        s = registry.stats()
        assert s.cap_evictions == 1
        assert s.open_connections == 2
        assert s.wakes == 3
        clock.advance(100, registry._hub)  # idle timeout
        await wait_until(lambda: registry.stats().sleeps == 3)
        s = registry.stats()
        assert (s.awake, s.open_connections, s.idle_connections) == (0, 0, 0)
        assert s.sleeps == 3
        assert s.idle_closes == 2
        assert s.waiting == 0


async def test_stats_count_waiters(make_backend):
    async with Registry(max_open_connections=1) as registry:
        a, b = registry.add("a", Database(make_backend())), registry.add("b", Database(make_backend()))
        await a.execute_write("create table t (x integer)")
        gate = asyncio.Event()
        holding = asyncio.Event()

        async def hold():
            async with a.transaction() as tx:
                await tx.execute("insert into t values (1)")
                holding.set()
                await gate.wait()

        holder = asyncio.create_task(hold())
        await holding.wait()
        reader = asyncio.create_task(b.execute("select 1"))
        await wait_until(lambda: registry.stats().waiting == 1)
        assert not reader.done()
        gate.set()
        await asyncio.gather(holder, reader)
        assert registry.stats().waiting == 0
