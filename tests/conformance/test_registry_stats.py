"""registry.stats(): awake databases, open connections, wakes, sleeps,
evictions."""

import asyncio

from dblayer import Database, Registry, RegistryStats


async def test_stats_follow_the_lifecycle(make_backend):
    async with Registry(max_open_connections=2, max_threads=3, idle_timeout=0.05) as registry:
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
        await asyncio.sleep(0.2)  # idle timeout
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

        async def hold():
            async with a.transaction() as tx:
                await tx.execute("insert into t values (1)")
                await gate.wait()

        holder = asyncio.create_task(hold())
        await asyncio.sleep(0.02)
        reader = asyncio.create_task(b.execute("select 1"))
        await asyncio.sleep(0.02)
        assert registry.stats().waiting == 1
        gate.set()
        await asyncio.gather(holder, reader)
        assert registry.stats().waiting == 0
