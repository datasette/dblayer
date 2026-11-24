"""Cold state, on_evict and the object-storage stand-in (tests/objectstore.py):
the wake lock on a cold database, eviction under a disk quota, and a
writable database being evicted only after it has been copied out."""

import asyncio
import random
from collections.abc import Callable
from pathlib import Path

import pytest

from dblayer import Database, Registry
from dblayer.backends.sqlite import SqliteInProcess

from ..objectstore import LocalCache, ObjectStore, StoreUnavailable

# Backends whose database is one local file, built from its path. Another
# single-file backend (DuckDB) can be appended here.
FILE_BACKENDS: list[tuple[str, Callable[[Path], object]]] = [
    ("sqlite", lambda path: SqliteInProcess(path)),
]


@pytest.fixture(params=FILE_BACKENDS, ids=lambda p: p[0])
def file_backend(request):
    return request.param[1]


@pytest.fixture
async def setup(tmp_path, file_backend):
    store = ObjectStore(tmp_path / "store")
    cache = LocalCache(store, tmp_path / "local")
    registry = Registry(on_wake=cache.on_wake, on_sleep=cache.on_sleep, on_evict=cache.on_evict)
    cache.registry = registry

    def add(name: str) -> Database:
        return registry.add(name, Database(file_backend(cache.path_for(name))))

    yield store, cache, registry, add
    await registry.close()


async def _make_cold(registry, db, rows=1):
    await db.execute_write("create table t (x integer)")
    for i in range(rows):
        await db.execute_write("insert into t (x) values (:i)", {"i": i})
    await db.sleep()
    assert await registry.evict(db.name)
    assert db.state == "cold"


async def test_cold_round_trip(setup):
    store, cache, registry, add = setup
    db = add("customer-1/sales")
    await _make_cold(registry, db, rows=3)
    assert not cache.path_for(db.name).exists()
    assert registry._hub.cold == 1
    assert (await db.execute("select count(*) from t")).rows == [(3,)]
    assert db.state == "awake"
    assert registry._hub.cold == 0
    assert ("fetch", db.name) in cache.events


async def test_wake_lock_fetches_a_cold_database_once(setup):
    store, cache, registry, add = setup
    store.latency = 0.03
    db = add("a")
    await _make_cold(registry, db)
    gets = store.gets
    results = await asyncio.gather(*(db.execute("select count(*) from t") for _ in range(20)))
    assert all(r.rows == [(1,)] for r in results)
    assert store.gets == gets + 1


async def test_written_database_is_copied_out_before_eviction(setup):
    store, cache, registry, add = setup
    db = add("a")
    await _make_cold(registry, db)
    await db.execute_write("insert into t (x) values (99)")  # wakes, writes
    store.fail_puts = True
    await db.sleep()  # on_sleep's copy-out fails (logged)
    with pytest.raises(StoreUnavailable):
        await registry.evict("a")
    # Not evicted: still local, still asleep, the write is safe.
    assert db.state == "asleep"
    assert cache.path_for("a").exists()
    assert registry._hub.evictions == 1  # only the first one
    store.fail_puts = False
    assert await registry.evict("a")
    assert db.state == "cold" and not cache.path_for("a").exists()
    assert (await db.execute("select max(x) from t")).rows == [(99,)]


async def test_unchanged_database_is_evicted_without_copying(setup):
    store, cache, registry, add = setup
    db = add("a")
    await _make_cold(registry, db)
    await db.execute("select count(*) from t")  # read only: wakes, fetches
    await db.sleep()
    puts = store.puts
    assert await registry.evict("a")
    assert store.puts == puts


async def test_awake_database_is_not_evicted(setup):
    store, cache, registry, add = setup
    db = add("a")
    await db.execute_write("create table t (x integer)")
    assert not await registry.evict("a")  # an idle connection is open
    async with db.transaction() as tx:
        await tx.execute("insert into t (x) values (1)")
        assert not await registry.evict("a")
    assert db.state == "awake"
    await db.sleep()
    assert await registry.evict("a")


async def test_request_during_eviction_waits_then_fetches(setup):
    store, cache, registry, add = setup
    db = add("a")
    await db.execute_write("create table t (x integer)")
    await db.execute_write("insert into t (x) values (1)")
    store.latency = 0.05
    await db.sleep()  # copies out (slowly)
    await db.execute_write("insert into t (x) values (2)")
    await db.sleep()
    evicting = asyncio.create_task(registry.evict("a"))
    await asyncio.sleep(0.01)
    reading = asyncio.create_task(db.execute("select count(*) from t"))
    assert await evicting
    assert (await reading).rows == [(2,)]
    names = [e for e, n in cache.events]
    assert names.index("evict") < len(names) - 1 - names[::-1].index("fetch")


async def test_eviction_needs_hooks(tmp_path):
    async with Registry() as registry:
        registry.add("a", Database(SqliteInProcess(tmp_path / "a.db")))
        with pytest.raises(RuntimeError):
            await registry.evict("a")


async def test_cold_database_without_on_wake_refuses_to_open(setup, tmp_path):
    store, cache, registry, add = setup
    db = add("a")
    await _make_cold(registry, db)
    await registry.remove("a", close=False)  # leaves the registry's hooks behind
    with pytest.raises(RuntimeError, match="cold"):
        await db.execute("select 1")
    assert not cache.path_for("a").exists()  # no empty file was created
    await db.close()


@pytest.mark.parametrize("seed", [1, 2])
async def test_disk_quota(setup, seed):
    store, cache, registry, add = setup
    n = 12
    dbs = [add(f"d{i}") for i in range(n)]
    for d in dbs:
        await d.execute_write("create table t (x integer, pad text)")
        await d.execute_write("insert into t values (0, :pad)", {"pad": "x" * 20000})
        await d.sleep()
    size = cache.path_for("d0").stat().st_size
    quota = 4 * size
    cache.quota_bytes = quota
    await cache.enforce_quota()
    assert cache.local_bytes() <= quota
    rng = random.Random(seed)
    writes = [1] * n
    for _ in range(60):
        i = rng.randrange(n)
        d = dbs[i]
        if rng.random() < 0.4:
            await d.execute_write("insert into t (x) values (1)")
            writes[i] += 1
        else:
            await d.execute("select count(*) from t")
        await d.sleep()
        assert cache.local_bytes() <= quota
    assert registry._hub.evictions >= 8
    for i, d in enumerate(dbs):
        assert (await d.execute("select count(*) from t")).rows == [(writes[i],)]
        await d.sleep()


async def test_add_cold_is_fetched_by_on_wake_on_first_call(tmp_path):
    # A fleet can be registered without touching storage: each database
    # starts cold, and on_wake builds its file on first use.
    woken: list[str] = []

    async def on_wake(db):
        if db.state == "cold":
            woken.append(db.name)
            seed = Database(SqliteInProcess(tmp_path / f"{db.name}.db"))
            await seed.execute_write("create table t (name text)")
            await seed.execute_write("insert into t values (:n)", {"n": db.name})
            await seed.close()

    registry = Registry(on_wake=on_wake)
    try:
        dbs = [
            registry.add(f"db{i}", Database(SqliteInProcess(tmp_path / f"db{i}.db")), cold=True)
            for i in range(3)
        ]
        assert [d.state for d in dbs] == ["cold"] * 3
        assert registry.stats().cold == 3
        assert not list(tmp_path.iterdir())
        assert (await dbs[1].execute("select name from t")).rows == [("db1",)]
        assert woken == ["db1"]
        assert dbs[1].state == "awake"
        s = registry.stats()
        assert (s.cold, s.awake, s.wakes) == (2, 1, 1)
    finally:
        await registry.close()


async def test_add_cold_needs_on_wake(tmp_path):
    registry = Registry()
    try:
        with pytest.raises(ValueError, match="on_wake"):
            registry.add("x", Database(SqliteInProcess(tmp_path / "x.db")), cold=True)
        assert "x" not in registry
    finally:
        await registry.close()
