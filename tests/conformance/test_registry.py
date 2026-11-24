"""Registry basics: add, get, remove, lazy lookup through the resolver."""

import asyncio

import pytest

from dblayer import Database, DatabaseClosed, Registry, UnknownDatabase


@pytest.fixture
async def registry():
    r = Registry()
    yield r
    await r.close()


async def test_add_get_remove(registry, make_backend):
    db = registry.add("a", Database(make_backend()))
    assert db.name == "a"
    assert "a" in registry and len(registry) == 1 and list(registry) == ["a"]
    assert await registry.get("a") is db
    assert registry.peek("a") is db
    await db.execute_write("create table t (x integer)")
    removed = await registry.remove("a")
    assert removed is db and db.closed
    assert "a" not in registry
    with pytest.raises(UnknownDatabase):
        await registry.get("a")


async def test_unknown_database_is_a_key_error(registry):
    with pytest.raises(KeyError):
        await registry.get("nope")
    with pytest.raises(UnknownDatabase):
        await registry.remove("nope")


async def test_adding_does_not_open_anything(registry, make_backend):
    db = registry.add("a", Database(make_backend()))
    assert db.stats().opened_total == 0 and db.state == "asleep"


async def test_duplicate_name_and_second_registry_rejected(registry, make_backend):
    db = registry.add("a", Database(make_backend()))
    with pytest.raises(ValueError):
        registry.add("a", Database(make_backend()))
    other = Registry()
    with pytest.raises(ValueError):
        other.add("b", db)
    await other.close()


async def test_adding_an_awake_database_is_refused(registry, make_backend):
    db = Database(make_backend())
    await db.execute("select 1")
    with pytest.raises(ValueError):
        registry.add("a", db)
    await db.sleep()
    registry.add("a", db)
    assert (await db.execute("select 1")).rows == [(1,)]


async def test_resolver_is_called_lazily_once_for_concurrent_gets(make_backend):
    calls = []

    async def resolve(name):
        calls.append(name)
        await asyncio.sleep(0.02)
        return Database(make_backend())

    async with Registry(resolver=resolve) as registry:
        assert calls == []
        dbs = await asyncio.gather(*(registry.get("x") for _ in range(10)))
        assert calls == ["x"]
        assert all(d is dbs[0] for d in dbs)
        assert dbs[0].name == "x"
        assert await registry.get("x") is dbs[0]
        assert calls == ["x"]


async def test_resolver_returning_none(make_backend):
    calls = 0

    async def resolve(name):
        nonlocal calls
        calls += 1
        return None

    async with Registry(resolver=resolve) as registry:
        with pytest.raises(UnknownDatabase):
            await registry.get("x")
        with pytest.raises(UnknownDatabase):
            await registry.get("x")
        assert calls == 2  # not cached
        assert "x" not in registry


async def test_resolver_error_reaches_every_waiter_then_retries(make_backend):
    attempts = 0

    async def resolve(name):
        nonlocal attempts
        attempts += 1
        await asyncio.sleep(0.01)
        if attempts == 1:
            raise OSError("store unavailable")
        return Database(make_backend())

    async with Registry(resolver=resolve) as registry:
        results = await asyncio.gather(
            *(registry.get("x") for _ in range(5)), return_exceptions=True
        )
        assert all(isinstance(r, OSError) for r in results)
        assert attempts == 1
        db = await registry.get("x")
        assert attempts == 2 and db is registry.peek("x")


async def test_cancelled_resolver_lets_waiters_retry(make_backend):
    started = asyncio.Event()
    attempts = 0

    async def resolve(name):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            started.set()
            await asyncio.sleep(10)
        return Database(make_backend())

    async with Registry(resolver=resolve) as registry:
        first = asyncio.create_task(registry.get("x"))
        await started.wait()
        second = asyncio.create_task(registry.get("x"))
        await asyncio.sleep(0)
        first.cancel()
        db = await asyncio.wait_for(second, 2)
        assert attempts == 2 and db.name == "x"


async def test_remove_without_closing(registry, make_backend):
    db = registry.add("a", Database(make_backend()))
    await db.execute_write("create table t (x integer)")
    await registry.remove("a", close=False)
    assert not db.closed and db.state == "asleep"
    assert (await db.execute("select count(*) from t")).rows == [(0,)]
    await db.close()


async def test_close_closes_every_database(make_backend):
    registry = Registry()
    dbs = [registry.add(f"d{i}", Database(make_backend())) for i in range(5)]
    for d in dbs:
        await d.execute("select 1")
    await registry.close()
    assert all(d.closed for d in dbs)
    with pytest.raises(DatabaseClosed):
        await dbs[0].execute("select 1")
    with pytest.raises(RuntimeError):
        await registry.get("d0")


async def test_registry_idle_timeout_applies_to_its_databases(make_backend):
    async with Registry(idle_timeout=0.05) as registry:
        db = registry.add("a", Database(make_backend()))
        own = registry.add("b", Database(make_backend(), idle_timeout=None))
        await db.execute("select 1")
        await own.execute("select 1")
        await asyncio.sleep(0.2)
        assert db.state == "asleep"
        assert own.state == "awake"
