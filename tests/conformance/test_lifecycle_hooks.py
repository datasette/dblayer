"""Wake lock and the on_wake / on_sleep hooks."""

import asyncio
import logging

import pytest

from dblayer import Database, Registry

from ..helpers import EVENTUALLY_S, CountingBackend, wait_until


async def test_concurrent_first_requests_trigger_one_on_wake(make_backend):
    calls = []

    async def on_wake(db):
        calls.append(db.name)
        await asyncio.sleep(0.05)

    async with Registry(on_wake=on_wake) as registry:
        db = registry.add("a", Database(make_backend()))
        results = await asyncio.gather(*(db.execute("select 1") for _ in range(20)))
        assert all(r.rows == [(1,)] for r in results)
        assert calls == ["a"]
        # Awake: further calls do not wake again.
        await db.execute("select 1")
        assert calls == ["a"]


async def test_on_wake_runs_before_the_first_connection_opens(make_backend):
    backend = CountingBackend(make_backend())
    seen = []

    async def on_wake(db):
        seen.append((backend.opens, db.stats().open_total, db.state))

    async with Registry(on_wake=on_wake) as registry:
        db = registry.add("a", Database(backend))  # type: ignore[arg-type]
        await db.execute("select 1")
        assert seen == [(0, 0, "asleep")]
        assert db.state == "awake"


async def test_on_wake_error_reaches_every_waiter_then_retries(make_backend):
    attempts = 0

    async def on_wake(db):
        nonlocal attempts
        attempts += 1
        await asyncio.sleep(0.02)
        if attempts == 1:
            raise OSError("object store unavailable")

    async with Registry(on_wake=on_wake) as registry:
        db = registry.add("a", Database(make_backend()))
        results = await asyncio.gather(
            *(db.execute("select 1") for _ in range(5)), return_exceptions=True
        )
        assert all(isinstance(r, OSError) for r in results)
        assert attempts == 1
        assert db.state == "asleep" and db.stats().opened_total == 0
        assert (await db.execute("select 1")).rows == [(1,)]
        assert attempts == 2


async def test_cancelled_waker_hands_over_to_a_waiter(make_backend):
    started = asyncio.Event()
    attempts = 0

    async def on_wake(db):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            started.set()
            await asyncio.sleep(10)

    async with Registry(on_wake=on_wake) as registry:
        db = registry.add("a", Database(make_backend()))
        first = asyncio.create_task(db.execute("select 1"))
        await started.wait()
        second = asyncio.create_task(db.execute("select 2"))
        await asyncio.sleep(0.01)
        first.cancel()
        assert (await asyncio.wait_for(second, EVENTUALLY_S)).rows == [(2,)]
        assert attempts == 2


async def test_on_sleep_runs_after_everything_is_released(make_backend):
    backend = CountingBackend(make_backend())
    seen = []

    async def on_sleep(db):
        seen.append((db.stats().open_total, backend.released, db.state))

    async with Registry(on_sleep=on_sleep, idle_timeout=0.05) as registry:
        db = registry.add("a", Database(backend))  # type: ignore[arg-type]
        await db.execute("select 1")
        await wait_until(lambda: seen)  # the idle timeout put it to sleep
        released = db.capabilities.sleep_releases_everything
        assert seen == [(0, released, "asleep")]


async def test_wake_waits_for_on_sleep_to_finish(make_backend):
    events = []
    sleep_started = asyncio.Event()

    async def on_sleep(db):
        events.append("sleep start")
        sleep_started.set()
        await asyncio.sleep(0.05)
        events.append("sleep end")

    async def on_wake(db):
        events.append("wake")

    async with Registry(on_sleep=on_sleep, on_wake=on_wake) as registry:
        db = registry.add("a", Database(make_backend()))
        await db.execute("select 1")
        sleeping = asyncio.create_task(db.sleep())
        # Wait until the sleep is under way (closing a server connection can
        # take a while; a call arriving before then just keeps it awake).
        await asyncio.wait_for(sleep_started.wait(), EVENTUALLY_S)
        assert (await db.execute("select 2")).rows == [(2,)]
        await sleeping
        assert events == ["wake", "sleep start", "sleep end", "wake"]


async def test_wakes_and_sleeps_balance(make_backend):
    wakes = sleeps = 0

    async def on_wake(db):
        nonlocal wakes
        wakes += 1

    async def on_sleep(db):
        nonlocal sleeps
        sleeps += 1

    async with Registry(on_wake=on_wake, on_sleep=on_sleep, max_open_connections=2) as registry:
        dbs = [registry.add(f"d{i}", Database(make_backend())) for i in range(6)]
        for _ in range(3):
            await asyncio.gather(*(d.execute("select 1") for d in dbs))
        for d in dbs:
            await d.sleep()
        await asyncio.sleep(0.01)
        assert wakes == sleeps > 6
        assert registry._hub.awake == 0
        assert registry._hub.wakes == wakes and registry._hub.sleeps == sleeps


async def test_on_sleep_error_is_logged_not_raised(make_backend, caplog):
    async def on_sleep(db):
        raise RuntimeError("upload failed")

    async with Registry(on_sleep=on_sleep) as registry:
        db = registry.add("a", Database(make_backend()))
        await db.execute("select 1")
        with caplog.at_level(logging.ERROR, logger="dblayer"):
            await db.sleep()
        assert "on_sleep hook failed" in caplog.text
        assert (await db.execute("select 1")).rows == [(1,)]


async def test_close_runs_on_sleep_for_an_awake_database(make_backend):
    slept = []

    async def on_sleep(db):
        slept.append(db.state)

    registry = Registry(on_sleep=on_sleep)
    db = registry.add("a", Database(make_backend()))
    await db.execute("select 1")
    await registry.close()
    assert slept == ["closed"]
