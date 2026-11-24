"""No leaked file descriptors across the lifecycle: idle timeout, cap
eviction, sleep, close and registry close all give every descriptor back,
and the open count stays within the cap plus a fixed allowance."""

import asyncio
import random

import pytest

from dblayer import Database, Registry

from ..helpers import IdleClock, count_open_fds, wait_until

pytestmark = pytest.mark.requires("sleep_releases_everything")

# Descriptors a connection may hold beyond its own while a statement runs
# (a rollback journal during a write, a temp file), per concurrent call.
ALLOWANCE_PER_THREAD = 2


async def _prime(make_backend, n):
    """Create the databases' files (and warm the thread pool) before the
    baseline is taken."""
    backends = [make_backend() for _ in range(n)]
    for b in backends:
        d = Database(b)
        await d.execute_write("create table t (x integer)")
        await d.close()
    return backends


async def test_idle_timeout_releases_every_descriptor(make_backend, monkeypatch):
    # Time is moved by hand: with a real 50 ms timeout a loaded machine
    # could close everything before the "descriptors are open" check.
    clock = IdleClock(monkeypatch)
    backends = await _prime(make_backend, 8)
    async with Registry(idle_timeout=100, max_threads=4) as registry:
        dbs = [registry.add(f"d{i}", Database(b)) for i, b in enumerate(backends)]
        await dbs[0].execute("select 1")
        await dbs[0].sleep()
        baseline = count_open_fds()
        for _ in range(3):
            await asyncio.gather(*(d.execute("select count(*) from t") for d in dbs))
            await asyncio.gather(*(d.execute_write("insert into t values (1)") for d in dbs))
            assert count_open_fds() > baseline
            clock.advance(100, registry._hub)
            await wait_until(
                lambda: registry.stats().open_connections == 0
                and all(d.state == "asleep" and d._transition is None for d in dbs)
            )
            # Every descriptor comes back (polled: a closed socket or file
            # can take a moment to leave the descriptor table).
            await wait_until(lambda: count_open_fds() == baseline)


async def test_cap_bounds_descriptors_under_load(make_backend):
    cap, threads = 6, 4
    backends = await _prime(make_backend, 30)
    rng = random.Random(7)
    async with Registry(max_open_connections=cap, max_threads=threads) as registry:
        dbs = [registry.add(f"d{i}", Database(b)) for i, b in enumerate(backends)]
        await dbs[0].execute("select 1")
        await dbs[0].sleep()
        baseline = count_open_fds()
        # Descriptors one awake database with one connection holds: 1 for
        # SQLite, more where the backend keeps files of its own (a DuckDB
        # instance holds the file and its WAL). The cap counts an awake
        # database as at least one place, so this bounds every place.
        await dbs[0].execute("select 1")
        per_place = count_open_fds() - baseline
        await dbs[0].sleep()
        assert count_open_fds() == baseline
        # A backend may declare more per running statement (ClickHouse: an
        # INSERT writes a new part, several files at once).
        allowance = max(ALLOWANCE_PER_THREAD, getattr(backends[0], "fds_per_statement", 0))
        limit = baseline + cap * max(per_place, 1) + allowance * threads
        peak = 0

        async def op(d, write):
            nonlocal peak
            if write:
                await d.execute_write("insert into t values (1)")
            else:
                await d.execute("select count(*) from t")
            peak = max(peak, count_open_fds())

        await asyncio.gather(*(op(rng.choice(dbs), rng.random() < 0.3) for _ in range(400)))
        assert peak <= limit, (peak, baseline, cap, per_place)
        assert registry.stats().cap_evictions > 0
        for d in dbs:
            await d.sleep()
        assert count_open_fds() == baseline


async def test_close_and_registry_close_release_everything(make_backend):
    backends = await _prime(make_backend, 5)
    baseline = count_open_fds()
    registry = Registry()
    dbs = [registry.add(f"d{i}", Database(b)) for i, b in enumerate(backends)]
    for d in dbs:
        await d.execute("select 1")
        await d.execute_write("insert into t values (1)")
    await dbs[0].close()
    await registry.close()
    assert count_open_fds() <= baseline
