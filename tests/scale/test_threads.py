"""In-process SQLite "must demonstrate" item 3: a writer queue with no
dedicated thread. 1,000 databases each written once must not start a
thread, standalone (module default executor) or in a registry (its fixed
thread pool). Standalone, a backend with a process per database holds
fewer open at once (``ScaleBackend.open_at_once``), still far more than
the pool has threads."""

from __future__ import annotations

import asyncio
import threading

import pytest

from dblayer import Database, Registry
from dblayer.core.executor import DEFAULT_MAX_THREADS

from .harness import SCALE_BACKENDS, ScaleBackend

pytestmark = pytest.mark.scale

N = 1000
CONCURRENCY = 100
WARM = (
    "with recursive c(x) as (select 1 union all select x + 1 from c where x < 20000) "
    "select count(*) from c"
)


@pytest.fixture(params=SCALE_BACKENDS, ids=lambda s: s.id)
def scale_backend(request) -> ScaleBackend:
    spec: ScaleBackend = request.param
    reason = spec.available()
    if reason:
        pytest.skip(reason)
    return spec


def _new_threads(before: set[threading.Thread]) -> set[threading.Thread]:
    return set(threading.enumerate()) - before


async def _write_each_once(dbs: list[Database], before: set[threading.Thread]) -> tuple[int, int, set]:
    """Write each database once, ``CONCURRENCY`` at a time. Returns the
    lowest and highest thread count seen and every thread seen running that
    was not in ``before``. A thread from an earlier test exiting meanwhile
    lowers the count; that is not a thread this load added, so the
    assertions are on new threads, not on the count."""
    gate = asyncio.Semaphore(CONCURRENCY)
    lo, hi = 1 << 30, 0
    new: set[threading.Thread] = set()

    async def one(db: Database) -> None:
        nonlocal lo, hi
        async with gate:
            await db.execute_write("insert into t (x) values (1)")
            n = threading.active_count()
            lo, hi = min(lo, n), max(hi, n)
            new.update(_new_threads(before))

    await asyncio.gather(*(one(d) for d in dbs))
    return lo, hi, new


async def test_thousand_standalone_writes_add_no_threads(scale_backend, tmp_path):
    n = min(N, scale_backend.open_at_once)  # fewer for a process per database (memory)
    paths = scale_backend.create_files(tmp_path / "dbs", n + DEFAULT_MAX_THREADS * 2)
    warm = [Database(scale_backend.make(p)) for p in paths[n:]]
    await asyncio.gather(*(d.execute(WARM) for d in warm))  # default pool at full size
    for d in warm:
        await d.close()
    before = set(threading.enumerate())
    dbs = [Database(scale_backend.make(p)) for p in paths[:n]]
    try:
        lo, hi, new = await _write_each_once(dbs, before)
        new |= _new_threads(before)
        print(f"\n[threads:{scale_backend.id}] standalone, {n} open: {len(before)} before, {lo}-{hi} during, "
              f"{threading.active_count()} after, new: {sorted(t.name for t in new)}")
        assert not new
        for d in dbs[:: max(n // 20, 1)]:
            assert (await d.execute("select count(*) from t")).rows == [(2,)]
    finally:
        for d in dbs:
            await d.close()


async def test_thousand_registry_writes_add_no_threads(scale_backend, tmp_path):
    paths = scale_backend.create_files(tmp_path / "dbs", N)
    async with Registry(max_threads=4, max_open_connections=50) as registry:
        dbs = [registry.add(f"d{i}", Database(scale_backend.make(p))) for i, p in enumerate(paths)]
        await asyncio.gather(*(d.execute(WARM) for d in dbs[:8]))  # registry pool at full size
        before = set(threading.enumerate())
        lo, hi, new = await _write_each_once(dbs, before)
        new |= _new_threads(before)
        print(f"\n[threads:{scale_backend.id}] registry: {len(before)} before, {lo}-{hi} during, "
              f"{threading.active_count()} after, new: {sorted(t.name for t in new)}")
        assert not new
        assert registry.stats().open_connections <= 50
