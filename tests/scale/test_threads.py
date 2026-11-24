"""In-process SQLite "must demonstrate" item 3: a writer queue with no
dedicated thread. 1,000 databases each written once must not raise the
process thread count, standalone (module default executor) or in a
registry (its fixed thread pool)."""

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


async def _write_each_once(dbs: list[Database]) -> tuple[int, int]:
    gate = asyncio.Semaphore(CONCURRENCY)
    lo, hi = 1 << 30, 0

    async def one(db: Database) -> None:
        nonlocal lo, hi
        async with gate:
            await db.execute_write("insert into t (x) values (1)")
            n = threading.active_count()
            lo, hi = min(lo, n), max(hi, n)

    await asyncio.gather(*(one(d) for d in dbs))
    return lo, hi


async def test_thousand_standalone_writes_add_no_threads(scale_backend, tmp_path):
    paths = scale_backend.create_files(tmp_path / "dbs", N + DEFAULT_MAX_THREADS * 2)
    warm = [Database(scale_backend.make(p)) for p in paths[N:]]
    await asyncio.gather(*(d.execute(WARM) for d in warm))  # default pool at full size
    for d in warm:
        await d.close()
    before = threading.active_count()
    dbs = [Database(scale_backend.make(p)) for p in paths[:N]]
    try:
        lo, hi = await _write_each_once(dbs)
        after = threading.active_count()
        print(f"\n[threads:{scale_backend.id}] standalone: {before} before, {lo}-{hi} during, {after} after")
        assert hi <= before and after <= before
        for d in dbs[:: N // 20]:
            assert (await d.execute("select count(*) from t")).rows == [(2,)]
    finally:
        for d in dbs:
            await d.close()


async def test_thousand_registry_writes_add_no_threads(scale_backend, tmp_path):
    paths = scale_backend.create_files(tmp_path / "dbs", N)
    async with Registry(max_threads=4, max_open_connections=50) as registry:
        dbs = [registry.add(f"d{i}", Database(scale_backend.make(p))) for i, p in enumerate(paths)]
        await asyncio.gather(*(d.execute(WARM) for d in dbs[:8]))  # registry pool at full size
        before = threading.active_count()
        lo, hi = await _write_each_once(dbs)
        after = threading.active_count()
        print(f"\n[threads:{scale_backend.id}] registry: {before} before, {lo}-{hi} during, {after} after")
        assert lo == hi == after == before
        assert registry.stats().open_connections <= 50
