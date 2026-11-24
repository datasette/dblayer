"""The spec's scale test: 10,000 small databases registered at once and a
skewed (Zipf) request load.

Asserted throughout the load: open file descriptors stay at or below
``max_open_connections`` plus a fixed allowance, and the thread count stays
constant. Also asserted: registering is linear in the count, every database
is asleep (no pool, no connections) once the load stops, and an asleep
database is small. The figures are printed (run with ``-s`` to see them).

Set ``DBLAYER_SCALE_N`` for a smaller or larger run.
"""

from __future__ import annotations

import asyncio
import gc
import random
import threading
import time
import tracemalloc

import pytest

from dblayer import Registry

from ..helpers import count_open_fds
from .harness import (
    SCALE_BACKENDS,
    Sampler,
    ScaleBackend,
    Zipf,
    register_all,
    scale_count,
    time_registration,
)

pytestmark = pytest.mark.scale

CAP = 64
THREADS = 8
WORKERS = 96  # more concurrent requests than the cap, so some wait
WRITE_SHARE = 0.1
IDLE_TIMEOUT = 0.3
# Per running statement: a rollback journal during a write, a temp file.
FD_ALLOWANCE = 2 * THREADS + 8
TARGET_BYTES_PER_ASLEEP_DB = 2048


@pytest.fixture(params=SCALE_BACKENDS, ids=lambda s: s.id)
def scale_backend(request) -> ScaleBackend:
    spec: ScaleBackend = request.param
    reason = spec.available()
    if reason:
        pytest.skip(reason)
    return spec


async def test_ten_thousand_databases(scale_backend, tmp_path):
    spec = scale_backend
    n = scale_count(spec)
    requests = spec.requests
    t0 = time.perf_counter()
    paths = spec.create_files(tmp_path / "dbs", n)
    create_s = time.perf_counter() - t0

    # Registering is linear in the count (best of three, fresh registries).
    small = max(n // 10, 100)
    t_small = time_registration(spec, paths[:small])
    t_full = time_registration(spec, paths)
    per_small = t_small / small
    per_full = t_full / n

    registry = Registry(
        max_open_connections=CAP, max_threads=THREADS, idle_timeout=IDLE_TIMEOUT
    )
    try:
        dbs, reg = register_all(registry, spec, paths)
        assert len(registry) == n
        assert all(d._pool is None for d in dbs)

        # Warm the thread pool to its full size, then take the baselines.
        await asyncio.gather(
            *(
                dbs[i].execute(
                    "with recursive c(x) as (select 1 union all select x + 1 from c "
                    "where x < 20000) select count(*) from c"
                )
                for i in range(THREADS * 2)
            )
        )
        for d in dbs[: THREADS * 2]:
            await d.sleep()
        # Queries alone do not guarantee the pool reached full size: on a
        # loaded machine a thread can finish before the next submit and be
        # reused, and the missing thread then starts during the load.
        # THREADS jobs that wait for one another force every thread up.
        barrier = threading.Barrier(THREADS)
        loop = asyncio.get_running_loop()
        await asyncio.gather(
            *(loop.run_in_executor(registry.executor, barrier.wait, 30) for _ in range(THREADS))
        )
        threads = threading.active_count()
        fd_baseline = count_open_fds()
        # Descriptors per place under the cap: 1 for SQLite (one per
        # connection); a DuckDB instance holds its file and WAL.
        await dbs[0].execute("select count(*) from t")
        per_place = max(count_open_fds() - fd_baseline, 1)
        await dbs[0].sleep()
        fd_limit = fd_baseline + CAP * per_place + FD_ALLOWANCE

        rng = random.Random(20261123)
        zipf = Zipf(n, 1.1, rng)
        plan = [(zipf.sample(), rng.random() < WRITE_SHARE) for _ in range(requests)]
        writes = [0] * n
        for i, w in plan:
            writes[i] += w
        touched = len({i for i, _ in plan})
        sampler = Sampler(count_open_fds)
        stop = asyncio.Event()

        async def sample_loop() -> None:
            while not stop.is_set():
                sampler.sample(fd_limit)
                await asyncio.sleep(0.005)

        it = iter(plan)

        async def worker() -> None:
            for i, write in it:
                db = dbs[i]
                if write:
                    await db.execute_write("insert into t (x) values (1)")
                else:
                    await db.execute("select count(*) from t")
                if sampler.samples % 4 == 0:
                    sampler.sample(fd_limit)

        sampling = asyncio.create_task(sample_loop())
        t0 = time.perf_counter()
        await asyncio.gather(*(worker() for _ in range(WORKERS)))
        load_s = time.perf_counter() - t0
        stats_after_load = registry.stats()
        stop.set()
        await sampling

        # Everything goes back to sleep through the idle timeout alone.
        deadline = time.monotonic() + IDLE_TIMEOUT * 10 + 5
        while registry.stats().awake and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
        await asyncio.sleep(0.05)
        final = registry.stats()
        fds_after = count_open_fds()
        no_pools = sum(1 for d in dbs if d._pool is not None)

        # Memory of an asleep database after it has been used: wake and
        # sleep a sample of databases under tracemalloc.
        sample_dbs = [dbs[i] for i in range(0, n, max(n // 1000, 1))][:1000]
        gc.collect()
        tracemalloc.start()
        before = tracemalloc.get_traced_memory()[0]
        for d in sample_dbs:
            await d.execute("select count(*) from t")
            await d.sleep()
        gc.collect()
        used_growth = (tracemalloc.get_traced_memory()[0] - before) / len(sample_dbs)
        tracemalloc.stop()

        # Correctness: every write landed.
        for i in rng.sample(range(n), 200) + [zipf.order[0]]:
            rows = (await dbs[i].execute("select count(*) from t")).rows
            assert rows == [(1 + writes[i],)], i
    finally:
        await registry.close()

    print(
        f"\n[scale:{spec.id}] {n} databases, created in {create_s:.1f}s\n"
        f"  register: {reg.seconds * 1e6 / n:.1f} us/db; best-of-3 "
        f"{per_small * 1e6:.2f} us/db at {small}, {per_full * 1e6:.2f} us/db at {n}\n"
        f"  asleep, never used: {reg.traced_bytes_per_db:.0f} B/db (tracemalloc), "
        + (f"{reg.rss_bytes_per_db:.0f} B/db (RSS delta)" if reg.rss_bytes_per_db is not None else "RSS n/a")
        + f"\n  asleep after a wake/sleep cycle: {used_growth:+.1f} B/db growth (tracemalloc, "
        f"{len(sample_dbs)} dbs)\n"
        f"  load: {requests} requests ({WRITE_SHARE:.0%} writes) over {touched} databases, "
        f"Zipf s=1.1, {WORKERS} workers, in {load_s:.1f}s ({requests / load_s:.0f} req/s)\n"
        f"  fds: baseline {fd_baseline}, peak {sampler.peak_fds}, limit {fd_limit} "
        f"(cap {CAP} x {per_place} per place + allowance {FD_ALLOWANCE}), after sleep {fds_after}; "
        f"{sampler.samples} samples\n"
        f"  threads: {threads} before, {sampler.min_threads}-{sampler.max_threads} during\n"
        f"  registry after load: wakes={stats_after_load.wakes} sleeps={stats_after_load.sleeps} "
        f"cap_evictions={stats_after_load.cap_evictions} idle_closes={stats_after_load.idle_closes}; "
        f"after idle timeout: awake={final.awake} open={final.open_connections}"
    )

    assert sampler.samples > 100
    assert not sampler.over_limit, f"fd count over {fd_limit}: max {max(sampler.over_limit)}"
    # No thread started during the load. (A thread left by an earlier test
    # may exit meanwhile, so the minimum is not asserted.)
    assert sampler.max_threads <= threads, (sampler, threads)
    assert len(registry.executor._threads) == THREADS  # type: ignore[attr-defined]
    assert per_full <= 3 * per_small, "registration is not linear in the count"
    assert reg.traced_bytes_per_db < TARGET_BYTES_PER_ASLEEP_DB
    if spec.growth_limit is not None:
        assert used_growth < spec.growth_limit  # waking and sleeping leaves nothing behind
    assert stats_after_load.cap_evictions > 0
    assert final.awake == 0 and final.open_connections == 0
    assert fds_after <= fd_baseline
    assert no_pools == 0
