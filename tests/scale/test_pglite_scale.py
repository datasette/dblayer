"""PGlite databases under one ``Registry``: many PostgreSQL data
directories, a Zipf load of reads and writes, and every database back
asleep through the idle timeout, with the harness of ``test_scale.py``.

Not part of ``SCALE_BACKENDS``: that test's cap of 64 open connections
would mean up to 64 awake PGlite processes (about 100 MB each), so this one
runs with a cap of ``CAP`` and fewer requests. ``DBLAYER_PGLITE_SCALE_N``
sets the database count (default 100); every write is checked at the end.
"""

from __future__ import annotations

import asyncio
import os
import random
import shutil
import subprocess
import time
from pathlib import Path

import pytest

from dblayer import Database, Registry
from dblayer.backends.pglite_worker import PGliteWorker, find_node, find_script, unavailable

from ..helpers import count_open_fds
from .harness import ScaleBackend, Zipf, register_all

UNAVAILABLE = unavailable()
pytestmark = pytest.mark.skipif(UNAVAILABLE is not None, reason=UNAVAILABLE or "")

N = int(os.environ.get("DBLAYER_PGLITE_SCALE_N", "100"))
REQUESTS = int(os.environ.get("DBLAYER_PGLITE_SCALE_REQUESTS", str(max(N * 4, 400))))
CAP = int(os.environ.get("DBLAYER_PGLITE_SCALE_CAP", "8"))
WORKERS = CAP * 2
WRITE_SHARE = 0.1
IDLE_TIMEOUT = 0.3


def pglite_dirs(root: Path, n: int) -> list[Path]:
    """One template cluster with ``t(x integer)`` holding one row, copied."""
    root.mkdir(parents=True, exist_ok=True)
    template = root / "template"
    script = Path(find_script()).parent.parent / "scripts" / "init_template.mjs"
    subprocess.run([find_node(), str(script), str(template)], check=True, capture_output=True, timeout=120)

    async def seed() -> None:
        db = Database(PGliteWorker(template))
        await db.execute_write("create table t (x integer)")
        await db.execute_write("insert into t values (0)")
        await db.close()

    asyncio.run(seed())
    os.unlink(template / "dblayer.lock")
    paths = [root / f"db{i:05d}" for i in range(n)]
    for p in paths:
        shutil.copytree(template, p)
    return paths


SPEC = ScaleBackend("pglite", pglite_dirs, lambda p: PGliteWorker(p), N, requests=REQUESTS, growth_limit=None)


def test_pglite_registry(tmp_path):
    t0 = time.perf_counter()
    paths = pglite_dirs(tmp_path / "dbs", N)
    create_s = time.perf_counter() - t0
    disk = sum(os.path.getsize(os.path.join(r, f)) for r, _, fs in os.walk(paths[0]) for f in fs)

    async def run() -> None:
        registry = Registry(max_open_connections=CAP, idle_timeout=IDLE_TIMEOUT)
        try:
            dbs, reg = register_all(registry, SPEC, paths)
            fd_baseline = count_open_fds()
            rng = random.Random(20261124)
            zipf = Zipf(N, 1.1, rng)
            plan = [(zipf.sample(), rng.random() < WRITE_SHARE) for _ in range(REQUESTS)]
            writes = [0] * N
            for i, w in plan:
                writes[i] += w
            touched = len({i for i, _ in plan})
            it = iter(plan)
            peak_awake = 0

            async def worker() -> None:
                nonlocal peak_awake
                for i, write in it:
                    db = dbs[i]
                    if write:
                        await db.execute_write("insert into t (x) values (1)")
                    else:
                        await db.execute("select count(*) from t")
                    peak_awake = max(peak_awake, registry.stats().awake)

            t1 = time.perf_counter()
            await asyncio.gather(*(worker() for _ in range(WORKERS)))
            load_s = time.perf_counter() - t1
            after = registry.stats()
            deadline = time.monotonic() + IDLE_TIMEOUT * 10 + 15
            while (registry.stats().awake or registry.stats().open_connections) and time.monotonic() < deadline:
                await asyncio.sleep(0.05)
            final = registry.stats()
            for i in rng.sample(range(N), min(N, 50)) + [zipf.order[0]]:
                rows = (await dbs[i].execute("select count(*) from t")).rows
                assert rows == [(1 + writes[i],)], i
            print(
                f"\n[scale:pglite] {N} databases (template copies, {disk / 1e6:.1f} MB each on disk), "
                f"created in {create_s:.1f}s\n"
                f"  register: {reg.seconds * 1e6 / N:.1f} us/db, {reg.traced_bytes_per_db:.0f} B/db asleep\n"
                f"  load: {REQUESTS} requests ({WRITE_SHARE:.0%} writes) over {touched} databases, Zipf s=1.1, "
                f"{WORKERS} workers, cap {CAP}: {load_s:.1f}s ({REQUESTS / load_s:.0f} req/s), "
                f"peak awake {peak_awake}\n"
                f"  registry: wakes={after.wakes} sleeps={after.sleeps} cap_evictions={after.cap_evictions}; "
                f"after idle timeout awake={final.awake} open={final.open_connections}; "
                f"fds baseline {fd_baseline} after {count_open_fds()}"
            )
            assert final.awake == 0 and final.open_connections == 0
        finally:
            await registry.close()

    asyncio.run(run())
