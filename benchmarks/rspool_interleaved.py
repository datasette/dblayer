"""One-row read and update round trips on the C and the Rust multi-tenant
worker, interleaved: both pools are up at once and blocks of requests
alternate between them, so a change in machine load hits both alike (the
sequential rounds of ``rspool_bench.py`` swung by 2x on a loaded machine)::

    uv run --all-extras python benchmarks/rspool_interleaved.py [--blocks 40] [--per-block 200]
"""

from __future__ import annotations

import argparse
import asyncio
import os
import statistics
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import cpool_bench  # noqa: E402

from dblayer import Database  # noqa: E402
from dblayer.backends import sqlite_cworker, sqlite_rspool  # noqa: E402
from dblayer.backends.sqlite_cpool import CPool, SqliteCPool  # noqa: E402


async def main_async(blocks: int, per_block: int) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        pools = {
            "c": CPool(1, binary=sqlite_cworker.find_binary(), linger=60),
            "rust": CPool(1, binary=sqlite_rspool.find_binary(), linger=60),
        }
        dbs = {}
        for k, pool in pools.items():
            p = root / f"{k}.db"
            await cpool_bench._seed(p)
            dbs[k] = Database(SqliteCPool(p, pool=pool))
        reads: dict[str, list[float]] = {"c": [], "rust": []}
        writes: dict[str, list[float]] = {"c": [], "rust": []}
        try:
            for db in dbs.values():
                for _ in range(200):
                    await db.execute("select id, title from r where id = :id", {"id": 7})
                    await db.execute_write("update r set title = 'x' where id = 3")
            loads = []
            for b in range(blocks):
                order = ["c", "rust"] if b % 2 == 0 else ["rust", "c"]
                for k in order:
                    db = dbs[k]
                    for i in range(per_block):
                        t0 = time.perf_counter()
                        await db.execute("select id, title from r where id = :id", {"id": 7})
                        reads[k].append(time.perf_counter() - t0)
                        if i % 4 == 0:
                            t0 = time.perf_counter()
                            await db.execute_write("update r set title = :t where id = 3", {"t": f"t{i}"})
                            writes[k].append(time.perf_counter() - t0)
                loads.append(os.getloadavg()[0])
        finally:
            for db in dbs.values():
                await db.close()
            for pool in pools.values():
                pool.close()
    print(f"load average during the run: {min(loads):.1f} to {max(loads):.1f}")
    print("| Round trip (µs) | C p50 | Rust p50 | C p90 | Rust p90 |\n| --- | --- | --- | --- | --- |")
    for name, d in (("one-row read", reads), ("one-row update", writes)):
        q = {k: statistics.quantiles(v, n=10) for k, v in d.items()}
        med = {k: statistics.median(v) for k, v in d.items()}
        print(f"| {name} | {med['c'] * 1e6:.1f} | {med['rust'] * 1e6:.1f} | {q['c'][8] * 1e6:.1f} | {q['rust'][8] * 1e6:.1f} |")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--blocks", type=int, default=40)
    p.add_argument("--per-block", type=int, default=200)
    a = p.parse_args()
    asyncio.run(main_async(a.blocks, a.per_block))


if __name__ == "__main__":
    main()
