"""Measurements for the multi-tenant C pool (``SqliteCPool``), used by
``worker_overhead.py`` (variants ``cpool-json`` and ``cpool-msgpack``, and
the "Multi-tenant C pool" section). Run alone for the section only::

    uv run --all-extras python benchmarks/cpool_bench.py [--quick] [--json out.json]

* open to first result of a new database on a warm pool;
* throughput with 1, 4, 16 and 64 concurrent clients, each on its own
  database (one-row reads), for in-process SQLite, the single-tenant C
  worker (a process per connection) and the pool, with the CPU time of
  the parent and of the workers over the run: when the parent's CPU time
  approaches the wall time, the event loop is the limit;
* memory inside a pool process per open database (footprint delta);
* processes and descriptors at peak.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from dblayer import Database  # noqa: E402
from dblayer.backends.sqlite import SqliteInProcess  # noqa: E402
from dblayer.backends.sqlite_worker import live_worker_pids, reap_workers  # noqa: E402

_pools: dict[str, Any] = {}


def cpool_available() -> bool:
    from dblayer.backends.sqlite_cworker import binary_unavailable

    return binary_unavailable() is None


def pool_for(codec: str) -> Any:
    from dblayer.backends.sqlite_cpool import CPool

    if codec not in _pools:
        _pools[codec] = CPool(2, codec=codec, linger=0)
    return _pools[codec]


def make_cpool(codec: str, path: Path) -> Any:
    from dblayer.backends.sqlite_cpool import SqliteCPool

    return SqliteCPool(path, pool=pool_for(codec))


def close_pools() -> None:
    for p in _pools.values():
        p.close()
    _pools.clear()


def _cpu_s(pid: int) -> float:
    """User + system CPU seconds of ``pid`` (``ps``, 10 ms resolution)."""
    out = subprocess.run(["ps", "-o", "time=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()
    if not out:
        return 0.0
    parts = out.replace("-", ":").split(":")
    secs = 0.0
    for p in parts:
        secs = secs * 60 + float(p)
    return secs


def _fds() -> int:
    for d in ("/dev/fd", "/proc/self/fd"):
        try:
            return len(os.listdir(d))
        except OSError:
            continue
    return -1


async def _seed(path: Path) -> None:
    db = Database(SqliteInProcess(path))
    await db.execute_write("create table if not exists r (id integer primary key, title text)")
    await db.execute_write(
        "insert into r (title) select 'title ' || i from "
        "(with recursive c(i) as (select 1 union all select i + 1 from c where i < 100) select i from c)"
    )
    await db.close()


def _make(kind: str, path: Path) -> Any:
    if kind == "in-process":
        return SqliteInProcess(path)
    if kind == "cworker":
        from dblayer.backends.sqlite_cworker import SqliteCWorker

        return SqliteCWorker(path, codec="msgpack")
    return make_cpool("msgpack", path)


async def throughput(kind: str, root: Path, clients: int, seconds: float) -> dict[str, float]:
    """``clients`` tasks, each reading one row from its own database in a
    loop for ``seconds``."""
    paths = [root / f"tp-{kind}-{clients}-{i}.db" for i in range(clients)]
    for p in paths:
        await _seed(p)
    dbs = [Database(_make(kind, p)) for p in paths]
    sql, params = "select id, title from r where id = :id", {"id": 7}
    try:
        for d in dbs:
            for _ in range(20):
                await d.execute(sql, params)
        pids = live_worker_pids()
        cpu0 = {pid: _cpu_s(pid) for pid in pids}
        stop = time.perf_counter() + seconds
        counts = [0] * clients
        peak_fds = 0

        async def client(i: int) -> None:
            db = dbs[i]
            n = 0
            while time.perf_counter() < stop:
                await db.execute(sql, params)
                n += 1
            counts[i] = n

        p0, t0 = time.process_time(), time.perf_counter()
        await asyncio.gather(*(client(i) for i in range(clients)))
        wall = time.perf_counter() - t0
        parent_cpu = time.process_time() - p0
        peak_fds = _fds()
        worker_cpu = sum(_cpu_s(pid) - cpu0.get(pid, 0.0) for pid in pids)
        return {
            "rps": sum(counts) / wall,
            "parent_cpu_share": parent_cpu / wall,
            "worker_cpu_cores": worker_cpu / wall,
            "processes": len(pids),
            "fds": peak_fds,
        }
    finally:
        for d in dbs:
            await d.close()


async def warm_open(root: Path, repeat: int) -> dict[str, float]:
    """A new database on a pool whose process is running: Database()
    to first result, and the bare ``open`` round trip."""
    from dblayer.backends.sqlite_cpool import CPool, SqliteCPool

    pool = CPool(1, codec="msgpack", linger=60)
    first = Database(SqliteCPool(root / "warm-0.db", pool=pool))
    await first.execute("select 1")
    times = []
    dbs = []
    try:
        for i in range(repeat):
            p = root / f"warm-{i + 1}.db"
            await _seed(p)
            db = Database(SqliteCPool(p, pool=pool))
            dbs.append(db)
            t0 = time.perf_counter()
            await db.execute("select title from r where id = 1")
            times.append(time.perf_counter() - t0)
        proc = pool.processes()[0]
        opens = []
        for i in range(repeat):
            t0 = time.perf_counter()
            r = await proc.call({"type": "open", "path": str(root / "warm-1.db"), "mode": "read"}, owner=None)
            opens.append(time.perf_counter() - t0)
            await proc.call({"type": "close", "conn": r["conn"]}, owner=None)
    finally:
        for d in dbs + [first]:
            await d.close()
        pool.close()
    return {"first_result_ms": statistics.median(times) * 1e3, "open_round_trip_ms": statistics.median(opens) * 1e3}


async def memory_per_open(root: Path, n: int, lookaside: tuple[int, int] | None = None) -> dict[str, float]:
    """Footprint of one pool process with 1 database open, then with
    ``n``: the delta per open database (each has run one small query, so
    its schema and a few pages are cached)."""
    from duckdb_memory import mem_of

    from dblayer.backends.sqlite_cpool import CPool, SqliteCPool

    pool = CPool(1, codec="msgpack", linger=60, lookaside=lookaside, max_connections=n + 8)
    paths = [root / f"mem-{i}.db" for i in range(n + 1)]
    for p in paths:
        await _seed(p)
    dbs = [Database(SqliteCPool(p, pool=pool)) for p in paths]
    try:
        await dbs[0].execute("select title from r where id = 1")
        pid = pool.processes()[0].pid
        assert pid is not None
        await asyncio.sleep(0.2)
        base = mem_of(pid)
        for d in dbs[1:]:
            await d.execute("select title from r where id = 1")
        await asyncio.sleep(0.2)
        after = mem_of(pid)
        fds = _fds()
        assert base is not None and after is not None
        return {
            "base_footprint_kb": base[1] / 1024,
            "footprint_kb_per_open": (after[1] - base[1]) / 1024 / n,
            "rss_kb_per_open": (after[0] - base[0]) / 1024 / n,
            "databases": n,
            "parent_fds_with_all_open": fds,
            "threads_in_worker": n + 1 + 3,  # a lane per connection, main, reader, watchdog
        }
    finally:
        for d in dbs:
            await d.close()
        pool.close()


async def run(quick: bool = False) -> dict[str, Any]:
    out: dict[str, Any] = {"load_average": os.getloadavg()}
    seconds = 1.0 if quick else 2.5
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        out["warm_open"] = await warm_open(root, 10 if quick else 30)
        out["memory"] = await memory_per_open(root, 100 if quick else 400)
        out["memory_no_lookaside"] = await memory_per_open(root, 100 if quick else 400, lookaside=(0, 0))
        scaling: dict[str, dict[int, Any]] = {}
        for kind in ("in-process", "cworker", "cpool"):
            scaling[kind] = {}
            for clients in (1, 4, 16, 64):
                scaling[kind][clients] = await throughput(kind, root, clients, seconds)
                reap_workers(5)
        out["scaling"] = scaling
    close_pools()
    reap_workers(5)
    out["load_average_after"] = os.getloadavg()
    return out


def print_section(r: dict[str, Any]) -> None:
    la = r["load_average"]
    print(f"\nMulti-tenant C pool (load average {la[0]:.1f} {la[1]:.1f} {la[2]:.1f} at start, "
          f"{r['load_average_after'][0]:.1f} after; provisional)")
    w = r["warm_open"]
    print(f"  new database on a warm pool: first result {w['first_result_ms']:.2f} ms; bare open round trip "
          f"{w['open_round_trip_ms']:.3f} ms")
    m = r["memory"]
    print(f"  memory per open database inside the process: {m['footprint_kb_per_open']:.1f} KB footprint, "
          f"{m['rss_kb_per_open']:.1f} KB RSS ({m['databases']} databases; process base "
          f"{m['base_footprint_kb'] / 1024:.1f} MB footprint); parent descriptors with all open: "
          f"{m['parent_fds_with_all_open']}")
    m2 = r.get("memory_no_lookaside")
    if m2:
        print(f"  the same with lookaside off (CPool(lookaside=(0, 0))): {m2['footprint_kb_per_open']:.1f} KB footprint")
    print("  one-row reads, each client on its own database: requests/s (parent CPU share of wall; "
          "worker CPU in cores; worker processes; parent descriptors)")
    for kind, rows in r["scaling"].items():
        cells = []
        for clients, x in rows.items():
            cells.append(f"{clients:>2}: {x['rps']:7.0f} ({x['parent_cpu_share']:.2f}; {x['worker_cpu_cores']:.2f}; "
                         f"{x['processes']}p; {x['fds']}fd)")
        print(f"    {kind:<10} " + "  ".join(cells))


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--quick", action="store_true")
    p.add_argument("--json", type=Path)
    args = p.parse_args()
    r = asyncio.run(run(args.quick))
    print_section(r)
    if args.json:
        args.json.write_text(json.dumps(r, indent=2))


if __name__ == "__main__":
    main()
