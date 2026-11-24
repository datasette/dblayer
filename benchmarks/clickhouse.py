"""ClickHouse (chDB, in-process) against DuckDB and SQLite, through dblayer.

    uv run --python 3.14 --all-extras python benchmarks/clickhouse.py [--rows 1000000] [--big]

Measures, on the same generated data in each engine (``events``: id
primary key, user_id, category, value; ``users``: 10,000 rows):

* open / wake: first statement with the chDB server down (server start),
  first statement of a second database while it is up, and after
  ``db.sleep()``;
* point read by primary key (median of 300);
* analytical reads: a GROUP BY over every row, and a JOIN to ``users``
  with GROUP BY; ``--big`` also runs them at 10x ``--rows``;
* inserting ``--rows`` rows with ``db.insert_rows`` (ClickHouse: native
  Arrow path and the default multi-row VALUES path; row-by-row
  ``execute_write`` measured on 2,000 rows and scaled);
* exporting every row as CSV and JSON: ClickHouse's own formats
  (``engine_export=True``) against the core's Python fallback;
* resident memory (macOS ``proc_pid_rusage``; Linux VmRSS) before chDB is
  imported, after, awake, and after sleep; disk size of each database.

Prints ``uptime``'s load average before each section: other work on the
machine skews every figure, which are provisional unless run alone.
"""

from __future__ import annotations

import argparse
import asyncio
import ctypes
import ctypes.util
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dblayer import Database  # noqa: E402


def load() -> str:
    out = subprocess.run(["uptime"], capture_output=True, text=True).stdout
    return out.split("load average")[-1].strip(" :s\n")


def rss_mb() -> float:
    """Resident memory of this process, MB (macOS: proc_pid_rusage, as
    benchmarks/duckdb_memory.py; Linux: VmRSS)."""
    if sys.platform == "darwin":
        class Info(ctypes.Structure):
            _fields_ = [("uuid", ctypes.c_uint8 * 16)] + [
                (n, ctypes.c_uint64)
                for n in (
                    "user", "system", "pkg_idle", "interrupt_wkups", "pageins",
                    "wired", "resident", "footprint",
                )
            ] + [("_spare", ctypes.c_uint8 * 1024)]  # the kernel writes the whole struct

        lib = ctypes.CDLL("/usr/lib/libproc.dylib")
        lib.proc_pid_rusage.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_void_p]
        info = Info()
        lib.proc_pid_rusage(os.getpid(), 2, ctypes.byref(info))
        return info.resident / 1e6
    with open("/proc/self/status") as f:
        return next(int(line.split()[1]) / 1e3 for line in f if line.startswith("VmRSS:"))


def du_mb(path: Path) -> float:
    total = 0
    for root, _dirs, files in os.walk(path, followlinks=False):
        for name in files:
            try:
                total += os.lstat(os.path.join(root, name)).st_size
            except OSError:
                pass
    return total / 1e6


def section(name: str) -> None:
    print(f"\n## {name}   (load average {load()})", flush=True)


async def timed(fn, repeat: int = 1) -> float:
    """Best of ``repeat`` runs, seconds."""
    best = float("inf")
    for _ in range(repeat):
        t0 = time.perf_counter()
        await fn()
        best = min(best, time.perf_counter() - t0)
    return best


GROUP_BY = "select category, count(*), sum(value), avg(value) from events group by category order by category"
JOIN = (
    "select u.country, count(*), sum(e.value) from events e join users u on u.user_id = e.user_id "
    "group by u.country order by u.country"
)


async def fill(name: str, db: Database, n: int) -> None:
    """events and users, same values in every engine."""
    await db.create_table(
        "events",
        {"id": "integer", "user_id": "integer", "category": "text", "value": "float"},
        pk="id",
    )
    await db.create_table("users", {"user_id": "integer", "country": "text"}, pk="user_id")
    if name == "clickhouse":
        await db.execute_write(
            f"insert into events select number + 1, (number + 1) % 10000, "
            f"concat('c', toString((number + 1) % 20)), ((number + 1) * 7 % 1000) / 10.0 "
            f"from numbers({n})"
        )
        await db.execute_write(
            "insert into users select number, concat('k', toString(number % 50)) from numbers(10000)"
        )
    elif name == "duckdb":
        await db.execute_write(
            f"insert into events select i, i % 10000, 'c' || (i % 20), (i * 7 % 1000) / 10.0 "
            f"from range(1, {n + 1}) t(i)"
        )
        await db.execute_write(
            "insert into users select i, 'k' || (i % 50) from range(0, 10000) t(i)"
        )
    else:
        await db.execute_write(
            "insert into events with recursive c(i) as (select 1 union all select i + 1 from c "
            f"where i < {n}) select i, i % 10000, 'c' || (i % 20), (i * 7 % 1000) / 10.0 from c"
        )
        await db.execute_write(
            "insert into users with recursive c(i) as (select 0 union all select i + 1 from c "
            "where i < 9999) select i, 'k' || (i % 50) from c"
        )


def make(name: str, root: Path, label: str, **kw):
    if name == "clickhouse":
        from dblayer.backends.clickhouse import ClickHouse

        return ClickHouse(root / "chdb", database=label, **kw)
    if name == "duckdb":
        from dblayer.backends.duckdb import DuckDB

        return DuckDB(root / f"{label}.duckdb", memory_limit="1GB", threads=2)
    from dblayer.backends.sqlite import SqliteInProcess

    return SqliteInProcess(root / f"{label}.db")


def db_size(name: str, root: Path, label: str) -> float:
    if name == "clickhouse":
        store = root / "chdb" / "data" / label
        return sum(du_mb(p.resolve()) for p in store.iterdir()) if store.exists() else 0.0
    if name == "duckdb":
        return du_mb(root) if False else os.path.getsize(root / f"{label}.duckdb") / 1e6
    return os.path.getsize(root / f"{label}.db") / 1e6


async def analytics(root: Path, n: int, label: str, results: dict) -> None:
    for name in ("clickhouse", "duckdb", "sqlite"):
        db = Database(make(name, root, f"{label}_{name}"), time_limit_ms=None, deadline_ms=None)
        t0 = time.perf_counter()
        await fill(name, db, n)
        results[(name, f"fill {n:,}")] = time.perf_counter() - t0
        g = await timed(lambda: db.execute(GROUP_BY), 3)
        j = await timed(lambda: db.execute(JOIN), 3)
        results[(name, f"group by {n:,}")] = g
        results[(name, f"join+group by {n:,}")] = j
        ids = [(i * 7919) % n + 1 for i in range(300)]
        lat = []
        for i in ids:
            t1 = time.perf_counter()
            await db.execute("select category, value from events where id = :id", {"id": i})
            lat.append(time.perf_counter() - t1)
        results[(name, f"point read (median of 300) {n:,}")] = statistics.median(lat)
        await db.close()
        results[(name, f"disk MB {n:,}")] = db_size(name, root, f"{label}_{name}")
        print(
            f"{name:10} fill {results[(name, f'fill {n:,}')]:.2f}s  group by {g * 1000:.0f} ms  "
            f"join {j * 1000:.0f} ms  point read {statistics.median(lat) * 1000:.2f} ms  "
            f"disk {results[(name, f'disk MB {n:,}')]:.1f} MB",
            flush=True,
        )


async def inserts(root: Path, n: int, results: dict) -> None:
    rows = [
        {"id": i, "user_id": i % 10000, "category": f"c{i % 20}", "value": (i * 7 % 1000) / 10.0}
        for i in range(1, n + 1)
    ]
    cols = {"id": "integer", "user_id": "integer", "category": "text", "value": "float"}
    for name, native in (("clickhouse", True), ("clickhouse", False), ("duckdb", True), ("sqlite", True)):
        label = f"ins_{name}_{int(native)}"
        db = Database(make(name, root, label), native_operations=native, time_limit_ms=None, deadline_ms=None)
        await db.create_table("events", cols, pk="id")
        t0 = time.perf_counter()
        await db.insert_rows("events", rows, batch_size=100_000)
        dt = time.perf_counter() - t0
        tag = f"{name}{' native' if name == 'clickhouse' and native else ' default path' if name == 'clickhouse' else ''}"
        results[(tag, f"insert_rows {n:,}")] = dt
        print(f"{tag:24} insert_rows {n:,}: {dt:.2f}s ({n / dt:,.0f} rows/s)", flush=True)
        await db.close()
    db = Database(make("clickhouse", root, "ins_rowwise"), time_limit_ms=None, deadline_ms=None)
    await db.create_table("events", cols, pk="id")
    k = 2000
    t0 = time.perf_counter()
    for r in rows[:k]:
        await db.execute_write(
            "insert into events (id, user_id, category, value) values (:id, :user_id, :category, :value)", r
        )
    dt = time.perf_counter() - t0
    results[("clickhouse row by row", f"insert {k:,} (scaled to {n:,})")] = dt * n / k
    print(f"clickhouse row by row: {dt / k * 1000:.2f} ms a row, {dt * n / k:.0f}s for {n:,} (scaled)", flush=True)
    await db.close()


async def exports(root: Path, n: int, results: dict) -> None:
    from dblayer.backends.clickhouse import ClickHouse

    db = Database(ClickHouse(root / "chdb", database="exp", engine_export=True), time_limit_ms=None, deadline_ms=None)
    await fill("clickhouse", db, n)
    for fmt in ("csv", "json"):
        for native in (True, False):
            async def run() -> None:
                size = 0
                async with db.export("select * from events", format=fmt, native=native) as s:
                    async for chunk in s:
                        size += len(chunk)
                results[("clickhouse", f"export {fmt} {'engine' if native else 'python'} MB")] = size / 1e6

            t0 = time.process_time()
            dt = await timed(run)
            cpu = time.process_time() - t0
            key = f"export {fmt} {n:,} {'engine' if native else 'python fallback'}"
            results[("clickhouse", key)] = dt
            print(f"clickhouse {key}: {dt:.2f}s wall, {cpu:.2f}s process CPU", flush=True)
    await db.close()


async def lifecycle(root: Path, results: dict) -> None:
    from dblayer.backends.clickhouse import ClickHouse

    m0 = rss_mb()
    t0 = time.perf_counter()
    import chdb  # noqa: F401

    results[("clickhouse", "import chdb s")] = time.perf_counter() - t0
    m1 = rss_mb()
    a = Database(ClickHouse(root / "chdb", database="life_a"))
    t0 = time.perf_counter()
    await a.execute("select 1")
    cold = time.perf_counter() - t0
    m2 = rss_mb()
    b = Database(ClickHouse(root / "chdb", database="life_b"))
    t0 = time.perf_counter()
    await b.execute("select 1")
    second = time.perf_counter() - t0
    await b.close()
    await a.create_table("t", {"id": "integer", "v": "text"}, pk="id")
    await a.execute_write("insert into t select number, toString(number) from numbers(1000000)")
    await a.execute("select v, count() from t group by v order by count() desc limit 5")
    m3 = rss_mb()
    await a.sleep()
    m4 = rss_mb()
    t0 = time.perf_counter()
    await a.execute("select count() from t")
    wake = time.perf_counter() - t0
    await a.close()
    results.update(
        {
            ("clickhouse", "first statement, server down (start) ms"): cold * 1000,
            ("clickhouse", "first statement, second database, server up ms"): second * 1000,
            ("clickhouse", "wake after sleep (server restart) ms"): wake * 1000,
            ("memory", "RSS MB before chdb import"): m0,
            ("memory", "RSS MB after import"): m1,
            ("memory", "RSS MB server up, one database"): m2,
            ("memory", "RSS MB after 1M-row insert and group by"): m3,
            ("memory", "RSS MB after sleep (server stopped)"): m4,
        }
    )
    print(
        f"import chdb {results[('clickhouse', 'import chdb s')]:.2f}s; first statement with the server down "
        f"{cold * 1000:.0f} ms; second database while up {second * 1000:.1f} ms; wake after sleep "
        f"{wake * 1000:.0f} ms",
        flush=True,
    )
    print(
        f"RSS MB: before import {m0:.0f}, after import {m1:.0f}, server up {m2:.0f}, "
        f"after 1M insert + group by {m3:.0f}, after sleep {m4:.0f}",
        flush=True,
    )


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--rows", type=int, default=1_000_000)
    p.add_argument("--big", action="store_true", help="also 10x --rows for the analytical reads")
    args = p.parse_args()
    root = Path(tempfile.mkdtemp(prefix="dblayer-chbench-"))
    results: dict = {}
    try:
        section("lifecycle and memory")
        await lifecycle(root, results)
        section(f"analytical reads and point reads, {args.rows:,} rows")
        await analytics(root, args.rows, "a", results)
        if args.big:
            section(f"analytical reads and point reads, {args.rows * 10:,} rows")
            await analytics(root, args.rows * 10, "b", results)
        section(f"insert {args.rows:,} rows")
        await inserts(root, args.rows, results)
        section(f"export {args.rows:,} rows")
        await exports(root, args.rows, results)
        print(f"\nload average at the end: {load()}")
    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    asyncio.run(main())
