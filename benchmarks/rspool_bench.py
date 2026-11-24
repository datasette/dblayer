"""The multi-tenant C worker against its Rust port (``sqlite-cpool`` against
``sqlite-rspool``), side by side in one session, alternating runs::

    uv run --all-extras python benchmarks/rspool_bench.py [--quick] [--rounds N] [--json out.json]

Both pools are the same Python code (``sqlite_cpool``); only the worker
executable differs, chosen through ``$DBLAYER_CWORKER`` for each run, so
``cpool_bench``'s measurements serve both. Per round, for each
implementation in alternating order:

* open to first result of a new database on a warm pool, and the bare
  ``open`` round trip;
* one-row read and one-row update round trips (medians);
* per-row cost of 1,000- and 100,000-row results, ``execute`` and
  ``stream``, MessagePack and JSON;
* throughput at 1, 4, 16 and 64 clients on different databases, with the
  CPU of the parent and of the worker processes;
* footprint of a process with nothing open (after ``hello``), and per open
  database.

Medians over rounds are printed with the load average of each round. The
throughput figures share one Python parent, which is the limit at high
concurrency; ``sqlite-rs-worker/examples/loadgen.rs`` drives the workers
from Rust instead (run separately; see the notes).
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

import cpool_bench  # noqa: E402

from dblayer import Database  # noqa: E402
from dblayer.backends.sqlite import SqliteInProcess  # noqa: E402
from dblayer.backends.sqlite_worker import reap_workers  # noqa: E402


def binaries() -> dict[str, str]:
    from dblayer.backends import sqlite_cworker, sqlite_rspool

    return {"c": sqlite_cworker.find_binary(), "rust": sqlite_rspool.find_binary()}


def use(path: str) -> None:
    os.environ["DBLAYER_CWORKER"] = path
    cpool_bench.close_pools()


async def seed_rows(path: Path, n: int) -> None:
    db = Database(SqliteInProcess(path))
    await db.execute_write("create table if not exists big (id integer primary key, title text, score real, n integer)")
    await db.execute_write(
        "insert into big (title, score, n) select 'row title ' || i, i * 0.5, i * 7 from "
        f"(with recursive c(i) as (select 1 union all select i + 1 from c where i < {n}) select i from c)"
    )
    await db.close()


def median_time(times: list[float]) -> float:
    return statistics.median(times)


async def round_trips(root: Path, repeat: int) -> dict[str, float]:
    from dblayer.backends.sqlite_cpool import CPool, SqliteCPool

    pool = CPool(1, codec="msgpack", linger=60)
    p = root / "rt.db"
    await cpool_bench._seed(p)
    db = Database(SqliteCPool(p, pool=pool))
    try:
        for _ in range(50):
            await db.execute("select id, title from r where id = :id", {"id": 7})
            await db.execute_write("update r set title = 'x' where id = :id", {"id": 3})
        reads, writes = [], []
        for i in range(repeat):
            t0 = time.perf_counter()
            await db.execute("select id, title from r where id = :id", {"id": 7})
            reads.append(time.perf_counter() - t0)
            t0 = time.perf_counter()
            await db.execute_write("update r set title = :t where id = :id", {"id": 3, "t": f"t{i}"})
            writes.append(time.perf_counter() - t0)
        return {"read_us": median_time(reads) * 1e6, "update_us": median_time(writes) * 1e6}
    finally:
        await db.close()
        pool.close()


async def per_row(root: Path, big: Path, codec: str, repeat: int) -> dict[str, float]:
    from dblayer.backends.sqlite_cpool import CPool, SqliteCPool

    pool = CPool(1, codec=codec, linger=60)
    db = Database(SqliteCPool(big, pool=pool, immutable=True))
    out: dict[str, float] = {}
    try:
        await db.execute("select 1")
        for n in (1000, 100_000):
            sql = f"select id, title, score, n from big limit {n}"
            ex, st = [], []
            for _ in range(repeat if n == 1000 else max(3, repeat // 10)):
                t0 = time.perf_counter()
                r = await db.execute(sql, max_rows=None)
                ex.append(time.perf_counter() - t0)
                assert len(r.rows) == n
                t0 = time.perf_counter()
                got = 0
                async with db.stream(sql, chunk_rows=1000) as s:
                    async for chunk in s:
                        got += len(chunk)
                st.append(time.perf_counter() - t0)
                assert got == n
            out[f"execute_{n}_us_per_row"] = median_time(ex) / n * 1e6
            out[f"stream_{n}_us_per_row"] = median_time(st) / n * 1e6
        return out
    finally:
        await db.close()
        pool.close()


def process_base(binary: str) -> dict[str, float]:
    """Footprint of a --multi process after hello, nothing open."""
    from duckdb_memory import mem_of

    p = subprocess.Popen([binary, "--multi"], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    try:
        hello = json.dumps({"type": "hello", "protocol": 1, "codecs": ["json"], "id": 1}).encode()
        assert p.stdin and p.stdout
        p.stdin.write(len(hello).to_bytes(4, "big") + hello)
        p.stdin.flush()
        n = int.from_bytes(p.stdout.read(4), "big")
        p.stdout.read(n)
        time.sleep(0.2)
        m = mem_of(p.pid)
        assert m is not None
        return {"base_rss_kb": m[0] / 1024, "base_footprint_kb": m[1] / 1024}
    finally:
        assert p.stdin
        p.stdin.close()
        p.wait(10)


async def one_round(impl: str, binary: str, root: Path, big: Path, quick: bool) -> dict[str, Any]:
    use(binary)
    r: dict[str, Any] = {"load": os.getloadavg()[0]}
    sub = root / f"w-{impl}-{time.monotonic_ns()}"
    sub.mkdir()
    r["warm_open"] = await cpool_bench.warm_open(sub, 10 if quick else 30)
    r["round_trip"] = await round_trips(root, 300 if quick else 2000)
    r["per_row_msgpack"] = await per_row(root, big, "msgpack", 5 if quick else 20)
    r["per_row_json"] = await per_row(root, big, "json", 5 if quick else 20)
    r["base"] = process_base(binary)
    r["memory"] = await cpool_bench.memory_per_open(root, 100 if quick else 400)
    r["scaling"] = {}
    for clients in (1, 4, 16, 64):
        r["scaling"][clients] = await cpool_bench.throughput("cpool", root, clients, 1.0 if quick else 2.5)
        cpool_bench.close_pools()
        reap_workers(5)
    return r


def _get(d: dict[str, Any], path: str) -> float:
    for k in path.split("."):
        d = d[int(k)] if k.isdigit() else d[k]
    return float(d)  # type: ignore[arg-type]


ROWS = [
    ("Warm open to first result (ms)", "warm_open.first_result_ms"),
    ("Bare open round trip (ms)", "warm_open.open_round_trip_ms"),
    ("One-row read (µs)", "round_trip.read_us"),
    ("One-row update (µs)", "round_trip.update_us"),
    ("execute 1,000 rows, msgpack (µs/row)", "per_row_msgpack.execute_1000_us_per_row"),
    ("execute 100,000 rows, msgpack (µs/row)", "per_row_msgpack.execute_100000_us_per_row"),
    ("stream 1,000 rows, msgpack (µs/row)", "per_row_msgpack.stream_1000_us_per_row"),
    ("stream 100,000 rows, msgpack (µs/row)", "per_row_msgpack.stream_100000_us_per_row"),
    ("execute 1,000 rows, JSON (µs/row)", "per_row_json.execute_1000_us_per_row"),
    ("execute 100,000 rows, JSON (µs/row)", "per_row_json.execute_100000_us_per_row"),
    ("stream 1,000 rows, JSON (µs/row)", "per_row_json.stream_1000_us_per_row"),
    ("stream 100,000 rows, JSON (µs/row)", "per_row_json.stream_100000_us_per_row"),
    ("Process base footprint (KB)", "base.base_footprint_kb"),
    ("Process base RSS (KB)", "base.base_rss_kb"),
    ("Footprint per open database (KB)", "memory.footprint_kb_per_open"),
] + [
    (f"{c} clients: {label}", f"scaling.{c}.{key}")
    for c in (1, 4, 16, 64)
    for label, key in (("requests/s", "rps"), ("parent CPU (cores)", "parent_cpu_share"), ("worker CPU (cores)", "worker_cpu_cores"))
]


async def main_async(rounds: int, quick: bool) -> dict[str, Any]:
    bins = binaries()
    results: dict[str, list[dict[str, Any]]] = {"c": [], "rust": []}
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        big = root / "big.db"
        await seed_rows(big, 100_000)
        for i in range(rounds):
            order = ["c", "rust"] if i % 2 == 0 else ["rust", "c"]
            for impl in order:
                results[impl].append(await one_round(impl, bins[impl], root, big, quick))
                print(f"round {i + 1} {impl}: load {results[impl][-1]['load']:.1f}", file=sys.stderr)
    cpool_bench.close_pools()
    reap_workers(5)
    return results


def print_table(results: dict[str, list[dict[str, Any]]]) -> None:
    loads = {k: [r["load"] for r in v] for k, v in results.items()}
    print(f"\nLoad average (1 min) per round: C {loads['c']}, Rust {loads['rust']}")
    print("\n| Measure (median over rounds) | C worker | Rust worker | Rust / C |\n| --- | --- | --- | --- |")
    for label, path in ROWS:
        c = statistics.median(_get(r, path) for r in results["c"])
        rs = statistics.median(_get(r, path) for r in results["rust"])
        ratio = rs / c if c else float("nan")
        print(f"| {label} | {c:,.3g} | {rs:,.3g} | {ratio:.2f} |")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--quick", action="store_true")
    p.add_argument("--rounds", type=int, default=4)
    p.add_argument("--json", type=Path)
    a = p.parse_args()
    results = asyncio.run(main_async(a.rounds, a.quick))
    print_table(results)
    if a.json:
        a.json.write_text(json.dumps(results, indent=1))


if __name__ == "__main__":
    main()
