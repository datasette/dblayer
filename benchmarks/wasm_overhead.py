"""What running SQLite in WebAssembly (``SqliteWasm``) costs, against
in-process SQLite, the Python worker and the C worker.

    make -C cworker && make -C cworker wasm
    uv run --all-extras python benchmarks/wasm_overhead.py [--quick] [--json out.json]

Measures:

* module compile time (once per process), and loading the compiled module
  that ``SqliteWasm`` caches next to the ``.wasm`` file;
* a new database to its first result (instantiation for wasm, a process
  start for the workers, a connect in-process);
* one-row read round trip, one-row update;
* per-row cost of reads of 1,000 and 100,000 rows (``execute``);
* ``execute_many`` of 100,000 rows in one transaction;
* memory per open connection: growth of this process's footprint (macOS
  ``phys_footprint``, Linux PSS) over 200 open read connections on 200
  databases; for the workers, the children's footprint;
* the cost of CPU interruption: a CPU-bound statement under no
  interruption, epoch interruption (what the backend uses, with the 2 ms
  ticker running) and fuel metering.

Prints one table with the load average at the start and end. Figures from a
loaded machine are provisional; compare columns from one run.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import worker_overhead as wo  # noqa: E402
from duckdb_memory import mem_of  # noqa: E402

from dblayer import Database  # noqa: E402
from dblayer.backends.sqlite import SqliteInProcess  # noqa: E402

VARIANTS = ["in-process", "apsw", "apsw-inline", "worker-msgpack", "cworker-msgpack", "cpool-msgpack", "wasm"]
"""``cpool-msgpack`` is the multi-tenant C worker pool (``SqliteCPool``), as
in ``worker_overhead.py``; apsw variants are measured when apsw is installed."""


def wasm_available() -> bool:
    from dblayer.backends.sqlite_wasm import module_unavailable

    return module_unavailable() is None


_base_make = wo.make_backend


def make_backend(variant: str, path: Path, **kw: Any) -> Any:
    if variant == "wasm":
        from dblayer.backends.sqlite_wasm import SqliteWasm

        return SqliteWasm(path, **kw)
    return _base_make(variant, path, **kw)


wo.make_backend = make_backend  # the worker_overhead benchmarks now know "wasm"


def load_average() -> str:
    try:
        return "/".join(f"{x:.1f}" for x in os.getloadavg())
    except OSError:
        return "?"


# -- module compile ------------------------------------------------------------------

COMPILE_PROBE = """
import json, sys, time
t0 = time.perf_counter()
import wasmtime
from dblayer.backends.sqlite_wasm import _runtime
t1 = time.perf_counter()
rt = _runtime(sys.argv[1])
print(json.dumps({"import_s": t1 - t0, "compile_s": rt.compile_s, "source": rt.module_source}))
"""


def bench_compile(repeat: int) -> dict[str, float]:
    from dblayer.backends.sqlite_wasm import find_module

    src = find_module()
    cold, cached = [], []
    for _ in range(repeat):
        with tempfile.TemporaryDirectory() as tmp:
            copy = Path(tmp) / "m.wasm"
            shutil.copyfile(src, copy)
            for into in (cold, cached):
                out = subprocess.run(
                    [sys.executable, "-c", COMPILE_PROBE, str(copy)],
                    capture_output=True, text=True, check=True, cwd=Path(__file__).resolve().parent.parent,
                )
                r = json.loads(out.stdout.strip().splitlines()[-1])
                into.append(r)
    assert all(r["source"] == "compiled" for r in cold) and all(r["source"] == "cache" for r in cached)
    return {
        "compile_ms": statistics.median(r["compile_s"] for r in cold) * 1e3,
        "cached_load_ms": statistics.median(r["compile_s"] for r in cached) * 1e3,
        "import_wasmtime_ms": statistics.median(r["import_s"] for r in cold) * 1e3,
        "module_kb": os.path.getsize(src) / 1024,
    }


# -- first result, rows, writes ----------------------------------------------------------


async def bench_first_result(variant: str, d: Path, repeat: int) -> float:
    times = []
    for i in range(repeat + 2):
        path = d / f"first-{variant}-{i}.db"
        db = Database(make_backend(variant, path))
        t0 = time.perf_counter()
        await db.execute("select 1")
        times.append(time.perf_counter() - t0)
        await db.close()
    wo.reap_workers()
    return statistics.median(times[2:]) * 1e3  # the first two warm imports and the module


async def bench_execute_many(variant: str, d: Path, repeat: int) -> float:
    db = Database(make_backend(variant, d / f"many-{variant}.db"))
    try:
        await db.execute_write("create table t (id integer primary key, title text, score real)")
        rows = [{"title": f"title {i}", "score": i * 1.5} for i in range(100_000)]
        out = []
        for _ in range(repeat):
            await db.execute_write("delete from t")
            t0 = time.perf_counter()
            async with db.transaction() as tx:
                await tx.execute_many("insert into t (title, score) values (:title, :score)", rows)
            out.append(time.perf_counter() - t0)
    finally:
        await db.close()
    return statistics.median(out) * 1e3


MEMORY_PROBE = """
import asyncio, json, os, sys, gc
sys.path.insert(0, "benchmarks")
import wasm_overhead as wb
from duckdb_memory import mem_of

async def main(variant, d, n):
    gc.collect()
    before = mem_of(os.getpid())[1]
    conns, backends = [], []
    for i in range(n):
        b = wb.make_backend(variant, os.path.join(d, f"mem-{variant}-{i}.db"))
        w = await b.open(write=True)
        await w.execute("create table t (id integer primary key, v text)", {}, time_limit_ms=None, max_rows=None)
        await w.close()
        c = await b.open(write=False)
        await c.execute("select * from t", {}, time_limit_ms=None, max_rows=None)
        conns.append(c); backends.append(b)
    await asyncio.sleep(0.3)
    gc.collect()
    after = mem_of(os.getpid())[1]
    children = 0
    for c in conns:
        pid = getattr(c, "pid", None)
        if pid:
            m = mem_of(pid)
            children += m[1] if m else 0
    if not children:  # pooled workers (cpool): every worker process this one started
        from dblayer.backends.sqlite_worker import live_worker_pids
        for pid in live_worker_pids():
            m = mem_of(pid)
            children += m[1] if m else 0
    print(json.dumps({"parent_kb_per_conn": (after - before) / n / 1024, "child_kb_per_conn": children / n / 1024}))
    for c in conns:
        await c.close()
    for b in backends:
        await b.close()
    import cpool_bench
    cpool_bench.close_pools()
    wb.wo.reap_workers()

asyncio.run(main(sys.argv[1], sys.argv[2], int(sys.argv[3])))
"""


def bench_memory(variant: str, d: Path, n: int) -> dict[str, float]:
    out = subprocess.run(
        [sys.executable, "-c", MEMORY_PROBE, variant, str(d), str(n)],
        capture_output=True, text=True, check=True, cwd=Path(__file__).resolve().parent.parent,
    )
    return json.loads(out.stdout.strip().splitlines()[-1])


# -- interruption: none, epoch, fuel -------------------------------------------------------

CPU_SQL = {
    "recursive_cte_1m": "with recursive c(x) as (select 1 union all select x + 1 from c where x < 1000000) select sum(x) from c",
    "sort_200k": "with recursive c(x) as (select 1 union all select x + 1 from c where x < 200000) "
    "select count(*) from (select x from c order by (x * 7919) % 200003)",
    "instr_quadratic": "select instr(printf('%.*c', 400000, 'a'), printf('%.*c', 20000, 'a') || 'b')",
}


def bench_interruption(d: Path, repeat: int) -> dict[str, dict[str, float]]:
    import sqlite3

    from dblayer.backends import sqlite_wasm as sw
    from dblayer.worker.protocol import get_codec

    path = str(d / "cpu.db")
    sqlite3.connect(path).close()
    out: dict[str, dict[str, float]] = {}
    native = sqlite3.connect(path)
    for name, sql in CPU_SQL.items():
        ts = []
        for _ in range(repeat):
            t0 = time.perf_counter()
            native.execute(sql).fetchall()
            ts.append(time.perf_counter() - t0)
        out.setdefault(name, {})["native_ms"] = statistics.median(ts) * 1e3
    native.close()
    mp, js = get_codec("msgpack"), get_codec("json")
    for mode in ("none", "epoch", "fuel"):
        rt = sw._runtime(sw.find_module(), mode)
        inst = sw._Instance(rt, path, None)
        inst.call(js.encode({"type": "hello", "protocol": 1, "codecs": ["msgpack"], "id": 1}))
        inst.call(mp.encode({"type": "open", "path": path, "mode": "read", "id": 2}))
        for name, sql in CPU_SQL.items():
            payload = mp.encode({"type": "execute", "sql": sql, "id": 3})
            ts = []
            for _ in range(repeat):
                rt.enter()
                try:
                    t0 = time.perf_counter()
                    reply = inst.call(payload)
                    ts.append(time.perf_counter() - t0)
                finally:
                    rt.leave()
                assert b"error" not in reply, reply
            out[name][f"{mode}_ms"] = statistics.median(ts) * 1e3
        inst.close()
    return out


# -- the host VFS: what a page read through Python costs ---------------------------------

VFS_PAGES = 4000


def bench_vfs(d: Path, repeat: int) -> dict[str, float]:
    """A full scan of a ~16 MB table (4 KB pages, in the OS page cache)
    in one instance, twice: with ``cache_size = 10`` every page is read
    through the host (``xRead`` -> Python ``os.pread`` -> copy into linear
    memory), with a warm 64 MB page cache none is. The difference over the
    number of host reads is the per-page cost of the VFS callback. Also
    counts the host calls a one-row read on a warm connection makes."""
    import sqlite3

    from dblayer.backends import sqlite_wasm as sw
    from dblayer.worker.protocol import get_codec

    path = str(d / "vfs.db")
    conn = sqlite3.connect(path)
    conn.execute("create table t (id integer primary key, pad blob)")
    conn.executemany("insert into t (pad) values (randomblob(3500))", [()] * VFS_PAGES)
    conn.commit()
    conn.close()
    mp, js = get_codec("msgpack"), get_codec("json")
    rt = sw._runtime(sw.find_module())
    inst = sw._Instance(rt, path, None)
    calls: dict[str, int] = {}

    def counting(name: str) -> None:
        orig = getattr(inst.files, name)

        def wrapper(*a: Any) -> Any:
            calls[name] = calls.get(name, 0) + 1
            return orig(*a)

        setattr(inst.files, name, wrapper)

    def run(sql: str) -> float:
        rt.enter()
        try:
            t0 = time.perf_counter()
            reply = inst.call(mp.encode({"type": "execute", "sql": sql, "id": 3}))
            dt = time.perf_counter() - t0
        finally:
            rt.leave()
        assert b"error" not in reply, reply
        return dt

    try:
        inst.call(js.encode({"type": "hello", "protocol": 1, "codecs": ["msgpack"], "id": 1}))
        inst.call(mp.encode({"type": "open", "path": path, "mode": "read", "id": 2}))
        scan = "select sum(length(pad)) from t"
        run("pragma cache_size = 10")
        run(scan)
        cold = statistics.median(run(scan) for _ in range(repeat))
        run("pragma cache_size = -65536")
        run(scan)
        warm = statistics.median(run(scan) for _ in range(repeat))
        run("pragma cache_size = 10")
        for name in sw._HOST_IMPORTS:
            counting(sw._METHOD.get(name, name))
        run(scan)
        reads = calls.get("read", 0)
        run("pragma cache_size = -65536")
        run(scan)
        calls.clear()
        run("select id from t where id = 7")
        one_row = dict(calls)
    finally:
        inst.close()
    native = sqlite3.connect(path)
    native.execute("pragma cache_size = 10")
    native.execute(scan).fetchall()
    ts = []
    for _ in range(repeat):
        t0 = time.perf_counter()
        native.execute(scan).fetchall()
        ts.append(time.perf_counter() - t0)
    native.close()
    return {
        "pages_read": reads,
        "scan_cold_ms": cold * 1e3,
        "scan_warm_ms": warm * 1e3,
        "native_scan_cold_ms": statistics.median(ts) * 1e3,
        "per_page_read_us": (cold - warm) / max(reads, 1) * 1e6,
        "one_row_host_calls": sum(one_row.values()),
        "one_row_host_calls_by_kind": one_row,  # type: ignore[dict-item]
    }


# -- all -----------------------------------------------------------------------------------


async def run_all(quick: bool, only: list[str] | None = None) -> dict[str, Any]:
    n = 500 if quick else 3000
    rep = 3 if quick else 7
    variants = [
        v for v in VARIANTS
        if (not v.startswith(("cworker", "cpool")) or wo.cworker_available())
        and (not v.startswith("apsw") or wo.apsw_available())
        and (v != "wasm" or wasm_available())
        and (only is None or v in only)
    ]
    r: dict[str, Any] = {"load_start": load_average(), "variants": variants, "per": {}}
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        if wasm_available():
            r["compile"] = bench_compile(2 if quick else 3)
            r["interruption"] = bench_interruption(d, rep)
            r["vfs"] = bench_vfs(d, rep)
        for v in variants:
            per: dict[str, Any] = {}
            per["first_result_ms"] = await bench_first_result(v, d, rep)
            per.update(await wo.bench_round_trip(v, d, n))
            for rows in (1000, 100_000):
                x = await wo.bench_rows(v, d, rows, rep if rows == 1000 else max(rep // 2, 2))
                per[f"rows_{rows}_us_per_row"] = x["execute_us_per_row"]
                per[f"rows_{rows}_stream_us_per_row"] = x["stream_us_per_row"]
            per["execute_many_100k_ms"] = await bench_execute_many(v, d, max(rep // 2, 2))
            per.update(bench_memory(v, d, 50 if quick else 200))
            import cpool_bench

            cpool_bench.close_pools()
            wo.reap_workers()
            r["per"][v] = per
    r["load_end"] = load_average()
    return r


def print_table(r: dict[str, Any]) -> None:
    print(f"load average at start {r['load_start']}, at end {r['load_end']} (provisional if high)")
    if "compile" in r:
        c = r["compile"]
        print(
            f"module: {c['module_kb']:.0f} KB; compile {c['compile_ms']:.0f} ms; load cached compilation "
            f"{c['cached_load_ms']:.1f} ms; import wasmtime {c['import_wasmtime_ms']:.0f} ms"
        )
    rows = [
        ("New database to first result (ms)", "first_result_ms", "{:.2f}"),
        ("One-row read, median (us)", "read_us", "{:.0f}"),
        ("One-row read, p90 (us)", "read_p90_us", "{:.0f}"),
        ("One-row update (us)", "write_us", "{:.0f}"),
        ("Per row, 1,000-row read (us)", "rows_1000_us_per_row", "{:.2f}"),
        ("Per row, 100,000-row read (us)", "rows_100000_us_per_row", "{:.2f}"),
        ("Per row, 100,000-row stream (us)", "rows_100000_stream_us_per_row", "{:.2f}"),
        ("execute_many, 100,000 rows (ms)", "execute_many_100k_ms", "{:.0f}"),
        ("Memory per open connection, this process (KB)", "parent_kb_per_conn", "{:.0f}"),
        ("Memory per open connection, worker process (KB)", "child_kb_per_conn", "{:.0f}"),
    ]
    vs = r["variants"]
    print("| Measure | " + " | ".join(vs) + " |")
    print("| --- |" + " --- |" * len(vs))
    for label, key, fmt in rows:
        print(f"| {label} | " + " | ".join(fmt.format(r["per"][v][key]) for v in vs) + " |")
    if "vfs" in r:
        x = r["vfs"]
        print(
            f"host VFS: full scan of {x['pages_read']} pages through the host {x['scan_cold_ms']:.1f} ms, "
            f"from a warm page cache {x['scan_warm_ms']:.1f} ms (native sqlite3, cache_size 10: "
            f"{x['native_scan_cold_ms']:.1f} ms): {x['per_page_read_us']:.2f} us per page read; "
            f"a one-row read makes {x['one_row_host_calls']} host calls {x['one_row_host_calls_by_kind']}"
        )
    if "interruption" in r:
        print("\n| CPU-bound statement | native sqlite3 (ms) | wasm, no interruption | wasm, epoch | wasm, fuel |")
        print("| --- | --- | --- | --- | --- |")
        for name, x in r["interruption"].items():
            print(f"| {name} | {x['native_ms']:.0f} | {x['none_ms']:.0f} | {x['epoch_ms']:.0f} | {x['fuel_ms']:.0f} |")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--json")
    ap.add_argument("--variants", help="comma-separated subset of " + ",".join(VARIANTS))
    args = ap.parse_args()
    r = asyncio.run(run_all(args.quick, args.variants.split(",") if args.variants else None))
    print_table(r)
    if args.json:
        Path(args.json).write_text(json.dumps(r, indent=2))


if __name__ == "__main__":
    main()
