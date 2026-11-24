"""What a PostgreSQL per database with no server costs: the PGlite worker
(``PGliteWorker``) against the psycopg backend on the local server and the
SQLite Node and C workers.

Run with ``uv run --all-extras python benchmarks/pglite_bench.py``
(``--only pglite,psycopg``, ``--awake 20``, ``--rows 100000``). Build the
comparisons first: ``npm ci --prefix pglite-worker``, ``npm ci --prefix
nodeworker``, ``make -C cworker``.

Measures, per backend: creating a new database (to the first result),
asleep to first result (with the PGlite worker's own breakdown: Node start,
WebAssembly compile, PGlite start and recovery, first query), a one-row
read round trip, reads of 1,000 and 100,000 rows, inserting 100,000 rows
(``execute_many`` in one transaction, and ``insert_rows``: COPY on PGlite
and psycopg), a small write transaction, sleep (close), the footprint of
one awake database and of ``--awake`` awake databases, and the disk size of
an empty database; for PGlite also what a time-limit kill costs and the
start-up options for a new database (initdb, a template directory copied,
PGlite's data-dir tarball). The server backend connects only to ``dblayer_test``, in
a schema of its own that it drops at the end; its memory is the server's
and is not measured.

Other work may be running on the machine: the load average is printed with
the results, and every figure is provisional."""

from __future__ import annotations

import argparse
import asyncio
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
import urllib.parse
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
from duckdb_memory import mem_of  # noqa: E402

from dblayer import Database  # noqa: E402

DSN = os.environ.get("DBLAYER_TEST_POSTGRES", "postgresql:///dblayer_test?host=/tmp")


def load() -> str:
    out = subprocess.run(["uptime"], capture_output=True, text=True).stdout.strip()
    return out.split("load average")[-1].lstrip("s").strip(": ")


def du(path: Path) -> int:
    if path.is_file():
        return path.stat().st_size
    total = 0
    for root, _dirs, files in os.walk(path):
        for f in files:
            try:
                total += os.lstat(os.path.join(root, f)).st_size
            except OSError:
                pass
    return total


class Kind:
    def __init__(self, name: str, make: Callable[[Path], Any], file: bool, pid: Callable[[Any], int | None] | None):
        self.name = name
        self.make = make
        self.file = file
        self.pid = pid


def kinds(base: Path, template: Path | None) -> dict[str, Kind]:
    out: dict[str, Kind] = {}
    from dblayer.backends.pglite_worker import PGliteWorker, unavailable

    def pglite_pid(b: Any) -> int | None:
        return b.pid

    if unavailable() is None:
        out["pglite"] = Kind("pglite", lambda p: PGliteWorker(p, template=template), True, pglite_pid)
        out["pglite-initdb"] = Kind("pglite-initdb", lambda p: PGliteWorker(p), True, pglite_pid)
        out["pglite-turbofan"] = Kind("pglite-turbofan", lambda p: PGliteWorker(p, template=template, optimize=True),
                                      True, pglite_pid)
    try:
        from dblayer.backends.sqlite_nodeworker import SqliteNodeWorker
        from dblayer.backends.sqlite_nodeworker import unavailable as node_unavailable

        if node_unavailable() is None:
            out["sqlite-nodeworker"] = Kind(
                "sqlite-nodeworker", lambda p: SqliteNodeWorker(p.with_suffix(".db")), True,
                lambda b: b.process.pid if b.process is not None else None,
            )
    except ImportError:
        pass
    from dblayer.backends.sqlite_cworker import SqliteCWorker, binary_unavailable

    if binary_unavailable() is None:
        out["sqlite-cworker"] = Kind("sqlite-cworker", lambda p: SqliteCWorker(p.with_suffix(".db")), True, None)
    try:
        import psycopg  # noqa: F401

        from dblayer.backends.postgres_psycopg import PostgresPsycopg

        if urllib.parse.urlsplit(DSN).path.lstrip("/").split("?")[0] == "dblayer_test" or "dbname=dblayer_test" in DSN:
            out["psycopg"] = Kind(
                "psycopg", lambda p: PostgresPsycopg(DSN, schema=f"dblbench_{os.getpid()}_{uuid.uuid4().hex[:6]}"),
                False, None,
            )
    except ImportError:
        pass
    return out


async def timed(fn: Callable[[], Any], n: int) -> float:
    """Median ms of n calls."""
    times = []
    for _ in range(n):
        t0 = time.perf_counter()
        await fn()
        times.append((time.perf_counter() - t0) * 1000)
    return statistics.median(times)


def footprint_mb(before: set[int]) -> float | None:
    """Total footprint (macOS phys_footprint, Linux PSS) of the worker
    processes started since ``before`` was taken."""
    from dblayer.backends.sqlite_worker import live_worker_pids

    pids = [p for p in live_worker_pids() if p not in before]
    if not pids:
        return None
    total = 0
    for pid in pids:
        m = mem_of(pid)
        total += m[1] if m else 0
    return total / 1e6


def live() -> set[int]:
    from dblayer.backends.sqlite_worker import live_worker_pids

    return set(live_worker_pids())


async def measure(kind: Kind, base: Path, rows: int) -> dict[str, Any]:
    r: dict[str, Any] = {}
    creates = []
    for i in range(3):
        b = kind.make(base / f"{kind.name}-new{i}")
        d = Database(b)
        t0 = time.perf_counter()
        await d.execute("select 1")
        creates.append((time.perf_counter() - t0) * 1000)
        await d.close()
    r["create_ms"] = statistics.median(creates)
    path = base / f"{kind.name}-main"
    backend = kind.make(path)
    db = Database(backend)
    schemas = [getattr(backend, "schema", None)]
    before = live()
    await db.execute("select 1")
    r["idle_floor_mb"] = footprint_mb(before)
    if kind.file:
        await db.sleep()
        r["empty_disk_mb"] = du(path if path.is_dir() else path.with_suffix(".db")) / 1e6
    # asleep -> first result, three times
    wakes = []
    breakdown: dict[str, float] = {}
    for _ in range(3):
        await db.sleep()
        t0 = time.perf_counter()
        await db.execute("select 1")
        wakes.append((time.perf_counter() - t0) * 1000)
        proc = getattr(backend, "process", None)
        if proc is not None and getattr(proc, "timings", None):
            breakdown = dict(proc.timings)
            breakdown["spawn_to_hello_ms"] = backend.placement_group.last_start_ms
    r["wake_ms"] = statistics.median(wakes)
    r["wake_breakdown"] = breakdown
    await db.execute_write("create table t (id bigint primary key, name text, val double precision)")
    r["read_1_ms"] = await timed(lambda: db.execute("select :i as x", {"i": 1}), 300)
    # insert
    data = [(i, f"name {i}", i * 0.5) for i in range(rows)]
    t0 = time.perf_counter()
    async with db.transaction(deadline_ms=None) as tx:
        await tx.execute_many("insert into t (id, name, val) values (:a, :b, :c)",
                              [{"a": a, "b": b, "c": c} for a, b, c in data])
    r["insert_many_ms"] = (time.perf_counter() - t0) * 1000
    await db.execute_write("create table u (id bigint primary key, name text, val double precision)")
    t0 = time.perf_counter()
    async with db.transaction(deadline_ms=None) as tx:
        await tx.insert_rows("u", [{"id": a, "name": b, "val": c} for a, b, c in data])
    r["insert_rows_ms"] = (time.perf_counter() - t0) * 1000
    r["read_1k_ms"] = await timed(lambda: db.execute("select * from t where id < 1000 order by id", max_rows=None), 20)
    r["read_100k_ms"] = await timed(lambda: db.execute(f"select * from t where id < {min(rows, 100000)}", max_rows=None), 3)

    async def small_tx() -> None:
        async with db.transaction() as tx:
            await tx.execute("update t set val = val + 1 where id = 1")
            await tx.execute("insert into u values (:i, 'x', 1) on conflict (id) do nothing", {"i": rows + 1})

    r["small_tx_ms"] = await timed(small_tx, 50)
    await db.execute("select count(*) from t")
    r["awake_footprint_mb"] = footprint_mb(before)
    r["disk_mb_200k_rows"] = du(path if path.is_dir() else path.with_suffix(".db")) / 1e6 if kind.file else None
    t0 = time.perf_counter()
    await db.sleep()
    r["sleep_ms"] = (time.perf_counter() - t0) * 1000
    await db.close()
    if kind.name == "psycopg":
        await drop([s for s in schemas if s])
    return r


async def awake_many(kind: Kind, base: Path, n: int) -> float | None:
    if not kind.file:
        return None
    before = live()
    dbs = [Database(kind.make(base / f"{kind.name}-awake{i}")) for i in range(n)]
    try:
        for db in dbs:
            await db.execute_write("create table if not exists t (id bigint primary key, s text)")
            await db.execute_write("insert into t values (1, 'x') on conflict do nothing")
            await db.execute("select * from t")
        return footprint_mb(before)
    finally:
        for db in dbs:
            await db.close()


async def kill_cost(kind: Kind, base: Path) -> dict[str, float]:
    """What a time limit costs on PGlite: nothing can stop a running query,
    so the process is killed at the limit plus the grace; a read queued
    behind it on another connection runs on the next process."""
    db = Database(kind.make(base / f"{kind.name}-kill"))
    out: dict[str, float] = {}
    try:
        await db.execute_write("create table t (id bigint)")
        endless = "select count(*) from generate_series(1, 10000000000)"
        t0 = time.perf_counter()
        slow = asyncio.create_task(db.execute(endless, time_limit_ms=300))
        await asyncio.sleep(0.1)
        t1 = time.perf_counter()
        got = await asyncio.gather(db.execute("select 1"), slow, return_exceptions=True)
        out["bystander_read_ms"] = (time.perf_counter() - t1) * 1000
        out["time_limit_300_raised_after_ms"] = (time.perf_counter() - t0) * 1000
        assert not isinstance(got[0], BaseException), got[0]
        await db.sleep()
        t0 = time.perf_counter()
        await db.execute("select 1")
        out["wake_after_kill_ms"] = (time.perf_counter() - t0) * 1000
    finally:
        await db.close()
    return out


def startup_options(base: Path, liftoff: bool) -> dict[str, Any]:
    """``pglite-worker/scripts/startup_options.mjs``: initdb, template copy
    and PGlite's data-dir tarball, timed inside one Node process."""
    import json

    from dblayer.backends.pglite_worker import find_node, find_script

    script = Path(find_script()).parent.parent / "scripts" / "startup_options.mjs"
    target = base / f"opts{int(liftoff)}"
    args = [find_node(), *(["--liftoff-only"] if liftoff else []), str(script), str(target), "3"]
    out = subprocess.run(args, check=True, capture_output=True, text=True, timeout=600).stdout
    shutil.rmtree(target, ignore_errors=True)
    return json.loads(out.strip().splitlines()[-1])


async def drop(schemas: list[str]) -> None:
    import psycopg

    async with await psycopg.AsyncConnection.connect(DSN, autocommit=True) as conn:
        for s in schemas:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{s}" CASCADE')


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default="")
    ap.add_argument("--rows", type=int, default=100_000)
    ap.add_argument("--awake", type=int, default=20)
    args = ap.parse_args()
    base = Path(tempfile.mkdtemp(prefix="dblayer-pglite-bench-"))
    try:
        template = None
        from dblayer.backends.pglite_worker import find_node, find_script

        script = Path(find_script()).parent.parent / "scripts" / "init_template.mjs"
        template = base / "template"
        subprocess.run([find_node(), str(script), str(template)], check=True, capture_output=True)
        ks = kinds(base, template)
        wanted = [k for k in args.only.split(",") if k] or list(ks)
        results: dict[str, dict[str, Any]] = {}
        for name in wanted:
            if name not in ks:
                print(f"{name}: unavailable here")
                continue
            print(f"# {name} (load average {load()})", flush=True)
            res = await measure(ks[name], base, args.rows)
            if name != "pglite-initdb":
                res["awake_n_footprint_mb"] = await awake_many(ks[name], base, args.awake)
            if name == "pglite":
                res["kill"] = await kill_cost(ks[name], base)
            res["load"] = load()
            results[name] = res
            for k, v in res.items():
                print(f"  {k}: {v if not isinstance(v, float) else round(v, 2)}", flush=True)
        print()
        print("| measurement | " + " | ".join(results) + " |")
        print("| --- | " + " | ".join("---" for _ in results) + " |")
        keys = ["create_ms", "wake_ms", "read_1_ms", "read_1k_ms", "read_100k_ms", "insert_many_ms", "insert_rows_ms",
                "small_tx_ms", "sleep_ms", "idle_floor_mb", "awake_footprint_mb", "awake_n_footprint_mb", "empty_disk_mb",
                "disk_mb_200k_rows", "load"]
        for k in keys:
            cells = []
            for res in results.values():
                v = res.get(k)
                cells.append("n/a" if v is None else (f"{v:.2f}" if isinstance(v, float) else str(v)))
            label = k.replace("awake_n", f"awake_{args.awake}")
            print(f"| {label} | " + " | ".join(cells) + " |")
        for name, res in results.items():
            if res.get("wake_breakdown"):
                print(f"\n{name} wake breakdown (ms): {res['wake_breakdown']}")
            if res.get("kill"):
                print(f"{name} time-limit kill: {res['kill']}")
        if "pglite" in wanted and "pglite" in ks:
            for liftoff in (True, False):
                print(f"\nnew-database options in one Node process (liftoff_only={liftoff}, load {load()}):")
                print(f"  {startup_options(base, liftoff)}")
    finally:
        shutil.rmtree(base, ignore_errors=True)


if __name__ == "__main__":
    asyncio.run(main())
