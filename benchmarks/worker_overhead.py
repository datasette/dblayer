"""What the worker process boundary costs, against in-process SQLite.

    uv run --all-extras python benchmarks/worker_overhead.py [--quick] [--json out.json]

Measures (the spec's "Benchmarks" rows that concern the worker):

* round trip of a one-row read: in-process, worker + JSON, worker + MessagePack;
* reads of 1,000 and 100,000 rows, with ``execute`` and with ``stream``;
* worker start to first result, dedicated and shared placement, with a
  breakdown of where a new worker's start-up goes (spawn, hello, open, first
  query) and the resident memory of one idle worker;
* "insert with alter": an interactive transaction (begin, schema, alter,
  execute_many, commit: 5 round trips) against one named operation (begin,
  op, commit: the op itself is one message).

The C worker (``SqliteCWorker``, ``cworker/``) is measured alongside when
its binary is built (``make -C cworker``): variants ``cworker-json`` and
``cworker-msgpack``. The apsw backend (``SqliteApsw``, extra ``apsw``) is
measured when apsw is installed: ``apsw`` (executor) and ``apsw-inline``
(``inline_reads=True``), with an extra table against in-process ``sqlite3``
and the C worker (connection open, ``execute_many`` of 100,000 rows,
memory per open connection). ``--variants a,b`` limits the run.

Prints one table. Every figure is a median over repeats; machine load moves
them a lot, so compare figures from one run. Later benchmark work can import
``run_all()`` and reuse the measurements.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import sys
import tempfile
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dblayer import Database  # noqa: E402
from dblayer.backends.sqlite import SqliteInProcess  # noqa: E402
from dblayer.backends.sqlite_worker import SharedWorkers, SqliteWorker, reap_workers  # noqa: E402
from dblayer.worker.protocol import FrameDecoder, available_codecs, frame, get_codec  # noqa: E402

VARIANTS = [
    "in-process", "apsw", "apsw-inline", "worker-json", "worker-msgpack", "cworker-json", "cworker-msgpack",
]


def apsw_available() -> bool:
    from dblayer.backends.sqlite_apsw import apsw_unavailable

    return apsw_unavailable() is None


def cworker_available() -> bool:
    from dblayer.backends.sqlite_cworker import binary_unavailable

    return binary_unavailable() is None


def make_backend(variant: str, path: Path, **kw: Any) -> Any:
    if variant == "in-process":
        return SqliteInProcess(path)
    if variant in ("apsw", "apsw-inline"):
        from dblayer.backends.sqlite_apsw import SqliteApsw

        return SqliteApsw(path, inline_reads=variant == "apsw-inline")
    kind, codec = variant.split("-", 1)
    if kind == "cworker":
        from dblayer.backends.sqlite_cworker import SqliteCWorker

        return SqliteCWorker(path, codec=codec, **kw)
    return SqliteWorker(path, codec=codec, **kw)


async def timed(fn: Callable[[], Awaitable[Any]], n: int) -> list[float]:
    out = []
    for _ in range(n):
        t0 = time.perf_counter()
        await fn()
        out.append(time.perf_counter() - t0)
    return out


def med_us(samples: list[float]) -> float:
    return statistics.median(samples) * 1e6


async def setup_rows(db: Database, n: int) -> None:
    await db.execute_write("create table if not exists r (id integer primary key, title text, score real, body blob)")
    await db.execute_write(
        "insert into r (title, score, body) select 'title ' || i, i * 1.5, randomblob(16) from "
        "(with recursive c(i) as (select 1 union all select i + 1 from c where i < :n) select i from c)",
        {"n": n},
    )


async def bench_round_trip(variant: str, d: Path, n: int) -> dict[str, float]:
    db = Database(make_backend(variant, d / f"rt-{variant}.db"))
    try:
        await setup_rows(db, 100)
        await timed(lambda: db.execute("select id, title from r where id = 1"), 50)  # warm
        s = await timed(lambda: db.execute("select id, title from r where id = :id", {"id": 7}), n)
        w = await timed(lambda: db.execute_write("update r set score = score + 1 where id = 7"), max(n // 4, 50))
    finally:
        await db.close()
    return {"read_us": med_us(s), "read_p90_us": statistics.quantiles(s, n=10)[-1] * 1e6, "write_us": med_us(w)}


async def bench_rows(variant: str, d: Path, rows: int, repeat: int) -> dict[str, float]:
    db = Database(make_backend(variant, d / f"rows-{variant}-{rows}.db"))
    try:
        await setup_rows(db, rows)
        sql = "select id, title, score, body from r"
        await db.execute(sql)  # warm
        ex = await timed(lambda: db.execute(sql), repeat)

        async def stream() -> None:
            async with db.stream(sql, chunk_rows=1000) as chunks:
                async for _ in chunks:
                    pass

        st = await timed(stream, repeat)
    finally:
        await db.close()
    return {
        "execute_ms": statistics.median(ex) * 1e3,
        "execute_us_per_row": statistics.median(ex) * 1e6 / rows,
        "stream_ms": statistics.median(st) * 1e3,
        "stream_us_per_row": statistics.median(st) * 1e6 / rows,
    }


async def bench_start(d: Path, repeat: int) -> dict[str, float]:
    path = d / "start.db"
    seed = Database(SqliteInProcess(path))
    await seed.execute_write("create table if not exists t (x)")
    await seed.close()

    async def first_result(backend: Any) -> float:
        db = Database(backend)
        t0 = time.perf_counter()
        await db.execute("select count(*) from t")
        elapsed = time.perf_counter() - t0
        await db.close()
        return elapsed

    dedicated = [await first_result(SqliteWorker(path)) for _ in range(repeat)]
    group = SharedWorkers(size=1, linger=60)
    try:
        shared_cold = await first_result(SqliteWorker(path, workers=group))  # starts the process
        shared_warm = [await first_result(SqliteWorker(path, workers=group)) for _ in range(repeat)]
    finally:
        group.close()
    inproc = [await first_result(SqliteInProcess(path)) for _ in range(repeat)]
    out = {
        "in-process_ms": statistics.median(inproc) * 1e3,
        "dedicated_ms": statistics.median(dedicated) * 1e3,
        "shared_first_ms": shared_cold * 1e3,
        "shared_warm_ms": statistics.median(shared_warm) * 1e3,
    }
    if cworker_available():
        from dblayer.backends.sqlite_cworker import SqliteCWorker, c_shared_workers

        c_dedicated = [await first_result(SqliteCWorker(path)) for _ in range(repeat)]
        cgroup = c_shared_workers(1, linger=60)
        try:
            c_cold = await first_result(SqliteCWorker(path, workers=cgroup))
            c_warm = [await first_result(SqliteCWorker(path, workers=cgroup)) for _ in range(repeat)]
        finally:
            cgroup.close()
        out.update(
            c_dedicated_ms=statistics.median(c_dedicated) * 1e3,
            c_shared_first_ms=c_cold * 1e3,
            c_shared_warm_ms=statistics.median(c_warm) * 1e3,
        )
    return out


def _spawn_wait(argv: list[str]) -> float:
    import subprocess

    t0 = time.perf_counter()
    subprocess.run(argv, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
    return time.perf_counter() - t0


def _protocol_phases(argv: list[str], path: Path) -> dict[str, float]:
    """Spawn a worker on raw pipes and time each step to the first result:
    spawn -> hello reply, open, first query (on a database with a table)."""
    import os
    import subprocess

    codec = get_codec("json")
    dec = FrameDecoder()
    t0 = time.perf_counter()
    p = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    assert p.stdin is not None and p.stdout is not None
    pending: list[bytes] = []

    def call(msg: dict[str, Any]) -> dict[str, Any]:
        p.stdin.write(frame(codec.encode(msg)))  # type: ignore[union-attr]
        p.stdin.flush()  # type: ignore[union-attr]
        while not pending:
            data = os.read(p.stdout.fileno(), 65536)  # type: ignore[union-attr]
            if not data:
                raise RuntimeError("worker exited")
            pending.extend(dec.feed(data))
        return codec.decode(pending.pop(0))

    call({"type": "hello", "protocol": 1, "codecs": ["json"], "id": 1})
    t1 = time.perf_counter()
    call({"type": "open", "path": str(path), "mode": "read", "id": 2})
    t2 = time.perf_counter()
    call({"type": "execute", "sql": "select count(*) from t", "id": 3})
    t3 = time.perf_counter()
    call({"type": "execute", "sql": "select count(*) from t", "id": 4})
    t4 = time.perf_counter()
    p.stdin.close()
    p.wait()
    p.stdout.close()
    return {"hello": t1 - t0, "open": t2 - t1, "first_query": t3 - t2, "second_query": t4 - t3, "total": t3 - t0}


def _rss_kb(pid: int) -> int:
    import subprocess

    out = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)], capture_output=True, text=True).stdout
    return int(out.strip() or 0)


async def bench_startup_breakdown(d: Path, repeat: int) -> dict[str, Any]:
    """Where a new worker's start-up goes, C against Python, plus the
    resident memory of one idle worker that has opened a database."""
    path = d / "phases.db"
    seed = Database(SqliteInProcess(path))
    await seed.execute_write("create table if not exists t (x)")
    await seed.close()
    workers: dict[str, list[str]] = {"python": [sys.executable, "-m", "dblayer.worker"]}
    if cworker_available():
        from dblayer.backends.sqlite_cworker import find_binary

        workers["c"] = [find_binary()]
    out: dict[str, Any] = {
        "spawn_true_ms": statistics.median(_spawn_wait(["/usr/bin/true"]) for _ in range(repeat * 3)) * 1e3,
        "python_bare_ms": statistics.median(_spawn_wait([sys.executable, "-S", "-c", "pass"]) for _ in range(repeat)) * 1e3,
        "python_site_ms": statistics.median(_spawn_wait([sys.executable, "-c", "pass"]) for _ in range(repeat)) * 1e3,
    }
    if "c" in workers:
        out["c_version_ms"] = statistics.median(_spawn_wait([workers["c"][0], "--version"]) for _ in range(repeat * 3)) * 1e3
    for name, argv in workers.items():
        runs = [_protocol_phases(argv, path) for _ in range(repeat)]
        out[f"{name}_phases_ms"] = {k: statistics.median(r[k] for r in runs) * 1e3 for k in runs[0]}
    for name in workers:
        backend: Any
        if name == "c":
            from dblayer.backends.sqlite_cworker import SqliteCWorker

            backend = SqliteCWorker(path)
        else:
            backend = SqliteWorker(path)
        db = Database(backend)
        try:
            await db.execute("select count(*) from t")
            pool = db._pool
            assert pool is not None
            pid = pool._idle_read[0].conn.pid  # type: ignore[attr-defined]
            await asyncio.sleep(0.2)
            out[f"{name}_idle_rss_kb"] = _rss_kb(pid)
        finally:
            await db.close()
    return out


async def bench_insert_with_alter(variant: str, d: Path, repeat: int) -> dict[str, float]:
    db = Database(make_backend(variant, d / f"alter-{variant}.db"))
    rows = [{"id": 1, "title": "a", "slug": "x"}, {"id": 2, "title": "b", "slug": "y"}]
    interactive, op = [], []
    try:
        # WAL, so that COMMIT's fsync does not drown the round trips.
        await db.execute_write("pragma journal_mode = wal")
        for i in range(repeat + 2):
            for name in (f"p{i}", f"q{i}"):
                await db.execute_write(f'create table "{name}" (id integer primary key, title text)')
            t0 = time.perf_counter()
            async with db.transaction() as tx:
                cols = [c.name for c in await tx.table_columns(f"p{i}")]
                if "slug" not in cols:
                    await tx.execute(f'alter table "p{i}" add column "slug" TEXT')
                await tx.execute_many(
                    f'insert into "p{i}" (id, title, slug) values (:id, :title, :slug)', rows
                )
            t1 = time.perf_counter()
            await db.insert_rows(f"q{i}", rows, alter=True)
            t2 = time.perf_counter()
            if i >= 2:  # warm-up
                interactive.append(t1 - t0)
                op.append(t2 - t1)
    finally:
        await db.close()
    return {"interactive_us": med_us(interactive), "op_us": med_us(op)}


async def bench_apsw_extras(variant: str, d: Path, repeat: int) -> dict[str, float]:
    """Connection open, execute_many of 100,000 rows in one transaction,
    and memory per open read connection (RSS growth of this process over
    200 open connections, each with its schema loaded; for the C worker,
    which holds no memory in this process, the worker's own RSS)."""
    path = d / f"x-{variant}.db"
    db = Database(make_backend(variant, path))
    await db.execute_write("create table t (id integer primary key, title text, score real)")
    rows = [{"title": f"title {i}", "score": i * 1.5} for i in range(100_000)]
    many = []
    for _ in range(max(repeat // 2, 2)):
        await db.execute_write("delete from t")
        t0 = time.perf_counter()
        async with db.transaction() as tx:
            await tx.execute_many("insert into t (title, score) values (:title, :score)", rows)
        many.append(time.perf_counter() - t0)
    await db.close()
    backend = make_backend(variant, path)
    opens = []
    for _ in range(repeat * 5):
        t0 = time.perf_counter()
        conn = await backend.open(write=False)
        opens.append(time.perf_counter() - t0)
        await conn.close()
    await backend.close()
    out = {"open_us": med_us(opens), "execute_many_100k_ms": statistics.median(many) * 1e3}
    if variant.startswith("cworker"):
        return out
    import gc
    import os

    gc.collect()
    before = _rss_kb(os.getpid())
    backends = [make_backend(variant, d / f"mem-{variant}-{i}.db") for i in range(200)]
    conns = []
    for i, b in enumerate(backends):
        c = await b.open(write=True)
        await c.execute("create table t (id integer primary key, v text)", {}, time_limit_ms=None, max_rows=None)
        await c.execute("select * from t", {}, time_limit_ms=None, max_rows=None)
        conns.append(c)
    gc.collect()
    after = _rss_kb(os.getpid())
    out["rss_per_conn_kb"] = (after - before) / len(conns)
    if hasattr(backends[0], "memory_status"):
        s = [b.memory_status() for b in backends]
        out["sqlite_bytes_per_conn"] = statistics.median(x["cache_used"] + x["schema_used"] + x["stmt_used"] for x in s)
    for c in conns:
        await c.close()
    for b in backends:
        await b.close()
    return out


async def run_all(quick: bool = False, only: list[str] | None = None) -> dict[str, Any]:
    n = 500 if quick else 3000
    rep = 3 if quick else 7
    variants = [
        v for v in VARIANTS
        if (not v.endswith("-msgpack") or "msgpack" in available_codecs())
        and (not v.startswith("cworker") or cworker_available())
        and (not v.startswith("apsw") or apsw_available())
        and (only is None or v in only)
    ]
    out: dict[str, Any] = {"round_trip": {}, "rows_1000": {}, "rows_100000": {}, "insert_with_alter": {}}
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        for v in variants:
            out["round_trip"][v] = await bench_round_trip(v, d, n)
            out["rows_1000"][v] = await bench_rows(v, d, 1000, rep * 3)
            out["rows_100000"][v] = await bench_rows(v, d, 100_000, rep)
            out["insert_with_alter"][v] = await bench_insert_with_alter(v, d, rep * 5)
        out["apsw_extras"] = {}
        for v in variants:
            if v in ("in-process", "apsw", "cworker-msgpack", "cworker-json"):
                out["apsw_extras"][v] = await bench_apsw_extras(v, d, rep)
        if only is None:
            out["start"] = await bench_start(d, rep)
            out["startup_breakdown"] = await bench_startup_breakdown(d, rep * 3)
    reap_workers(5)
    return out


def print_table(r: dict[str, Any]) -> None:
    variants = list(r["round_trip"])
    w = max(len(v) for v in variants) + 2
    print(f"\nSQLite worker overhead (python {sys.version.split()[0]})\n")
    print("One-row read by primary key, round trip (median / p90 us); one-row update (median us)")
    for v in variants:
        x = r["round_trip"][v]
        print(f"  {v:<{w}} read {x['read_us']:8.1f} / {x['read_p90_us']:8.1f}   write {x['write_us']:8.1f}")
    for key, label in (("rows_1000", "1,000 rows"), ("rows_100000", "100,000 rows")):
        print(f"\nRead of {label} (4 columns: int, text, real, 16-byte blob): total ms, us per row")
        for v in variants:
            x = r[key][v]
            print(
                f"  {v:<{w}} execute {x['execute_ms']:9.2f} ms {x['execute_us_per_row']:6.2f} us/row   "
                f"stream {x['stream_ms']:9.2f} ms {x['stream_us_per_row']:6.2f} us/row"
            )
    print("\nInsert with alter (2 rows, add one column): median us")
    for v in variants:
        x = r["insert_with_alter"][v]
        print(f"  {v:<{w}} interactive transaction {x['interactive_us']:8.1f}   one named operation {x['op_us']:8.1f}")
    x = r.get("apsw_extras") or {}
    if x:
        import sqlite3

        line = f"in-process sqlite3 {sqlite3.sqlite_version}"
        if apsw_available():
            import apsw

            line += f"; apsw {apsw.apsw_version()} bundles SQLite {apsw.sqlite_lib_version()}"
        print(f"\nSQLite versions: {line}")
        print("Connection open (median us), execute_many of 100,000 rows in a transaction (ms), "
              "memory per open connection (KB of process RSS; SQLite's own bytes)")
        for v, e in x.items():
            mem = ""
            if "rss_per_conn_kb" in e:
                mem = f"   rss {e['rss_per_conn_kb']:6.1f} KB/conn"
                if "sqlite_bytes_per_conn" in e:
                    mem += f" (db_status {e['sqlite_bytes_per_conn'] / 1024:.1f} KB)"
            print(f"  {v:<{w}} open {e['open_us']:8.1f}   execute_many {e['execute_many_100k_ms']:8.1f} ms{mem}")
    if "start" not in r:
        return
    s = r["start"]
    print("\nOpen to first result of a new Database (ms)")
    print(f"  in-process {s['in-process_ms']:.2f}; worker dedicated {s['dedicated_ms']:.1f} (a process per connection); "
          f"shared: first {s['shared_first_ms']:.1f}, then {s['shared_warm_ms']:.2f} on the running process")
    if "c_dedicated_ms" in s:
        print(f"  C worker dedicated {s['c_dedicated_ms']:.1f}; shared: first {s['c_shared_first_ms']:.1f}, "
              f"then {s['c_shared_warm_ms']:.2f} on the running process")
    b = r["startup_breakdown"]
    print("\nStart-up breakdown, raw pipes (median ms)")
    print(f"  spawn and wait for /usr/bin/true {b['spawn_true_ms']:.2f}; python -S -c pass {b['python_bare_ms']:.1f}; "
          f"python -c pass {b['python_site_ms']:.1f}"
          + (f"; dblayer-cworker --version {b['c_version_ms']:.2f}" if "c_version_ms" in b else ""))
    for name in ("python", "c"):
        ph = b.get(f"{name}_phases_ms")
        if ph:
            print(f"  {name:<7} spawn to hello {ph['hello']:7.2f}  open {ph['open']:6.2f}  first query {ph['first_query']:6.2f}"
                  f"  (second {ph['second_query']:5.2f})  total {ph['total']:7.2f}")
    print("\nResident memory of one idle worker with a database open (KB)")
    print("  " + "; ".join(f"{name} {b[f'{name}_idle_rss_kb']}" for name in ("python", "c") if f"{name}_idle_rss_kb" in b))


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--quick", action="store_true")
    p.add_argument("--json", type=Path)
    p.add_argument("--variants", help="comma-separated subset of " + ",".join(VARIANTS))
    args = p.parse_args()
    r = asyncio.run(run_all(args.quick, args.variants.split(",") if args.variants else None))
    print_table(r)
    if args.json:
        args.json.write_text(json.dumps(r, indent=2))


if __name__ == "__main__":
    main()
