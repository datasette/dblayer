"""Benchmark: ``db.export()`` with the backend serialising ("native") against
the Python fallback, on the three native backends.

Each case runs in a fresh process, so peak memory is that case's own.
Reported per case: wall time; CPU time of this (the "server") process,
user + system, which is the number that matters for a web server; CPU of
reaped child processes (the C worker); peak RSS and its growth over the
baseline; MB/s and rows/s; and event-loop lag (a ticker coroutine sleeping
1 ms: max and p99 lateness) while exporting. The bytes are consumed the way
an HTTP server would forward them (counted, then dropped).

Modes: ``native``, ``fallback``, ``datasette`` (what Datasette does today:
rows from ``stream()``; CSV through ``csv.writer`` per page, JSON as one
``json.dumps`` of the whole response). On the C worker, ``native`` is run
per transport: ``frames``, ``raw``, ``fifo``; ``handoff`` (socket handoff)
is measured end to end by ``--http``.

    uv run --all-extras python -m benchmarks.export            # default matrix
    uv run --all-extras python -m benchmarks.export --quick
    uv run --all-extras --group demo python -m benchmarks.export --http

Data (built once per size into a cache directory): ``wide`` has an integer
key, a nullable integer, a float, a short and a long text, a nullable text
and a timestamp (text on SQLite, TIMESTAMP elsewhere); ``narrow`` is four
integers. PostgreSQL tables live in a ``bench_export`` schema of the
``dblayer_test`` database only.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import io
import json
import os
import resource
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

CACHE = Path(os.environ.get("DBLAYER_BENCH_CACHE", Path.home() / ".cache" / "dblayer-bench-export"))
PG_DSN = os.environ.get("DBLAYER_TEST_POSTGRES", "postgresql:///dblayer_test?host=/tmp")
PG_SCHEMA = "bench_export"

WIDE_SQLITE = """
with recursive c(x) as (select 1 union all select x + 1 from c where x < {n})
select x as id,
       case when x % 7 = 0 then null else x * 31 % 100003 end as qty,
       (x * 7919 % 1000003) / 997.0 as price,
       'item ' || x as name,
       'This is a longer description for row ' || x || ', with "quotes", commas, and enough words to be realistic.' as description,
       case when x % 5 = 0 then null when x % 5 = 1 then '' else 'note ' || (x % 13) end as note,
       strftime('%Y-%m-%d %H:%M:%S', 1700000000 + x * 61, 'unixepoch') as created
from c
"""
WIDE_DUCKDB = """
select range + 1 as id,
       case when (range + 1) % 7 = 0 then null else (range + 1) * 31 % 100003 end as qty,
       ((range + 1) * 7919 % 1000003) / 997.0 as price,
       'item ' || (range + 1) as name,
       'This is a longer description for row ' || (range + 1) || ', with "quotes", commas, and enough words to be realistic.' as description,
       case when (range + 1) % 5 = 0 then null when (range + 1) % 5 = 1 then '' else 'note ' || ((range + 1) % 13) end as note,
       timestamp '2023-11-14 22:13:20' + to_seconds((range + 1) * 61) as created
from range({n})
"""
WIDE_PG = """
select x as id,
       case when x % 7 = 0 then null else x * 31 % 100003 end as qty,
       ((x * 7919 % 1000003) / 997.0)::float8 as price,
       'item ' || x as name,
       'This is a longer description for row ' || x || ', with "quotes", commas, and enough words to be realistic.' as description,
       case when x % 5 = 0 then null when x % 5 = 1 then '' else 'note ' || (x % 13) end as note,
       timestamp '2023-11-14 22:13:20' + x * interval '61 seconds' as created
from generate_series(1::bigint, {n}) x
"""
NARROW = {
    "sqlite": "with recursive c(x) as (select 1 union all select x + 1 from c where x < {n}) "
              "select x as a, x * 2 as b, x % 1000 as c, -x as d from c",
    "duckdb": "select range as a, range * 2 as b, range % 1000 as c, -range as d from range({n})",
    "postgres": "select x::bigint as a, x * 2::bigint as b, x % 1000 as c, -x::bigint as d from generate_series(1::bigint, {n}) x",
}


# -- data ------------------------------------------------------------------------------


def sqlite_path(n: int) -> Path:
    p = CACHE / f"export-{n}.db"
    if not p.exists():
        import sqlite3

        CACHE.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".tmp")
        tmp.unlink(missing_ok=True)
        c = sqlite3.connect(tmp)
        c.execute("create table wide as " + WIDE_SQLITE.format(n=n))
        c.execute("create table narrow as " + NARROW["sqlite"].format(n=n))
        c.commit()
        c.close()
        tmp.rename(p)
    return p


def duckdb_path(n: int) -> Path:
    p = CACHE / f"export-{n}.duckdb"
    if not p.exists():
        import duckdb

        CACHE.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".tmp")
        tmp.unlink(missing_ok=True)
        c = duckdb.connect(str(tmp))
        c.execute("create table wide as " + WIDE_DUCKDB.format(n=n))
        c.execute("create table narrow as " + NARROW["duckdb"].format(n=n))
        c.close()
        tmp.rename(p)
    return p


def postgres_tables(n: int) -> None:
    import psycopg

    assert "dblayer_test" in PG_DSN
    with psycopg.connect(PG_DSN, autocommit=True) as c:
        c.execute(f"create schema if not exists {PG_SCHEMA}")
        for kind, sql in (("wide", WIDE_PG), ("narrow", NARROW["postgres"])):
            name = f"{PG_SCHEMA}.{kind}_{n}"
            if c.execute("select to_regclass(%s)", (name,)).fetchone()[0] is None:
                c.execute(f"create table {name} as " + sql.format(n=n))


def table_sql(backend: str, kind: str, n: int) -> str:
    if backend == "postgres":
        return f"select * from {kind}_{n} order by 1"
    return f"select * from {kind} order by 1"


def make_db(backend: str, n: int, transport: str = "raw") -> Any:
    from dblayer import Database

    if backend == "cworker":
        from dblayer.backends.sqlite_cworker import SqliteCWorker

        t = transport if transport in ("raw", "frames", "fifo") else "raw"
        return Database(SqliteCWorker(sqlite_path(n), export_transport=t, immutable=True))
    if backend == "duckdb":
        from dblayer.backends.duckdb import DuckDB

        return Database(DuckDB(duckdb_path(n)))
    if backend == "postgres":
        from dblayer.backends.postgres_psycopg import PostgresPsycopg

        return Database(PostgresPsycopg(PG_DSN, schema=PG_SCHEMA))
    raise ValueError(backend)


# -- one case (in its own process) ----------------------------------------------------


def _cpu(who: int) -> float:
    r = resource.getrusage(who)
    return r.ru_utime + r.ru_stime


def _maxrss() -> int:
    r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return r if sys.platform == "darwin" else r * 1024


async def _ticker(lags: list[float], stop: asyncio.Event) -> None:
    while not stop.is_set():
        t = time.perf_counter()
        await asyncio.sleep(0.001)
        lags.append(time.perf_counter() - t - 0.001)


async def _datasette_body(db: Any, sql: str, fmt: str) -> Any:
    """What Datasette's renderers do with rows today."""
    if fmt in ("csv", "tsv"):
        buf = io.StringIO()
        w = csv.writer(buf, dialect="excel" if fmt == "csv" else "excel-tab")
        first = True
        async with db.stream(sql) as chunks:
            async for rows in chunks:
                if first:
                    w.writerow(list(rows[0].keys()))
                    first = False
                w.writerows(rows)
                yield buf.getvalue().encode()
                buf.seek(0)
                buf.truncate()
        return
    # .json builds the whole response, then one json.dumps
    import base64

    def default(o: Any) -> Any:
        if isinstance(o, bytes):
            return {"$base64": True, "encoded": base64.b64encode(o).decode("latin1")}
        return str(o)

    rows: list[dict[str, Any]] = []
    columns: list[str] = []
    async with db.stream(sql) as chunks:
        async for chunk in chunks:
            if not columns:
                columns = list(chunk[0].keys())
            rows.extend(dict(zip(columns, r)) for r in chunk)
    if fmt == "ndjson":
        yield "\n".join(json.dumps(r, default=default) for r in rows).encode()
    else:
        yield json.dumps({"ok": True, "rows": rows, "columns": columns, "truncated": False}, default=default).encode()


async def run_one(backend: str, kind: str, n: int, fmt: str, mode: str, transport: str) -> dict[str, Any]:
    db = make_db(backend, n, transport)
    sql = table_sql(backend, kind, n)
    await db.execute("select 1 as warm")  # open the connection (and the worker) first
    lags: list[float] = []
    stop = asyncio.Event()
    base_rss = _maxrss()
    ticker = asyncio.create_task(_ticker(lags, stop))
    await asyncio.sleep(0.01)
    lags.clear()
    cpu0, wall0 = _cpu(resource.RUSAGE_SELF), time.perf_counter()
    nbytes = 0
    rows = n
    native = None
    if mode == "datasette":
        async for chunk in _datasette_body(db, sql, fmt):
            nbytes += len(chunk)
    elif mode == "handoff":
        # The "client" is a `wc -c` process reading the other end of a
        # socketpair: no body byte passes through this process.
        import socket

        from dblayer.backends.cworker_handoff import chunk as http_chunk, handoff_export
        from dblayer.core.export import ExportSpec

        a, b = socket.socketpair()
        reader = subprocess.Popen(["wc", "-c"], stdin=a.fileno(), stdout=subprocess.PIPE, text=True)
        a.close()
        b.setblocking(False)
        spec = ExportSpec(fmt)

        async def send_head(columns: Any) -> None:
            b.sendall(http_chunk(spec.head(columns)))

        res = await handoff_export(db, sql, client=b, send_head=send_head, format=fmt)
        b.sendall(http_chunk(spec.tail(res.truncated)) + b"0\r\n\r\n")
        b.close()
        out, _ = reader.communicate()
        nbytes = int(out.split()[0])
        rows = res.row_count
        native = True
    else:
        async with db.export(sql, format=fmt, max_rows=None, native=mode == "native") as s:
            async for chunk in s:
                nbytes += len(chunk)
        rows = s.row_count
        native = s.native
    wall = time.perf_counter() - wall0
    cpu = _cpu(resource.RUSAGE_SELF) - cpu0
    stop.set()
    await ticker
    peak = _maxrss()
    child0 = _cpu(resource.RUSAGE_CHILDREN)
    await db.close()
    if backend == "cworker":
        from dblayer.backends.sqlite_worker import reap_workers

        reap_workers()
    lags.sort()
    return {
        "backend": backend, "table": kind, "rows": rows, "format": fmt, "mode": mode,
        "transport": transport if backend == "cworker" and mode == "native" else "",
        "native": native, "wall_s": wall, "cpu_s": cpu,
        "child_cpu_s": _cpu(resource.RUSAGE_CHILDREN) - child0,
        "peak_rss_mb": peak / 1e6, "rss_growth_mb": (peak - base_rss) / 1e6,
        "mb_s": nbytes / wall / 1e6, "rows_s": rows / wall, "bytes": nbytes,
        "lag_max_ms": (lags[-1] * 1000) if lags else 0.0,
        "lag_p99_ms": (lags[int(len(lags) * 0.99)] * 1000) if lags else 0.0,
        "ticks": len(lags),
    }


# -- the matrix ---------------------------------------------------------------------------


def cases(sizes: list[int], quick: bool) -> list[tuple[str, str, int, str, str, str]]:
    out = []
    for n in sizes:
        fmts = ["csv", "tsv", "json", "ndjson"] if n <= 100_000 and not quick else ["csv", "json"]
        for backend in ("cworker", "duckdb", "postgres"):
            for kind in ("wide", "narrow"):
                kfmts = fmts if kind == "wide" else ["csv", "json"]
                for fmt in kfmts:
                    modes = ["native", "fallback"] + (["datasette"] if kind == "wide" else [])
                    for mode in modes:
                        transports = ["raw"]
                        if backend == "cworker" and mode == "native" and kind == "wide" and fmt in ("csv", "json"):
                            transports = ["raw", "frames", "fifo"]
                        for t in transports:
                            out.append((backend, kind, n, fmt, mode, t))
                    if backend == "cworker" and kind == "wide" and fmt in ("csv", "json"):
                        out.append((backend, kind, n, fmt, "handoff", "socket"))
    return out


def load() -> str:
    return os.getloadavg().__repr__() if hasattr(os, "getloadavg") else "?"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--one", nargs=6, metavar=("BACKEND", "TABLE", "N", "FORMAT", "MODE", "TRANSPORT"))
    ap.add_argument("--sizes", default="10000,100000,1000000")
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--backends", default="cworker,duckdb,postgres")
    ap.add_argument("--out", default=None, help="append JSON lines here")
    ap.add_argument("--http", action="store_true", help="end-to-end HTTP benchmark instead")
    args = ap.parse_args()
    if args.one:
        b, k, n, f, m, t = args.one
        print(json.dumps(asyncio.run(run_one(b, k, int(n), f, m, t))))
        return
    if args.http:
        from .export_http import main as http_main

        http_main()
        return
    sizes = [int(s) for s in args.sizes.split(",")]
    backends = args.backends.split(",")
    for n in sizes:
        if "cworker" in backends:
            sqlite_path(n)
        if "duckdb" in backends:
            duckdb_path(n)
        if "postgres" in backends:
            postgres_tables(n)
    out = open(args.out, "a") if args.out else None
    print(f"# load average at start: {load()}")
    hdr = ("backend", "table", "rows", "format", "mode", "transport", "cpu_s", "wall_s", "child_cpu_s",
           "rss_growth_mb", "mb_s", "rows_s", "lag_max_ms", "lag_p99_ms")
    print("\t".join(hdr) + "\tload1")
    for case in cases(sizes, args.quick):
        if case[0] not in backends:
            continue
        cmd = [sys.executable, "-m", "benchmarks.export", "--one", *map(str, case)]
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=900)
        if p.returncode != 0:
            print("FAILED", case, p.stderr[-800:], file=sys.stderr)
            continue
        r = json.loads(p.stdout.strip().splitlines()[-1])
        r["load1"] = os.getloadavg()[0]
        if out:
            out.write(json.dumps(r) + "\n")
            out.flush()
        cells = []
        for h in hdr:
            v = r[h]
            cells.append(f"{v:.3f}" if isinstance(v, float) else str(v))
        print("\t".join(cells) + f"\t{r['load1']:.1f}", flush=True)


if __name__ == "__main__":
    main()
