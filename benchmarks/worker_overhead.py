"""What the worker process boundary costs, against in-process SQLite.

    uv run --all-extras python benchmarks/worker_overhead.py [--quick] [--json out.json]

Measures (the spec's "Benchmarks" rows that concern the worker):

* round trip of a one-row read: in-process, worker + JSON, worker + MessagePack;
* reads of 1,000 and 100,000 rows, with ``execute`` and with ``stream``;
* worker start to first result, dedicated and shared placement;
* "insert with alter": an interactive transaction (begin, schema, alter,
  execute_many, commit: 5 round trips) against one named operation (begin,
  op, commit: the op itself is one message).

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
from dblayer.worker.protocol import available_codecs  # noqa: E402

VARIANTS = ["in-process", "worker-json", "worker-msgpack"]


def make_backend(variant: str, path: Path, **kw: Any) -> Any:
    if variant == "in-process":
        return SqliteInProcess(path)
    codec = variant.split("-", 1)[1]
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
    return {
        "in-process_ms": statistics.median(inproc) * 1e3,
        "dedicated_ms": statistics.median(dedicated) * 1e3,
        "shared_first_ms": shared_cold * 1e3,
        "shared_warm_ms": statistics.median(shared_warm) * 1e3,
    }


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


async def run_all(quick: bool = False) -> dict[str, Any]:
    n = 500 if quick else 3000
    rep = 3 if quick else 7
    variants = [v for v in VARIANTS if v != "worker-msgpack" or "msgpack" in available_codecs()]
    out: dict[str, Any] = {"round_trip": {}, "rows_1000": {}, "rows_100000": {}, "insert_with_alter": {}}
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        for v in variants:
            out["round_trip"][v] = await bench_round_trip(v, d, n)
            out["rows_1000"][v] = await bench_rows(v, d, 1000, rep * 3)
            out["rows_100000"][v] = await bench_rows(v, d, 100_000, rep)
            out["insert_with_alter"][v] = await bench_insert_with_alter(v, d, rep * 5)
        out["start"] = await bench_start(d, rep)
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
    s = r["start"]
    print("\nOpen to first result of a new Database (ms)")
    print(f"  in-process {s['in-process_ms']:.2f}; worker dedicated {s['dedicated_ms']:.1f} (a process per connection); "
          f"shared: first {s['shared_first_ms']:.1f}, then {s['shared_warm_ms']:.2f} on the running process")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--quick", action="store_true")
    p.add_argument("--json", type=Path)
    args = p.parse_args()
    r = asyncio.run(run_all(args.quick))
    print_table(r)
    if args.json:
        args.json.write_text(json.dumps(r, indent=2))


if __name__ == "__main__":
    main()
