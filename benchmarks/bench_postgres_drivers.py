"""asyncpg against psycopg 3, through the two dblayer PostgreSQL backends.

Run with ``uv run --all-extras python benchmarks/bench_postgres_drivers.py``
(``--runs N``: runs per measurement, the best is kept; ``--only NAME`` to
measure some variants).

Connects only to the ``dblayer_test`` database (``DBLAYER_TEST_POSTGRES``,
default the local Unix socket), in schemas of its own that it drops at the
end. Variants are interleaved run by run, so a change in machine load hits
them alike. Prints one table: the best time per measurement and variant.

* ``asyncpg``: ``Postgres``;
* ``psycopg``: ``PostgresPsycopg`` as configured by default (no server-side
  prepared statements);
* ``psycopg prepared``: ``PostgresPsycopg(prepare_threshold=0)``: every
  statement prepared on the server, the closest to asyncpg's statement
  cache.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import time
import urllib.parse
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

from dblayer import Database
from dblayer.backends.postgres import Postgres
from dblayer.backends.postgres_psycopg import PostgresPsycopg

DSN = os.environ.get("DBLAYER_TEST_POSTGRES", "postgresql:///dblayer_test?host=/tmp")

VARIANTS: dict[str, Callable[..., Any]] = {
    "asyncpg": lambda **kw: Postgres(DSN, **kw),
    "psycopg": lambda **kw: PostgresPsycopg(DSN, **kw),
    "psycopg prepared": lambda **kw: PostgresPsycopg(DSN, prepare_threshold=0, **kw),
}

ROWS_SQL = (
    "select g as id, 'row number ' || g as name, g * 1.5 as value "
    "from generate_series(1, {n}) g"
)

type Timed = Callable[[], Awaitable[float]]


def _check_dsn() -> None:
    parts = urllib.parse.urlsplit(DSN)
    query = urllib.parse.parse_qs(parts.query)
    name = query["dbname"][0] if "dbname" in query else parts.path.lstrip("/")
    if name != "dblayer_test":
        raise SystemExit(f"refusing {DSN!r}: only the dblayer_test database may be used")


def per_call(n: int, fn: Callable[[], Awaitable[Any]]) -> Timed:
    """Mean seconds per call over ``n`` calls."""

    async def run() -> float:
        start = time.perf_counter()
        for _ in range(n):
            await fn()
        return (time.perf_counter() - start) / n

    return run


class Bench:
    def __init__(self, names: list[str], runs: int) -> None:
        self.names = names
        self.runs = runs
        self.results: dict[str, dict[str, float]] = {n: {} for n in names}
        self.labels: list[str] = []
        self.schemas: list[str] = []

    def backend(self, name: str, **kw: Any) -> Any:
        schema = f"dblbench_{os.getpid()}_{uuid.uuid4().hex[:6]}"
        self.schemas.append(schema)
        return VARIANTS[name](schema=schema, **kw)

    async def measure(self, label: str, fns: dict[str, Timed], runs: int | None = None) -> None:
        """Best of ``runs`` for each variant, the variants taking turns."""
        self.labels.append(label)
        best: dict[str, float] = {}
        for _ in range(runs or self.runs):
            for name in self.names:
                t = await fns[name]()
                best[name] = min(t, best.get(name, t))
        for name, t in best.items():
            self.results[name][label] = t
        print(f"  {label}: " + ", ".join(f"{n} {_fmt(best[n])}" for n in self.names), flush=True)


async def run(bench: Bench) -> None:
    names = bench.names

    # -- connect
    backends = {n: bench.backend(n) for n in names}
    for b in backends.values():
        conn = await b.open(write=False)  # creates the schema
        await conn.close()

    def connect(b: Any) -> Timed:
        async def one() -> None:
            conn = await b.open(write=False)
            await conn.close()

        return per_call(20, one)

    await bench.measure("connect + close", {n: connect(backends[n]) for n in names})

    # -- reads
    dbs = {n: Database(bench.backend(n)) for n in names}
    try:
        for db in dbs.values():
            await db.execute("select 1")
        await bench.measure(
            "read 1 row", {n: per_call(500, lambda db=dbs[n]: db.execute("select 1")) for n in names}
        )
        await bench.measure(
            "read 1 row, max_rows=1000",
            {n: per_call(500, lambda db=dbs[n]: db.execute("select 1", max_rows=1000)) for n in names},
        )
        for rows in (1_000, 100_000):
            sql = ROWS_SQL.format(n=rows)
            reps = 20 if rows == 1_000 else 2
            await bench.measure(
                f"read {rows:,} rows, fetch",
                {n: per_call(reps, lambda db=dbs[n]: db.execute(sql)) for n in names},
            )
            await bench.measure(
                f"read {rows:,} rows, fetch, max_rows={rows}",
                {n: per_call(reps, lambda db=dbs[n]: db.execute(sql, max_rows=rows)) for n in names},
            )
            await bench.measure(
                f"read {rows:,} rows, stream",
                {n: per_call(reps, lambda db=dbs[n]: _stream_all(db, sql)) for n in names},
            )
    finally:
        for db in dbs.values():
            await db.close()

    # -- insert_rows of 100,000 rows: default path and native (COPY)
    rows_data = [{"id": i, "name": f"row number {i}", "value": i * 1.5} for i in range(100_000)]
    for label, native in (("default path", False), ("COPY", True)):
        dbs = {n: Database(bench.backend(n), native_operations=native) for n in names}
        try:
            for db in dbs.values():
                await db.create_table("t", {"id": "integer", "name": "text", "value": "float"}, pk="id")

            def insert(db: Database) -> Timed:
                async def one() -> float:
                    await db.execute_write("truncate t")
                    start = time.perf_counter()
                    await db.insert_rows("t", rows_data, batch_size=10_000)
                    return time.perf_counter() - start

                return one

            await bench.measure(
                f"insert_rows 100,000, {label}",
                {n: insert(dbs[n]) for n in names},
                runs=max(1, bench.runs // 2),
            )
        finally:
            for db in dbs.values():
                await db.close()

    # -- one statement inside a transaction, with and without savepoints
    for label, savepoints in (("with savepoints", True), ("without savepoints", False)):
        dbs = {n: Database(bench.backend(n, statement_savepoints=savepoints)) for n in names}
        try:
            for db in dbs.values():
                await db.execute_write("create table k (id integer primary key, n integer)")
                await db.execute_write("insert into k values (1, 0)")

            def tx_statements(db: Database) -> Timed:
                async def one() -> float:
                    async with db.transaction() as tx:
                        await tx.execute("select 1")
                        start = time.perf_counter()
                        for _ in range(200):
                            await tx.execute("update k set n = n + 1 where id = :id", {"id": 1})
                        return (time.perf_counter() - start) / 200

                return one

            await bench.measure(
                f"tx statement (1-row update), {label}", {n: tx_statements(dbs[n]) for n in names}
            )
        finally:
            for db in dbs.values():
                await db.close()

    # -- schema() for 50 tables
    dbs = {n: Database(bench.backend(n)) for n in names}
    conns: dict[str, Any] = {}
    try:
        for db in dbs.values():
            for i in range(50):
                parent = f", parent bigint references t{i - 1} (id)" if i else ""
                await db.execute_write(
                    f"create table t{i} (id bigint primary key, name text not null default '', "
                    f"value double precision{parent})"
                )
                await db.execute_write(f"create index t{i}_name on t{i} (name)")
        for n, db in dbs.items():
            conns[n] = await db._backend.open(write=False)  # type: ignore[attr-defined]
        await bench.measure(
            "schema(), 50 tables", {n: per_call(10, conns[n].schema) for n in names}
        )
        await bench.measure(
            "schema_version()", {n: per_call(50, conns[n].schema_version) for n in names}
        )
    finally:
        for conn in conns.values():
            await conn.close()
        for db in dbs.values():
            await db.close()


async def _stream_all(db: Database, sql: str) -> None:
    async with db.stream(sql, chunk_rows=1000) as rows:
        async for _ in rows:
            pass


def _fmt(seconds: float) -> str:
    if seconds < 1e-3:
        return f"{seconds * 1e6:.0f} us"
    if seconds < 1:
        return f"{seconds * 1e3:.2f} ms"
    return f"{seconds:.2f} s"


def _version(module: str) -> str:
    import importlib.metadata

    return importlib.metadata.version(module)


async def _drop(schemas: list[str]) -> None:
    if not schemas:
        return
    import psycopg

    async with await psycopg.AsyncConnection.connect(DSN, autocommit=True) as conn:
        cur = await conn.execute("select current_database()")
        assert (await cur.fetchone()) == ("dblayer_test",)
        for s in schemas:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{s}" CASCADE')


async def main() -> None:
    _check_dsn()
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--only", choices=list(VARIANTS), action="append")
    args = parser.parse_args()
    bench = Bench(args.only or list(VARIANTS), args.runs)
    try:
        await run(bench)
    finally:
        await _drop(bench.schemas)
    import psycopg

    width = max(len(r) for r in bench.labels)
    load = ", ".join(f"{x:.1f}" for x in os.getloadavg())
    print(
        f"\nbest of {args.runs} runs (inserts: {max(1, args.runs // 2)}), local socket, "
        f"load average {load}; asyncpg {_version('asyncpg')}, psycopg {psycopg.__version__} "
        f"(libpq {psycopg.pq.version()}, {psycopg.pq.__impl__})\n"
    )
    header = f"{'':{width}}  " + "  ".join(f"{n:>16}" for n in bench.names)
    print(header)
    print("-" * len(header))
    for label in bench.labels:
        print(
            f"{label:{width}}  "
            + "  ".join(f"{_fmt(bench.results[n][label]):>16}" for n in bench.names)
        )


if __name__ == "__main__":
    asyncio.run(main())
