"""Benchmark bulk-insert candidates for DuckDB's native ``insert_rows``.

Run: ``uv run --extra duckdb --with pyarrow python scripts/bench_duckdb_insert.py``
(without pyarrow the Arrow candidate is skipped).

Each candidate inserts N rows of five columns (BIGINT key, two TEXT, DOUBLE,
nullable BIGINT) from Python dicts, inside one transaction on a cursor of a
file-backed instance, starting from an empty table with a primary key. The
time includes turning the dicts into whatever the candidate needs. Best of
three runs. Results on an M-series Mac, DuckDB 2.0.1, CPython 3.15 are in
RESULTS at the bottom.
"""

from __future__ import annotations

import tempfile
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import duckdb

COLS = ["id", "name", "city", "score", "parent"]


def make_rows(n: int) -> list[dict[str, Any]]:
    return [
        {
            "id": i,
            "name": f"name {i}",
            "city": ("London", "Paris", "Lagos", "Lima")[i % 4],
            "score": i * 0.5,
            "parent": None if i % 3 else i // 3,
        }
        for i in range(n)
    ]


def executemany(cur: Any, rows: list[dict[str, Any]]) -> None:
    sql = f"INSERT INTO t ({', '.join(COLS)}) VALUES ({', '.join('?' for _ in COLS)})"
    cur.executemany(sql, [[r[c] for c in COLS] for r in rows])


def multi_values(cur: Any, rows: list[dict[str, Any]], chunk: int = 1000) -> None:
    one = "(" + ", ".join("?" for _ in COLS) + ")"
    for i in range(0, len(rows), chunk):
        part = rows[i : i + chunk]
        sql = f"INSERT INTO t ({', '.join(COLS)}) VALUES " + ", ".join(one for _ in part)
        cur.execute(sql, [r[c] for r in part for c in COLS])


def unnest_lists(cur: Any, rows: list[dict[str, Any]]) -> None:
    # One statement, one list parameter per column.
    cols = {c: [r[c] for r in rows] for c in COLS}
    sel = ", ".join(f"unnest(${c}) AS {c}" for c in COLS)
    cur.execute(f"INSERT INTO t ({', '.join(COLS)}) SELECT {sel}", cols)


def appender(cur: Any, rows: list[dict[str, Any]]) -> None:
    app = cur.appender("t", columns=COLS)
    app.append_rows([[r[c] for c in COLS] for r in rows])
    app.flush()


def arrow(cur: Any, rows: list[dict[str, Any]]) -> None:
    import pyarrow as pa

    tbl = pa.Table.from_pylist(rows)  # noqa: F841  (found by name below)
    cur.execute(f"INSERT INTO t ({', '.join(COLS)}) SELECT {', '.join(COLS)} FROM tbl")


CANDIDATES: dict[str, Callable[[Any, list[dict[str, Any]]], None]] = {
    "executemany": executemany,
    "multi-row VALUES (1000/stmt)": multi_values,
    "unnest list params": unnest_lists,
    "appender": appender,
    "arrow table": arrow,
}


def bench(n: int, fn: Callable[[Any, list[dict[str, Any]]], None], tmp: Path) -> float | str:
    rows = make_rows(n)
    best = float("inf")
    for attempt in range(3):
        con = duckdb.connect(str(tmp / f"b{n}_{fn.__name__}_{attempt}.duckdb"))
        con.execute(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, name TEXT, city TEXT, score DOUBLE, parent BIGINT)"
        )
        cur = con.cursor()
        try:
            t0 = time.perf_counter()
            cur.execute("BEGIN")
            fn(cur, rows)
            cur.execute("COMMIT")
            best = min(best, time.perf_counter() - t0)
            assert con.execute("SELECT count(*), sum(id) FROM t").fetchone() == (n, n * (n - 1) // 2)
        except ImportError as e:
            return f"skipped ({e.name} missing)"
        finally:
            con.close()
        if best > 20:
            break
    return best


def main() -> None:
    sizes = [100, 1000, 10_000, 100_000]
    print(f"duckdb {duckdb.__version__}")
    with tempfile.TemporaryDirectory() as d:
        print(f"{'candidate':30}" + "".join(f"{n:>14,}" for n in sizes))
        for name, fn in CANDIDATES.items():
            cells = []
            for n in sizes:
                if name == "executemany" and n > 10_000:
                    cells.append("        (slow)")
                    continue
                r = bench(n, fn, Path(d))
                if isinstance(r, str):
                    cells.append(f"{r:>14}")
                else:
                    cells.append(f"{r * 1000:>11.1f} ms")
            print(f"{name:30}" + "".join(cells))


RESULTS = """
duckdb 2.0.1
candidate                                100         1,000        10,000       100,000
executemany                          18.1 ms      193.2 ms     1448.4 ms        (slow)
multi-row VALUES (1000/stmt)          2.7 ms       20.4 ms      200.4 ms     1623.5 ms
unnest list params                    0.7 ms        1.4 ms        8.6 ms       86.8 ms
appender                              0.7 ms        1.4 ms        8.0 ms       77.4 ms
arrow table                           1.1 ms        1.5 ms        4.4 ms       37.5 ms

Chosen: the appender. Within 2x of Arrow at 100,000 rows and as fast at the
default batch size of 100, with no pyarrow dependency, and it converts
values exactly as executemany parameters are converted (so the default and
native paths agree). unnest of list parameters is as fast but infers one
type per list, which differs from per-column conversion for mixed values.
"""

if __name__ == "__main__":
    main()
