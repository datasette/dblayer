"""The Node worker (SQLite-WASM in a Node.js process) against in-process
SQLite, the Python worker and the C worker: start-up and memory.

    uv run --all-extras python benchmarks/nodeworker_bench.py [--quick] [--json out.json]

Per-query costs (one-row read and update, 1,000 and 100,000 rows) are
columns of ``benchmarks/worker_overhead.py`` (variants ``nodeworker-json``
and ``nodeworker-msgpack``). This adds what is specific to the Node worker:

* open to first result of a new database, through ``Database``;
* where a Node worker's start-up goes (``DBLAYER_NODEWORKER_TIMING=1``):
  Node's own bootstrap, compiling the WebAssembly module, starting the
  executor thread, loading the SQLite module's JavaScript, instantiating;
  with and without V8's compile cache and the spare executor;
* idle memory (footprint and RSS) of a worker with one connection, and of
  a Node worker with four (one writer, three readers: one thread each);
* SQLite inside Node with no protocol: the WebAssembly build against Node's
  native ``node:sqlite``.

Prints the load average beside the figures: they move a lot with load.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import struct
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
from dblayer.backends.sqlite_nodeworker import SqliteNodeWorker, find_node, find_script  # noqa: E402
from dblayer.backends.sqlite_worker import SqliteWorker, reap_workers  # noqa: E402
from duckdb_memory import mem_of  # noqa: E402
from worker_overhead import _protocol_phases, _spawn_wait, cworker_available  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent


def load() -> str:
    return " ".join(f"{x:.1f}" for x in os.getloadavg())


def node_phases(path: Path, env: dict[str, str]) -> dict[str, float]:
    """Raw pipes: spawn -> hello, open, first query, with the worker's own
    timing of its phases (ms since its process started)."""
    full_env = dict(os.environ, DBLAYER_NODEWORKER_TIMING="1", **env)
    t0 = time.perf_counter()
    p = subprocess.Popen([find_node(), find_script()], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, env=full_env)
    assert p.stdin is not None and p.stdout is not None and p.stderr is not None

    def call(m: dict[str, Any]) -> None:
        b = json.dumps(m).encode()
        p.stdin.write(struct.pack(">I", len(b)) + b)  # type: ignore[union-attr]
        p.stdin.flush()  # type: ignore[union-attr]
        n = struct.unpack(">I", p.stdout.read(4))[0]  # type: ignore[union-attr]
        p.stdout.read(n)  # type: ignore[union-attr]

    call({"type": "hello", "protocol": 1, "codecs": ["json"], "id": 1})
    t1 = time.perf_counter()
    call({"type": "open", "path": str(path), "mode": "read", "id": 2})
    t2 = time.perf_counter()
    call({"type": "execute", "sql": "select count(*) from t", "id": 3})
    t3 = time.perf_counter()
    call({"type": "execute", "sql": "select count(*) from t", "id": 4})
    t4 = time.perf_counter()
    p.stdin.close()
    err = p.stderr.read().decode()
    p.wait()
    out = {"hello": t1 - t0, "open": t2 - t1, "first_query": t3 - t2, "second_query": t4 - t3, "total": t3 - t0}
    out = {k: v * 1e3 for k, v in out.items()}
    for line in err.splitlines():
        if "timing " in line:
            out.update(json.loads(line.split("timing ", 1)[1]))
            break
    return out


def _ms(phases: dict[str, float]) -> dict[str, float]:
    return {k: v * 1e3 for k, v in phases.items()}


def median_dict(runs: list[dict[str, float]]) -> dict[str, float]:
    return {k: statistics.median(r[k] for r in runs) for k in runs[0]}


async def first_result_ms(make: Any, d: Path, repeat: int) -> float:
    """Open to first result of a new database (a new file each time)."""
    times = []
    for i in range(repeat):
        db = Database(make(d / f"fr-{id(make)}-{i}.db"))
        t0 = time.perf_counter()
        await db.execute("select 1")
        times.append(time.perf_counter() - t0)
        await db.close()
    return statistics.median(times) * 1e3


async def idle_memory(make: Any, path: Path, conns: int = 1) -> dict[str, float]:
    """Footprint and RSS (MB) of the worker process(es) once idle with
    ``conns`` connections open."""
    backend = make(path)
    opened = []
    try:
        w = await backend.open(write=True)
        opened.append(w)
        await w.execute("create table if not exists t (x)", (), time_limit_ms=None, max_rows=None)
        for _ in range(conns - 1):  # read connections
            c = await backend.open(write=False)
            await c.execute("select count(*) from t", (), time_limit_ms=None, max_rows=None)
            opened.append(c)
        if conns == 1:  # one read connection only, as a read-only Database would have
            await w.close()
            opened = [await backend.open(write=False)]
            await opened[0].execute("select count(*) from t", (), time_limit_ms=None, max_rows=None)
        await asyncio.sleep(0.5)
        pids = {c.pid for c in opened}
        rss = fp = 0
        for pid in pids:
            m = mem_of(pid)
            if m:
                rss += m[0]
                fp += m[1]
        return {"processes": len(pids), "rss_mb": rss / 2**20, "footprint_mb": fp / 2**20}
    finally:
        for c in opened:
            await c.close()
        await backend.close()


async def run(quick: bool) -> dict[str, Any]:
    rep = 3 if quick else 9
    out: dict[str, Any] = {"load_start": load()}
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        seed = d / "phases.db"
        import sqlite3

        sqlite3.connect(seed).execute("create table t (x)")
        out["node_e_ms"] = statistics.median(_spawn_wait([find_node(), "-e", "0"]) for _ in range(rep)) * 1e3
        out["true_ms"] = statistics.median(_spawn_wait(["/usr/bin/true"]) for _ in range(rep)) * 1e3
        out["phases"] = {
            "python": median_dict([_ms(_protocol_phases([sys.executable, "-m", "dblayer.worker"], seed)) for _ in range(rep)]),
            "node": median_dict([node_phases(seed, {}) for _ in range(rep)]),
            "node, no compile cache": median_dict(
                [node_phases(seed, {"DBLAYER_NODEWORKER_COMPILE_CACHE": "0"}) for _ in range(rep)]
            ),
            "node, no spare": median_dict([node_phases(seed, {"DBLAYER_NODEWORKER_SPARES": "0"}) for _ in range(rep)]),
        }
        makers: dict[str, Any] = {
            "in-process": SqliteInProcess,
            "python worker": SqliteWorker,
            "node worker": SqliteNodeWorker,
        }
        if cworker_available():
            from dblayer.backends.sqlite_cworker import SqliteCWorker, find_binary

            makers["C worker"] = SqliteCWorker
            out["phases"]["c"] = median_dict([_ms(_protocol_phases([find_binary()], seed)) for _ in range(rep)])
        out["first_result_ms"] = {k: await first_result_ms(m, d, rep) for k, m in makers.items()}
        out["idle_memory"] = {}
        for k, m in makers.items():
            if k != "in-process":
                out["idle_memory"][k] = await idle_memory(m, d / f"idle-{k.replace(' ', '-')}.db")
        out["idle_memory"]["node worker, 4 connections"] = await idle_memory(SqliteNodeWorker, d / "idle4.db", conns=4)
        r = subprocess.run([find_node(), "--no-warnings", str(ROOT / "nodeworker" / "bench" / "inside.mjs")],
                           capture_output=True, text=True, check=True)
        out["inside_node"] = json.loads(r.stdout.strip().splitlines()[-1])
    reap_workers(5)
    out["load_end"] = load()
    return out


def print_report(r: dict[str, Any]) -> None:
    print(f"\nNode worker start-up and memory (load average {r['load_start']} at start, {r['load_end']} at end)\n")
    print(f"Spawn and wait: /usr/bin/true {r['true_ms']:.1f} ms; node -e 0 {r['node_e_ms']:.1f} ms")
    print("\nRaw pipes, median ms: spawn to hello, open, first query (second), total")
    for k, ph in r["phases"].items():
        print(f"  {k:<24} hello {ph['hello']:6.1f}  open {ph['open']:6.1f}  first {ph['first_query']:5.2f}"
              f"  ({ph['second_query']:5.2f})  total {ph['total']:6.1f}")
    print("\nInside a Node worker, ms since its process started (median)")
    for k, ph in r["phases"].items():
        if "sqlite_ready" in ph:
            print(f"  {k:<24} main loaded {ph['main_loaded']:5.1f}  wasm compiled {ph['wasm_compiled']:5.1f}"
                  f"  thread created {ph['executor_created']:5.1f}  thread running {ph['thread_start']:5.1f}"
                  f"  SQLite JS loaded {ph['sqlite_imported']:5.1f}  SQLite ready {ph['sqlite_ready']:5.1f}")
    print("\nOpen to first result of a new database, through Database (median ms)")
    print("  " + "; ".join(f"{k} {v:.2f}" for k, v in r["first_result_ms"].items()))
    print("\nIdle memory with a database open (MB, summed over the worker processes)")
    for k, m in r["idle_memory"].items():
        print(f"  {k:<28} footprint {m['footprint_mb']:6.1f}  RSS {m['rss_mb']:6.1f}  ({m['processes']} process(es))")
    x = r["inside_node"]
    print(f"\nInside Node, no protocol, in-memory database (SQLite {x['wasm_sqlite']} WASM vs node:sqlite {x['native_sqlite']} native)")
    print(f"  one-row read: WASM (oo1 API) {x['wasm_row_us']:.2f} us, native {x['native_row_us']:.2f} us")
    print(f"  100,000 rows into JS values: WASM raw exports {x['wasm_100k_ms']:.1f} ms (oo1 exec {x['wasm_oo1_100k_ms']:.1f}), "
          f"native .all() {x['native_100k_ms']:.1f} ms")
    print(f"  WASM module load and init {x['wasm_init_ms']:.1f} ms; native open {x['native_open_ms']:.2f} ms")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--quick", action="store_true")
    p.add_argument("--json", type=Path)
    args = p.parse_args()
    r = asyncio.run(run(args.quick))
    print_report(r)
    if args.json:
        args.json.write_text(json.dumps(r, indent=2))


if __name__ == "__main__":
    main()
