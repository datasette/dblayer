"""Does running DuckDB in a worker process give memory back? And what does
the process boundary cost?

    uv run --all-extras python benchmarks/duckdb_memory.py memory [--n 50 150] [--modes ...]
    uv run --all-extras python benchmarks/duckdb_memory.py overhead
    uv run --all-extras python benchmarks/duckdb_memory.py all [--json out.json]

``memory``: for each mode and each N, a fresh Python process registers N
DuckDB databases (copies of one template with ``--rows`` rows) in a
``Registry`` with a cap of ``--cap`` places, then runs ``--cycles`` cycles of:
wake every database (``--concurrency`` at a time) and run a point read, an
analytical query that makes DuckDB allocate (a GROUP BY over every row plus
a sort) and a 1,000-row read; then put every database to sleep. Memory is
read at start, at the peak of each wake phase (sampled every 50 ms) and after
each sleep phase (1 s after the last database slept).

Modes:

* ``in-process``: ``DuckDB`` backend, nothing else.
* ``in-process+gc``: plus ``gc.collect()`` after the sleep phase.
* ``in-process+trim``: plus the C allocator's own trim after the sleep
  phase: ``malloc_zone_pressure_relief(NULL, 0)`` on macOS,
  ``malloc_trim(0)`` on glibc (both through ctypes).
* ``in-process+settings``: instances opened with
  ``allocator_flush_threshold='0MB'``,
  ``allocator_bulk_deallocation_flush_threshold='0MB'`` and
  ``allocator_background_threads=true`` (DuckDB's own allocator release
  knobs), plus gc and trim.
* ``worker``: ``DuckDBWorker`` (one process per awake database, exiting on
  sleep), JSON codec.

How memory is measured (``mem_of``): on macOS, ``proc_pid_rusage``
(``RUSAGE_INFO_V2``) through ctypes gives ``ri_resident_size`` (RSS, what
``ps`` shows) and ``ri_phys_footprint`` (the "Memory" column of Activity
Monitor: dirty private memory plus compressed and swapped pages, which is
what a process really costs). On Linux, ``/proc/<pid>/status`` ``VmRSS`` and
``smaps_rollup`` ``Pss``. The parent is the benchmark process; children are
the worker processes it started (``live_worker_pids()``), summed.

``overhead``: one database of ``--rows`` rows; median round trip for a
one-row read, and for 1,000-row and 100,000-row results in two shapes
(``plain``: BIGINT, DOUBLE, VARCHAR; ``typed``: DATE, TIMESTAMP,
DECIMAL(12,2), so every value goes through the codec's value types), for
in-process DuckDB, the worker with JSON and with MessagePack; plus time from
asleep to first result.

Check the machine is quiet first: the script prints the load average and
any ``yes`` CPU burners, and marks results "provisional" when the 1-minute
load average is over ``--max-load``.
"""

from __future__ import annotations

import argparse
import asyncio
import ctypes
import ctypes.util
import gc
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

MODES = ["in-process", "in-process+gc", "in-process+trim", "in-process+settings", "worker"]

ANALYTICAL = (
    "select grp, count(*) n, avg(val) a, max(s) m, sum(length(s)) l "
    "from (select * from t order by val desc) group by grp order by n desc, grp limit 20"
)
POINT = "select * from t where id = :id"
ROWS_1K = "select id, grp, val, s from t where id < 1000 order by id"

# -- memory readings -------------------------------------------------------------


class _RusageInfoV2(ctypes.Structure):
    _fields_ = [("ri_uuid", ctypes.c_uint8 * 16)] + [
        (name, ctypes.c_uint64)
        for name in (
            "ri_user_time", "ri_system_time", "ri_pkg_idle_wkups", "ri_interrupt_wkups",
            "ri_pageins", "ri_wired_size", "ri_resident_size", "ri_phys_footprint",
            "ri_proc_start_abstime", "ri_proc_exit_abstime", "ri_child_user_time",
            "ri_child_system_time", "ri_child_pkg_idle_wkups", "ri_child_interrupt_wkups",
            "ri_child_pageins", "ri_child_elapsed_abstime", "ri_diskio_bytesread",
            "ri_diskio_byteswritten",
        )
    ]


_libproc: Any = None
if sys.platform == "darwin":
    _libproc = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
    _libproc.proc_pid_rusage.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_void_p]


def mem_of(pid: int) -> tuple[int, int] | None:
    """(resident bytes, footprint bytes) of ``pid``; footprint is
    ``phys_footprint`` on macOS and PSS on Linux. None if it has gone."""
    if _libproc is not None:
        info = _RusageInfoV2()
        if _libproc.proc_pid_rusage(pid, 2, ctypes.byref(info)) != 0:
            return None
        return int(info.ri_resident_size), int(info.ri_phys_footprint)
    try:
        with open(f"/proc/{pid}/status") as f:
            rss = next(int(line.split()[1]) * 1024 for line in f if line.startswith("VmRSS:"))
        pss = rss
        try:
            with open(f"/proc/{pid}/smaps_rollup") as f:
                pss = next(int(line.split()[1]) * 1024 for line in f if line.startswith("Pss:"))
        except (OSError, StopIteration):
            pass
        return rss, pss
    except (OSError, StopIteration):
        return None


def fd_count(pid: int | None = None) -> int | None:
    if pid is None:
        for d in ("/proc/self/fd", "/dev/fd"):
            try:
                return len(os.listdir(d))
            except OSError:
                continue
        return None
    out = subprocess.run(["lsof", "-p", str(pid), "-Ff"], capture_output=True, text=True)
    return sum(1 for line in out.stdout.splitlines() if line.startswith("f") and line[1:].isdigit())


def trim_allocator() -> str:
    """Ask the C allocator to return free memory to the OS."""
    if sys.platform == "darwin":
        libc = ctypes.CDLL(ctypes.util.find_library("c"))
        libc.malloc_zone_pressure_relief.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
        libc.malloc_zone_pressure_relief.restype = ctypes.c_size_t
        return f"malloc_zone_pressure_relief released {libc.malloc_zone_pressure_relief(None, 0)} bytes"
    try:
        libc = ctypes.CDLL("libc.so.6")
        return f"malloc_trim -> {libc.malloc_trim(0)}"
    except (OSError, AttributeError):
        return "no allocator trim available"


def machine_state() -> dict[str, Any]:
    yes = subprocess.run(["pgrep", "-x", "yes"], capture_output=True, text=True).stdout.split()
    return {"load": [round(x, 2) for x in os.getloadavg()], "yes_burners": len(yes), "cpus": os.cpu_count()}


# -- data ---------------------------------------------------------------------------


def make_template(root: Path, rows: int) -> Path:
    import duckdb

    path = root / "template.duckdb"
    if path.exists():
        return path
    conn = duckdb.connect(str(path))
    conn.execute(
        f"""create table t as select range id, range % 97 grp, random() * 1000 val,
            md5(range::varchar) s, date '2020-01-01' + (range % 2000)::int d,
            timestamp '2020-01-01' + to_seconds(range) ts, (range % 100000)::decimal(12,2) / 7 dec
            from range({rows})"""
    )
    conn.close()
    return path


def copies(template: Path, root: Path, n: int) -> list[Path]:
    paths = []
    for i in range(n):
        p = root / f"db{i:04d}.duckdb"
        if not p.exists():
            shutil.copyfile(template, p)
        paths.append(p)
    return paths


# -- the memory run (one mode, one N, in its own process) -------------------------------


class Sampler:
    def __init__(self) -> None:
        self.peak_parent = (0, 0)
        self.peak_children = (0, 0)
        self.peak_procs = 0
        self.peak_total = (0, 0)
        self._task: asyncio.Task[None] | None = None

    def read(self) -> dict[str, Any]:
        from dblayer.backends.sqlite_worker import live_worker_pids

        parent = mem_of(os.getpid()) or (0, 0)
        kids = [m for m in (mem_of(p) for p in live_worker_pids()) if m is not None]
        children = (sum(k[0] for k in kids), sum(k[1] for k in kids))
        return {"parent": parent, "children": children, "procs": len(kids)}

    def note(self, r: dict[str, Any]) -> None:
        p, c = r["parent"], r["children"]
        self.peak_parent = tuple(max(a, b) for a, b in zip(self.peak_parent, p))  # type: ignore[assignment]
        self.peak_children = tuple(max(a, b) for a, b in zip(self.peak_children, c))  # type: ignore[assignment]
        total = (p[0] + c[0], p[1] + c[1])
        self.peak_total = tuple(max(a, b) for a, b in zip(self.peak_total, total))  # type: ignore[assignment]
        self.peak_procs = max(self.peak_procs, r["procs"])

    async def _run(self) -> None:
        while True:
            self.note(self.read())
            await asyncio.sleep(0.05)

    def start(self) -> None:
        self.__init__()  # type: ignore[misc]
        self._task = asyncio.ensure_future(self._run())

    async def stop(self) -> None:
        assert self._task is not None
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self.note(self.read())


def _mb(b: int) -> float:
    return round(b / 1e6, 1)


async def memory_child(mode: str, paths: list[Path], cap: int, cycles: int, concurrency: int) -> dict[str, Any]:
    from dblayer import Database, Registry

    if mode.startswith("in-process"):
        import duckdb  # noqa: F401  (the library is a fixed cost: load it before "start")

        from dblayer.backends.duckdb import DuckDB

        cfg = None
        if mode == "in-process+settings":
            cfg = {
                "allocator_flush_threshold": "0MB",
                "allocator_bulk_deallocation_flush_threshold": "0MB",
                "allocator_background_threads": True,
            }

        def make(p: Path) -> Any:
            return DuckDB(p, config=cfg)
    else:
        from dblayer.backends.duckdb_worker import DuckDBWorker

        def make(p: Path) -> Any:
            return DuckDBWorker(p)

    registry = Registry(max_open_connections=cap, idle_timeout=None, max_threads=8)
    dbs = [registry.add(f"db{i}", Database(make(p))) for i, p in enumerate(paths)]
    gc.collect()
    sampler = Sampler()
    start = sampler.read()
    out: dict[str, Any] = {
        "mode": mode, "n": len(paths), "cap": cap,
        "start": {"parent": start["parent"], "children": start["children"]},
        "cycles": [],
    }
    sem = asyncio.Semaphore(concurrency)

    async def use(db: Database, i: int) -> float:
        async with sem:
            t0 = time.perf_counter()
            await db.execute(POINT, {"id": i})
            wake = time.perf_counter() - t0
            await db.execute(ANALYTICAL)
            await db.execute(ROWS_1K)
            return wake

    for c in range(cycles):
        sampler.start()
        t0 = time.perf_counter()
        wakes = await asyncio.gather(*[use(db, i) for i, db in enumerate(dbs)])
        wall = time.perf_counter() - t0
        fds_parent = fd_count()
        from dblayer.backends.sqlite_worker import live_worker_pids

        pids = live_worker_pids()
        fds_children = sum(fd_count(p) or 0 for p in pids) if c == 0 else None
        await sampler.stop()
        awake_before_sleep = registry.stats().awake
        for db in dbs:
            await db.sleep()
        trim = None
        if mode != "in-process" and mode != "worker":
            gc.collect()
        if mode in ("in-process+trim", "in-process+settings"):
            trim = trim_allocator()
        await asyncio.sleep(1.0)
        after = sampler.read()
        wakes_ms = sorted(w * 1000 for w in wakes)
        out["cycles"].append({
            "wall_s": round(wall, 2),
            "peak": {"parent": sampler.peak_parent, "children": sampler.peak_children,
                     "total": sampler.peak_total, "procs": sampler.peak_procs},
            "awake_at_end_of_wake_phase": awake_before_sleep,
            "fds_parent": fds_parent,
            "fds_children": fds_children,
            "procs_at_end_of_wake_phase": len(pids),
            "after_sleep": {"parent": after["parent"], "children": after["children"], "procs": after["procs"]},
            "wake_ms": {"median": round(statistics.median(wakes_ms), 1),
                        "p95": round(wakes_ms[int(len(wakes_ms) * 0.95) - 1], 1)},
            "trim": trim,
        })
    await registry.close()
    return out


# -- the overhead run ----------------------------------------------------------------------

SHAPES = {
    "plain": "select id, val, s from t where id < :n",
    "typed": "select d, ts, dec from t where id < :n",
}


async def overhead(template: Path, root: Path) -> dict[str, Any]:
    from dblayer import Database
    from dblayer.backends.duckdb import DuckDB
    from dblayer.backends.duckdb_worker import DuckDBWorker
    from dblayer.worker.protocol import available_codecs

    variants: dict[str, Any] = {"in-process": lambda p: DuckDB(p)}
    variants["worker-json"] = lambda p: DuckDBWorker(p, codec="json")
    if "msgpack" in available_codecs():
        variants["worker-msgpack"] = lambda p: DuckDBWorker(p, codec="msgpack")
    out: dict[str, Any] = {}
    for name, make in variants.items():
        path = root / f"overhead-{name}.duckdb"
        shutil.copyfile(template, path)
        db = Database(make(path), idle_timeout=None)
        r: dict[str, Any] = {}
        try:
            await db.execute("select 1")
            times = []
            for i in range(400):
                t0 = time.perf_counter()
                await db.execute(POINT, {"id": i})
                times.append(time.perf_counter() - t0)
            r["one_row_us"] = round(statistics.median(times) * 1e6, 1)
            for shape, sql in SHAPES.items():
                for n, reps in ((1000, 60), (100_000, 7)):
                    times = []
                    for _ in range(reps):
                        t0 = time.perf_counter()
                        res = await db.execute(sql, {"n": n}, max_rows=None)
                        times.append(time.perf_counter() - t0)
                        assert len(res.rows) == n
                    r[f"{shape}_{n}_ms"] = round(statistics.median(times) * 1000, 2)
            wakes = []
            for _ in range(15):
                await db.sleep()
                t0 = time.perf_counter()
                await db.execute(POINT, {"id": 1})
                wakes.append(time.perf_counter() - t0)
            r["wake_to_first_result_ms"] = round(statistics.median(wakes) * 1000, 1)
            r["wake_p90_ms"] = round(sorted(wakes)[int(len(wakes) * 0.9)] * 1000, 1)
        finally:
            await db.close()
        out[name] = r
    base = out["in-process"]
    for name, r in out.items():
        if name == "in-process":
            continue
        for shape in SHAPES:
            for n in (1000, 100_000):
                extra_ms = r[f"{shape}_{n}_ms"] - base[f"{shape}_{n}_ms"]
                r[f"{shape}_{n}_extra_us_per_row"] = round(extra_ms * 1000 / n, 3)
    return out


# -- orchestration --------------------------------------------------------------------------


def run_child(args: argparse.Namespace) -> None:
    paths = sorted(Path(args.dir).glob("db*.duckdb"))[: args.child_n]
    res = asyncio.run(memory_child(args.child, paths, args.cap, args.cycles, args.concurrency))
    from dblayer.backends.sqlite_worker import live_worker_pids

    res["leftover_workers"] = live_worker_pids()
    print("RESULT " + json.dumps(res))


def run_memory(args: argparse.Namespace, root: Path, template: Path) -> list[dict[str, Any]]:
    results = []
    copies(template, root, max(args.n))
    for n in args.n:
        for mode in args.modes:
            before = machine_state()
            cmd = [sys.executable, __file__, "--child", mode, "--child-n", str(n), "--dir", str(root),
                   "--cap", str(args.cap), "--cycles", str(args.cycles), "--concurrency", str(args.concurrency)]
            proc = subprocess.run(cmd, capture_output=True, text=True)
            line = next((ln for ln in proc.stdout.splitlines() if ln.startswith("RESULT ")), None)
            if line is None:
                print(proc.stdout, proc.stderr, file=sys.stderr)
                raise SystemExit(f"{mode} n={n} failed")
            res = json.loads(line[7:])
            res["machine_before"] = before
            res["machine_after"] = machine_state()
            res["provisional"] = max(before["load"][0], res["machine_after"]["load"][0]) > args.max_load or bool(before["yes_burners"])
            results.append(res)
            print_memory_row(res)
    return results


def print_memory_row(r: dict[str, Any]) -> None:
    def pc(pair: list[int]) -> str:
        return f"{_mb(pair[0]):.0f}/{_mb(pair[1]):.0f}"

    s = r["start"]
    tag = " PROVISIONAL" if r["provisional"] else ""
    print(f"\n{r['mode']} n={r['n']} cap={r['cap']} load {r['machine_before']['load']} -> {r['machine_after']['load']}{tag}")
    print(f"  start: parent {pc(s['parent'])} MB (RSS/footprint), children {pc(s['children'])}")
    for i, c in enumerate(r["cycles"], 1):
        p = c["peak"]
        a = c["after_sleep"]
        print(
            f"  cycle {i}: peak parent {pc(p['parent'])} children {pc(p['children'])} total {pc(p['total'])} "
            f"procs {p['procs']} | after sleep parent {pc(a['parent'])} children {pc(a['children'])} procs {a['procs']} "
            f"| wake median {c['wake_ms']['median']} ms p95 {c['wake_ms']['p95']} ms | wall {c['wall_s']} s "
            f"| fds parent {c['fds_parent']} children {c['fds_children']}"
            + (f" | {c['trim']}" if c.get("trim") else "")
        )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("what", nargs="?", default="all", choices=["memory", "overhead", "all"])
    ap.add_argument("--n", type=int, nargs="+", default=[50, 150])
    ap.add_argument("--modes", nargs="+", default=MODES, choices=MODES)
    ap.add_argument("--rows", type=int, default=100_000)
    ap.add_argument("--cap", type=int, default=40)
    ap.add_argument("--cycles", type=int, default=3)
    ap.add_argument("--concurrency", type=int, default=8)
    ap.add_argument("--max-load", type=float, default=4.0)
    ap.add_argument("--json", type=Path)
    ap.add_argument("--keep", type=Path, help="directory for the database files (default: a temporary one)")
    ap.add_argument("--child", help=argparse.SUPPRESS)
    ap.add_argument("--child-n", type=int, help=argparse.SUPPRESS)
    ap.add_argument("--dir", help=argparse.SUPPRESS)
    args = ap.parse_args()
    if args.child:
        run_child(args)
        return
    state = machine_state()
    print(f"machine: load average {state['load']}, {state['cpus']} CPUs, {state['yes_burners']} 'yes' processes")
    if state["load"][0] > args.max_load or state["yes_burners"]:
        print("WARNING: machine is busy; figures are provisional")
    root = args.keep or Path(tempfile.mkdtemp(prefix="duckdb-memory-"))
    root.mkdir(parents=True, exist_ok=True)
    out: dict[str, Any] = {"machine": state, "rows": args.rows}
    try:
        template = make_template(root, args.rows)
        if args.what in ("memory", "all"):
            out["memory"] = run_memory(args, root, template)
        if args.what in ("overhead", "all"):
            before = machine_state()
            out["overhead"] = asyncio.run(overhead(template, root))
            out["overhead_machine"] = {"before": before, "after": machine_state()}
            print(f"\noverhead (load {before['load']} -> {out['overhead_machine']['after']['load']}):")
            for name, r in out["overhead"].items():
                print(f"  {name}: {json.dumps(r)}")
    finally:
        if args.keep is None:
            shutil.rmtree(root, ignore_errors=True)
    if args.json:
        args.json.write_text(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
