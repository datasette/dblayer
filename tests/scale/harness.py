"""Shared pieces for the scale tests, parametrised by a ``ScaleBackend`` so
that other backends (the worker backend, DuckDB at 1,000 databases) can be
added as one more entry in ``SCALE_BACKENDS``."""

from __future__ import annotations

import bisect
import gc
import itertools
import os
import random
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
import tracemalloc
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from dblayer import Database, Registry
from dblayer.backends.base import Backend
from dblayer.backends.sqlite import SqliteInProcess


@dataclass(frozen=True)
class ScaleBackend:
    id: str
    create_files: Callable[[Path, int], list[Path]]
    """Make ``n`` small databases under the directory (each with a table
    ``t(x integer)`` holding one row); return their paths."""
    make: Callable[[Path], Backend]
    count: int
    """How many databases the scale test uses for this backend."""
    requests: int = 20_000
    """Requests in the load (fewer for backends whose wake is costly)."""
    growth_limit: float | None = 64
    """Bytes per database a wake/sleep cycle may leave behind; ``None``
    reports the figure without asserting it."""
    available: Callable[[], str | None] = lambda: None


def sqlite_files(root: Path, n: int) -> list[Path]:
    root.mkdir(parents=True, exist_ok=True)
    template = root / "template.sqlite"
    conn = sqlite3.connect(template)
    conn.execute("create table t (x integer)")
    conn.execute("insert into t values (0)")
    conn.commit()
    conn.close()
    paths = [root / f"db{i:05d}.db" for i in range(n)]
    for p in paths:
        shutil.copyfile(template, p)
    return paths


def duckdb_files(root: Path, n: int) -> list[Path]:
    import duckdb

    root.mkdir(parents=True, exist_ok=True)
    template = root / "template.duckdb"
    conn = duckdb.connect(str(template))
    conn.execute("create table t (x integer)")
    conn.execute("insert into t values (0)")
    conn.close()  # checkpoints: the template is one self-contained file
    paths = [root / f"db{i:05d}.duckdb" for i in range(n)]
    for p in paths:
        shutil.copyfile(template, p)
    return paths


def _duckdb_backend(path: Path) -> Backend:
    from dblayer.backends.duckdb import DuckDB

    return DuckDB(path)  # type: ignore[return-value]


def _duckdb_available() -> str | None:
    try:
        import duckdb  # noqa: F401
    except ImportError:
        return "duckdb is not installed (uv run --extra duckdb pytest)"
    return None


SCALE_BACKENDS: list[ScaleBackend] = [
    ScaleBackend("sqlite", sqlite_files, lambda p: SqliteInProcess(p), 10_000),
    # Waking DuckDB opens an instance (a few ms), so a shorter load. Its
    # backend object keeps per-database caches after a wake (55-550 B per
    # cycle measured), so that figure is reported, not asserted.
    ScaleBackend(
        "duckdb", duckdb_files, _duckdb_backend, 1_000,
        available=_duckdb_available, requests=3_000, growth_limit=None,
    ),
]


def scale_count(spec: ScaleBackend) -> int:
    """``DBLAYER_SCALE_N`` overrides the count (for a quick local run)."""
    return int(os.environ.get("DBLAYER_SCALE_N", spec.count))


# -- measurements ---------------------------------------------------------------


def current_rss() -> int | None:
    """Resident set size of this process in bytes, now (not the peak)."""
    try:
        with open("/proc/self/statm") as f:
            return int(f.read().split()[1]) * os.sysconf("SC_PAGE_SIZE")
    except OSError:
        pass
    if sys.platform == "darwin":
        out = subprocess.run(
            ["ps", "-o", "rss=", "-p", str(os.getpid())], capture_output=True, text=True
        )
        if out.returncode == 0 and out.stdout.strip():
            return int(out.stdout.strip()) * 1024
    return None


@dataclass
class RegistrationFigures:
    count: int
    seconds: float
    traced_bytes_per_db: float
    rss_bytes_per_db: float | None


def register_all(
    registry: Registry, spec: ScaleBackend, paths: list[Path], *, measure: bool = True
) -> tuple[list[Database], RegistrationFigures]:
    """Create a Database per path and add it to the registry. With
    ``measure``, report the cost per database by tracemalloc (exact Python
    allocations: Database, backend, name, registry entry) and by RSS delta
    (includes allocator overhead, noisier)."""
    names = [f"tenant-{i}/db" for i in range(len(paths))]
    gc.collect()
    rss_before = current_rss() if measure else None
    if measure:
        tracemalloc.start()
        traced_before = tracemalloc.get_traced_memory()[0]
    t0 = time.perf_counter()
    dbs = [registry.add(n, Database(spec.make(p))) for n, p in zip(names, paths)]
    seconds = time.perf_counter() - t0
    traced = 0.0
    if measure:
        gc.collect()
        traced = (tracemalloc.get_traced_memory()[0] - traced_before) / len(paths)
        tracemalloc.stop()
    rss_after = current_rss() if measure else None
    rss = (
        (rss_after - rss_before) / len(paths)
        if rss_before is not None and rss_after is not None
        else None
    )
    return dbs, RegistrationFigures(len(paths), seconds, traced, rss)


def time_registration(spec: ScaleBackend, paths: list[Path], repeat: int = 5) -> float:
    """Best-of-``repeat`` seconds to create and register ``len(paths)``
    databases into a fresh registry.

    The cyclic garbage collector is paused while timing: a collection
    scans every live object, so whether one lands inside a sub-millisecond
    small run made the "linear" comparison fail on CI (9 vs 33 us/db). That
    is Python's cost, not dblayer's; the per-database work is what this
    measures."""
    best = float("inf")
    for _ in range(repeat):
        r = Registry()
        names = [f"n{i}" for i in range(len(paths))]
        gc.collect()
        gc.disable()
        try:
            t0 = time.perf_counter()
            for n, p in zip(names, paths):
                r.add(n, Database(spec.make(p)))
            best = min(best, time.perf_counter() - t0)
        finally:
            gc.enable()
        r.executor.shutdown()
    return best


class Zipf:
    """Ranks 0..n-1 with P(k) proportional to 1/(k+1)**s, shuffled so the
    hot databases are spread across the set."""

    def __init__(self, n: int, s: float, rng: random.Random) -> None:
        weights = [1.0 / (k + 1) ** s for k in range(n)]
        self.cum = list(itertools.accumulate(weights))
        self.order = list(range(n))
        rng.shuffle(self.order)
        self.rng = rng

    def sample(self) -> int:
        x = self.rng.random() * self.cum[-1]
        return self.order[bisect.bisect_left(self.cum, x)]


@dataclass
class Sampler:
    """Samples open descriptors and thread count while a load runs."""

    fds: Callable[[], int]
    peak_fds: int = 0
    min_threads: int = 1 << 30
    max_threads: int = 0
    samples: int = 0
    over_limit: list[int] = field(default_factory=list)

    def sample(self, fd_limit: int) -> None:
        n = self.fds()
        t = threading.active_count()
        self.samples += 1
        self.peak_fds = max(self.peak_fds, n)
        self.min_threads = min(self.min_threads, t)
        self.max_threads = max(self.max_threads, t)
        if n > fd_limit:
            self.over_limit.append(n)
