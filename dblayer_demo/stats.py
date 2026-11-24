"""Live numbers for the stats panel: the registry's and the process's."""

from __future__ import annotations

import asyncio
import ctypes
import os
import resource
import sys
import threading
import time
from collections import Counter
from typing import Any

from .fleet import Fleet

__all__ = ["process_stats", "worker_stats", "process_memory", "fleet_stats", "state_counts"]


def open_fds() -> int | None:
    for d in ("/proc/self/fd", "/dev/fd"):
        try:
            return len(os.listdir(d)) - 1  # minus the listing's own descriptor
        except OSError:
            continue
    return None


async def current_rss() -> int | None:
    """Resident set size now, in bytes (``/proc`` on Linux, ``ps`` elsewhere)."""
    try:
        with open("/proc/self/statm") as f:
            return int(f.read().split()[1]) * os.sysconf("SC_PAGE_SIZE")
    except OSError:
        pass
    try:
        proc = await asyncio.create_subprocess_exec(
            "ps", "-o", "rss=", "-p", str(os.getpid()),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
        )
        out, _ = await proc.communicate()
        return int(out.strip()) * 1024
    except (OSError, ValueError):
        return None


def peak_rss() -> int:
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak if sys.platform == "darwin" else peak * 1024  # bytes vs KiB


_STARTED = time.time()


class _RusageInfoV2(ctypes.Structure):
    _fields_ = [("ri_uuid", ctypes.c_uint8 * 16)] + [
        (n, ctypes.c_uint64)
        for n in (
            "ri_user_time", "ri_system_time", "ri_pkg_idle_wkups", "ri_interrupt_wkups",
            "ri_pageins", "ri_wired_size", "ri_resident_size", "ri_phys_footprint",
            "ri_proc_start_abstime", "ri_proc_exit_abstime", "ri_child_user_time",
            "ri_child_system_time", "ri_child_pkg_idle_wkups", "ri_child_interrupt_wkups",
            "ri_child_pageins", "ri_child_elapsed_abstime", "ri_diskio_bytesread",
            "ri_diskio_byteswritten",
        )
    ] + [("_spare", ctypes.c_uint8 * 512)]  # the kernel writes the whole V2 struct: never short


_libproc: Any = None
if sys.platform == "darwin":
    try:
        _libproc = ctypes.CDLL("/usr/lib/libproc.dylib")
        _libproc.proc_pid_rusage.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_void_p]
    except OSError:
        _libproc = None


def process_memory(pid: int) -> tuple[int, int] | None:
    """``(resident, footprint)`` bytes of a process, or None if it is gone.

    Footprint is what the process really costs: on macOS
    ``phys_footprint`` (Activity Monitor's "Memory": dirty private pages,
    compressed and swapped included; RSS there also counts freed pages the
    allocator has marked reusable, so it stays high after memory is
    returned), on Linux PSS from ``smaps_rollup``."""
    if _libproc is not None:
        info = _RusageInfoV2()
        if _libproc.proc_pid_rusage(pid, 2, ctypes.byref(info)) != 0:
            return None
        return int(info.ri_resident_size), int(info.ri_phys_footprint)
    try:
        with open(f"/proc/{pid}/status") as f:
            rss = next(int(line.split()[1]) * 1024 for line in f if line.startswith("VmRSS:"))
    except (OSError, StopIteration):
        return None
    try:
        with open(f"/proc/{pid}/smaps_rollup") as f:
            pss = next(int(line.split()[1]) * 1024 for line in f if line.startswith("Pss:"))
    except (OSError, StopIteration):
        pss = rss
    return rss, pss


def _mb(n: int | None) -> float | None:
    return None if n is None else round(n / 2**20, 1)


def worker_stats() -> dict[str, Any]:
    """The worker processes this app started (SQLite and DuckDB workers):
    how many are alive and their memory, summed."""
    from dblayer.backends.sqlite_worker import live_worker_pids

    mems = [m for m in (process_memory(p) for p in live_worker_pids()) if m is not None]
    return {
        "processes": len(mems),
        "rss_mb": _mb(sum(m[0] for m in mems)),
        "footprint_mb": _mb(sum(m[1] for m in mems)),
    }


async def process_stats() -> dict[str, Any]:
    mem = process_memory(os.getpid())
    rss = mem[0] if mem is not None else await current_rss()
    return {
        "threads": threading.active_count(),
        "open_fds": open_fds(),
        "rss_mb": _mb(rss),
        "footprint_mb": _mb(mem[1]) if mem is not None else None,
        "peak_rss_mb": round(peak_rss() / 2**20, 1),
        "uptime_s": round(time.time() - _STARTED),
    }


def state_counts(fleet: Fleet) -> dict[str, Counter[str]]:
    """``{kind: Counter(state)}`` over every member: one property read each."""
    out: dict[str, Counter[str]] = {}
    for m in fleet.members.values():
        out.setdefault(m.kind, Counter())[m.state] += 1
    return out


def fleet_stats(fleet: Fleet | None) -> dict[str, Any]:
    if fleet is None:
        return {}
    s = fleet.registry.stats()
    asleep = s.databases - s.awake - s.cold
    return {
        "fleet_size": len(fleet.members),
        "registered": s.databases,
        "unavailable": len(fleet.members) - s.databases,
        "awake": s.awake,
        "asleep": asleep,
        "cold": s.cold,
        "open_connections": s.open_connections,
        "max_open_connections": s.max_open_connections,
        "idle_connections": s.idle_connections,
        "waiting": s.waiting,
        "wakes": s.wakes,
        "sleeps": s.sleeps,
        "evictions": s.evictions,
        "cap_evictions": s.cap_evictions,
        "idle_closes": s.idle_closes,
        "max_threads": s.max_threads,
        "idle_timeout_s": fleet.registry.idle_timeout,
        "startup_s": round(fleet.startup_seconds, 3),
    }


async def all_stats(fleet: Fleet | None) -> dict[str, Any]:
    process = await process_stats()
    workers = worker_stats()
    total = {
        k: None if process[k] is None or workers[k] is None else round(process[k] + workers[k], 1)
        for k in ("rss_mb", "footprint_mb")
    }
    return {"fleet": fleet_stats(fleet), "process": process, "workers": workers, "total": total}
