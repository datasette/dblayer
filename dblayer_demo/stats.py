"""Live numbers for the stats panel: the registry's and the process's."""

from __future__ import annotations

import asyncio
import os
import resource
import sys
import threading
import time
from collections import Counter
from typing import Any

from .fleet import Fleet

__all__ = ["process_stats", "fleet_stats", "state_counts"]


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


async def process_stats() -> dict[str, Any]:
    rss = await current_rss()
    return {
        "threads": threading.active_count(),
        "open_fds": open_fds(),
        "rss_mb": None if rss is None else round(rss / 2**20, 1),
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
    return {"fleet": fleet_stats(fleet), "process": await process_stats()}
