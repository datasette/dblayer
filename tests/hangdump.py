"""Hang diagnostics for CI: ``DBLAYER_HANG_DUMP_S=240`` arms a timer for
each test (setup, call and teardown together). If the test is still running
when it fires, this prints, to stderr, the test's id, every thread's stack,
every pending asyncio task's stack on every running event loop, and this
process's children (pid, state, RSS, command line). It does not stop the
test: ``pytest-timeout`` (``--timeout``, set longer) does that. With
``DBLAYER_HANG_DUMP_FILE`` set, the dump is appended to that file too.

The dump runs on the timer's thread while the loop keeps running, so the
task list is a snapshot and may be slightly inconsistent; it is for reading,
not for asserting."""

from __future__ import annotations

import asyncio
import faulthandler
import gc
import os
import subprocess
import sys
import threading
import traceback


def _children() -> str:
    try:
        out = subprocess.run(
            ["ps", "-A", "-o", "pid=,ppid=,stat=,rss=,etime=,command="],
            capture_output=True, text=True, timeout=10,
        ).stdout
    except Exception as e:  # noqa: BLE001
        return f"(ps failed: {e!r})"
    rows = [line.split(None, 5) for line in out.splitlines() if line.strip()]
    by_parent: dict[str, list[list[str]]] = {}
    for r in rows:
        if len(r) >= 6:
            by_parent.setdefault(r[1], []).append(r)
    lines: list[str] = []
    stack = [(str(os.getpid()), 0)]
    while stack:
        pid, depth = stack.pop()
        for r in by_parent.get(pid, []):
            lines.append(f"{'  ' * depth}{r[0]} {r[2]} rss={int(r[3]) // 1024}MB up={r[4]} {r[5][:160]}")
            stack.append((r[0], depth + 1))
    return "\n".join(lines) or "(no child processes)"


def _tasks() -> str:
    loops = [o for o in gc.get_objects() if isinstance(o, asyncio.AbstractEventLoop)]
    parts: list[str] = []
    for loop in loops:
        if loop.is_closed() or not loop.is_running():
            continue
        try:
            tasks = list(asyncio.all_tasks(loop))
        except RuntimeError as e:  # set changed size during iteration
            parts.append(f"(could not list tasks: {e!r})")
            continue
        parts.append(f"--- event loop {loop!r}: {len(tasks)} pending tasks ---")
        for t in tasks[:200]:
            parts.append(f"* {t!r}")
            try:
                for frame in t.get_stack(limit=20):
                    parts.extend("    " + s.rstrip() for s in traceback.format_stack(frame, limit=1))
                coro = t.get_coro()
                # Follow the await chain (get_stack shows only the outermost frame).
                depth = 0
                while coro is not None and depth < 30:
                    frame = getattr(coro, "cr_frame", None) or getattr(coro, "gi_frame", None)
                    if frame is not None:
                        parts.append(f"    awaiting {frame.f_code.co_qualname} "
                                     f"({frame.f_code.co_filename}:{frame.f_lineno})")
                    coro = getattr(coro, "cr_await", None) or getattr(coro, "gi_yieldfrom", None)
                    depth += 1
            except Exception as e:  # noqa: BLE001
                parts.append(f"    (stack unavailable: {e!r})")
    return "\n".join(parts) or "(no running event loop)"


def _write(err, nodeid: str, seconds: float) -> None:  # type: ignore[no-untyped-def]
    err.write(f"\n\n+++ DBLAYER HANG DUMP: {nodeid} still running after {seconds:.0f} s +++\n")
    err.write("--- children of this process ---\n" + _children() + "\n")
    err.write("--- asyncio tasks ---\n" + _tasks() + "\n")
    err.write("--- threads ---\n")
    err.flush()
    faulthandler.dump_traceback(file=err, all_threads=True)
    err.write(f"+++ END HANG DUMP: {nodeid} +++\n\n")
    err.flush()


def dump(nodeid: str, seconds: float) -> None:
    # pytest captures stderr (shown if the test then fails or times out);
    # DBLAYER_HANG_DUMP_FILE also keeps a copy that survives a killed run.
    _write(sys.__stderr__ or sys.stderr, nodeid, seconds)
    path = os.environ.get("DBLAYER_HANG_DUMP_FILE")
    if path:
        with open(path, "a") as f:
            _write(f, nodeid, seconds)


def seconds() -> float | None:
    value = os.environ.get("DBLAYER_HANG_DUMP_S")
    return float(value) if value else None


def arm(nodeid: str) -> threading.Timer | None:
    s = seconds()
    if not s:
        return None
    timer = threading.Timer(s, dump, args=(nodeid, s))
    timer.daemon = True
    timer.start()
    return timer
