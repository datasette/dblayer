"""The shared thread pool that blocking drivers (SQLite, DuckDB) run on.

A module-level default executor is created lazily. Milestone 3's registry is
expected to own the pool and inject it: backends accept ``executor=`` and
otherwise resolve ``get_default_executor()`` at call time, so swapping the
default with ``set_default_executor()`` takes effect for backends that were
not given an explicit one.
"""

from __future__ import annotations

import asyncio
import contextvars
import functools
import threading
from collections.abc import Callable
from concurrent.futures import Executor, ThreadPoolExecutor
from typing import Any

__all__ = [
    "DEFAULT_MAX_THREADS",
    "get_default_executor",
    "set_default_executor",
    "run_blocking",
]

DEFAULT_MAX_THREADS = 8

_lock = threading.Lock()
_default: Executor | None = None


def get_default_executor() -> Executor:
    global _default
    if _default is None:
        with _lock:
            if _default is None:
                _default = ThreadPoolExecutor(
                    max_workers=DEFAULT_MAX_THREADS, thread_name_prefix="dblayer"
                )
    return _default


def set_default_executor(executor: Executor | None) -> Executor | None:
    """Replace the default executor; returns the previous one (not shut
    down). ``None`` means "create a fresh one lazily next time"."""
    global _default
    with _lock:
        previous, _default = _default, executor
    return previous


async def run_blocking[T](
    executor: Executor | None,
    fn: Callable[..., T],
    *args: Any,
    on_cancel: Callable[[], None] | None = None,
) -> T:
    """Run ``fn(*args)`` on the executor with the caller's context variables.

    If the awaiting task is cancelled while ``fn`` runs, ``on_cancel`` is
    called (e.g. ``sqlite3.Connection.interrupt``) and this coroutine waits
    for ``fn`` to finish before re-raising ``CancelledError``. That guarantees
    that whatever ``fn`` was using (a connection) is no longer in use by a
    thread once this returns or raises, so the pool can hand it to someone
    else safely.
    """
    loop = asyncio.get_running_loop()
    ctx = contextvars.copy_context()
    fut = loop.run_in_executor(
        executor or get_default_executor(), functools.partial(ctx.run, fn, *args)
    )
    # asyncio.wait, not asyncio.shield: cancelling the wait leaves ``fut``
    # alone, and a shielded future that fails after its waiter was
    # cancelled is logged by asyncio as "exception in shielded future" (an
    # ERROR line for every cancelled query).
    try:
        await asyncio.wait((fut,))
    except asyncio.CancelledError:
        if on_cancel is not None and not fut.done():
            try:
                on_cancel()
            except Exception:
                pass
        while not fut.done():
            try:
                await asyncio.wait((fut,))
            except asyncio.CancelledError:
                continue
        if not fut.cancelled():
            fut.exception()  # mark retrieved
        raise
    return fut.result()
