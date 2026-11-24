"""Engine factories: what a worker process runs.

An ``open`` message names an *engine* as ``"module:callable"``; the worker
imports it and calls ``factory(path, **config)`` to get an in-process
``Backend`` (``config`` is plain data, so it crosses the wire). The worker
then talks to that backend only through the ``Backend`` /
``BackendConnection`` protocols and the optional ``SupportsOperations`` /
``SupportsBatch``, so any in-process backend can run in a worker.

The parent calls the same factory locally (``WorkerBackend``) to learn the
engine's ``capabilities``, ``dialect`` and ``change_hint()``; factories must
therefore open nothing when called, and defer anything with side effects
(importing init hooks) to the first connection.

Engine-specific limits stay here, behind the engine boundary:
``sqlite`` applies ``PRAGMA hard_heap_limit``, ``Connection.setlimit()`` and
the ``module:function`` init hooks, each of which receives the raw
``sqlite3.Connection``.
"""

from __future__ import annotations

import importlib
import sqlite3
import threading
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from ..backends.base import Backend
from ..backends.sqlite import SqliteInProcess
from ..errors import QueryError

__all__ = ["SQLITE_ENGINE", "sqlite", "SQL_LIMITS", "load_engine", "import_hook"]

SQLITE_ENGINE = "dblayer.worker.engines:sqlite"

SQL_LIMITS: dict[str, int] = {
    name[len("SQLITE_LIMIT_") :].lower(): getattr(sqlite3, name)
    for name in dir(sqlite3)
    if name.startswith("SQLITE_LIMIT_")
}
"""``sql_limits`` keys (``length``, ``sql_length``, ``expr_depth``,
``like_pattern_length``...) and their ``sqlite3.SQLITE_LIMIT_*`` constant."""

_heap_lock = threading.Lock()
_heap_limit_set: int | None = None


def import_hook(name: str) -> Callable[..., Any]:
    module, sep, attr = name.partition(":")
    if not sep or not module or not attr:
        raise QueryError(f"init hook {name!r} must be 'module:function'", "INIT")
    try:
        obj: Any = importlib.import_module(module)
        for part in attr.split("."):
            obj = getattr(obj, part)
    except (ImportError, AttributeError) as e:
        raise QueryError(f"cannot import {name!r}: {e}", "INIT") from e
    if not callable(obj):
        raise QueryError(f"{name!r} is not callable", "INIT")
    return obj


def load_engine(name: str) -> Callable[..., Backend]:
    return import_hook(name)


def sqlite(
    path: str,
    *,
    init: Sequence[str] = (),
    sql_limits: Mapping[str, int] | None = None,
    memory_limit_mb: int | None = None,
    immutable: bool = False,
    create: bool = True,
    busy_timeout: float = 5.0,
) -> SqliteInProcess:
    """The in-process SQLite backend, with the worker's SQLite-only limits
    applied to every new connection: ``sql_limits`` with ``setlimit()``,
    ``memory_limit_mb`` as ``PRAGMA hard_heap_limit`` (process-wide), then
    the ``init`` hooks in order."""
    limits = dict(sql_limits or {})
    for key in limits:
        if key not in SQL_LIMITS:
            raise ValueError(f"unknown SQL limit {key!r}; known: {', '.join(sorted(SQL_LIMITS))}")
    names = tuple(init)
    hooks: list[Callable[[sqlite3.Connection], Any]] | None = None

    def on_connect(raw: sqlite3.Connection) -> None:
        nonlocal hooks
        global _heap_limit_set
        if memory_limit_mb:
            n = int(memory_limit_mb) * 1024 * 1024
            with _heap_lock:
                if _heap_limit_set != n:
                    raw.execute(f"pragma hard_heap_limit = {n}")
                    _heap_limit_set = n
        for key, value in limits.items():
            raw.setlimit(SQL_LIMITS[key], int(value))
        if hooks is None:
            hooks = [import_hook(n) for n in names]
        for hook in hooks:
            hook(raw)

    needs_init = bool(names or limits or memory_limit_mb)
    return SqliteInProcess(
        path,
        immutable=immutable,
        create=create,
        busy_timeout=busy_timeout,
        init=on_connect if needs_init else None,
    )
