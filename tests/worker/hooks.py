"""Init hooks imported by worker processes in the tests (``open`` takes
``module:function`` names; each is called with every new raw connection)."""

from __future__ import annotations

import os
import sqlite3


def register_functions(conn: sqlite3.Connection) -> None:
    conn.create_function("reverse_text", 1, lambda s: None if s is None else s[::-1], deterministic=True)
    conn.create_function("worker_pid", 0, os.getpid)


def hog(conn: sqlite3.Connection) -> None:
    """``hog(mb)`` allocates that much Python memory inside the worker,
    outside SQLite's own heap limit."""

    def allocate(mb: int) -> int:
        blocks = [bytearray(1024 * 1024) for _ in range(mb)]
        return len(blocks)

    conn.create_function("hog", 1, allocate)


def fail(conn: sqlite3.Connection) -> None:
    raise RuntimeError("this init hook always fails")
