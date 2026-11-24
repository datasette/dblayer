"""Test helpers."""

from __future__ import annotations

import os


def is_text_type(declared: str) -> bool:
    """True for a text column type as backends report it: ``TEXT`` on SQLite
    and PostgreSQL, ``VARCHAR`` on DuckDB (its name for ``TEXT``)."""
    t = declared.lower()
    return "text" in t or "char" in t or "string" in t


def count_open_fds() -> int:
    """Number of open file descriptors in this process.

    Counts entries in ``/proc/self/fd`` where it exists (Linux) and otherwise
    ``/dev/fd`` (macOS). Listing the directory itself opens one descriptor;
    that is the same on every call, so differences between calls are exact.
    """
    for path in ("/proc/self/fd", "/dev/fd"):
        if os.path.isdir(path):
            return len(os.listdir(path))
    raise RuntimeError("no /proc/self/fd or /dev/fd on this platform")


class CountingBackend:
    """Wraps a backend and counts ``open()`` and ``close()`` calls. Has a
    ``live`` count of backend-level opens not yet followed by ``close()``:
    ``closes_since_open`` says whether the backend was released after the
    most recent connection was opened."""

    def __init__(self, inner: object) -> None:
        self.inner = inner
        self.capabilities = inner.capabilities  # type: ignore[attr-defined]
        self.dialect = inner.dialect  # type: ignore[attr-defined]
        self.opens = 0
        self.closes = 0
        self.released = True

    @property
    def executor(self) -> object:
        return getattr(self.inner, "executor", None)

    @executor.setter
    def executor(self, value: object) -> None:
        self.inner.executor = value  # type: ignore[attr-defined]

    async def open(self, *, write: bool) -> object:
        self.opens += 1
        self.released = False
        return await self.inner.open(write=write)  # type: ignore[attr-defined]

    def change_hint(self) -> object:
        return self.inner.change_hint()  # type: ignore[attr-defined]

    async def close(self) -> None:
        self.closes += 1
        self.released = True
        await self.inner.close()  # type: ignore[attr-defined]
