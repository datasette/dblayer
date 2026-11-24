"""Per-database connection pool.

* Up to ``max_read_connections`` read connections (default 3), checked out by
  one caller at a time and handed back afterwards.
* One write connection behind a FIFO ``asyncio.Lock``: holding the write
  connection *is* holding the database's write slot, so writes are
  serialised in the event loop with no writer thread.
* Reset on return: ``rollback()`` is called on every returned connection
  (backends make that a no-op when nothing is open). A connection whose reset
  fails, or that the caller marks ``discard``, is closed instead of reused.
* Only checked-in connections are ever closed. ``sleep()`` closes idle
  connections and bumps a generation counter so that connections still in
  use at that moment are closed when they come back.

Milestone 3 adds idle timeout, a global cap and registry integration; the
``last_used`` timestamp on each entry and ``close_idle()`` are the hooks for
that.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

from ..backends.base import Backend, BackendConnection
from ..errors import DatabaseClosed

__all__ = ["Pool", "PoolStats", "DEFAULT_MAX_READ_CONNECTIONS"]

DEFAULT_MAX_READ_CONNECTIONS = 3


@dataclass(frozen=True, slots=True)
class PoolStats:
    open_read: int
    open_write: int
    idle_read: int
    idle_write: int
    in_use_read: int
    in_use_write: int
    opened_total: int
    closed_total: int

    @property
    def open_total(self) -> int:
        return self.open_read + self.open_write


class _Entry:
    __slots__ = ("conn", "write", "generation", "last_used")

    def __init__(self, conn: BackendConnection, write: bool, generation: int) -> None:
        self.conn = conn
        self.write = write
        self.generation = generation
        self.last_used = time.monotonic()


class Pool:
    def __init__(
        self,
        backend: Backend,
        *,
        max_read_connections: int = DEFAULT_MAX_READ_CONNECTIONS,
    ) -> None:
        if max_read_connections < 1:
            raise ValueError("max_read_connections must be at least 1")
        self._backend = backend
        self.max_read_connections = max_read_connections
        # asyncio primitives bind to the running loop lazily (3.10+), so
        # creating them here opens nothing and needs no loop.
        self._read_slots = asyncio.Semaphore(max_read_connections)
        self._write_slot = asyncio.Lock()
        self._idle_read: list[_Entry] = []
        self._idle_write: _Entry | None = None
        self._in_use: dict[int, _Entry] = {}
        self._generation = 0
        self._closed = False
        self._opened_total = 0
        self._closed_total = 0

    # -- checkout ------------------------------------------------------------

    @asynccontextmanager
    async def connection(self, *, write: bool) -> AsyncIterator[BackendConnection]:
        """Check out a connection for the duration of the block."""
        conn = await self.acquire(write=write)
        discard = False
        try:
            yield conn
        except BaseException as e:
            discard = _should_discard(e)
            raise
        finally:
            await self.release(conn, discard=discard)

    async def acquire(self, *, write: bool) -> BackendConnection:
        """Check out a connection. Every successful ``acquire`` must be
        paired with exactly one ``release``. A write checkout holds the write
        slot until released."""
        if self._closed:
            raise DatabaseClosed("database is closed")
        slot = self._write_slot if write else self._read_slots
        await slot.acquire()
        try:
            if self._closed:
                raise DatabaseClosed("database is closed")
            entry = self._take_idle(write)
            if entry is None:
                conn = await self._backend.open(write=write)
                self._opened_total += 1
                entry = _Entry(conn, write, self._generation)
        except BaseException:
            slot.release()
            raise
        self._in_use[id(entry.conn)] = entry
        return entry.conn

    async def release(self, conn: BackendConnection, *, discard: bool = False) -> None:
        entry = self._in_use.pop(id(conn), None)
        if entry is None:
            raise RuntimeError("connection was not checked out from this pool")
        slot = self._write_slot if entry.write else self._read_slots
        reuse = False
        try:
            if not discard and self._reusable(entry):
                try:
                    await conn.rollback()  # reset on return
                    reuse = True
                except Exception:
                    reuse = False
        finally:
            try:
                if reuse and self._reusable(entry):
                    entry.last_used = time.monotonic()
                    if entry.write:
                        self._idle_write = entry
                    else:
                        self._idle_read.append(entry)
                else:
                    await self._close_entry(entry)
            finally:
                slot.release()

    def _reusable(self, entry: _Entry) -> bool:
        return not self._closed and entry.generation == self._generation

    def _take_idle(self, write: bool) -> _Entry | None:
        if write:
            entry, self._idle_write = self._idle_write, None
            return entry
        return self._idle_read.pop() if self._idle_read else None

    # -- shutdown ------------------------------------------------------------

    async def close_idle(self) -> int:
        """Close every checked-in connection and arrange for connections in
        use now to be closed when they are returned. Returns how many were
        closed now. The pool stays usable."""
        self._generation += 1
        entries = self._idle_read
        self._idle_read = []
        if self._idle_write is not None:
            entries.append(self._idle_write)
            self._idle_write = None
        for entry in entries:
            await self._close_entry(entry)
        return len(entries)

    async def close(self) -> None:
        """Close for good: idle connections now, in-use ones on return.
        Further ``acquire`` calls raise ``DatabaseClosed``."""
        self._closed = True
        await self.close_idle()

    async def _close_entry(self, entry: _Entry) -> None:
        self._closed_total += 1
        try:
            await entry.conn.close()
        except Exception:
            pass

    # -- introspection -------------------------------------------------------

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def in_use(self) -> int:
        return len(self._in_use)

    def stats(self) -> PoolStats:
        in_use_write = sum(1 for e in self._in_use.values() if e.write)
        in_use_read = len(self._in_use) - in_use_write
        idle_write = 1 if self._idle_write is not None else 0
        return PoolStats(
            open_read=len(self._idle_read) + in_use_read,
            open_write=idle_write + in_use_write,
            idle_read=len(self._idle_read),
            idle_write=idle_write,
            in_use_read=in_use_read,
            in_use_write=in_use_write,
            opened_total=self._opened_total,
            closed_total=self._closed_total,
        )


def _should_discard(exc: BaseException) -> bool:
    """Errors from the SQL itself leave the connection fine; anything else
    (driver crash, programming error) means we stop trusting it.
    Cancellation is fine because backends wait for the blocking call to
    finish before letting ``CancelledError`` out."""
    from ..errors import DblayerError, DatabaseClosed as _Closed, WorkerCrashed

    if isinstance(exc, (asyncio.CancelledError, GeneratorExit)):
        return False
    if isinstance(exc, (WorkerCrashed, _Closed)):
        return True
    return not isinstance(exc, DblayerError)
