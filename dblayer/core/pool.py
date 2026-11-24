"""Per-database connection pool.

* Up to ``max_read_connections`` read connections (default 3), checked out by
  one caller at a time and handed back afterwards.
* One write connection behind a FIFO ``asyncio.Lock``: holding the write
  connection *is* holding the database's write slot, so writes are
  serialised in the event loop with no writer thread.
* Reset on return: ``rollback()`` is called on every returned connection
  (backends make that a no-op when nothing is open). A connection whose reset
  fails, or that the caller marks ``discard``, is closed instead of reused.
* Only checked-in connections are ever closed. ``close_idle()`` closes idle
  connections and bumps a generation counter so that connections still in
  use at that moment are closed when they come back.

The pool counts ``open`` connections (including ones being opened or
closed) and ``users`` (callers between the start of ``acquire`` and the end
of ``release``). When both reach zero the pool tells its database, which
then goes to sleep: it closes the backend (where
``capabilities.sleep_releases_everything``) and drops the pool, so an asleep
database holds no pool at all.

A pool belongs to the event loop it was created on; its database replaces it
when used from another loop (see ``Database._use_pool``).
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ..backends.base import Backend, BackendConnection
from ..errors import DatabaseClosed

if TYPE_CHECKING:
    from .database import Database

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
    __slots__ = ("conn", "write", "generation", "last_used", "pool")

    def __init__(
        self, conn: BackendConnection, write: bool, generation: int, pool: "Pool"
    ) -> None:
        self.conn = conn
        self.write = write
        self.generation = generation
        self.last_used = time.monotonic()
        self.pool = pool


class Pool:
    """Created by its ``Database`` on first use and dropped when the
    database goes to sleep; cumulative open/close counts live on the
    database."""

    __slots__ = (
        "_db",
        "_backend",
        "loop",
        "max_read_connections",
        "_read_slots",
        "_write_slot",
        "_idle_read",
        "_idle_write",
        "_in_use",
        "_generation",
        "_closed",
        "open",
        "users",
    )

    def __init__(self, db: "Database", loop: asyncio.AbstractEventLoop | None = None) -> None:
        self._db = db
        self._backend: Backend = db._backend
        self.loop = loop
        self.max_read_connections = db.max_read_connections
        self._read_slots = asyncio.Semaphore(self.max_read_connections)
        self._write_slot = asyncio.Lock()
        self._idle_read: list[_Entry] = []
        self._idle_write: _Entry | None = None
        self._in_use: dict[int, _Entry] = {}
        self._generation = 0
        self._closed = False
        self.open = 0
        """Connections open, being opened or being closed."""
        self.users = 0
        """Callers inside ``acquire`` or holding a connection."""

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
        self.users += 1
        try:
            slot = self._write_slot if write else self._read_slots
            await slot.acquire()
            try:
                if self._closed:
                    raise DatabaseClosed("database is closed")
                entry = self._take_idle(write)
                if entry is None:
                    entry = await self._open_entry(write)
            except BaseException:
                slot.release()
                raise
        except BaseException:
            self.users -= 1
            self._check_empty()
            raise
        self._in_use[id(entry.conn)] = entry
        return entry.conn

    async def _open_entry(self, write: bool) -> _Entry:
        await self._db._ensure_awake()
        if self._closed:
            raise DatabaseClosed("database is closed")
        self.open += 1
        try:
            conn = await self._backend.open(write=write)
        except BaseException:
            self.open -= 1
            raise
        self._db._opened_total += 1
        return _Entry(conn, write, self._generation, self)

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
                    self._check_in(entry)
                else:
                    await self._close_entry(entry)
            finally:
                slot.release()
                self.users -= 1
                self._check_empty()

    def _check_in(self, entry: _Entry) -> None:
        if entry.write:
            self._idle_write = entry
        else:
            self._idle_read.append(entry)

    def _reusable(self, entry: _Entry) -> bool:
        return not self._closed and entry.generation == self._generation

    def _take_idle(self, write: bool) -> _Entry | None:
        if write:
            entry, self._idle_write = self._idle_write, None
            return entry
        return self._idle_read.pop() if self._idle_read else None

    def _check_empty(self) -> None:
        if self.open == 0 and self.users == 0:
            self._db._pool_empty(self)

    @property
    def empty(self) -> bool:
        return self.open == 0 and self.users == 0

    # -- shutdown ------------------------------------------------------------

    def _take_all_idle(self) -> list[_Entry]:
        entries = self._idle_read
        self._idle_read = []
        if self._idle_write is not None:
            entries.append(self._idle_write)
            self._idle_write = None
        return entries

    async def close_idle(self) -> int:
        """Close every checked-in connection and arrange for connections in
        use now to be closed when they are returned. Returns how many were
        closed now. The pool stays usable."""
        self._generation += 1
        entries = self._take_all_idle()
        if entries:
            await asyncio.gather(*(self._close_entry(e) for e in entries))
        return len(entries)

    async def close(self) -> None:
        """Close for good: idle connections now, in-use ones on return.
        Further ``acquire`` calls raise ``DatabaseClosed``."""
        self._closed = True
        await self.close_idle()

    async def _close_entry(self, entry: _Entry) -> None:
        try:
            await entry.conn.close()
        except Exception:
            pass
        finally:
            self.open -= 1
            self._db._closed_total += 1
            self._check_empty()

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
            opened_total=self._db._opened_total,
            closed_total=self._db._closed_total,
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
