"""Connection bookkeeping shared by many databases: idle timeout and the
global connection cap.

A ``Hub`` tracks every *checked-in* (idle) connection of the databases
attached to it, in least-recently-used order. It costs nothing per asleep
database: an asleep database has no connections, so it has no entries here,
and there is one timer for the whole hub, not one per database.

* **Idle timeout.** Idle connections are kept in one ``OrderedDict`` per
  distinct timeout value (normally just one), in check-in order, so the
  oldest entry of each is the next to expire. One ``loop.call_later`` timer
  is armed for the earliest expiry; when it fires, expired entries are taken
  out of their pools and closed. Check-in and check-out are O(1).

* **Global cap.** ``max_open_connections`` bounds the connections open
  across every database of the hub (counting ones being opened or closed,
  so the bound holds for file descriptors too). A pool calls ``reserve()``
  before opening a connection. At the cap, the least recently used idle
  connection in any database is closed and its place handed over; if none
  is idle the caller waits (FIFO). A place freed while callers wait goes
  straight to the first of them, and a connection checked in while callers
  wait is closed at once to make room, rather than kept idle.

A ``Registry`` owns one hub. Databases not in a registry share a
module-level default hub (``default_hub()``), so idle timeout works for
standalone databases too.

A hub is used from one event loop at a time. Its timer follows the loop
that last checked a connection in.
"""

from __future__ import annotations

import asyncio
import time
from collections import OrderedDict, deque
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from .common import spawn

if TYPE_CHECKING:
    from .database import Database
    from .pool import _Entry

__all__ = ["Hub", "default_hub", "DEFAULT_IDLE_TIMEOUT", "LifecycleHook"]

type LifecycleHook = Callable[["Database"], Awaitable[object]]

DEFAULT_IDLE_TIMEOUT = 30.0


class Hub:
    def __init__(
        self,
        *,
        idle_timeout: float | None = DEFAULT_IDLE_TIMEOUT,
        max_open_connections: int | None = None,
    ) -> None:
        if idle_timeout is not None and idle_timeout < 0:
            raise ValueError("idle_timeout must be >= 0 or None")
        if max_open_connections is not None and max_open_connections < 1:
            raise ValueError("max_open_connections must be at least 1 or None")
        self.idle_timeout = idle_timeout
        self.max_open_connections = max_open_connections
        self.open = 0
        """Connections open, opening or closing, across the hub."""
        self._waiters: deque[asyncio.Future[None]] = deque()
        self.cap_evictions = 0
        """Connections closed to make room under the cap: idle ones taken
        by ``reserve()``, and ones closed on check-in for a waiter."""
        self.on_wake: LifecycleHook | None = None
        """Async ``(db) -> None``, run before a database's first connection
        opens (under its wake lock)."""
        self.on_sleep: LifecycleHook | None = None
        """Async ``(db) -> None``, run after a database's last connection
        (and, where allowed, its backend) has closed."""
        self.wakes = 0
        self.sleeps = 0
        self.awake = 0
        """Databases awake now."""
        self.evictions = 0
        """Databases moved to cold by ``Registry.evict``."""
        self.cold = 0
        """Databases cold now."""
        """Default for databases that do not set their own."""
        self._idle: dict[float | None, OrderedDict[_Entry, None]] = {}
        self._timer: asyncio.TimerHandle | None = None
        self._timer_at = 0.0
        self._timer_loop: asyncio.AbstractEventLoop | None = None
        self.idle_closes = 0
        """Connections closed by the idle timeout."""

    # -- global cap ------------------------------------------------------------

    async def reserve(self) -> None:
        """Take one place under the cap; called before opening a connection.
        The caller must ``release()`` it once the connection is closed (or
        failed to open)."""
        cap = self.max_open_connections
        if cap is None or (self.open < cap and not self._has_waiters()):
            self.open += 1
            return
        entry = self.pop_lru()
        if entry is not None:
            # Close it and take its place (``open`` is unchanged).
            self.cap_evictions += 1
            try:
                await entry.pool._close_entry(entry, release=False)
            except BaseException:
                self.release()
                raise
            return
        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._waiters.append(fut)
        try:
            await fut  # resolved by release(): its place is handed to us
        except asyncio.CancelledError:
            if fut.done() and not fut.cancelled():
                self.release()  # handed over just as we were cancelled
            else:
                try:
                    self._waiters.remove(fut)
                except ValueError:
                    pass
            raise

    def release(self) -> None:
        """Give a place back: to the first waiter if any, else to the pool
        of free places."""
        while self._waiters:
            w = self._waiters.popleft()
            if not w.done() and not w.get_loop().is_closed():
                w.set_result(None)
                return
        self.open -= 1

    def _has_waiters(self) -> bool:
        while self._waiters and self._waiters[0].done():
            self._waiters.popleft()
        return bool(self._waiters)

    @property
    def waiting(self) -> int:
        return sum(1 for w in self._waiters if not w.done())

    def wants_connection_closed(self) -> bool:
        """True when a caller is waiting for a place: a connection being
        checked in should then be closed instead of kept idle."""
        return self.max_open_connections is not None and self._has_waiters()

    def pop_lru(self) -> "_Entry | None":
        """Take the least recently used idle connection, in any database,
        out of its pool; the caller closes it."""
        best: _Entry | None = None
        for bucket in self._idle.values():
            if bucket:
                e = next(iter(bucket))
                if best is None or e.last_used < best.last_used:
                    best = e
        if best is not None:
            self.untrack(best)
            best.pool._remove_idle(best)
        return best

    # -- idle tracking -------------------------------------------------------

    def track(self, entry: "_Entry") -> None:
        """``entry`` was just checked in (``entry.last_used`` is now)."""
        timeout = entry.timeout
        bucket = self._idle.get(timeout)
        if bucket is None:
            bucket = self._idle[timeout] = OrderedDict()
        bucket[entry] = None
        if timeout is not None:
            self._schedule(entry.last_used + timeout)

    def untrack(self, entry: "_Entry") -> None:
        """``entry`` left the idle set (checked out, or closed by its
        pool)."""
        bucket = self._idle.get(entry.timeout)
        if bucket is not None:
            bucket.pop(entry, None)
            if not bucket:
                del self._idle[entry.timeout]

    @property
    def idle_count(self) -> int:
        return sum(len(b) for b in self._idle.values())

    # -- the one timer -------------------------------------------------------

    def _schedule(self, at: float) -> None:
        loop = asyncio.get_running_loop()
        if self._timer is not None:
            if self._timer_loop is loop and self._timer_at <= at:
                return  # an earlier (or equal) expiry is already armed
            self._timer.cancel()
        self._timer_at = at
        self._timer_loop = loop
        self._timer = loop.call_later(max(0.0, at - time.monotonic()), self._on_timer)

    def _on_timer(self) -> None:
        self._timer = None
        now = time.monotonic()
        expired: list[_Entry] = []
        next_at: float | None = None
        for timeout in list(self._idle):
            if timeout is None:
                continue
            bucket = self._idle[timeout]
            while bucket:
                entry = next(iter(bucket))
                due = entry.last_used + timeout
                if due > now:
                    next_at = due if next_at is None else min(next_at, due)
                    break
                del bucket[entry]
                entry.pool._remove_idle(entry)
                expired.append(entry)
            if not bucket:
                del self._idle[timeout]
        if expired:
            self.idle_closes += len(expired)
            spawn(_close_all(expired))
        if next_at is not None:
            self._schedule(next_at)


async def _close_all(entries: list["_Entry"]) -> None:
    await asyncio.gather(*(e.pool._close_entry(e) for e in entries), return_exceptions=True)


_default: Hub | None = None


def default_hub() -> Hub:
    """The hub shared by databases that are not in a registry."""
    global _default
    if _default is None:
        _default = Hub()
    return _default
