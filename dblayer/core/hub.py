"""Connection bookkeeping shared by many databases: idle timeout.

A ``Hub`` tracks every *checked-in* (idle) connection of the databases
attached to it, in least-recently-used order. It costs nothing per asleep
database: an asleep database has no connections, so it has no entries here,
and there is one timer for the whole hub, not one per database.

* **Idle timeout.** Idle connections are kept in one ``OrderedDict`` per
  distinct timeout value (normally just one), in check-in order, so the
  oldest entry of each is the next to expire. One ``loop.call_later`` timer
  is armed for the earliest expiry; when it fires, expired entries are taken
  out of their pools and closed. Check-in and check-out are O(1).

A ``Registry`` owns one hub. Databases not in a registry share a
module-level default hub (``default_hub()``), so idle timeout works for
standalone databases too.

A hub is used from one event loop at a time. Its timer follows the loop
that last checked a connection in.
"""

from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from typing import TYPE_CHECKING

from .common import spawn

if TYPE_CHECKING:
    from .pool import _Entry

__all__ = ["Hub", "default_hub", "DEFAULT_IDLE_TIMEOUT"]

DEFAULT_IDLE_TIMEOUT = 30.0


class Hub:
    def __init__(self, *, idle_timeout: float | None = DEFAULT_IDLE_TIMEOUT) -> None:
        if idle_timeout is not None and idle_timeout < 0:
            raise ValueError("idle_timeout must be >= 0 or None")
        self.idle_timeout = idle_timeout
        """Default for databases that do not set their own."""
        self._idle: dict[float | None, OrderedDict[_Entry, None]] = {}
        self._timer: asyncio.TimerHandle | None = None
        self._timer_at = 0.0
        self._timer_loop: asyncio.AbstractEventLoop | None = None
        self.idle_closes = 0
        """Connections closed by the idle timeout."""

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
