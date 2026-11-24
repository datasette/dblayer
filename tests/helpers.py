"""Test helpers."""

from __future__ import annotations

import asyncio
import inspect
import os
import time
from collections.abc import Callable

STRICT_TIMING = os.environ.get("DBLAYER_STRICT_TIMING") == "1"
"""``DBLAYER_STRICT_TIMING=1`` makes ``check_latency`` assert the spec's
tight latency figures as well as the generous functional bound."""

# Generous bound for "this must happen at all": several seconds, so a
# loaded CI machine cannot fail it, while a hang still fails promptly.
EVENTUALLY_S = 10.0

latencies: list[tuple[str, float, float]] = []
"""Every figure ``check_latency`` measured in this run: (name, seconds,
spec bound in seconds). Printed at the end of the session."""


async def wait_until(
    condition: Callable[[], object], timeout: float = EVENTUALLY_S, interval: float = 0.005
) -> None:
    """Poll ``condition`` (a plain or async callable) until it is truthy.
    Fails with ``AssertionError`` after ``timeout`` seconds. Use this
    instead of sleeping a fixed time and then asserting."""
    deadline = time.monotonic() + timeout
    while True:
        result = condition()
        if inspect.isawaitable(result):
            result = await result
        if result:
            return
        if time.monotonic() > deadline:
            raise AssertionError(f"condition not met within {timeout} s: {condition}")
        await asyncio.sleep(interval)


class IdleClock:
    """Moves the idle-timeout clock by hand.

    Replaces ``time`` in ``dblayer.core.pool`` (``last_used`` stamps) and
    ``dblayer.core.hub`` (the timer) with real ``time.monotonic()`` plus an
    offset. ``advance(s)`` makes ``s`` seconds pass at once and runs the
    hub's timer, so a test can say "60 s later" exactly, however slowly the
    machine runs. Use long timeouts (tens of seconds) so the real time the
    test takes is negligible next to them."""

    def __init__(self, monkeypatch: object) -> None:
        import types

        import dblayer.core.hub as hub_module
        import dblayer.core.pool as pool_module

        self.offset = 0.0
        fake = types.SimpleNamespace(**vars(time))
        fake.monotonic = lambda: time.monotonic() + self.offset
        for module in (hub_module, pool_module):
            monkeypatch.setattr(module, "time", fake)  # type: ignore[attr-defined]

    def advance(self, seconds: float, hub: object) -> None:
        """``seconds`` pass; ``hub``'s timer runs now (the real one it had
        armed would only fire ``seconds`` too late)."""
        self.offset += seconds
        timer = hub._timer  # type: ignore[attr-defined]
        if timer is not None:
            timer.cancel()
            hub._timer = None  # type: ignore[attr-defined]
        hub._on_timer()  # type: ignore[attr-defined]


def check_latency(name: str, seconds: float, spec_bound: float, *, functional: float = EVENTUALLY_S) -> None:
    """A latency promise from the spec, split in two.

    ``functional`` (several seconds) must always hold: it proves the thing
    happens at all. ``spec_bound`` is the figure the spec promises; it is
    recorded and printed (``pytest -s`` shows it as it happens, and the
    session summary lists them all) and only asserted when
    ``DBLAYER_STRICT_TIMING=1``, since shared or loaded machines cannot
    honour tens of milliseconds."""
    latencies.append((name, seconds, spec_bound))
    print(f"[latency] {name}: {seconds * 1000:.1f} ms (spec {spec_bound * 1000:.0f} ms)")
    assert seconds < functional, f"{name}: {seconds:.3f} s, functional bound {functional} s"
    if STRICT_TIMING:
        assert seconds < spec_bound, (
            f"{name}: {seconds * 1000:.1f} ms, spec bound {spec_bound * 1000:.0f} ms "
            "(DBLAYER_STRICT_TIMING=1)"
        )


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
