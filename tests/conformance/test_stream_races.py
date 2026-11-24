"""Streams cancelled, or cut off by a time limit, at random points.

An engine that drops an ``interrupt()`` arriving before its statement has
really started (DuckDB, SQLite) loses a cancel or time limit that lands in
that window, and an unbounded query then runs forever. A backend whose
stream hops are not interruptible at all hangs on any cancel after the
query has started. These tests aim at both, many times over: a hang fails
them (bounded by ``EVENTUALLY_S``), and the connection must be usable
afterwards.

In-process backends get many iterations (each costs a few milliseconds);
worker and server backends fewer, since a cut-off stream there may cost a
worker restart or a server round trip.
"""

from __future__ import annotations

import asyncio
import random
import sys

import pytest

from dblayer import QueryInterrupted

from ..helpers import EVENTUALLY_S

UNBOUNDED_COUNT = (
    "with recursive c(x) as (select 1 union all select x + 1 from c) select count(*) from c"
)
"""Never returns its first chunk: only an interrupt stops it."""
UNBOUNDED_ROWS = "with recursive c(x) as (select 1 union all select x + 1 from c) select x from c"

IN_PROCESS = {"sqlite", "sqlite-memory", "sqlite-apsw", "duckdb"}


def _iterations(backend_spec, in_process: int, other: int) -> int:  # type: ignore[no-untyped-def]
    return in_process if backend_spec.id in IN_PROCESS else other


def _delay(rnd: random.Random) -> float:
    """Half the cancels land in the first 4 ms (around the start of the
    statement, where a single interrupt is dropped), half later, once the
    query is certainly running."""
    return rnd.uniform(0, 0.004) if rnd.random() < 0.5 else rnd.uniform(0.004, 0.03)


async def _settle(task: asyncio.Task[object], what: str) -> None:
    done, _ = await asyncio.wait({task}, timeout=EVENTUALLY_S)
    if not done:
        # Teardown may then hang on the still-running statement: say why
        # first, so a hung run is not a silent one.
        msg = f"{what}: the stream did not stop within {EVENTUALLY_S} s (lost interrupt)"
        print(msg, file=sys.stderr, flush=True)
        pytest.fail(msg)


async def test_random_point_cancels_of_a_stream(db, backend_spec) -> None:  # type: ignore[no-untyped-def]
    rnd = random.Random(20261124)
    n = _iterations(backend_spec, 200, 25)
    cancelled = 0
    for i in range(n):
        sql = UNBOUNDED_COUNT if i % 2 == 0 else UNBOUNDED_ROWS

        async def consume() -> None:
            async with db.stream(sql, chunk_rows=50) as chunks:
                async for _ in chunks:
                    await asyncio.sleep(0)

        task = asyncio.create_task(consume())
        await asyncio.sleep(_delay(rnd))
        task.cancel()
        await _settle(task, f"cancel {i + 1} of {n}")
        if task.cancelled():
            cancelled += 1
        else:
            # Only a cancel or an interrupt can end these queries.
            exc = task.exception()
            assert isinstance(exc, QueryInterrupted), exc
    assert cancelled > 0
    assert db.stats().in_use_read == 0
    assert (await db.execute("select 1 as one")).rows == [(1,)]


async def test_random_short_time_limits_on_a_stream(db, backend_spec) -> None:  # type: ignore[no-untyped-def]
    rnd = random.Random(1124)
    n = _iterations(backend_spec, 200, 25)
    for i in range(n):
        sql = UNBOUNDED_COUNT if i % 2 == 0 else UNBOUNDED_ROWS
        limit = rnd.randint(1, 6)

        async def consume() -> None:
            async for _ in db.stream(sql, chunk_rows=50, time_limit_ms=limit):
                pass

        task = asyncio.create_task(consume())
        await _settle(task, f"time limit {i + 1} of {n} ({limit} ms)")
        exc = task.exception()
        assert isinstance(exc, QueryInterrupted), exc
    assert db.stats().in_use_read == 0
    assert (await db.execute("select 1 as one")).rows == [(1,)]
