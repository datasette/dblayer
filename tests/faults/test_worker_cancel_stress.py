"""Worker backends: cancels at random points during start-up and during
requests.

A cancelled acquire or open while the child is still starting, a cancelled
call whose reply arrives later, the per-process FIFO of requests and the
kill timer must never leave a later request hanging, a child process
running or a descriptor open. Covers the Python worker (dedicated
placement), the C worker, the multi-tenant ``sqlite-cpool`` backend and
the DuckDB worker's ``DatabaseWorker`` placement.
"""

from __future__ import annotations

import asyncio
import gc
import random
import sys

import pytest

from dblayer import Database, DblayerError

from ..helpers import EVENTUALLY_S, count_open_fds, wait_until

WORKERS = {"sqlite-worker", "sqlite-cworker", "sqlite-cpool", "duckdb-worker"}

UNBOUNDED = (
    "with recursive c(x) as (select 1 union all select x + 1 from c) select count(*) from c"
)

# Generous: a hang, not slowness, is what fails these.
HANG_S = 30.0


@pytest.fixture
async def worker_spec(backend_spec):  # type: ignore[no-untyped-def]
    """The backend, with the loop's exception handler watched: a cancel
    must not leave asyncio logging "exception in shielded future" or
    "Future exception was never retrieved" (the symptoms reported with the
    hang)."""
    if backend_spec.id not in WORKERS:
        pytest.skip("worker backends only")
    loop = asyncio.get_running_loop()
    seen: list[str] = []
    old = loop.get_exception_handler()
    loop.set_exception_handler(
        lambda _loop, ctx: seen.append(f"{ctx.get('message')}: {ctx.get('exception')!r}")
    )
    try:
        yield backend_spec
        gc.collect()
        await asyncio.sleep(0)
    finally:
        loop.set_exception_handler(old)
    assert not seen, seen


def _delay(rnd: random.Random, upto: float) -> float:
    # Weighted to the start: most windows are a few ms wide.
    return rnd.uniform(0, upto) if rnd.random() < 0.5 else rnd.uniform(0, upto / 10)


async def _bounded(aw, what: str):  # type: ignore[no-untyped-def]
    try:
        return await asyncio.wait_for(aw, HANG_S)
    except TimeoutError:
        msg = f"{what}: no answer within {HANG_S} s (hang)"
        print(msg, file=sys.stderr, flush=True)
        pytest.fail(msg)


async def _cancel_at(coro, delay: float) -> None:  # type: ignore[no-untyped-def]
    task = asyncio.ensure_future(coro)
    await asyncio.sleep(delay)
    task.cancel()
    done, _ = await asyncio.wait({task}, timeout=HANG_S)
    if not done:
        msg = f"a request cancelled after {delay * 1000:.1f} ms did not end within {HANG_S} s"
        print(msg, file=sys.stderr, flush=True)
        pytest.fail(msg)
    if not task.cancelled():
        exc = task.exception()
        # Finishing first, or an engine error for an interrupted query,
        # is fine; anything else must at least be one of ours.
        assert exc is None or isinstance(exc, DblayerError), repr(exc)


async def _no_children_left() -> None:
    from dblayer.backends.sqlite_worker import live_worker_pids

    try:
        await wait_until(lambda: not live_worker_pids(), timeout=EVENTUALLY_S)
    except AssertionError:
        from dblayer.backends.sqlite_worker import WorkerProcess

        procs = [o for o in gc.get_objects() if isinstance(o, WorkerProcess)]
        state = [(p.pid, type(p).__name__, p.handles, p.dead, p.idle_timer) for p in procs if p.pid in live_worker_pids()]
        pytest.fail(f"worker processes still running: {live_worker_pids()}; {state}")


async def test_cancel_during_start_up(worker_spec, make_backend) -> None:  # type: ignore[no-untyped-def]
    """Each round: a new database (so a new child), its first request
    cancelled at a random point of the child's start-up, then a request
    that must succeed."""
    rnd = random.Random(20261124)
    await _no_children_left()
    baseline = count_open_fds()
    rounds = 20
    for i in range(rounds):
        db = Database(make_backend(), name=f"s{i}")
        try:
            # Start-up takes 3 ms (C) to 200 ms (Python, DuckDB).
            await _cancel_at(db.execute("select 1"), _delay(rnd, 0.25))
            # A second cancel, mid-flight on a running query.
            if i % 2:
                await _cancel_at(db.execute(UNBOUNDED), _delay(rnd, 0.05))
            r = await _bounded(db.execute("select 2 as two"), f"round {i}: request after a cancel")
            assert r.rows == [(2,)]
        finally:
            await _bounded(db.close(), f"round {i}: close")
    await _no_children_left()
    await wait_until(lambda: count_open_fds() <= baseline, timeout=EVENTUALLY_S)


async def test_cancel_during_requests_with_concurrency(worker_spec, make_backend) -> None:  # type: ignore[no-untyped-def]
    """Several tasks share a database: reads, writes and unbounded queries,
    each cancelled at a random point; the per-process request queue and the
    kill timer must keep every later request answered."""
    rnd = random.Random(1124)
    await _no_children_left()
    baseline = count_open_fds()
    db = Database(make_backend(), name="c")
    try:
        await db.execute_write("create table t (x integer)")
        for i in range(15):
            ops = []
            for j in range(6):
                kind = rnd.choice(("read", "write", "unbounded", "stream"))
                if kind == "read":
                    coro = db.execute("select count(*) from t")
                elif kind == "write":
                    coro = db.execute_write("insert into t values (?)", [j])
                elif kind == "unbounded":
                    coro = db.execute(UNBOUNDED)
                else:

                    async def consume() -> None:
                        async with db.stream(UNBOUNDED, chunk_rows=10) as chunks:
                            async for _ in chunks:
                                pass

                    coro = consume()
                ops.append(_cancel_at(coro, _delay(rnd, 0.1)))
            await _bounded(asyncio.gather(*ops), f"round {i}: cancelled requests")
            r = await _bounded(db.execute("select 3 as three"), f"round {i}: request after cancels")
            assert r.rows == [(3,)]
            assert db.stats().in_use_read == 0
    finally:
        await _bounded(db.close(), "close")
    await _no_children_left()
    await wait_until(lambda: count_open_fds() <= baseline, timeout=EVENTUALLY_S)


async def test_concurrent_first_opens_some_cancelled(worker_spec, make_backend) -> None:  # type: ignore[no-untyped-def]
    """Several databases start at once (on ``sqlite-cpool`` they share the
    start of one process) and a random subset of the first requests is
    cancelled: the others must not inherit the cancellation, and every
    database answers afterwards."""
    rnd = random.Random(7)
    await _no_children_left()
    baseline = count_open_fds()
    for i in range(6):
        dbs = [Database(make_backend(), name=f"m{i}.{j}") for j in range(4)]
        try:
            tasks = [asyncio.ensure_future(d.execute("select 1")) for d in dbs]
            victims = rnd.sample(range(len(tasks)), 2)
            await asyncio.sleep(_delay(rnd, 0.2))
            for v in victims:
                tasks[v].cancel()
            done, _ = await asyncio.wait(tasks, timeout=HANG_S)
            assert len(done) == len(tasks), "first opens did not finish (hang)"
            for k, t in enumerate(tasks):
                if k in victims:
                    continue
                # Not cancelled ourselves: neither cancelled nor failed.
                assert not t.cancelled(), f"database {k} inherited another's cancel"
                assert t.exception() is None, repr(t.exception())
            for d in dbs:
                r = await _bounded(d.execute("select 4 as four"), f"round {i}: after the opens")
                assert r.rows == [(4,)]
        finally:
            for d in dbs:
                await _bounded(d.close(), f"round {i}: close")
    await _no_children_left()
    await wait_until(lambda: count_open_fds() <= baseline, timeout=EVENTUALLY_S)


async def test_cap_load_cut_short_then_next_registry_works(worker_spec, make_backend) -> None:  # type: ignore[no-untyped-def]
    """The reported case: ``test_cap_holds_under_concurrent_load``'s
    ``wait_for`` timed out under load, cancelling hundreds of operations at
    once (some waiting for a place under the cap, some starting a child,
    some mid-request), and later tests in the process hung. Here the cut
    comes at random points, and a new registry must then work."""
    from dblayer import Registry

    rnd = random.Random(3)
    await _no_children_left()
    baseline = count_open_fds()
    for i in range(3):
        async with Registry(max_open_connections=5) as registry:
            dbs = [registry.add(f"d{j}", Database(make_backend())) for j in range(12)]
            for d in dbs:
                await _bounded(d.execute_write("create table t (x integer)"), "setup")
            plan = [(rnd.choice(dbs), rnd.random() < 0.3) for _ in range(150)]

            async def op(db: Database, write: bool) -> None:
                if write:
                    await db.execute_write("insert into t (x) values (1)")
                else:
                    await db.execute("select count(*) from t")

            try:
                await asyncio.wait_for(
                    asyncio.gather(*(op(d, w) for d, w in plan)), rnd.uniform(0.05, 1.0)
                )
            except TimeoutError:
                pass
            for d in dbs:
                r = await _bounded(d.execute("select 5 as five"), f"round {i}: after the cut")
                assert r.rows == [(5,)]
    async with Registry(max_open_connections=2) as registry:
        d = registry.add("after", Database(make_backend()))
        assert (await _bounded(d.execute("select 6 as six"), "a new registry")).rows == [(6,)]
    await _no_children_left()
    await wait_until(lambda: count_open_fds() <= baseline, timeout=EVENTUALLY_S)
