"""The C worker's ``export`` request: bad messages, frame caps, and limits
and cancellation on every transport (the conformance suite runs the
default, ``raw``)."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from dblayer import Database, QueryInterrupted, ResourceLimitExceeded

# On the C worker and on its Rust port, each skipped when not built.
from ..native_impl import autouse_native_impl, native_impl  # noqa: F401

INFINITE = "with recursive c(x) as (select 1 union all select x + 1 from c) select x, 'row ' || x as t from c"


@pytest.fixture(params=["raw", "frames", "fifo"])
async def db(request: pytest.FixtureRequest, tmp_path: Path) -> Any:
    from dblayer.backends.sqlite_cworker import SqliteCWorker

    d = Database(SqliteCWorker(tmp_path / "e.db", export_transport=request.param, max_frame_bytes=1 << 20))
    yield d
    await d.close()


async def _call(db: Database, msg: dict[str, Any]) -> Any:
    """Send one raw message on a read connection; return the frames."""
    pool = await db._use_pool()
    conn = await pool.acquire(write=False)
    try:
        return await conn._proc.call(msg, owner=conn, expect_frames=4)
    finally:
        await pool.release(conn)


@pytest.mark.parametrize(
    "extra, error",
    [
        ({"encoding": "xml"}, "unknown export encoding"),
        ({"encoding": 7}, "unknown export encoding"),
        ({"transport": "carrier-pigeon"}, "unknown export transport"),
        ({"chunk_bytes": 0}, "chunk_bytes"),
        ({"chunk_bytes": 1 << 30}, "chunk_bytes"),
        ({"transport": "fifo"}, "needs data_path"),
        ({"transport": "fifo", "data_path": "/nonexistent/x"}, "cannot open the export pipe"),
        ({"transport": "socket"}, "needs handoff_path"),
        ({"transport": "socket", "handoff_path": "/nonexistent/x.sock"}, "cannot connect"),
        ({"transport": "socket", "handoff_path": "x" * 300}, "too long"),
        ({"sql": 42}, "sql must be a string"),
        ({"sql": "select ?", "params": [1, 2]}, "Incorrect number of bindings"),
        ({"sql": "delete from t"}, None),
        ({"tx": "t99"}, "not open"),
    ],
)
async def test_bad_export_messages(db, extra, error, native_impl):
    if native_impl == "rust" and extra.get("transport") == "socket":
        error = "no socket handoff"  # the Rust worker has no socket transport
    msg = {"type": "export", "sql": "select 1 as a", "encoding": "csv", "credit": 2, **extra}
    frames = await _call(db, msg)
    err = frames[-1].get("error")
    assert err is not None, frames
    if error:
        assert error in err["message"], err
    # the worker is still fine
    assert (await db.execute("select 1 as a")).rows == [(1,)]


async def test_frame_over_the_cap_is_refused(db):
    # chunk_bytes over max_frame_bytes (1 MB here): a frame transport must
    # refuse to send it, not send it.
    big = "x" * 600_000
    if db._backend.export_transport == "fifo":
        pytest.skip("fifo data does not travel in frames")
    with pytest.raises(ResourceLimitExceeded):
        await db.export("select ? as t union all select ?", [big, big], format="csv",
                        chunk_bytes=4 << 20).read_all()
    assert (await db.execute("select 2 as a")).rows == [(2,)]


async def test_time_limit_mid_export(db):
    n = 0
    with pytest.raises(QueryInterrupted):
        async with db.export(INFINITE, format="ndjson", time_limit_ms=300) as s:
            async for _ in s:
                n += 1
    assert n > 0
    assert (await db.execute("select 3 as a")).rows == [(3,)]


async def test_cancel_mid_export(db):
    started = asyncio.Event()

    async def consume() -> None:
        async with db.export(INFINITE, format="csv", chunk_bytes=4096) as s:
            async for _ in s:
                started.set()
                await asyncio.sleep(0.005)

    task = asyncio.create_task(consume())
    await asyncio.wait_for(started.wait(), 10)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert db.stats().in_use_read == 0
    assert (await db.execute("select 4 as a")).rows == [(4,)]


async def test_slow_consumer_holds_the_worker_back(db):
    # Backpressure: with nobody reading, the worker stops producing (the
    # credit or the pipe runs out) instead of buffering the whole result.
    async with db.export(INFINITE, format="csv", chunk_bytes=4096) as s:
        await s.__anext__()
        await asyncio.sleep(0.5)
        # Still at the start: the next chunk is a low row number.
        chunk = await s.__anext__()
        first = int(chunk.split(b",", 1)[0])
        assert first < 2_000_000
    assert (await db.execute("select 5 as a")).rows == [(5,)]


async def test_close_early_then_reuse(db):
    for _ in range(3):
        async with db.export(INFINITE, format="json", chunk_bytes=2048) as s:
            await s.__anext__()
            await s.__anext__()
    assert (await db.execute("select 6 as a")).rows == [(6,)]
    assert await db.export("select 1 as a", format="csv").read_all() == b"a\r\n1\r\n"
