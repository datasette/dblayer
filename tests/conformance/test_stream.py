"""Streaming: chunks, backpressure, slow consumers, limits, early close."""

import asyncio
from contextlib import aclosing

import pytest

from ..helpers import infinite_series, series_sql

from dblayer import QueryInterrupted, ResourceLimitExceeded, Row






async def test_stream_chunks(db):
    sizes = []
    values = []
    async for chunk in db.stream(series_sql(db, 95), chunk_rows=10):
        sizes.append(len(chunk))
        assert all(isinstance(r, Row) for r in chunk)
        values.extend(r["x"] for r in chunk)
    assert sizes == [10] * 9 + [5]
    assert values == list(range(1, 96))


async def test_stream_params(db):
    chunks = [c async for c in db.stream("select :a as a, :b as b", {"a": 1, "b": 2})]
    assert chunks == [[(1, 2)]]
    chunks = [c async for c in db.stream("select ? as a, ? as b", [3, 4])]
    assert chunks == [[(3, 4)]]


async def test_stream_empty(db):
    assert [c async for c in db.stream(series_sql(db, 5) + " where x > 100")] == []


async def test_stream_releases_connection_when_exhausted(db):
    async for _ in db.stream(series_sql(db, 20), chunk_rows=5):
        assert db.stats().in_use_read == 1
    assert db.stats().in_use_read == 0


async def test_slow_consumer_does_not_use_up_time_limit(db):
    # 5 chunks, 0.25 s pause after each: 1.25 s wall clock against a limit
    # of 1 s of database time. Only time spent inside the database counts.
    # (With 200 ms the fetches themselves - a thread hop or a round trip per
    # chunk - added up past the limit on a loaded machine.)
    n = 0
    async for chunk in db.stream(series_sql(db, 50), chunk_rows=10, time_limit_ms=1000):
        n += len(chunk)
        await asyncio.sleep(0.25)
    assert n == 50


async def test_stream_time_limit_interrupts(db):
    sql = (
        "with recursive c(x) as (select 1 union all select x + 1 from c) "
        "select count(*) from c"
    )
    with pytest.raises(QueryInterrupted):
        async for _ in db.stream(sql, time_limit_ms=50):
            pass
    assert db.stats().in_use_read == 0
    assert (await db.execute("select 1")).rows == [(1,)]


async def test_backpressure_infinite_stream_can_be_abandoned(db):
    # If the backend read ahead, this would never finish.
    got = []
    async with aclosing(db.stream(infinite_series(db), chunk_rows=100)) as s:
        async for chunk in s:
            got.extend(chunk)
            if len(got) >= 300:
                break
    assert len(got) == 300
    assert db.stats().in_use_read == 0
    assert (await db.execute("select 2")).rows == [(2,)]


async def test_stream_max_result_bytes(db):
    sql = series_sql(db, 1000).replace(") select x from", ") select x, :pad as pad from").replace(
        "as x from", "as x, :pad as pad from"
    )
    with pytest.raises(ResourceLimitExceeded):
        async for _ in db.stream(sql, {"pad": "p" * 100}, chunk_rows=50, max_result_bytes=5000):
            pass
    assert db.stats().in_use_read == 0


async def test_concurrent_streams_and_reads(make_db):
    db = make_db(max_read_connections=2)

    async def consume():
        total = 0
        async for chunk in db.stream(series_sql(db, 100), chunk_rows=7):
            total += sum(r[0] for r in chunk)
            await asyncio.sleep(0)
        return total

    results = await asyncio.gather(*(consume() for _ in range(5)))
    assert results == [5050] * 5
    assert db.stats().open_read <= 2


async def test_stream_context_manager_releases_on_break(db):
    async with db.stream(infinite_series(db), chunk_rows=10) as chunks:
        async for chunk in chunks:
            assert db.stats().in_use_read == 1
            break
    # Released when the block ends, not when the iterator is collected.
    assert db.stats().in_use_read == 0


async def test_stream_context_manager_releases_on_error(db):
    with pytest.raises(KeyError):
        async with db.stream(infinite_series(db), chunk_rows=10) as chunks:
            async for _ in chunks:
                raise KeyError
    assert db.stats().in_use_read == 0


async def test_stream_context_manager_unstarted(db):
    async with db.stream(series_sql(db, 5)):
        pass
    assert db.stats().in_use_read == 0


async def test_stream_aclose(db):
    s = db.stream(infinite_series(db), chunk_rows=10)
    await s.__anext__()
    await s.aclose()
    assert db.stats().in_use_read == 0
