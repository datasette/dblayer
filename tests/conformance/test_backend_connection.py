"""The BackendConnection protocol methods that Database does not expose yet
(begin/commit/rollback, execute_many, interrupt), tested directly so that
milestone 2 builds on verified ground. SQL here is already in the backend's
native placeholder style, so it goes through the core rewriter first."""

import asyncio

import pytest

from dblayer import QueryInterrupted
from dblayer.backends.base import BackendConnection
from dblayer.core.database import map_errors
from dblayer.core.params import rewrite

from ..helpers import EVENTUALLY_S


def native(backend, sql, params=None):
    d = backend.dialect
    return rewrite(sql, params, d.paramstyle, d.flavor)


@pytest.fixture
async def conns(backend):
    w = await backend.open(write=True)
    await w.execute(*native(backend, "create table t (x integer)"), time_limit_ms=None, max_rows=None)
    r = await backend.open(write=False)
    yield backend, w, r
    await r.close()
    await w.close()
    await backend.close()


async def count(backend, conn):
    res = await conn.execute(*native(backend, "select count(*) from t"), time_limit_ms=None, max_rows=None)
    return res.rows[0][0]


async def test_protocol_instances(conns):
    _, w, r = conns
    assert isinstance(w, BackendConnection) and isinstance(r, BackendConnection)


async def test_commit(conns):
    backend, w, r = conns
    await w.begin("write")
    await w.execute(*native(backend, "insert into t (x) values (:x)", {"x": 1}), time_limit_ms=None, max_rows=None)
    await w.commit()
    assert await count(backend, r) == 1


async def test_rollback(conns):
    backend, w, r = conns
    await w.begin("write")
    await w.execute(*native(backend, "insert into t (x) values (1)"), time_limit_ms=None, max_rows=None)
    assert await count(backend, w) == 1
    await w.rollback()
    assert await count(backend, w) == 0
    assert await count(backend, r) == 0


async def test_rollback_and_commit_are_noops_without_transaction(conns):
    _, w, r = conns
    await w.rollback()
    await w.commit()
    await r.rollback()


async def test_read_transaction(conns):
    backend, w, r = conns
    await r.begin("read")
    assert await count(backend, r) == 0
    await r.commit()


async def test_unknown_mode(conns):
    _, w, _r = conns
    with pytest.raises(ValueError):
        await w.begin("sideways")


async def test_execute_many(conns):
    backend, w, r = conns
    sql, _ = native(backend, "insert into t (x) values (:x)", {"x": 0})
    d = backend.dialect
    seq = [rewrite("insert into t (x) values (:x)", {"x": i}, d.paramstyle, d.flavor)[1] for i in range(10)]
    res = await w.execute_many(sql, seq)
    assert res.rowcount == 10
    assert await count(backend, r) == 10


async def test_interrupt(conns):
    backend, _w, r = conns
    sql, params = native(
        backend,
        "with recursive c(x) as (select 1 union all select x + 1 from c) select count(*) from c",
    )
    task = asyncio.create_task(r.execute(sql, params, time_limit_ms=None, max_rows=None))

    async def interrupt_until_done():
        # interrupt() is a no-op while no statement runs (SQLite, DuckDB),
        # so a caller repeats it until the statement returns, as the core's
        # deadline does; one sent before the thread starts would be lost.
        while not task.done():
            await r.interrupt()
            await asyncio.sleep(0.01)

    await asyncio.sleep(0.05)
    await asyncio.wait_for(interrupt_until_done(), EVENTUALLY_S)
    with pytest.raises(QueryInterrupted):
        with map_errors(backend):
            await task
    assert await count(backend, r) == 0


async def test_schema_and_version(conns):
    backend, w, r = conns
    schema = await r.schema()
    assert "t" in schema.tables
    assert schema.version == await r.schema_version()


async def test_stream_direct(conns):
    backend, w, r = conns
    sql, params = native(
        backend,
        "with recursive c(x) as (select 1 union all select x + 1 from c where x < 25) select x from c",
    )
    stream = r.stream(sql, params, chunk_rows=10)
    chunks = [c async for c in stream]
    assert [len(c) for c in chunks] == [10, 10, 5]
    assert stream.columns == ("x",)
