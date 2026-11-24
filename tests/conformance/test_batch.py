"""db.batch(): a whole write transaction in one call."""

import asyncio

import pytest

from dblayer import IntegrityError, NestedWriteError, SchemaChanged, WriteResult

from ..helpers import EVENTUALLY_S


@pytest.fixture
async def t(db):
    await db.execute_write("create table log (msg text unique)")
    await db.execute_write("create table counters (name text primary key, n integer)")
    await db.execute_write("insert into counters values ('runs', 0)")
    return db


async def test_batch_runs_all(t):
    results = await t.batch(
        [
            ("insert into log (msg) values (:m)", {"m": "start"}),
            ("update counters set n = n + 1 where name = :n", {"n": "runs"}),
            ("insert into log (msg) values (?)", ["end"]),
        ]
    )
    assert len(results) == 3 and all(isinstance(r, WriteResult) for r in results)
    assert [r.rowcount for r in results] == [1, 1, 1]
    assert (await t.execute("select msg from log order by msg")).rows == [("end",), ("start",)]
    assert (await t.execute("select n from counters")).rows == [(1,)]


async def test_batch_accepts_bare_sql(t):
    await t.batch(["insert into log (msg) values ('a')", "insert into log (msg) values ('b')"])
    assert (await t.execute("select count(*) from log")).rows[0][0] == 2


async def test_batch_is_all_or_nothing(t):
    with pytest.raises(IntegrityError):
        await t.batch(
            [
                ("insert into log (msg) values (?)", ["dup"]),
                ("update counters set n = 99", None),
                ("insert into log (msg) values (?)", ["dup"]),
            ]
        )
    assert (await t.execute("select count(*) from log")).rows[0][0] == 0
    assert (await t.execute("select n from counters")).rows == [(0,)]


async def test_batch_with_matching_schema_version(t):
    version = await t.schema_version()
    await t.batch([("insert into log (msg) values ('x')", None)], expect_schema_version=version)
    assert (await t.execute("select count(*) from log")).rows[0][0] == 1


async def test_batch_schema_changed_runs_nothing(t):
    version = await t.schema_version()
    await t.execute_write("create table other (x integer)")
    with pytest.raises(SchemaChanged):
        await t.batch(
            [("insert into log (msg) values ('x')", None)], expect_schema_version=version
        )
    assert (await t.execute("select count(*) from log")).rows[0][0] == 0


async def test_batch_ddl_invalidates_schema_cache(t):
    assert "made" not in await t.table_names()
    await t.batch(["create table made (x integer)", "insert into made values (1)"])
    assert "made" in await t.table_names()


@pytest.mark.requires("returning")
async def test_batch_returning(t):
    [r] = await t.batch([("insert into log (msg) values ('a'), ('b') returning msg", None)])
    assert sorted(r.rows) == [("a",), ("b",)]


async def test_batch_empty(t):
    assert await t.batch([]) == []


async def test_batch_inside_transaction_raises(t):
    async with t.transaction():
        with pytest.raises(NestedWriteError):
            await asyncio.wait_for(t.batch(["select 1"]), EVENTUALLY_S)


async def test_batches_are_serialised(t):
    await asyncio.gather(
        *(t.batch(["update counters set n = n + 1", "update counters set n = n + 1"]) for _ in range(10))
    )
    assert (await t.execute("select n from counters")).rows == [(20,)]
