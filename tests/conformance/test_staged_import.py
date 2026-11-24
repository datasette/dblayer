"""Staged import: all three modes, failure part-way, target with indexes,
visibility, dependents. Orphan sweeping is in test_sweep_staged.py."""

import asyncio

import pytest

from dblayer import QueryError, TableNotFound
from dblayer.core.staged import STAGING_PREFIX

from ..helpers import EVENTUALLY_S


async def dump(db, table, order="1"):
    return (await db.execute(f"select * from {table} order by {order}")).dicts()


def staging_tables(schema):
    return [n for n in schema.tables if n.startswith(STAGING_PREFIX)]


def chunks(n, size, start=1):
    return [
        [{"id": i, "title": f"t{i}"} for i in range(s, min(s + size, start + n))]
        for s in range(start, start + n, size)
    ]


@pytest.fixture
async def posts(ops_db):
    await ops_db.create_table(
        "posts", {"id": "integer", "title": "text"}, pk="id", not_null=["title"]
    )
    await ops_db.create_index("posts", ["title"], name="idx_posts_title")
    await ops_db.insert_rows("posts", [{"id": 1000, "title": "old"}])
    return ops_db


async def test_create_mode(ops_db):
    async with ops_db.staged_import("posts", mode="create", pk="id") as imp:
        for chunk in chunks(25, 10):
            await imp.write(chunk)
        # Not visible under the target name until the swap.
        assert "posts" not in await ops_db.table_names()
        # Hidden from listings, readable by name.
        assert imp.staging not in await ops_db.table_names()
        r = await ops_db.execute(f"select count(*) from {imp.staging}")
        assert r.rows[0][0] == 25
    assert imp.rowcount == 25
    assert await ops_db.table_names() == ["posts"]
    assert await ops_db.primary_keys("posts") == ["id"]
    assert len(await dump(ops_db, "posts")) == 25
    assert staging_tables(await ops_db.schema()) == []


async def test_create_mode_with_columns_and_no_rows(ops_db):
    async with ops_db.staged_import("t", columns={"id": "integer", "a": "text"}, pk="id"):
        pass
    assert [c.name for c in await ops_db.table_columns("t")] == ["id", "a"]


async def test_create_mode_no_rows_no_columns_fails(ops_db):
    with pytest.raises(ValueError):
        async with ops_db.staged_import("t"):
            pass
    assert staging_tables(await ops_db.schema()) == []


async def test_create_mode_target_exists(posts):
    with pytest.raises(QueryError):
        async with posts.staged_import("posts", mode="create"):
            pass
    assert staging_tables(await posts.schema()) == []


async def test_create_mode_target_appears_during_import(ops_db):
    with pytest.raises(QueryError):
        async with ops_db.staged_import("t", pk="id") as imp:
            await imp.write([{"id": 1}])
            await ops_db.create_table("t", {"x": "text"})
    assert [c.name for c in await ops_db.table_columns("t")] == ["x"]
    assert staging_tables(await ops_db.schema()) == []


async def test_replace_mode(posts):
    async with posts.staged_import("posts", mode="replace") as imp:
        for chunk in chunks(30, 10):
            await imp.write(chunk)
        # Old contents still served while the import runs.
        assert await dump(posts, "posts") == [{"id": 1000, "title": "old"}]
    rows = await dump(posts, "posts")
    assert len(rows) == 30 and rows[0] == {"id": 1, "title": "t1"}
    # Target definition kept: pk, NOT NULL, and the index recreated.
    assert await posts.primary_keys("posts") == ["id"]
    assert {c.name: c.not_null for c in await posts.table_columns("posts")}["title"]
    assert "idx_posts_title" in {i.name for i in await posts.indexes("posts")}
    assert staging_tables(await posts.schema()) == []


async def test_replace_mode_missing_target_creates(ops_db):
    async with ops_db.staged_import("t", mode="replace", pk="id") as imp:
        await imp.write([{"id": 1, "a": "x"}])
    assert await dump(ops_db, "t") == [{"id": 1, "a": "x"}]


async def test_replace_with_nothing_empties(posts):
    async with posts.staged_import("posts", mode="replace"):
        pass
    assert await dump(posts, "posts") == []


async def test_append_mode(posts):
    async with posts.staged_import("posts", mode="append") as imp:
        for chunk in chunks(20, 7):
            await imp.write(chunk)
        assert len(await dump(posts, "posts")) == 1
    assert len(await dump(posts, "posts")) == 21
    assert staging_tables(await posts.schema()) == []


async def test_append_mode_alter(posts):
    async with posts.staged_import("posts", mode="append", alter=True) as imp:
        await imp.write([{"id": 1, "title": "a", "extra": 5}])
    assert (await dump(posts, "posts"))[0] == {"id": 1, "title": "a", "extra": 5}


async def test_append_mode_conflict_with_target_rolls_back(posts):
    with pytest.raises(QueryError):
        async with posts.staged_import("posts", mode="append") as imp:
            await imp.write([{"id": 1, "title": "a"}, {"id": 1000, "title": "dup"}])
    assert await dump(posts, "posts") == [{"id": 1000, "title": "old"}]
    assert staging_tables(await posts.schema()) == []


async def test_append_mode_missing_target(ops_db):
    with pytest.raises(TableNotFound):
        async with ops_db.staged_import("nope", mode="append"):
            pass


async def test_failure_part_way_drops_staging(posts):
    with pytest.raises(RuntimeError):
        async with posts.staged_import("posts", mode="replace") as imp:
            await imp.write(chunks(10, 10)[0])
            raise RuntimeError("source broke")
    assert await dump(posts, "posts") == [{"id": 1000, "title": "old"}]
    assert staging_tables(await posts.schema()) == []


async def test_bad_chunk_drops_staging(posts):
    with pytest.raises(QueryError):
        async with posts.staged_import("posts", mode="replace") as imp:
            await imp.write(chunks(10, 10)[0])
            await imp.write([{"id": 99, "title": None}])  # NOT NULL
    assert await dump(posts, "posts") == [{"id": 1000, "title": "old"}]
    assert staging_tables(await posts.schema()) == []


async def test_cancelled_import_drops_staging(posts):
    started = asyncio.Event()

    async def run():
        async with posts.staged_import("posts", mode="replace") as imp:
            await imp.write(chunks(5, 5)[0])
            started.set()
            await asyncio.sleep(10)

    task = asyncio.create_task(run())
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert staging_tables(await posts.schema()) == []
    assert await dump(posts, "posts") == [{"id": 1000, "title": "old"}]


async def test_chunks_are_short_transactions(posts):
    """Other writers get in between chunks."""
    async with posts.staged_import("posts", mode="append") as imp:
        await imp.write(chunks(5, 5)[0])
        await asyncio.wait_for(posts.execute_write("insert into posts values (2000, 'x')"), EVENTUALLY_S)
        await imp.write(chunks(5, 5, start=6)[0])
    assert len(await dump(posts, "posts")) == 12


async def test_bad_mode(ops_db):
    with pytest.raises(ValueError):
        ops_db.staged_import("t", mode="merge")


@pytest.mark.requires("replace_keeps_views")
async def test_replace_keeps_views_working(posts):
    await posts.execute_write("create view titles as select title from posts")
    async with posts.staged_import("posts", mode="replace") as imp:
        await imp.write([{"id": 1, "title": "new"}])
    assert (await posts.execute("select * from titles")).rows == [("new",)]


@pytest.mark.requires_not("replace_keeps_views")
async def test_replace_with_a_dependent_view_fails_and_rolls_back(posts):
    before = await dump(posts, "posts")
    await posts.execute_write("create view titles as select title from posts")
    with pytest.raises(QueryError):
        async with posts.staged_import("posts", mode="replace") as imp:
            await imp.write([{"id": 1, "title": "new"}])
    assert await dump(posts, "posts") == before
    assert len((await posts.execute("select * from titles")).rows) == len(before)
    assert not [t for t in (await posts.schema()).tables if t.startswith("_dblayer_")]
