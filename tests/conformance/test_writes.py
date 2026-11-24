"""Single writes and WriteResult."""

import asyncio

import pytest

from dblayer import IntegrityError, QueryInterrupted, WriteResult


@pytest.fixture
async def items(db):
    # UNIQUE only where the engine enforces it (not ClickHouse); the
    # uniqueness test itself is skipped there.
    unique = " unique" if db.capabilities.unique_constraints else ""
    await db.execute_write(
        f"create table items (id integer primary key, name text not null{unique}, n integer)"
    )
    return db


async def test_ddl_write(db):
    r = await db.execute_write("create table t (x integer)")
    assert isinstance(r, WriteResult)
    assert r.rows == [] and r.last_pk is None


@pytest.mark.requires("implicit_rowid")
async def test_insert_rowcount_and_last_pk(items):
    r = await items.execute_write(
        "insert into items (name, n) values (:name, :n)", {"name": "a", "n": 1}
    )
    assert r.rowcount == 1
    assert r.last_pk == 1
    r = await items.execute_write("insert into items (name, n) values (?, ?)", ["b", 2])
    assert r.last_pk == 2


async def test_insert_explicit_pk_rowcount(items):
    r = await items.execute_write(
        "insert into items (id, name, n) values (10, 'x', 1), (11, 'y', 1)"
    )
    assert r.rowcount == 2


async def test_update_and_delete_rowcount(items):
    for i, name in enumerate("abcd"):
        await items.execute_write(
            "insert into items (id, name, n) values (:id, :name, :n)",
            {"id": i + 1, "name": name, "n": i % 2},
        )
    r = await items.execute_write("update items set n = 5 where n = :n", {"n": 1})
    assert r.rowcount == 2
    assert r.last_pk is None
    r = await items.execute_write("delete from items where id > 1")
    assert r.rowcount == 3
    assert len(await items.execute("select * from items")) == 1


async def test_write_visible_to_next_read(items):
    await items.execute_write("insert into items (id, name) values (1, 'a')")
    assert (await items.execute("select name from items")).rows == [("a",)]


@pytest.mark.requires("returning")
async def test_returning(items):
    r = await items.execute_write(
        "insert into items (id, name, n) values (1, 'a', 1), (2, 'b', 2) returning id, name"
    )
    assert r.columns == ("id", "name")
    assert sorted(r.rows) == [(1, "a"), (2, "b")]
    assert r.rowcount == 2
    assert r.truncated is False


@pytest.mark.requires("returning")
async def test_returning_limit(items):
    values = ", ".join(f"({i}, 'n{i}', {i})" for i in range(1, 21))
    await items.execute_write(f"insert into items (id, name, n) values {values}")
    r = await items.execute_write(
        "update items set n = n + 100 returning id", returning_limit=5
    )
    assert len(r.rows) == 5
    assert r.truncated is True
    assert r.rowcount == 20
    # Every row was updated even though only five came back.
    assert (await items.execute("select min(n) from items")).rows[0][0] == 101


@pytest.mark.requires("returning")
async def test_returning_limit_default_from_database(make_db):
    db = make_db(returning_limit=2)
    await db.execute_write("create table t (x integer)")
    r = await db.execute_write("insert into t (x) values (1), (2), (3) returning x")
    assert len(r.rows) == 2 and r.truncated and r.rowcount == 3
    r = await db.execute_write(
        "insert into t (x) values (1), (2), (3) returning x", returning_limit=None
    )
    assert len(r.rows) == 3 and not r.truncated


@pytest.mark.requires("unique_constraints")
async def test_integrity_error_unique(items):
    await items.execute_write("insert into items (id, name) values (1, 'a')")
    with pytest.raises(IntegrityError) as e:
        await items.execute_write("insert into items (id, name) values (2, 'a')")
    assert e.value.__cause__ is not None
    assert e.value.message


async def test_integrity_error_not_null(items):
    with pytest.raises(IntegrityError):
        await items.execute_write("insert into items (id, name) values (1, null)")


async def test_writes_are_serialised(items):
    await items.execute_write("insert into items (id, name, n) values (1, 'counter', 0)")

    async def bump():
        await items.execute_write("update items set n = n + 1 where id = 1")

    await asyncio.gather(*(bump() for _ in range(25)))
    assert (await items.execute("select n from items where id = 1")).rows[0][0] == 25
    if items.capabilities.concurrent_writers:
        # No write queue: the server arbitrates, up to the pool's writers.
        assert 1 <= items.stats().open_write <= items._get_pool().max_write_connections
    else:
        assert items.stats().open_write <= 1


async def test_write_time_limit_rolls_back_statement(items):
    sql = (
        "insert into items (id, name) "
        "with recursive c(x) as (select 1 union all select x + 1 from c) "
        "select x, 'n' || x from c"
    )
    with pytest.raises(QueryInterrupted):
        await items.execute_write(sql, time_limit_ms=50)
    assert (await items.execute("select count(*) from items")).rows[0][0] == 0
    # The write connection is still usable.
    await items.execute_write("insert into items (id, name) values (1, 'ok')")


async def test_failed_write_leaves_write_slot_free(items):
    with pytest.raises(IntegrityError):
        await items.execute_write("insert into items (id, name) values (1, null)")
    r = await asyncio.wait_for(
        items.execute_write("insert into items (id, name) values (1, 'a')"), timeout=2
    )
    assert r.rowcount == 1


@pytest.mark.requires("implicit_rowid")
async def test_last_pk_none_after_upsert_that_updates(items):
    await items.execute_write("insert into items (id, name) values (5, 'a')")
    # Make the connection's last_insert_rowid point somewhere else.
    await items.execute_write("insert into items (id, name) values (9, 'z')")
    upsert = (
        "insert into items (id, name, n) values (:id, :name, 1) "
        "on conflict (id) do update set n = excluded.n"
    )
    r = await items.execute_write(upsert, {"id": 5, "name": "a"})
    assert r.rowcount == 1
    assert r.last_pk is None  # updated, did not insert
    r = await items.execute_write(upsert, {"id": 6, "name": "b"})
    assert r.last_pk == 6


@pytest.mark.requires("implicit_rowid")
async def test_last_pk_when_the_previous_rowid_is_reused_or_updated(items):
    # A before/after comparison of last_insert_rowid() cannot tell these
    # apart: inserting rowid 5 again after deleting it, and an upsert that
    # updates the row the previous insert made.
    r = await items.execute_write("insert into items (id, name) values (5, 'a')")
    assert r.last_pk == 5
    await items.execute_write("delete from items where id = 5")
    r = await items.execute_write("insert into items (id, name) values (5, 'b')")
    assert r.rowcount == 1 and r.last_pk == 5
    r = await items.execute_write(
        "insert into items (id, name, n) values (5, 'b', 2) on conflict (id) do update set n = 2"
    )
    assert r.rowcount == 1 and r.last_pk is None
    r = await items.execute_write("insert into items (id, name) values (5, 'b') on conflict do nothing")
    assert r.rowcount == 0 and r.last_pk is None
    # SQL still sees the last real insert.
    assert (await items.execute_write("insert into items (id, name) values (7, 'c')")).last_pk == 7
    async with items.transaction() as tx:
        await tx.execute("insert into items (name) select 'x' where 0")
        assert (await tx.execute("select last_insert_rowid() as r")).rows == [(7,)]


@pytest.mark.requires("implicit_rowid")
async def test_last_pk_none_when_conflict_skipped(items):
    await items.execute_write("insert into items (id, name) values (1, 'a')")
    r = await items.execute_write(
        "insert into items (id, name) values (1, 'a') on conflict do nothing"
    )
    assert r.rowcount == 0 and r.last_pk is None


async def test_last_pk_none_for_non_inserts(items):
    await items.execute_write("insert into items (id, name) values (1, 'a')")
    assert (await items.execute_write("update items set n = 2")).last_pk is None
    assert (await items.execute_write("delete from items")).last_pk is None


@pytest.mark.requires_not("implicit_rowid")
async def test_last_pk_none_without_rowids(items):
    r = await items.execute_write("insert into items (id, name) values (1, 'a')")
    assert r.last_pk is None
