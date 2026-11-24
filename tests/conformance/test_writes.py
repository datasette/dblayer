"""Single writes and WriteResult."""

import asyncio

import pytest

from dblayer import IntegrityError, QueryInterrupted, WriteResult


@pytest.fixture
async def items(db):
    await db.execute_write(
        "create table items (id integer primary key, name text not null unique, n integer)"
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
