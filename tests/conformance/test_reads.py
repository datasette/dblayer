"""Reads: values, parameter styles, max_rows, time limits, max_result_bytes."""

import time

import pytest

from dblayer import (
    DEFAULT,
    ParameterError,
    QueryError,
    QueryInterrupted,
    ResourceLimitExceeded,
    Results,
)

UNBOUNDED = (
    "with recursive c(x) as (select 1 union all select x + 1 from c) "
    "select count(*) from c"
)


def series(n: int) -> str:
    return (
        "with recursive c(x) as (select 1 union all select x + 1 from c "
        f"where x < {n}) select x from c"
    )


@pytest.fixture
async def posts(db):
    await db.execute_write(
        "create table posts (id integer primary key, title text, author text)"
    )
    for i in range(1, 6):
        await db.execute_write(
            "insert into posts (id, title, author) values (:id, :title, :author)",
            {"id": i, "title": f"Post {i}", "author": "simon" if i % 2 else "cleo"},
        )
    return db


async def test_simple_select(db):
    r = await db.execute("select 1 as a, 'two' as b")
    assert isinstance(r, Results)
    assert r.columns == ("a", "b")
    assert r.rows == [(1, "two")]
    assert r.rows[0]["b"] == "two"
    assert r.truncated is False


async def test_named_params(posts):
    r = await posts.execute(
        "select id, title from posts where author = :author order by id", {"author": "simon"}
    )
    assert r.columns == ("id", "title")
    assert [row["id"] for row in r.rows] == [1, 3, 5]


async def test_positional_params(posts):
    r = await posts.execute(
        "select id from posts where author = ? and id > ? order by id", ["simon", 1]
    )
    assert [row[0] for row in r] == [3, 5]


async def test_repeated_named_param(db):
    r = await db.execute("select :a as x, :a as y, :b as z", {"a": 1, "b": 2})
    assert r.rows[0] == (1, 1, 2)


async def test_placeholders_in_strings_comments_and_identifiers(db):
    r = await db.execute(
        "select ':nope' as s, '?' as q, :real as \":quoted\" -- :also_nope ?\n"
        " /* :and_nope ? */",
        {"real": 5},
    )
    assert r.columns == ("s", "q", ":quoted")
    assert r.rows[0] == (":nope", "?", 5)


async def test_cast_of_parameter(db):
    r = await db.execute("select cast(:x as integer) as n", {"x": "42"})
    assert r.rows[0]["n"] == 42


@pytest.mark.parametrize(
    "value",
    [None, 0, -1, 2**53, 1.5, "", "héllo ☃", b"\x00\x01bytes", True, False],
    ids=repr,
)
async def test_portable_values_round_trip(db, value):
    r = await db.execute("select :v as v", {"v": value})
    got = r.rows[0]["v"]
    assert got == value
    if isinstance(value, bytes):
        assert isinstance(got, bytes)


async def test_missing_parameter_raises_parameter_error(db):
    with pytest.raises(ParameterError):
        await db.execute("select :a, :b", {"a": 1})


async def test_wrong_positional_count(db):
    with pytest.raises(ParameterError):
        await db.execute("select ?, ?", [1])


async def test_execute_is_read_only(posts):
    with pytest.raises(QueryError):
        await posts.execute("delete from posts")
    assert len(await posts.execute("select * from posts")) == 5


async def test_empty_result(posts):
    r = await posts.execute("select id from posts where id < 0")
    assert r.rows == [] and r.columns == ("id",) and not r.truncated


# -- max_rows ----------------------------------------------------------------


async def test_max_rows_truncates(posts):
    r = await posts.execute("select id from posts order by id", max_rows=3)
    assert [row[0] for row in r] == [1, 2, 3]
    assert r.truncated is True


async def test_max_rows_exact_is_not_truncated(posts):
    r = await posts.execute("select id from posts", max_rows=5)
    assert len(r) == 5 and r.truncated is False


async def test_max_rows_zero(posts):
    r = await posts.execute("select id from posts", max_rows=0)
    assert r.rows == [] and r.truncated is True


async def test_max_rows_default_from_database(make_db):
    db = make_db(max_rows=10)
    r = await db.execute(series(50))
    assert len(r) == 10 and r.truncated
    r = await db.execute(series(50), max_rows=None)
    assert len(r) == 50 and not r.truncated
    r = await db.execute(series(50), max_rows=DEFAULT)
    assert len(r) == 10


async def test_max_rows_stops_an_unbounded_query(db):
    # Fetching one extra row means an infinite result is no problem.
    sql = "with recursive c(x) as (select 1 union all select x + 1 from c) select x from c"
    r = await db.execute(sql, max_rows=100, time_limit_ms=5000)
    assert len(r) == 100 and r.truncated


# -- time limit ----------------------------------------------------------------


async def test_time_limit_interrupts(db):
    start = time.perf_counter()
    with pytest.raises(QueryInterrupted):
        await db.execute(UNBOUNDED, time_limit_ms=50)
    assert time.perf_counter() - start < 2


async def test_time_limit_default_from_database(make_db):
    db = make_db(time_limit_ms=30)
    with pytest.raises(QueryInterrupted):
        await db.execute(UNBOUNDED)


async def test_fast_query_within_time_limit(db):
    r = await db.execute(series(10), time_limit_ms=1000)
    assert len(r) == 10


async def test_time_limit_does_not_persist(db):
    with pytest.raises(QueryInterrupted):
        await db.execute(UNBOUNDED, time_limit_ms=20)
    # Same pooled connection, no limit: a moderately long query completes.
    r = await db.execute(series(200_000) + " order by x desc", max_rows=1)
    assert r.rows[0][0] == 200_000


async def test_connection_usable_after_interrupt(db):
    for _ in range(3):
        with pytest.raises(QueryInterrupted):
            await db.execute(UNBOUNDED, time_limit_ms=10)
        assert (await db.execute("select 1")).rows == [(1,)]


# -- max_result_bytes -----------------------------------------------------------


async def test_max_result_bytes(db):
    sql = (
        "with recursive c(x) as (select 1 union all select x + 1 from c where x < 1000) "
        "select x, :pad as pad from c"
    )
    with pytest.raises(ResourceLimitExceeded):
        await db.execute(sql, {"pad": "x" * 100}, max_result_bytes=10_000)
    r = await db.execute(sql, {"pad": "x" * 100}, max_result_bytes=1_000_000)
    assert len(r) == 1000


async def test_max_result_bytes_default_from_database(make_db):
    db = make_db(max_result_bytes=1000)
    with pytest.raises(ResourceLimitExceeded):
        await db.execute("select :v", {"v": "y" * 5000})
    assert len(await db.execute("select :v", {"v": "y" * 5000}, max_result_bytes=None)) == 1
