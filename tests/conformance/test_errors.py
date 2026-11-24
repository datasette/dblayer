"""Each milestone-1 exception class is raised by the same action on every
backend, with the driver exception (where there is one) as __cause__."""

import pytest

from dblayer import (
    DatabaseClosed,
    DblayerError,
    IntegrityError,
    NotSupported,
    ParameterError,
    QueryError,
    QueryInterrupted,
    ResourceLimitExceeded,
    TableNotFound,
)


def assert_driver_cause(exc: BaseException) -> None:
    assert exc.__cause__ is not None
    assert not isinstance(exc.__cause__, DblayerError)


async def test_syntax_error_is_query_error(db):
    with pytest.raises(QueryError) as e:
        await db.execute("selec 1")
    assert type(e.value) is QueryError
    assert e.value.message
    assert e.value.code is not None
    assert_driver_cause(e.value)


async def test_missing_table_is_query_error(db):
    with pytest.raises(QueryError) as e:
        await db.execute("select * from no_such_table")
    assert "no_such_table" in e.value.message
    assert_driver_cause(e.value)


async def test_write_syntax_error(db):
    with pytest.raises(QueryError) as e:
        await db.execute_write("insert inot t values (1)")
    assert_driver_cause(e.value)


@pytest.mark.requires("unique_constraints")
async def test_integrity_error(db):
    await db.execute_write("create table t (id integer primary key)")
    await db.execute_write("insert into t (id) values (1)")
    with pytest.raises(IntegrityError) as e:
        await db.execute_write("insert into t (id) values (1)")
    assert isinstance(e.value, QueryError)
    assert e.value.code is not None
    assert_driver_cause(e.value)


async def test_query_interrupted(db):
    sql = (
        "with recursive c(x) as (select 1 union all select x + 1 from c) "
        "select count(*) from c"
    )
    with pytest.raises(QueryInterrupted) as e:
        await db.execute(sql, time_limit_ms=20)
    assert_driver_cause(e.value)


async def test_resource_limit_exceeded(db):
    with pytest.raises(ResourceLimitExceeded):
        await db.execute("select :v", {"v": "z" * 10_000}, max_result_bytes=100)


async def test_parameter_error_is_query_error(db):
    with pytest.raises(QueryError):
        await db.execute("select :x", {})
    with pytest.raises(ParameterError):
        await db.execute("select :x", [1])


async def test_table_not_found(db):
    with pytest.raises(TableNotFound):
        await db.table_columns("nope")


async def test_not_supported_definition(db):
    await db.execute_write("create table t (x integer)")
    if db.capabilities.table_definition_sql:
        assert await db.definition("t")
    else:
        with pytest.raises(NotSupported):
            await db.definition("t")


async def test_database_closed(db):
    await db.execute("select 1")
    await db.close()
    for call in (
        lambda: db.execute("select 1"),
        lambda: db.execute_write("select 1"),
        lambda: db.table_names(),
        lambda: db.schema(),
        lambda: db.schema_version(),
        lambda: db.sleep(),
    ):
        with pytest.raises(DatabaseClosed):
            await call()
    with pytest.raises(DatabaseClosed):
        db.change_hint()
    with pytest.raises(DatabaseClosed):
        async for _ in db.stream("select 1"):
            pass


async def test_errors_do_not_poison_the_pool(db):
    for _ in range(10):
        with pytest.raises(QueryError):
            await db.execute("selec 1")
    assert (await db.execute("select 1")).rows == [(1,)]
    assert db.stats().open_read <= 3
