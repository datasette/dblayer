"""ClickHouse (chDB) backend specifics: parameters and injection, read-only
enforcement against untrusted SQL, limits and cancellation, the shared
embedded server, no-transaction semantics, key checks, native paths.

The conformance suite covers the portable behaviour (``-k clickhouse``)."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from pathlib import Path

import pytest

from dblayer import (
    Database,
    IntegrityError,
    NotSupported,
    ParameterError,
    QueryError,
    QueryInterrupted,
    ResourceLimitExceeded,
)

from . import clickhouse as ch

pytestmark = pytest.mark.skipif(ch.unavailable() is not None, reason=ch.unavailable() or "")


@pytest.fixture
async def db():
    backend = ch.make()
    d = Database(backend, name="ch")
    yield d
    await d.close()
    await ch.cleanup(backend)


@pytest.fixture
async def items(db):
    await db.create_table("items", {"id": "integer", "name": "text", "n": "integer"}, pk="id")
    await db.insert_rows("items", [{"id": i, "name": f"n{i}", "n": i % 3} for i in range(1, 11)])
    return db


# -- parameters ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "'); DROP TABLE items; --",
        "a\\nb",
        "tab\there\nnewline\r",
        "\\N",
        "back\\\\slash'quote\"",
        "\x00nul",
        "{p1:String}",
        "é ☃ \U0001f600",
        2**63 - 1,
        -(2**63),
        2**64 - 1,
        float("inf"),
        -0.5,
        None,
        True,
    ],
    ids=repr,
)
async def test_parameter_values_round_trip_and_never_reach_the_parser(items, value):
    r = await items.execute("select :v as v, (select count() from items) as c", {"v": value})
    assert r.rows[0]["v"] == value
    assert r.rows[0]["c"] == 10  # nothing was injected


async def test_bytes_parameter_round_trip(db):
    raw = bytes(range(256))
    r = await db.execute("select :b as b, length(:b) as n", {"b": raw})
    assert r.rows[0]["b"] == raw and r.rows[0]["n"] == 256
    # Valid UTF-8 bytes come back as text (one String type: distinct_blob_type).
    assert (await db.execute("select :b as b", {"b": b"abc"})).rows[0]["b"] == "abc"


async def test_placeholders_inside_strings_comments_and_heredocs_are_ignored(db):
    sql = (
        "select ':x' as a, '\\' :x' as b, $$ :x $$ as c, /* :x /* :x */ */ :x as d "
        "-- :x\n"
    )
    r = await db.execute(sql, {"x": 5})
    assert r.rows[0] == (":x", "' :x", " :x ", 5)


async def test_unsupported_parameter_type(db):
    with pytest.raises(ParameterError):
        await db.execute("select :v", {"v": object()})
    with pytest.raises(ParameterError):
        await db.execute("select :v", {"v": 2**64})  # as SQLite: 64 bits at most


# -- read-only enforcement for untrusted SQL --------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "insert into items (id, name) values (99, 'x')",
        "create table evil (x integer)",
        "drop table items",
        "alter table items delete where 1",
        "select * from file('/etc/hosts', 'LineAsString')",
        "select * from url('http://127.0.0.1:9/', 'LineAsString')",
        "select * from remote('127.0.0.1:9', system.one)",
        "select * from s3('http://127.0.0.1:9/x.csv')",
        "select * from executable('ls', 'LineAsString', 'x String')",
        "select * from sqlite('/etc/hosts', 't')",
        "select 1 into outfile '/tmp/dblayer-clickhouse-outfile.txt'",
        "select 1 settings max_execution_time = 1000",
        "set max_memory_usage = 0",
        "use default",
        "system drop dns cache",
        "optimize table items",
        "select 1; drop table items",
        "select 1 format CSV",
        # The scalar file() is not stopped by readonly=1, and chDB reads any
        # absolute path with it.
        "select file('/etc/hosts')",
        "select `fi\\x6Ce`('/etc/hosts')",
        "select \"fi\\x6Ce\"('/etc/hosts')",
        "select * from python('x')",
        # In the Linux chDB build only, and not refused until found by
        # test_host_function_lists_cover_this_clickhouse on Linux CI.
        "select * from deltaLakeLocal('/etc')",
        "select * from hdfs('hdfs://127.0.0.1:9/x', 'LineAsString')",
        # Lexer agreement: ClickHouse's '# ' comments and backslash escapes
        # in quoted names hid INTO OUTFILE from the scanner, and the engine
        # wrote the file (found and fixed here).
        "select 1 # '\n into outfile '/tmp/dblayer-clickhouse-outfile.txt'",
        "select 1 as \"a\\\"\" into outfile '/tmp/dblayer-clickhouse-outfile.txt'",
        "select 1 as `a\\`` into outfile '/tmp/dblayer-clickhouse-outfile.txt'",
        "select 1 #! '\n into outfile '/tmp/dblayer-clickhouse-outfile.txt'",
    ],
)
async def test_untrusted_reads_cannot_write_or_reach_the_host(items, sql):
    with pytest.raises(QueryError):
        await items.execute(sql)
    assert not Path("/tmp/dblayer-clickhouse-outfile.txt").exists()
    assert (await items.execute("select count() from items")).rows[0][0] == 10


@pytest.mark.parametrize(
    "sql",
    [
        "select * from file('/etc/hosts', 'LineAsString')",
        "select file('/etc/hosts')",
        "insert into function file('dblayer-x.csv', 'CSV', 'x Int64') select 1",
        "insert into items (id, name) values (500, file('/etc/hosts'))",
        "insert into items (id, name) select 500, line from url('http://127.0.0.1:9/', 'LineAsString')",
        "create table ft (line String) engine = File('LineAsString', '/etc/hosts')",
        "create table ft (line String) engine = URL('http://127.0.0.1:9/', 'LineAsString')",
        "create table ft engine = Memory as select * from s3('http://127.0.0.1:9/x.csv')",
        "create dictionary dd (id UInt64, v String) primary key id "
        "source(file(path '/etc/hosts' format 'TSV')) layout(flat()) lifetime(0)",
        "create or replace dictionary dd (id UInt64) primary key id source(null()) layout(flat()) lifetime(0)",
        "attach table ft from '/etc' (x String) engine = Memory",
        "backup table items to File('dblayer-backup')",
        "select `fi\\x6Ce`('/etc/hosts')",
    ],
)
async def test_writes_cannot_reach_the_host_unless_allowed(items, sql):
    with pytest.raises(QueryError) as e:
        await items.execute_write(sql)
    assert e.value.code == "HOST_ACCESS"


async def test_host_access_allowed_on_write_connections(tmp_path):
    """allow_host_access=True lifts the checks on the write path only."""
    backend = ch.make(allow_host_access=True)
    d = Database(backend)
    try:
        r = await d.execute_write("select count() > 0 from file('/etc/hosts', 'LineAsString')")
        assert r.rows == [(1,)] or r.rows == [(True,)]
        with pytest.raises(QueryError):
            await d.execute("select file('/etc/hosts')")
    finally:
        await d.close()
        await ch.cleanup(backend)


async def test_host_function_lists_cover_this_clickhouse(db):
    """Every table function and engine of the running ClickHouse is either
    known safe or refused, so a new version's additions fail here."""
    from dblayer.backends import clickhouse as mod

    fns = {r[0] for r in (await db.execute("select name from system.table_functions")).rows}
    engines = {r[0] for r in (await db.execute("select name from system.table_engines")).rows}
    unknown_fns = fns - mod._SAFE_TABLE_FUNCTIONS - mod._HOST_TABLE_FUNCTIONS
    assert not unknown_fns, f"classify these table functions: {sorted(unknown_fns)}"
    host_engines = engines - mod._SAFE_ENGINES
    for name in sorted(host_engines):
        with pytest.raises(QueryError):
            await db.execute_write(f"create table e_{name.lower()} (x String) engine = {name}")


async def test_ordinary_sql_is_not_refused(items):
    """The checks do not catch ordinary names: a column called file, the
    format() string function, backslashes in strings, a profile column."""
    await items.execute_write("create table docs (id Int64, file String, profile String) order by id")
    await items.execute_write("insert into docs values (1, 'a\\\\b', 'p')")
    r = await items.execute(
        "select file, format('{}-{}', file, profile), 'file(' as s, `file` from docs # note\n"
    )
    assert r.rows == [("a\\b", "a\\b-p", "file(", "a\\b")]


async def test_writes_cannot_use_outfile_unless_allowed(items, tmp_path):
    target = tmp_path / "out.txt"
    with pytest.raises(QueryError):
        await items.execute_write(
            f"insert into items select 100, 'x', 0 from file('{target}', 'CSV', 'x Int64')"
            " into outfile 'x'"
        )
    with pytest.raises(QueryError):
        await items.execute_write(f"insert into items from infile '{target}' format CSV")


async def test_other_databases_of_the_server_are_readable(items):
    """Not isolated: one chDB process serves every ClickHouse database, and
    readonly=1 allows reading any of them (documented security finding)."""
    other = ch.make()
    o = Database(other)
    try:
        await o.create_table("secret", {"id": "integer"}, pk="id")
        await o.insert_rows("secret", [{"id": 42}])
        q = o.dialect.quote_identifier(other.database)
        r = await items.execute(f"select id from {q}.secret")
        assert r.rows == [(42,)]
    finally:
        await o.close()
        await ch.cleanup(other)


# -- limits and cancellation ------------------------------------------------------------

INFINITE = "select count() from system.numbers"


async def test_time_limit_kills_the_query(db):
    t0 = time.perf_counter()
    with pytest.raises(QueryInterrupted):
        await db.execute(INFINITE, time_limit_ms=150)
    elapsed = time.perf_counter() - t0
    assert elapsed < 5, elapsed  # lands in ~0.2 s; generous bound for loaded machines
    assert (await db.execute("select 1 as a")).rows == [(1,)]


async def test_time_limit_on_a_write(items):
    with pytest.raises(QueryInterrupted):
        await items.execute_write(
            "insert into items select number + 1000, 'x', 0 from system.numbers",
            time_limit_ms=150,
        )
    await items.execute_write("insert into items (id, name) values (500, 'after')")


async def test_cancelling_the_task_kills_the_query(db):
    task = asyncio.create_task(db.execute(INFINITE, time_limit_ms=None))
    await asyncio.sleep(0.2)
    t0 = time.perf_counter()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert time.perf_counter() - t0 < 5
    assert db.stats().in_use_read == 0
    assert (await db.execute("select 2 as a")).rows == [(2,)]


async def test_memory_limit():
    backend = ch.make(memory_limit=50 << 20)
    d = Database(backend)
    try:
        with pytest.raises(ResourceLimitExceeded):
            await d.execute(
                "select groupArray(toString(number)) from numbers(50000000)", time_limit_ms=20000
            )
    finally:
        await d.close()
        await ch.cleanup(backend)


async def test_max_rows_stops_early(db):
    t0 = time.perf_counter()
    r = await db.execute("select number from system.numbers", max_rows=5)
    assert len(r) == 5 and r.truncated
    assert time.perf_counter() - t0 < 5


async def test_max_result_bytes(db):
    with pytest.raises(ResourceLimitExceeded):
        await db.execute("select repeat('x', 1000) from numbers(100000)", max_result_bytes=100_000)


# -- the shared embedded server ------------------------------------------------------------


async def test_one_data_directory_per_process(tmp_path, db):
    await db.execute("select 1")  # the shared server is up
    from dblayer.backends.clickhouse import ClickHouse

    elsewhere = Database(ClickHouse(tmp_path / "other"))
    with pytest.raises(NotSupported):
        await elsewhere.execute("select 1")
    await elsewhere.close()


async def test_server_stops_when_the_last_connection_closes(db):
    from dblayer.backends.clickhouse import ClickHouse

    from dblayer.backends import clickhouse as chmod

    await db.execute("select 1")
    assert ClickHouse.server_running()
    starts = ClickHouse.server_starts()
    await db.sleep()
    # Failed once on macOS CI with the server still up: the count says
    # whether another connection (left open by an earlier test) held it.
    assert not ClickHouse.server_running(), f"{chmod._server.open} chDB connections still open"
    await db.execute("select 1")
    assert ClickHouse.server_starts() == starts + 1


async def test_cancelled_connects_do_not_keep_the_server_up():
    """A task cancelled while its connection was being opened (a deadline,
    a cancelled stream) left that chDB connection open, so the shared
    server never stopped (found by tests/conformance/test_stream_races.py
    running before this file)."""
    from dblayer.backends.clickhouse import ClickHouse

    backend = ch.make()
    d = Database(backend)
    try:
        for i in range(30):
            await d.sleep()
            task = asyncio.create_task(d.execute("select 1"))
            await asyncio.sleep(0.0005 * (i % 6))
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        await d.sleep()
        assert not ClickHouse.server_running()
    finally:
        await d.close()
        await ch.cleanup(backend)


async def test_many_databases_share_the_server(db):
    backends = [ch.make() for _ in range(5)]
    dbs = [Database(b) for b in backends]
    try:
        for i, d in enumerate(dbs):
            await d.create_table("t", {"id": "integer"}, pk="id")
            await d.insert_rows("t", [{"id": j} for j in range(i + 1)])
        counts = [(await d.execute("select count() from t")).rows[0][0] for d in dbs]
        assert counts == [1, 2, 3, 4, 5]
    finally:
        for d, b in zip(dbs, backends):
            await d.close()
            await ch.cleanup(b)


# -- no transactions --------------------------------------------------------------------------


async def test_rolled_back_writes_are_kept_and_logged(items, caplog):
    with caplog.at_level(logging.WARNING, logger="dblayer.backends.clickhouse"):
        with pytest.raises(RuntimeError):
            async with items.transaction() as tx:
                await tx.execute("delete from items where id = 1")
                raise RuntimeError
    assert (await items.execute("select count() from items")).rows[0][0] == 9
    assert "were already applied and are kept" in caplog.text


async def test_update_delete_rowcounts_and_visibility(items):
    r = await items.execute_write("update items set name = 'z' where n = :n", {"n": 0})
    assert r.rowcount == 3
    assert (await items.execute("select count() from items where name = 'z'")).rows[0][0] == 3
    r = await items.execute_write("delete from items where id > 8")
    assert r.rowcount == 2
    r = await items.execute_write("delete from items")
    assert r.rowcount == 8


async def test_update_on_a_table_without_lightweight_update_columns(db):
    await db.execute_write(
        "create table plain (id Int64, v String) engine = MergeTree order by id "
        "settings enable_block_number_column = 0, enable_block_offset_column = 0"
    )
    await db.execute_write("insert into plain values (1, 'a')")
    r = await db.execute_write("update plain set v = 'b' where id = 1")
    assert r.rowcount == 1
    assert (await db.execute("select v from plain")).rows == [("b",)]


# -- keys ---------------------------------------------------------------------------------------


async def test_named_operations_check_keys_plain_sql_does_not(items):
    with pytest.raises(IntegrityError):
        await items.insert_rows("items", [{"id": 1, "name": "dup"}])
    # The engine itself accepts the duplicate (unique_constraints is false).
    await items.execute_write("insert into items (id, name) values (1, 'dup')")
    assert (await items.execute("select count() from items where id = 1")).rows[0][0] == 2


async def test_native_insert_is_used_and_checks_keys(items):
    backend = items._backend
    before = backend.native_calls["insert_rows"]
    r = await items.insert_rows("items", [{"id": 100 + i, "name": "x"} for i in range(50)])
    assert r.rowcount == 50
    assert backend.native_calls["insert_rows"] == before + 1
    with pytest.raises(IntegrityError):
        await items.insert_rows("items", [{"id": 200}, {"id": 200}])
    with pytest.raises(IntegrityError):
        await items.insert_rows("items", [{"id": 100, "name": "again"}])


async def test_execute_many_insert_goes_by_column_name(db):
    """execute_many of INSERT ... VALUES ($1, ...) is sent as one Arrow
    stream; Arrow columns are matched by name, so the column list in the
    SQL, not the table's order, decides where each value lands."""
    await db.execute_write(
        "create table m (id Int64, a Nullable(String), b Nullable(Float64), d Nullable(Date)) order by id"
    )
    async with db.transaction() as tx:
        r = await tx.execute_many(
            "insert into m (b, id, a) values (:b, :id, :a)",
            [{"b": 1.5, "id": 2, "a": "x"}, {"b": None, "id": 1, "a": None}],
        )
        assert r.rowcount == 2
        # A Date column is not converted by the Arrow path: VALUES instead.
        r = await tx.execute_many(
            "insert into m (id, d) values (:id, :d)", [{"id": 3, "d": "2024-01-02"}]
        )
        assert r.rowcount == 1
    rows = (await db.execute("select id, a, b, toString(d) from m order by id")).rows
    assert rows == [(1, None, None, None), (2, "x", 1.5, None), (3, None, None, "2024-01-02")]


async def test_many_placeholders_inline_numbers(items):
    ids = list(range(1, 201))
    sql = "select count() from items where id in (" + ", ".join(f":p{i}" for i in ids) + ")"
    r = await items.execute(sql, {f"p{i}": i for i in ids})
    assert r.rows == [(10,)]
    from dblayer.backends.clickhouse import bind

    native, values = bind(
        "select " + ", ".join(f"${i}" for i in range(1, 70)),
        [-(2**63), 2**64 - 1, 1.5, -2.5, None, True, "s'; drop"] + list(range(62)),
    )
    assert native.startswith("select (-9223372036854775808), 18446744073709551615, 1.5, (-2.5), NULL, true, {p7:String}")
    assert values == {"p7": "s'; drop"}


async def test_staged_replace_uses_exchange(items):
    async with items.staged_import("items", mode="replace") as imp:
        await imp.write([{"id": 1, "name": "new", "n": 0}])
    assert (await items.execute("select id, name from items")).rows == [(1, "new")]
    assert await items.table_names() == ["items"]


# -- introspection ------------------------------------------------------------------------------


async def test_schema_version_and_change_hint(items):
    v1 = await items.schema_version()
    h1 = items.change_hint()
    await items.add_column("items", "extra", "float")
    assert await items.schema_version() != v1
    await asyncio.sleep(0.01)
    await items.insert_rows("items", [{"id": 77, "name": "x"}])
    assert items.change_hint() != h1


async def test_definition_is_show_create(items):
    d = await items.definition("items")
    assert "CREATE TABLE" in d and "MergeTree" in d and "ORDER BY" in d


# -- engine export (opt-in) ---------------------------------------------------------------------


async def test_engine_export_is_parse_equal_not_identical():
    backend = ch.make(engine_export=True)
    d = Database(backend)
    try:
        assert d.export_formats == {"csv", "json"}
        await d.create_table("t", {"id": "integer", "x": "float", "s": "text"}, pk="id")
        await d.insert_rows("t", [{"id": 1, "x": 2.0, "s": "a"}, {"id": 2, "x": None, "s": ""}])
        native = b"".join([c async for c in d.export("select * from t order by id", format="csv")])
        fallback = b"".join(
            [c async for c in d.export("select * from t order by id", format="csv", native=False)]
        )
        assert fallback == b'id,x,s\r\n1,2.0,a\r\n2,,""\r\n'
        assert native == b'id,x,s\r\n1,2,"a"\r\n2,,""\r\n'
        import json

        body = b"".join([c async for c in d.export("select * from t order by id", format="json")])
        assert json.loads(body)["rows"] == [
            {"id": 1, "x": 2, "s": "a"},
            {"id": 2, "x": None, "s": ""},
        ]
    finally:
        await d.close()
        await ch.cleanup(backend)
