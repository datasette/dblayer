"""asyncpg against psycopg 3, problem by problem.

Each test pins down one of the asyncpg problems recorded in ``notes.md``
(milestones 3 and 5) at the *driver* level, so it is evidence of what each
driver does by itself, and where useful checks what the two dblayer
backends make of it. Both drivers talk to the local ``dblayer_test``
database only (``tests/pg.py`` refuses any other DSN), each test in a fresh
schema.

Timing and memory figures are printed (``-s``) and asserted only with
generous bounds.
"""

from __future__ import annotations

import asyncio
import re
import threading
import time
import tracemalloc
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest

from dblayer import Database, QueryError, ResourceLimitExceeded

from . import pg
from .helpers import count_open_fds

pytestmark = pytest.mark.skipif(
    pg.unavailable_psycopg() is not None, reason=pg.unavailable_psycopg() or ""
)

BACKENDS_DIR = Path(__file__).parent.parent / "dblayer" / "backends"


@pytest.fixture
async def schema() -> AsyncIterator[str]:
    name = pg.new_schema()
    conn = await pg.connect()
    try:
        await conn.execute(f'CREATE SCHEMA "{name}"')
    finally:
        await conn.close()
    yield name
    await pg.drop_schema(name)


async def apg(schema: str, **kwargs: Any) -> Any:
    import asyncpg

    return await asyncpg.connect(
        pg.DSN,
        server_settings={"search_path": f'"{schema}"', "application_name": pg.app_name(schema)},
        **kwargs,
    )


async def ppg(schema: str, **kwargs: Any) -> Any:
    import psycopg

    kwargs.setdefault("autocommit", True)
    kwargs.setdefault("cursor_factory", psycopg.AsyncRawCursor)
    return await psycopg.AsyncConnection.connect(
        pg.DSN,
        options=f"-c search_path={schema}",
        application_name=pg.app_name(schema),
        **kwargs,
    )


async def pfetch(conn: Any, sql: str, params: list[Any] | None = None) -> list[tuple[Any, ...]]:
    cur = await conn.execute(sql, params)
    return await cur.fetchall()


# -- 1. private driver APIs ---------------------------------------------------------


def _private_uses(path: Path, names: tuple[str, ...]) -> list[str]:
    """Attribute accesses ``<name>._something`` on driver objects."""
    src = path.read_text()
    pattern = re.compile(r"\b(?:" + "|".join(names) + r")\._[a-z]\w*")
    return sorted(set(pattern.findall(src)))


def test_private_driver_apis():
    asyncpg_uses = _private_uses(BACKENDS_DIR / "postgres.py", ("raw", r"self\._raw", "transport"))
    psycopg_uses = _private_uses(
        BACKENDS_DIR / "postgres_psycopg.py", ("raw", r"self\._raw", "cur", "aux", "pgconn")
    )
    print("asyncpg backend:", asyncpg_uses, "psycopg backend:", psycopg_uses)
    assert {"raw._prepare", "raw._execute"} <= set(asyncpg_uses)
    assert "_sock" in (BACKENDS_DIR / "postgres.py").read_text()
    assert psycopg_uses == []


# -- 2. statement cache after DDL --------------------------------------------------------


async def test_statement_cache_after_ddl_by_another_connection(schema):
    import asyncpg
    import psycopg

    admin = await apg(schema)
    await admin.execute("create table t (id int primary key, a text)")

    # asyncpg: its statement cache is always on. Inside a transaction it
    # does not retry a statement made stale by another connection's DDL.
    a = await apg(schema)
    await a.fetch("select * from t")
    await admin.execute("alter table t add column b text")
    async with a.transaction():
        with pytest.raises(asyncpg.InvalidCachedStatementError):
            await a.fetch("select * from t")
    await a.close()

    # psycopg with server-side prepared statements (prepare_threshold=0:
    # prepare at once) has the same failure, and does not retry either.
    p = await ppg(schema, prepare_threshold=0)
    await pfetch(p, "select * from t")
    await admin.execute("alter table t add column c text")
    await p.execute("BEGIN")
    with pytest.raises(psycopg.errors.FeatureNotSupported, match="cached plan"):
        await pfetch(p, "select * from t")
    await p.execute("ROLLBACK")
    await p.close()

    # psycopg without them (prepare_threshold=None, the backend's default):
    # nothing to go stale, and still one round trip per statement.
    p = await ppg(schema, prepare_threshold=None)
    for _ in range(10):
        await pfetch(p, "select * from t")
    await admin.execute("alter table t add column d text")
    await p.execute("BEGIN")
    assert len((await p.execute("select * from t")).description) == 5
    await p.execute("ROLLBACK")
    await p.close()
    await admin.close()


# -- 3. executemany row counts --------------------------------------------------------


async def test_executemany_row_counts(schema):
    a = await apg(schema)
    await a.execute("create table t (id int primary key)")
    assert await a.executemany("insert into t values ($1) on conflict do nothing", [(1,), (2,), (1,)]) is None
    await a.close()  # asyncpg: no counts at all (the backend wraps the SQL in a counting CTE)

    p = await ppg(schema)
    cur = p.cursor()
    await cur.executemany("insert into t values ($1) on conflict do nothing", [[3], [4], [3]])
    assert cur.rowcount == 2  # psycopg: the total across the parameter sets
    await cur.executemany("delete from t where id = $1", [[1], [3], [99]])
    assert cur.rowcount == 2
    await p.close()


# -- 4. untyped parameters --------------------------------------------------------------


async def test_untyped_parameters(schema):
    import asyncpg

    a = await apg(schema)
    with pytest.raises(asyncpg.DataError):
        await a.fetchval("select $1", 1)  # the server typed $1 as text
    with pytest.raises(asyncpg.DataError):
        await a.fetchval("select cast($1 as integer) + 1", "41")  # typed int4; a str
    await a.close()

    p = await ppg(schema)
    # psycopg sends the type with the value; a str is sent as "unknown".
    assert await pfetch(p, "select $1", [1]) == [(1,)]
    assert await pfetch(p, "select cast($1 as integer) + 1", ["41"]) == [(42,)]
    # Its default int dumper picks the smallest type (int2), which overflows
    # in arithmetic between two parameters; the backend sends int4 at least.
    import psycopg

    with pytest.raises(psycopg.errors.NumericValueOutOfRange):
        await pfetch(p, "select $1 * $2", [300, 300])
    await p.close()

    db = Database(pg.make("psycopg", schema=schema))
    try:
        assert (await db.execute("select :a * :b", {"a": 300, "b": 300})).rows == [(90000,)]
        assert (await db.execute("select :b", {"b": True})).rows == [(1,)]
    finally:
        await db.close()


# -- 5. a timer per cached statement ----------------------------------------------------


async def test_timers_scheduled_per_statement(schema):
    loop = asyncio.get_running_loop()

    def timers() -> int:
        return len(loop._scheduled)  # type: ignore[attr-defined]

    a = await apg(schema)  # asyncpg defaults: max_cached_statement_lifetime=300
    baseline = timers()
    for i in range(20):
        await a.fetchval(f"select {i}")
    asyncpg_timers = timers() - baseline
    await a.close()

    p = await ppg(schema, prepare_threshold=0)  # psycopg, preparing every statement
    baseline = timers()
    for i in range(20):
        await pfetch(p, f"select {i}")
    psycopg_timers = timers() - baseline
    await p.close()
    print(f"timers after 20 cached statements: asyncpg {asyncpg_timers}, psycopg {psycopg_timers}")
    assert asyncpg_timers >= 15
    assert psycopg_timers == 0


# -- 6. connections stranded on a closed event loop ------------------------------------------


def test_close_from_another_loop(schema):
    def stranded(make: Any) -> Any:
        holder: list[Any] = []

        async def open_one() -> None:
            holder.append(await make(schema))

        asyncio.run(open_one())  # the loop is closed, the connection open
        return holder[0]

    a = stranded(apg)
    p = stranded(ppg)
    steps: list[str] = []

    async def close_all() -> None:
        before = count_open_fds()  # this loop has descriptors of its own
        try:
            await asyncio.wait_for(a.close(), 2)
            steps.append("asyncpg close(): ok")
        except Exception as e:  # noqa: BLE001
            steps.append(f"asyncpg close(): {type(e).__name__}: {e}")
        try:
            a.terminate()
            steps.append("asyncpg terminate(): returned")
        except Exception as e:  # noqa: BLE001
            steps.append(f"asyncpg terminate(): {type(e).__name__}")
        steps.append(f"descriptors closed by asyncpg: {before - count_open_fds()}")
        before = count_open_fds()
        await p.close()  # PQfinish: no event loop involved
        steps.append(f"descriptors closed by psycopg: {before - count_open_fds()}")

    asyncio.run(close_all())
    print(steps)
    assert steps[0].startswith("asyncpg close(): RuntimeError")
    # terminate() returns but cannot close the socket on the dead loop.
    assert steps[2] == "descriptors closed by asyncpg: 0"
    assert steps[3] == "descriptors closed by psycopg: 1"
    assert p.closed
    # The asyncpg backend closes it through the private transport._sock.
    before = count_open_fds()
    a._transport._sock.close()  # type: ignore[attr-defined]
    assert count_open_fds() == before - 1


# -- 7. max_result_bytes: checked during the fetch or after it ------------------------------

_BIG = "select repeat('x', 1000) as s from generate_series(1, 50000)"  # ~50 MB


@pytest.mark.parametrize("driver", pg.DRIVERS)
async def test_max_result_bytes_bounds_memory(schema, driver):
    db = Database(pg.make(driver, schema=schema))
    try:
        await db.execute("select 1")  # connect outside the measurement
        tracemalloc.start()
        start = time.perf_counter()
        with pytest.raises(ResourceLimitExceeded):
            await db.execute(_BIG, max_result_bytes=1_000_000)
        elapsed = time.perf_counter() - start
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        print(f"{driver}: limit 1 MB of a 50 MB result: peak {peak / 1e6:.1f} MB, {elapsed * 1000:.0f} ms")
        if driver == "psycopg":
            # Rows are measured as they arrive (libpq chunked mode) and the
            # statement is cancelled at the limit.
            assert peak < 15e6
        else:
            # asyncpg fetches every row, then checks.
            assert peak > 40e6
        # The connection is usable afterwards.
        assert (await db.execute("select 2")).rows == [(2,)]
    finally:
        tracemalloc.stop()
        await db.close()


@pytest.mark.parametrize("driver", pg.DRIVERS)
async def test_max_rows_fetches_only_what_it_returns(schema, driver):
    # Both drivers fetch at most max_rows + 1 rows (asyncpg: a portal row
    # limit; psycopg: chunked mode outside a transaction, DECLARE / FETCH in
    # one pipeline inside one).
    db = Database(pg.make(driver, schema=schema))
    try:
        await db.execute("select 1")
        for in_tx in (False, True):
            tracemalloc.start()
            if in_tx:
                async with db.transaction("read") as tx:
                    r = await tx.execute(_BIG, max_rows=10)
            else:
                r = await db.execute(_BIG, max_rows=10)
            _, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            print(f"{driver} in_tx={in_tx}: max_rows=10 of 50,000: peak {peak / 1e6:.2f} MB")
            assert r.truncated and len(r.rows) == 10
            assert peak < 10e6
    finally:
        tracemalloc.stop()
        await db.close()


# -- 8. statement savepoints: round trips --------------------------------------------------------


DELAY = 0.02  # seconds added to every server reply by the latency proxy


@pytest.mark.parametrize("driver", pg.DRIVERS)
async def test_round_trips_per_statement_in_a_transaction(schema, driver):
    # A proxy adds DELAY to every reply, so the time a call takes divided by
    # DELAY is the number of round trips it made.
    from .latency_proxy import latency_proxy

    turns = [0]
    async with latency_proxy(DELAY, turns) as dsn:
        figures = {}
        counted = {}
        for savepoints in (True, False):
            db = Database(pg.make(driver, dsn=dsn, schema=schema, statement_savepoints=savepoints))
            try:
                await db.execute_write(
                    f"create table if not exists t_{driver} (id integer primary key)"
                )
                async with db.transaction() as tx:
                    await tx.execute(f"insert into t_{driver} values (0) on conflict do nothing")
                    start = time.perf_counter()
                    before = turns[0]
                    for i in range(10):
                        await tx.execute(
                            f"insert into t_{driver} values (:i) on conflict do nothing",
                            {"i": i + 100 * savepoints},
                        )
                    counted[savepoints] = (turns[0] - before) / 10
                    figures[savepoints] = (time.perf_counter() - start) / 10 / DELAY
            finally:
                await db.close()
    print(
        f"{driver}: round trips per tx.execute: with statement savepoints "
        f"{counted[True]:.2f} counted ({figures[True]:.2f} timed), "
        f"without {counted[False]:.2f} counted ({figures[False]:.2f} timed)"
    )
    # Asserted on the proxy's count, not the timing: on a loaded machine
    # scheduling delay alone made a one-round-trip call time at 2.05.
    # (asyncpg prepares the first statement: one extra round trip in ten.)
    assert counted[False] < 1.6  # one round trip per statement on both
    if driver == "psycopg":
        assert counted[True] < 1.6  # SAVEPOINT, statement, RELEASE: one pipeline
    else:
        assert counted[True] > 1.6  # "RELEASE; SAVEPOINT", then the statement


# -- 9. COPY value conversion -----------------------------------------------------------------


async def test_copy_value_conversion(schema):
    import asyncpg

    admin = await apg(schema)
    await admin.execute("create table t (id bigint primary key, v text, x bigint)")
    await admin.close()

    a = await apg(schema)
    # asyncpg's binary COPY: each value must be the exact Python type.
    with pytest.raises((asyncpg.DataError, TypeError)):
        await a.copy_records_to_table("t", records=[(1, 5, None)], schema_name=schema)
    with pytest.raises((asyncpg.DataError, TypeError)):
        await a.copy_records_to_table("t", records=[("2", "a", None)], schema_name=schema)
    await a.close()

    import psycopg

    p = await ppg(schema)
    cur = p.cursor()
    # psycopg's text COPY: the server parses each value as it would a literal.
    async with cur.copy("COPY t (id, v, x) FROM STDIN") as copy:
        await copy.write_row((1, 5, "7"))
        await copy.write_row(("2", b"\x00", 8))
    assert await pfetch(p, "select id, v, x from t order by id") == [(1, "5", 7), (2, "\\x00", 8)]
    with pytest.raises(psycopg.errors.InvalidTextRepresentation):
        async with cur.copy("COPY t (id, x) FROM STDIN") as copy:
            await copy.write_row((3, 1.5))  # "1.5" is not a bigint literal
    await p.close()


# -- 10. "char" catalog columns ----------------------------------------------------------------


async def test_catalog_char_columns(schema):
    a = await apg(schema)
    assert await a.fetchval("select relkind from pg_class where relname = 'pg_class'") == b"r"
    await a.close()
    p = await ppg(schema)
    assert await pfetch(p, "select relkind from pg_class where relname = 'pg_class'") == [("r",)]
    await p.close()


# -- more differences found while building the psycopg backend --------------------------------


async def test_float_for_an_integer_parameter(schema):
    a = await apg(schema)
    assert await a.fetchval("select $1::int8", 1.5) == 1  # truncated in the client
    await a.close()
    p = await ppg(schema)
    assert await pfetch(p, "select $1::int8", [1.5]) == [(2,)]  # rounded by the server
    await p.close()


async def test_several_statements_in_one_call(schema):
    import asyncpg

    a = await apg(schema)
    with pytest.raises(asyncpg.PostgresSyntaxError):
        await a.fetch("select 1; select 2")
    await a.close()
    # psycopg runs a statement without parameters with the simple query
    # protocol: both statements run, and the cursor holds the first result.
    p = await ppg(schema)
    assert await pfetch(p, "select 1; select 2") == [(1,)]
    await p.close()
    # The psycopg backend counts statements first and refuses, as DuckDB's does.
    db = Database(pg.make("psycopg", schema=schema))
    try:
        with pytest.raises(QueryError) as e:
            await db.execute("select 1; select 2")
        assert e.value.code == "MULTIPLE_STATEMENTS"
        assert (await db.execute("select ';' as s;")).rows == [(";",)]
        with pytest.raises(QueryError):
            await db.execute("select :a; select 2", {"a": 1})  # extended protocol
    finally:
        await db.close()


async def test_bool_parameters(schema):
    # Both backends send a bool where portable integer columns expect 1/0.
    # asyncpg's backend asks the server for parameter types first, so a
    # native boolean column works too; psycopg's sends int4 and needs a cast.
    for driver in pg.DRIVERS:
        db = Database(pg.make(driver, schema=schema))
        try:
            await db.execute_write(f"create table if not exists b_{driver} (n bigint, f boolean)")
            await db.execute_write(f"insert into b_{driver} (n) values (:v)", {"v": True})
            if driver == "asyncpg":
                await db.execute_write(f"insert into b_{driver} (f) values (:v)", {"v": True})
            else:
                with pytest.raises(QueryError):
                    await db.execute_write(f"insert into b_{driver} (f) values (:v)", {"v": True})
                await db.execute_write(f"insert into b_{driver} (f) values (:v::boolean)", {"v": True})
        finally:
            await db.close()


def test_threads_not_used():
    # Both are natively async: no executor threads for a query.
    before = threading.active_count()

    async def run() -> None:
        for driver in pg.DRIVERS:
            db = Database(pg.make(driver))
            try:
                await db.execute("select 1")
            finally:
                await db.close()
                await pg.drop_schema(db._backend.schema)  # type: ignore[attr-defined]

    asyncio.run(run())
    assert threading.active_count() == before
