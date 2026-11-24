"""The DuckDB C++ worker (``duckdb-cpp-worker/``, ``DuckDBCppWorker``) beyond
the conformance suite: finding the binary, what ``hello`` says, concurrent
connections, a differential test that runs awkward statements on
in-process DuckDB and on the C++ worker and requires the same results,
column types and errors, malformed input, and end of input.

Skipped when the binary is not built (``make -C duckdb-cpp-worker``)."""

from __future__ import annotations

import asyncio
import datetime as dt
import decimal
import json
import math
import os
import struct
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from dblayer import Database, NotSupported, QueryError, QueryInterrupted
from dblayer.backends.duckdb_cppworker import (
    BUILD_COMMAND,
    ENV_VAR,
    DuckDBCppWorker,
    binary_unavailable,
    find_binary,
)
from dblayer.backends.sqlite_worker import WorkerError, live_worker_pids
from dblayer.worker.protocol import available_codecs

from .test_cworker import MALFORMED_JSON, MALFORMED_MSGPACK
from .test_value_types import DUCKDB_VALUES_SQL, _same

UNAVAILABLE = binary_unavailable()
pytestmark = pytest.mark.skipif(UNAVAILABLE is not None, reason=UNAVAILABLE or "")

CODECS = ["json"] + (["msgpack"] if "msgpack" in available_codecs() else [])
UNBOUNDED = "with recursive c(x) as (select 1 union all select x + 1 from c) select count(*) from c"


def _conn(db: Database) -> Any:
    return db._pool._idle_read[0].conn  # type: ignore[union-attr]


# -- the binary --------------------------------------------------------------------


def test_binary_lookup_order(tmp_path, monkeypatch):
    real = find_binary()
    monkeypatch.delenv(ENV_VAR, raising=False)
    if binary_unavailable() is None:
        assert find_binary().endswith("duckdb-cpp-worker/build/dblayer-duckdb-worker")
    fake = tmp_path / "nope"
    monkeypatch.setenv(ENV_VAR, str(fake))
    with pytest.raises(NotSupported) as e:
        find_binary()
    assert BUILD_COMMAND in str(e.value) and ENV_VAR in str(e.value)
    assert find_binary(real) == real
    monkeypatch.setenv(ENV_VAR, real)
    assert find_binary() == real
    with pytest.raises(NotSupported, match="binary not built"):
        DuckDBCppWorker(tmp_path / "x.duckdb", binary=fake)


def test_version_flag_and_no_python():
    import duckdb

    out = subprocess.run([find_binary(), "--version"], capture_output=True, text=True, check=True).stdout
    assert out.strip() == f"dblayer-duckdb-worker protocol 1, DuckDB v{duckdb.__version__}"
    head = Path(find_binary()).read_bytes()[:4]
    assert head in (b"\x7fELF", b"\xcf\xfa\xed\xfe", b"\xca\xfe\xba\xbe")


@pytest.mark.parametrize("codec", CODECS)
async def test_hello_and_round_trip(tmp_path, codec):
    db = Database(DuckDBCppWorker(tmp_path / "a.duckdb", codec=codec))
    try:
        await db.execute_write("create table t (id bigint primary key, v blob)")
        w = await db.execute_write("insert into t values (:id, :v)", {"id": 1, "v": b"\x00\xff"})
        assert w.rowcount == 1
        r = await db.execute("select id, v from t")
        assert r.rows == [(1, b"\x00\xff")]
        assert [c.type for c in r.columns] == ["BIGINT", "BLOB"]
        hello = _conn(db).process.hello
        assert hello["implementation"] == "cpp" and hello["concurrent"] is True
        assert hello["ops"] == ["insert_rows"] and hello["codec"] == codec
    finally:
        await db.close()
    assert live_worker_pids() == []


def test_parent_does_not_import_duckdb(tmp_path):
    code = (
        "import asyncio, sys\n"
        "from dblayer import Database\n"
        "from dblayer.backends.duckdb_cppworker import DuckDBCppWorker\n"
        "async def main():\n"
        f"    db = Database(DuckDBCppWorker({str(tmp_path / 'p.duckdb')!r}))\n"
        "    try:\n"
        "        assert (await db.execute('select 42')).rows == [(42,)]\n"
        "        try:\n"
        "            await db.execute('select * from nope')\n"
        "        except Exception as e:\n"
        "            print(type(e).__name__, type(e.__cause__).__name__)\n"
        "    finally:\n"
        "        await db.close()\n"
        "asyncio.run(main())\n"
        "print('duckdb' in sys.modules)\n"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert out.stdout.split() == ["QueryError", "WorkerError", "False"], out.stdout


async def test_options_reach_the_engine(tmp_path):
    db = Database(DuckDBCppWorker(tmp_path / "m.duckdb", memory_limit="64MB", threads=1))
    try:
        r = await db.execute(
            "select current_setting('memory_limit'), current_setting('threads'), "
            "current_setting('enable_external_access'), current_setting('lock_configuration')"
        )
        limit, threads, external, locked = r.rows[0]
        assert "MiB" in limit and threads == 1 and external is False and locked is True
        from dblayer import QueryError

        with pytest.raises(QueryError):
            await db.execute_write("set memory_limit = '8GB'")
        with pytest.raises(QueryError):
            await db.execute("select * from read_csv('/etc/hosts')")
    finally:
        await db.close()


async def test_read_only_database_and_create_false(tmp_path):
    path = tmp_path / "ro.duckdb"
    db = Database(DuckDBCppWorker(path))
    await db.execute_write("create table t as select 1 x")
    await db.close()
    ro = Database(DuckDBCppWorker(path, read_only=True))
    try:
        assert (await ro.execute("select x from t")).rows == [(1,)]
        with pytest.raises(NotSupported):
            await ro.execute_write("insert into t values (2)")
    finally:
        await ro.close()
    missing = Database(DuckDBCppWorker(tmp_path / "missing.duckdb", create=False))
    try:
        from dblayer import QueryError

        with pytest.raises(QueryError) as e:
            await missing.execute("select 1")
        assert e.value.code == "CANTOPEN"
    finally:
        await missing.close()


async def test_session_state_does_not_reach_a_later_connection(tmp_path):
    """The worker reuses DuckDB connections it configured before locking
    the settings (a small streaming_buffer_size); one that holds session
    state (a temporary table, a variable) is closed instead, so a new
    connection starts clean, as a new cursor does in-process. A connection
    kept open throughout keeps the database (and its spares) open."""
    b = DuckDBCppWorker(tmp_path / "s.duckdb")
    keeper = await b.open(write=False)
    try:
        w = await b.open(write=True)
        try:
            await w.execute("create temp table tt as select 1 x", (), time_limit_ms=None, max_rows=None)
            await w.execute("set variable v = 42", (), time_limit_ms=None, max_rows=None)
        finally:
            await w.close()
        for _ in range(10):  # every spare connection in turn
            c = await b.open(write=True)
            try:
                with pytest.raises(QueryError, match="tt"):
                    await c.execute("select * from tt", (), time_limit_ms=None, max_rows=None)
                r = await c.execute("select getvariable('v'), current_setting('streaming_buffer_size')", (),
                                    time_limit_ms=None, max_rows=None)
                assert r.rows[0][0] is None
                assert r.rows[0][1] == "15.6 KiB", r.rows  # 16KB (16,000 bytes), not the 10 MB default
            finally:
                await c.close()
    finally:
        await keeper.close()
        await b.close()


async def test_process_exits_when_the_database_sleeps(tmp_path):
    path = tmp_path / "s.duckdb"
    b = DuckDBCppWorker(path)
    db = Database(b)
    try:
        await db.execute_write("create table t as select range i from range(1000)")
        pid = b.pid
        await db.sleep()
        assert b.pid is None and live_worker_pids() == []
        import duckdb

        # The file lock is free: another process (here, this one) can open it.
        con = duckdb.connect(str(path))
        assert con.execute("select count(*) from t").fetchone() == (1000,)
        con.close()
        assert (await db.execute("select count(*) from t")).rows == [(1000,)]
        assert b.pid not in (None, pid)
    finally:
        await db.close()


# -- concurrency -------------------------------------------------------------------------


async def test_a_long_query_does_not_hold_up_a_point_read(tmp_path):
    """Two read connections of one database in one process: while one runs
    an unbounded query (stopped by its time limit), the other answers point
    reads. The Python worker serves them one at a time."""
    db = Database(DuckDBCppWorker(tmp_path / "c.duckdb", threads=2))
    try:
        await db.execute_write("create table t as select range id, range * 2 v from range(1000)")
        long = asyncio.ensure_future(db.execute(UNBOUNDED, time_limit_ms=8000))
        await asyncio.sleep(0.3)
        assert not long.done()
        times = []
        for i in range(20):
            t0 = time.perf_counter()
            r = await db.execute("select v from t where id = :i", {"i": i})
            times.append(time.perf_counter() - t0)
            assert r.rows == [(2 * i,)]
        assert not long.done()  # every point read finished while it ran
        times.sort()
        print(f"\n[duckdb-cppworker] point read during a long query: median {times[10] * 1000:.2f} ms, "
              f"max {times[-1] * 1000:.2f} ms")
        long.cancel()
        with pytest.raises(asyncio.CancelledError):
            await long
        assert (await db.execute("select count(*) from t")).rows == [(1000,)]
    finally:
        await db.close()


async def test_concurrent_reads_on_many_connections(tmp_path):
    db = Database(DuckDBCppWorker(tmp_path / "m.duckdb"), max_read_connections=6)
    try:
        await db.execute_write("create table t as select range i from range(10000)")
        results = await asyncio.gather(*[db.execute("select sum(i) from t where i % :m = 0", {"m": m}) for m in range(1, 31)])
        assert [r.rows[0][0] for r in results] == [sum(range(0, 10000, m)) for m in range(1, 31)]
        assert db._backend.placement_group.started == 1  # type: ignore[attr-defined]
    finally:
        await db.close()


async def test_cancel_one_connection_leaves_the_others_running(tmp_path):
    db = Database(DuckDBCppWorker(tmp_path / "k.duckdb"))
    try:
        await db.execute("select 1")
        a = asyncio.ensure_future(db.execute(UNBOUNDED, time_limit_ms=8000))
        b = asyncio.ensure_future(db.execute(UNBOUNDED, time_limit_ms=300))
        await asyncio.sleep(0.2)
        a.cancel()
        with pytest.raises(asyncio.CancelledError):
            await a
        with pytest.raises(QueryInterrupted):
            await b
        assert (await db.execute("select 2")).rows == [(2,)]
    finally:
        await db.close()


# -- differential: in-process DuckDB vs the C++ worker ------------------------------------

SETUP = [
    "create type mood as enum ('sad', 'ok', 'it''s fine')",
    "create sequence seq",
    "create table t (id bigint primary key, name varchar not null, b blob, f double, d date, "
    "ts timestamp, tz timestamptz, dec decimal(10,2), tags varchar[], m mood)",
    "insert into t values (1, 'a', '\\x00\\xFF'::blob, 1.5, date '2024-01-02', timestamp '2024-01-02 03:04:05', "
    "timestamptz '2024-01-02 03:04:05+00', 1.25, ['x', 'y'], 'ok'), "
    "(2, 'b', null, -0.0, null, null, null, -0.5, [], 'it''s fine'), (3, 'c', '', 1e308, date '1969-12-31', "
    "timestamp '1900-01-01', timestamptz '2024-07-01 00:00:00+02', 0, null, null)",
]

MORE_VALUES = (
    "select 1::tinyint a, 2::utinyint b, 3::smallint c, 4::usmallint d, 5::int e, 6::uinteger f, 7::bigint g, "
    "18446744073709551615::ubigint h, 340282366920938463463374607431768211455::uhugeint i, "
    "-170141183460469231731687303715884105727::hugeint j, 9223372036854775808::hugeint k, "
    "union_value(num := 2)::union(num int, s varchar) un, union_value(s := 'x')::union(num int, s varchar) un2, "
    "true t, timestamp_s '2024-01-01 00:00:01' ts_s, timestamp_ms '2024-01-01 00:00:01.5' ts_ms, "
    "time_ns '01:02:03.123456789' tns, 123456789012345678901234567890::bignum bn, -256::bignum bn2, "
    "interval '-1 month' iv1, interval '400 days 5 seconds' iv2, interval '-3 days -1 microsecond' iv3, "
    "[1,2]::int[] as \"weird name\", struct_pack(\"a b\" := 1, \"C\" := 2) sp, 1.123456789::decimal(38,10) d38, "
    "-0.05::decimal(4,2) d4, 0::decimal(38,10) dz, 0.0000001234::decimal(18,10) dsmall, [[1]]::int[][] ll, "
    "[[1,2]]::int[2][] la, 'infinity'::date dinf, '-infinity'::timestamptz tzninf, 'infinity'::timestamptz tzinf, "
    "'nan'::double nan, null n, null::int ni, {'x': null}::struct(x int) sn, map{}::map(int,int) me, "
    "[null]::int[] ln, 'infinity'::double inf, '-infinity'::double ninf, '2024-01-01'::timestamp_ns tsns, "
    "time with time zone '12:00:00-03:30' ttz1, '12:00:00.5+05:45:30'::timetz ttz2, '101'::bit bits, "
    "'b'::varchar collate nocase coll, map([[1,2]::int[2]], ['a']) mk, struct_pack() es, 0.1::float fl, "
    "3.4028235e38::float fmax, '00000000-0000-0000-0000-000000000000'::uuid u0, "
    "'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid u1, [1,2,3]::int[3] a3, [null, null]::int[2] an, null::int[2] a0, "
    "{'select': 1, 'name': 2, 'value': 3, 'order': 4, '1a': 5, '_x': 6, 'é': 7, 'Ab': 8, 'a.b': 9, 'a\"b': 10} kw, "
    "'sad'::mood en, ['ok'::mood] enl, map {'k': 1::decimal(3,1)} mdec, {'$x': 1, 'y': 2} sx, map(['$b'], [1]) mb, "
    "[{'a': [map([date '2024-01-01'], [interval '1 day'])]}] deep, '{}'::json js, ['{}'::json] jl"
)

CASES: list[tuple[str, str, Any, dict[str, Any]]] = [
    # (connection, sql, params, keyword arguments)
    ("r", "select * from t order by id", None, {}),
    ("r", "select * from t order by id", None, {"max_rows": 1}),
    ("r", "select * from t order by id", None, {"max_rows": 0}),
    ("r", "select id from t order by id desc", None, {"max_rows": 2}),
    ("r", "select 1 a, 2 a, 3 \"A\"", None, {"max_rows": 1}),
    ("r", "select 1; ", None, {"max_rows": 1}),
    ("r", "select 1 -- comment", None, {"max_rows": 1}),
    ("r", "show tables", None, {"max_rows": 1}),
    ("r", "select $1 x from range(10)", [7], {"max_rows": 3}),
    ("r", "select error('late') from range(5000) where range > 4000", None, {"max_rows": 10}),
    ("r", "select * from t", None, {"max_result_bytes": 5}),
    ("r", "select name from t order by id", None, {"max_result_bytes": 3}),
    ("r", "select repeat('é', 10) s", None, {"max_result_bytes": 10}),
    ("r", "select repeat('é', 10) s", None, {"max_result_bytes": 9}),
    ("r", DUCKDB_VALUES_SQL, None, {}),
    ("r", MORE_VALUES, None, {}),
    ("r", "select 1; select 2", None, {}),
    ("r", "select 1; -- trailing comment", None, {}),
    ("r", "", None, {}),
    ("r", "-- nothing", None, {}),
    ("r", ";", None, {}),
    ("r", "begin", None, {}),
    ("w", "commit", None, {}),
    ("r", "insert into t values (9, 'z')", None, {}),
    ("r", "insert into missing values (1)", None, {}),
    ("r", "create table u (x int)", None, {}),
    ("r", "drop table t", None, {}),
    ("r", "select nextval('seq')", None, {}),
    ("r", "pragma table_info('t')", None, {}),
    ("r", "pragma enable_progress_bar", None, {}),
    ("r", "call pragma_table_info('t')", None, {}),
    ("r", "show tables", None, {}),
    ("r", "describe t", None, {}),
    ("r", "checkpoint", None, {}),
    ("r", "set threads = 1", None, {}),
    ("r", "with x as (select 1 a) select * from x", None, {}),
    ("r", "values (1, 'a'), (2, 'b')", None, {}),
    ("r", "from t select id order by id", None, {}),
    ("r", "select $a", {"a": 1}, {}),
    ("r", "select $a", {}, {}),
    ("r", "select $a", {"a": 1, "b": 2}, {}),
    ("r", "select $a, $b", {"b": 1}, {}),
    ("r", "select $a", [1], {}),
    ("r", "select ?", {"a": 1}, {}),
    ("r", "select ?", [1], {}),
    ("r", "select ?, ?", [1], {}),
    ("r", "select ?", [1, 2], {}),
    ("r", "select 1", [1], {}),
    ("r", "select 1", {"a": 1}, {}),
    ("r", "select $1, $2", [5, "x"], {}),
    ("r", "select $1", [2**31], {}),
    ("r", "select $1", [2**63 + 5], {}),
    ("r", "select $1", [-(2**63) - 5], {}),
    ("r", "select $1", [2**100], {}),
    ("r", "select $1", [2**200], {}),
    ("r", "select $1, $2, $3, $4", [1.5, "é\x00x", b"\x00\xff", True], {}),
    ("r", "select $1", [None], {}),
    ("r", "select $1, $2, $3", [float("inf"), float("-inf"), float("nan")], {}),
    ("r", "select $1", [[1, 2]], {}),
    ("r", "select $1", [[1, 2.5]], {}),
    ("r", "select $1", [[None, 1]], {}),
    ("r", "select $1", [[[1]]], {}),
    ("r", "select $1", [{"a": 1, "b": "x"}], {}),
    ("r", "select $1", [{"a": [1]}], {}),
    ("r", "select $1, $2, $3", [decimal.Decimal("1.25"), decimal.Decimal("0.05"), decimal.Decimal("-1.5")], {}),
    ("r", "select $1, $2, $3", [decimal.Decimal("1E+5"), decimal.Decimal("0.000"), decimal.Decimal("1e-3")], {}),
    ("r", "select $1", [decimal.Decimal("123456789012345678901234567890.5")], {}),
    ("r", "select $1", [decimal.Decimal("12345678901234567890123456789012345678.5")], {}),
    ("r", "select $1, $2", [decimal.Decimal("NaN"), decimal.Decimal("-Infinity")], {}),
    ("r", "select $1, $2", [dt.date(2024, 2, 29), dt.date(1, 1, 1)], {}),
    ("r", "select $1", [dt.datetime(2024, 1, 2, 3, 4, 5, 6)], {}),
    ("r", "select $1", [dt.datetime(2024, 1, 2, 3, 4, 5, tzinfo=dt.timezone.utc)], {}),
    ("r", "select $1", [dt.datetime(2024, 7, 2, 3, 4, 5, tzinfo=ZoneInfo("Europe/Paris"))], {}),
    ("r", "select $1", [dt.datetime(2024, 1, 2, 3, 4, 5, tzinfo=dt.timezone(dt.timedelta(hours=-5, minutes=-30)))], {}),
    ("r", "select $1, $2", [dt.time(1, 2, 3), dt.time(1, 2, 3, 4, tzinfo=dt.timezone(dt.timedelta(hours=2)))], {}),
    ("r", "select $1, $2", [dt.timedelta(days=1, microseconds=5), dt.timedelta(microseconds=-1)], {}),
    ("r", "select $1", [uuid.UUID("9e2c6b1c-8a52-4e4f-9d1a-0c6f5a1b2c3d")], {}),
    ("r", "select * from nope", None, {}),
    ("r", "selec 1", None, {}),
    ("r", "select nope from t", None, {}),
    ("r", "select 'x'::int", None, {}),
    ("r", "select 2147483647::int + 1", None, {}),
    ("r", "select error('boom')", None, {}),
    ("r", "select * from read_csv('/etc/hosts')", None, {}),
    ("r", "select 1/0, [1][5]", None, {}),
    ("w", "attach 'other.duckdb'", None, {}),
    ("w", "set memory_limit = '1GB'", None, {}),
    ("w", "pragma nope", None, {}),
    ("w", "insert into t values (1, 'dup')", None, {}),
    ("w", "insert into t values (10, null)", None, {}),
    ("w", "insert into t (id, name) values (10, 'j')", None, {}),
    ("w", "insert into t (id, name) values (11, 'k'), (12, 'l') returning id, name", None, {}),
    ("w", "insert into t (id, name) values (13, 'k'), (14, 'l') returning id", None, {"max_rows": 1}),
    ("w", "insert into t (id, name) select range + 100, 'r' from range(5000)", None, {}),
    ("w", "update t set f = 2 where id > 100", None, {}),
    ("w", "update t set f = 3 where id > 100 returning id", None, {"max_rows": 2}),
    ("w", "delete from t where id >= 4000", None, {}),
    ("w", "merge into t using (select 1 id) s on t.id = s.id when matched then update set f = 9", None, {}),
    ("w", "create table u (x int)", None, {}),
    ("w", "create table u (x int)", None, {}),
    ("w", "drop table u", None, {}),
    ("w", "insert into t (id, name) values ($id, $name)", {"id": 20, "name": "p"}, {}),
    ("w", "select nextval('seq')", None, {}),
    ("w", "explain select 1", None, {}),
]


def _norm(v: Any) -> Any:
    if isinstance(v, float) and math.isnan(v):
        return "nan"
    if isinstance(v, (list, tuple)):
        return type(v)(_norm(x) for x in v)
    if isinstance(v, dict):
        return {k: _norm(x) for k, x in v.items()}
    return v


def _error(conn: Any, e: BaseException) -> tuple[Any, ...]:
    cause = e.__cause__
    if not isinstance(conn, type(None)) and not hasattr(conn, "_proc"):
        mapped = conn._backend.dialect.map_exception(e)
        if mapped is not None:
            cause, e = e, mapped
    if isinstance(cause, WorkerError):
        cause_name = str(cause).split(":", 1)[0]
    elif cause is not None:
        cause_name = f"{type(cause).__module__}.{type(cause).__qualname__}"
    else:
        cause_name = None
    return ("error", type(e).__name__, str(e), getattr(e, "code", None), cause_name)


async def _run(conn: Any, sql: str, params: Any, kw: dict[str, Any]) -> Any:
    try:
        r = await conn.execute(sql, params if params is not None else (), time_limit_ms=None, **kw)
    except Exception as e:  # noqa: BLE001
        return _error(conn, e)
    return (
        tuple(str(c) for c in r.columns),
        tuple(c.type for c in r.columns),
        [tuple(_norm(v) for v in row.as_tuple()) for row in r.rows],
        r.truncated,
        r.rowcount,
    )


def _same_result(a: Any, b: Any) -> bool:
    if a[0] == "error" or b[0] == "error":
        return a == b
    return a[:2] == b[:2] and a[3:] == b[3:] and len(a[2]) == len(b[2]) and all(
        _same(list(x), list(y)) for x, y in zip(a[2], b[2])
    )


@pytest.mark.parametrize("codec", CODECS)
async def test_differential_against_in_process(tmp_path, codec):
    from dblayer.backends.duckdb import DuckDB

    # The same file name: DuckDB names the database after it in messages.
    (tmp_path / "local").mkdir()
    (tmp_path / "remote").mkdir()
    local = DuckDB(tmp_path / "local" / "db.duckdb")
    remote = DuckDBCppWorker(tmp_path / "remote" / "db.duckdb", codec=codec)
    lw, lr = await local.open(write=True), await local.open(write=False)
    rw, rr = await remote.open(write=True), await remote.open(write=False)
    conns = {"w": (lw, rw), "r": (lr, rr)}
    try:
        for sql in SETUP:
            await lw.execute(sql, (), time_limit_ms=None, max_rows=None)
            await rw.execute(sql, (), time_limit_ms=None, max_rows=None)
        mismatches = []
        for which, sql, params, kw in CASES:
            kw = {"max_rows": None, **kw}
            lc, rc = conns[which]
            a = await _run(lc, sql, params, kw)
            b = await _run(rc, sql, params, kw)
            if not _same_result(a, b):
                mismatches.append((which, sql[:80], params, kw, a, b))
        assert not mismatches, "\n\n".join(map(repr, mismatches))
        # execute_many and batches
        for many in (
            ("w", "insert into t (id, name) values ($1, $2)", [[30, "a"], [31, "b"]]),
            ("w", "insert into t (id, name) values ($1, $2)", [[30, "dup"]]),
            ("r", "insert into t (id, name) values ($1, $2)", [[40, "a"]]),
            ("w", "select $1", [[1], [2]]),
            ("w", "update t set f = $1 where id = $2", [[1.0, 30], [2.0, 31], [3.0, 999]]),
        ):
            lc, rc = conns[many[0]]
            ra = await _many(lc, many[1], many[2])
            rb = await _many(rc, many[1], many[2])
            assert ra == rb, (many, ra, rb)
        for statements in ([("select 1", ()), ("select $1", [2])], [("select 1", ()), ("select nope", ())]):
            ra = await _batch(lr, statements)
            rb = await _batch(rr, statements)
            assert ra == rb, (statements, ra, rb)
        for sql in (
            "create table p (id bigint primary key, code varchar unique, n int default 7)",
            "create table child (id bigint primary key, p bigint references p, q varchar, unique (q, p))",
            "create table g (a int, b int generated always as (a * 2) virtual, \"Odd Name\" text)",
            "create index ix on t (name)",
            "create unique index ix2 on g (a, (a + 1), \"Odd Name\")",
            "create view vv as select id, name from t",
        ):
            await lw.execute(sql, (), time_limit_ms=None, max_rows=None)
            await rw.execute(sql, (), time_limit_ms=None, max_rows=None)
        assert await lr.schema() == await rr.schema()
        assert await lr.schema_version() == await rr.schema_version()
        # In a transaction, too.
        for c in (lw, rw):
            await c.begin("write")
        assert await lw.schema() == await rw.schema()
        for c in (lw, rw):
            await c.rollback()
    finally:
        for c in (lw, lr, rw, rr):
            await c.close()
        await local.close()
        await remote.close()


async def _many(conn: Any, sql: str, seq: list[Any]) -> Any:
    try:
        return (await conn.execute_many(sql, seq)).rowcount
    except Exception as e:  # noqa: BLE001
        return _error(conn, e)


async def _batch(conn: Any, statements: list[Any]) -> Any:
    try:
        rs = await conn.execute_batch(statements, time_limit_ms=None, max_rows=None)
    except Exception as e:  # noqa: BLE001
        return _error(conn, e)
    return [(tuple(r.columns), [tuple(x.as_tuple()) for x in r.rows]) for r in rs]


@pytest.mark.parametrize("codec", CODECS)
async def test_streams_match(tmp_path, codec):
    from dblayer.backends.duckdb import DuckDB

    for backend in (DuckDB(tmp_path / "a.duckdb"), DuckDBCppWorker(tmp_path / "b.duckdb", codec=codec)):
        w = await backend.open(write=True)
        await w.execute("create table t as select range x from range(25)", (), time_limit_ms=None, max_rows=None)
        conn = await backend.open(write=False)
        try:
            chunks = [c async for c in conn.stream("select x, x * 2 y from t order by x", (), chunk_rows=7)]
            assert [len(c) for c in chunks] == [7, 7, 7, 4]
            assert [tuple(r) for c in chunks for r in c][-1] == (24, 48)
            s = conn.stream("select x from t order by x", (), chunk_rows=2)
            first = await s.__anext__()
            assert list(map(tuple, first)) == [(0,), (1,)]
            await s.aclose()  # abandoned: the worker closes its result
            r = await conn.execute("select count(*) from t", (), time_limit_ms=None, max_rows=None)
            assert r.rows == [(25,)]
            # A stream of a write on a write connection; a refused one.
            ws = [c async for c in w.stream("insert into t values (100), (101) returning x", (), chunk_rows=1)]
            assert [list(map(tuple, c)) for c in ws] == [[(100,)], [(101,)]]
            with pytest.raises(Exception) as e:
                [c async for c in conn.stream("delete from t", (), chunk_rows=1)]
            assert "read-only" in str(e.value)
        finally:
            await conn.close()
            await w.close()
            await backend.close()


async def test_aborted_transaction_cannot_commit(tmp_path):
    from dblayer import IntegrityError, QueryError

    db = Database(DuckDBCppWorker(tmp_path / "a.duckdb"))
    try:
        await db.execute_write("create table t (id bigint primary key)")
        with pytest.raises(QueryError) as e:
            async with db.transaction() as tx:
                await tx.execute("insert into t values (1)")
                with pytest.raises(IntegrityError):
                    await tx.execute("insert into t values (1)")
        assert e.value.code == "TRANSACTION_ABORTED"
        assert (await db.execute("select count(*) from t")).rows == [(0,)]
    finally:
        await db.close()


# -- malformed input and end of input ---------------------------------------------------


def _raw_session(codec: str, payloads: list[bytes]) -> tuple[int, bytes]:
    p = subprocess.Popen([find_binary()], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    hello = json.dumps({"type": "hello", "protocol": 1, "codecs": [codec], "id": 1}).encode()
    data = struct.pack(">I", len(hello)) + hello
    for payload in payloads:
        data += struct.pack(">I", len(payload)) + payload
    try:
        _out, err = p.communicate(data, timeout=20)
    except subprocess.TimeoutExpired:
        p.kill()
        p.communicate()
        raise
    return p.returncode, err


@pytest.mark.parametrize("payload", MALFORMED_MSGPACK, ids=[str(i) for i in range(len(MALFORMED_MSGPACK))])
def test_malformed_msgpack_drops_the_connection_cleanly(payload):
    if "msgpack" not in available_codecs():
        pytest.skip("msgpack is not installed")
    code, err = _raw_session("msgpack", [payload])
    assert code == 0, err.decode(errors="replace")[-2000:]


@pytest.mark.parametrize("payload", MALFORMED_JSON, ids=[str(i) for i in range(len(MALFORMED_JSON))])
def test_malformed_json_drops_the_connection_cleanly(payload):
    code, err = _raw_session("json", [payload])
    assert code == 0, err.decode(errors="replace")[-2000:]


def test_oversized_and_truncated_frames(tmp_path):
    p = subprocess.Popen([find_binary()], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    hello = json.dumps({"type": "hello", "protocol": 1, "codecs": ["json"], "max_frame_bytes": 1000, "id": 1}).encode()
    _out, err = p.communicate(struct.pack(">I", len(hello)) + hello + struct.pack(">I", 5000) + b"x" * 10, timeout=20)
    assert p.returncode == 0 and b"frame size cap" in err
    code, err = _raw_session("json", [])
    assert code == 0
    p = subprocess.Popen([find_binary()], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    _out, err = p.communicate(struct.pack(">I", len(hello)) + hello + struct.pack(">I", 100) + b'{"id":', timeout=20)
    assert p.returncode == 0 and b"inside a frame" in err


@pytest.mark.parametrize("codec", ["json", "msgpack"])
def test_random_payloads_never_crash_the_worker(codec, tmp_path):
    """A small seeded fuzz of the request handlers: mutated valid requests,
    a few per process. Exit code 0 means the worker answered or dropped
    the connection; a crash (or a sanitizer report) is a non-zero exit."""
    import random

    if codec == "msgpack":
        if "msgpack" not in available_codecs():
            pytest.skip("msgpack is not installed")
        import msgpack

        def enc(m: dict[str, Any]) -> bytes:
            return msgpack.packb(m, use_bin_type=True)
    else:

        def enc(m: dict[str, Any]) -> bytes:
            return json.dumps(m).encode()

    rng = random.Random(4321)
    engine = "dblayer.backends.duckdb_cppworker:LocalDuckDB"
    seeds: list[dict[str, Any]] = [
        {"type": "open", "path": str(tmp_path / "f.duckdb"), "mode": "write", "engine": engine, "id": 2,
         "config": {"memory_limit": "64MB", "threads": 1}},
        {"type": "execute", "sql": "select $1, $2", "params": [1, "x"], "id": 3},
        {"type": "execute", "sql": "select $a", "params": {"a": [1, {"b": None}]}, "id": 3},
        {"type": "execute_many", "sql": "select $1", "params_seq": [[1], [2]], "id": 4},
        {"type": "batch", "statements": [["select 1", None], ["select $1", [1]]], "id": 5},
        {"type": "stream", "sql": "select range from range(10)", "chunk_rows": 1, "credit": 2, "id": 6},
        {"type": "begin", "mode": "read", "deadline_ms": 10, "id": 7},
        {"type": "schema", "tx": "t1", "id": 8},
        {"type": "rollback", "tx": "t99999999999", "id": 9},
        {"type": "cancel", "target": 3},
        {"type": "more", "stream": 6, "credit": 1},
        {"type": "op", "name": "insert_rows", "args": {"table": "x", "rows": [{"a": 1}]}, "id": 10},
    ]
    failures = []
    for i in range(int(os.environ.get("DBLAYER_CPPWORKER_FUZZ", "30"))):
        payloads = [enc(seeds[0])]
        for _ in range(3):
            b = bytearray(enc(rng.choice(seeds[1:])))
            for _ in range(rng.randint(1, 4)):
                op = rng.random()
                pos = rng.randrange(len(b))
                if op < 0.5:
                    b[pos] = rng.randrange(256)
                elif op < 0.75:
                    del b[pos : pos + rng.randint(1, 4)]
                else:
                    b[pos:pos] = bytes(rng.randrange(256) for _ in range(rng.randint(1, 4)))
            payloads.append(bytes(b))
        code, err = _raw_session(codec, payloads)
        if code != 0:
            failures.append((i, code, err.decode(errors="replace")[-1500:]))
    assert not failures, failures[:3]


async def test_end_of_input_cancels_rolls_back_and_exits(tmp_path):
    path = tmp_path / "eof.duckdb"
    db = Database(DuckDBCppWorker(path))
    try:
        await db.execute_write("create table t (x int)")
        long = asyncio.ensure_future(db.execute(UNBOUNDED))
        await asyncio.sleep(0.2)
        async with db.transaction() as tx:
            await tx.execute("insert into t values (1)")
            proc = tx._conn.process  # type: ignore[attr-defined]
            t0 = time.perf_counter()
            proc.close()  # the parent goes away: stdin EOF
            while proc.popen is not None and proc.popen.poll() is None:
                assert time.perf_counter() - t0 < 10
                await asyncio.sleep(0.01)
            print(f"\n[duckdb-cppworker] exit after stdin EOF with a query running: "
                  f"{(time.perf_counter() - t0) * 1000:.0f} ms")
            assert proc.popen.poll() == 0
            raise asyncio.CancelledError
    except (asyncio.CancelledError, Exception):
        pass
    finally:
        long.cancel()
        await db.close()
    import duckdb

    con = duckdb.connect(str(path))
    assert con.execute("select count(*) from t").fetchone() == (0,)
    con.close()
