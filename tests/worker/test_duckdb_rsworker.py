"""The Rust DuckDB worker (``duckdb-rs-worker/``, ``DuckDBRustWorker``)
beyond the conformance suite: the binary, ``hello``, concurrent requests on
one database, a differential test against in-process DuckDB (results,
column types, errors, schema), streams, the aborted-COMMIT guard, the
appender, malformed input and a seeded fuzz of the decoders, and end of
input.

Skipped when the binary is not built (``make -C duckdb-rs-worker``)."""

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

from dblayer import Database, NotSupported, QueryError
from dblayer.backends.duckdb_rsworker import (
    BUILD_COMMAND,
    ENV_VAR,
    DuckDBRustWorker,
    binary_unavailable,
    find_binary,
)
from dblayer.backends.sqlite_worker import WorkerProcess, live_worker_pids
from dblayer.worker.protocol import available_codecs

from .test_cworker import MALFORMED_JSON, MALFORMED_MSGPACK
from .test_value_types import DUCKDB_VALUES_SQL, _same

UNAVAILABLE = binary_unavailable()
pytestmark = pytest.mark.skipif(UNAVAILABLE is not None, reason=UNAVAILABLE or "")

CODECS = ["json"] + (["msgpack"] if "msgpack" in available_codecs() else [])


def _duckdb_or_skip() -> Any:
    return pytest.importorskip("duckdb")


# -- the binary ----------------------------------------------------------------------


def test_binary_lookup_order(tmp_path, monkeypatch):
    real = find_binary()
    monkeypatch.delenv(ENV_VAR, raising=False)
    if binary_unavailable() is None:
        assert find_binary().endswith("duckdb-rs-worker/target/release/dblayer-duckdb-rsworker")
    monkeypatch.setenv(ENV_VAR, str(tmp_path / "nope"))
    with pytest.raises(NotSupported) as e:
        find_binary()
    assert BUILD_COMMAND in str(e.value) and ENV_VAR in str(e.value)
    assert find_binary(real) == real
    with pytest.raises(NotSupported, match="not built"):
        DuckDBRustWorker(tmp_path / "x.duckdb", binary=tmp_path / "nope")


def test_version_flag_and_no_python():
    exe = find_binary()
    out = subprocess.run([exe, "--version"], capture_output=True, text=True, check=True).stdout
    assert out.startswith("dblayer-duckdb-rsworker protocol 1, DuckDB v2.0.1")
    assert Path(exe).read_bytes()[:4] in (b"\x7fELF", b"\xcf\xfa\xed\xfe", b"\xca\xfe\xba\xbe")


def test_finds_libduckdb_without_environment(tmp_path):
    """The rpath is enough: no DYLD_/LD_LIBRARY_PATH."""
    env = {k: v for k, v in os.environ.items() if "LIBRARY_PATH" not in k}
    out = subprocess.run([find_binary(), "--version"], capture_output=True, text=True, env=env, check=True)
    assert "DuckDB v2.0.1" in out.stdout


@pytest.mark.parametrize("codec", CODECS)
async def test_hello_and_round_trip(tmp_path, codec):
    db = Database(DuckDBRustWorker(tmp_path / "a.duckdb", codec=codec))
    try:
        assert (await db.execute("select 42 as x")).rows == [(42,)]
        proc = db._backend.placement_group.process  # type: ignore[attr-defined]
        assert proc.hello["implementation"] == "rust"
        assert proc.hello["ops"] is False and proc.hello["concurrent"] is True
        assert proc.hello["codec"] == codec
        assert proc.hello["duckdb_version"] == "v2.0.1"
    finally:
        await db.close()
    assert live_worker_pids() == []


def test_parent_does_not_import_duckdb(tmp_path):
    code = (
        "import asyncio, sys\n"
        "from dblayer import Database\n"
        "from dblayer.backends.duckdb_rsworker import DuckDBRustWorker\n"
        "async def main():\n"
        f"    db = Database(DuckDBRustWorker({str(tmp_path / 'p.duckdb')!r}))\n"
        "    try:\n"
        "        assert (await db.execute('select 42')).rows == [(42,)]\n"
        "        try:\n"
        "            await db.execute('select * from nope')\n"
        "        except Exception as e:\n"
        "            print(type(e).__name__)\n"
        "    finally:\n"
        "        await db.close()\n"
        "asyncio.run(main())\n"
        "print('duckdb' in sys.modules)\n"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert out.stdout.split() == ["QueryError", "False"], out.stdout + out.stderr


def test_capabilities_match_in_process_duckdb(tmp_path):
    _duckdb_or_skip()
    from dataclasses import replace

    from dblayer.backends.duckdb import DuckDB

    for kw in ({}, {"memory_limit": None}):
        rust = DuckDBRustWorker(tmp_path / "c.duckdb", **kw).capabilities
        local = DuckDB(tmp_path / "c.duckdb", **kw).capabilities
        assert replace(rust, hard_kill=False, memory_limit=local.memory_limit) == local


async def test_options_reach_the_engine(tmp_path):
    db = Database(DuckDBRustWorker(tmp_path / "o.duckdb", memory_limit="64MB", threads=1))
    try:
        limit, threads, ext, locked = (
            await db.execute(
                "select current_setting('memory_limit'), current_setting('threads'), "
                "current_setting('enable_external_access'), current_setting('lock_configuration')"
            )
        ).rows[0]
        assert "MiB" in limit and threads == 1 and ext is False and locked is True
        with pytest.raises(QueryError, match="locked"):
            await db.execute_write("set threads = 8")
    finally:
        await db.close()


async def test_read_only_database_and_create_false(tmp_path):
    path = tmp_path / "r.duckdb"
    db = Database(DuckDBRustWorker(path))
    await db.execute_write("create table t as select 1 x")
    await db.close()
    ro = Database(DuckDBRustWorker(path, read_only=True))
    try:
        assert (await ro.execute("select x from t")).rows == [(1,)]
        with pytest.raises(NotSupported):
            await ro.execute_write("insert into t values (2)")
    finally:
        await ro.close()
    missing = Database(DuckDBRustWorker(tmp_path / "missing.duckdb", create=False))
    try:
        with pytest.raises(QueryError) as e:
            await missing.execute("select 1")
        assert e.value.code == "CANTOPEN"
    finally:
        await missing.close()


async def test_session_state_does_not_reach_a_later_connection(tmp_path):
    """The worker reuses DuckDB connections it configured before locking
    the settings; one that holds session state (a temporary table, a
    variable) is closed instead, so a new connection starts clean, as a new
    cursor does in-process."""
    b = DuckDBRustWorker(tmp_path / "s.duckdb")
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
            r = await c.execute("select getvariable('v')", (), time_limit_ms=None, max_rows=None)
            assert r.rows[0][0] is None
        finally:
            await c.close()
    await b.close()


# -- concurrency -------------------------------------------------------------------------

SLOW = "select count(*) from range(4000000000) a where (a.range * 7) % 1000003 = 5"


async def test_a_long_query_does_not_hold_up_a_point_read(tmp_path):
    """Connections of one database run on their own threads in the worker:
    a point read on another connection answers while a long analytical
    query runs (the Python worker would queue it behind)."""
    db = Database(DuckDBRustWorker(tmp_path / "c.duckdb"))
    try:
        await db.execute_write("create table t as select range id from range(1000)")
        long = asyncio.ensure_future(db.execute(SLOW, time_limit_ms=20000))
        await asyncio.sleep(0.3)
        assert not long.done()
        times = []
        for i in range(5):
            t0 = time.perf_counter()
            r = await db.execute("select id from t where id = :i", {"i": i})
            times.append(time.perf_counter() - t0)
            assert r.rows == [(i,)]
        assert not long.done(), "the long query should still be running"
        long.cancel()
        with pytest.raises(asyncio.CancelledError):
            await long
        times.sort()
        print(f"\n[duckdb-rsworker] point read during a long query: median {times[2] * 1000:.1f} ms")
        assert times[2] < 5.0
    finally:
        await db.close()


async def test_concurrent_reads_on_many_connections(tmp_path):
    db = Database(DuckDBRustWorker(tmp_path / "m.duckdb"), max_read_connections=6)
    try:
        await db.execute_write("create table t as select range id from range(100000)")
        results = await asyncio.gather(*[db.execute("select sum(id) from t where id % :m = 0", {"m": m}) for m in range(1, 13)])
        assert [r.rows[0][0] for r in results] == [sum(range(0, 100000, m)) for m in range(1, 13)]
    finally:
        await db.close()


# -- differential: in-process DuckDB against the Rust worker ---------------------------------

SETUP = [
    "create table t (id integer primary key, name varchar, b blob, f double, d date, n decimal(10, 2))",
    "insert into t values (1, 'a', 'x'::blob, 1.5, date '2024-01-02', 1.25), "
    "(2, null, null, -0.0, null, null), (3, 'é中', ''::blob, 1e308, date '1970-01-01', -3.5)",
    "create sequence seq",
]

LA = ZoneInfo("America/Los_Angeles")

# (sql, params, keyword arguments, write connection?)
CASES: list[tuple[str, Any, dict[str, Any], bool]] = [
    ("select * from t order by id", None, {}, False),
    ("select * from t order by id", None, {"max_rows": 1}, False),
    ("select * from t order by id", None, {"max_rows": 0}, False),
    ("select * from t", None, {"max_result_bytes": 5}, False),
    ("select 1; select 2", None, {}, False),
    ("", None, {}, False),
    ("-- nothing", None, {}, False),
    ("select 1 as a, null as b, [] as c, {} as e, [null] as f", None, {}, False),
    ("select ?", [], {}, False),
    ("select ?, ?", [1], {}, False),
    ("select ?", [1, 2], {}, False),
    ("select 1", [1], {}, False),
    ("select ?", {"a": 1}, {}, False),
    ("select $a, $b", {"a": 1, "b": "x"}, {}, False),
    ("select $a", {"A": 1}, {}, False),
    ("select $a, $b", {"c": 1}, {}, False),
    ("select $a", [1], {}, False),
    ("select ?", [2**31], {}, False),
    ("select ?", [2**63], {}, False),
    ("select ?", [2**64], {}, False),
    ("select ?", [-(2**63) - 1], {}, False),
    ("select ?", [2**127], {}, False),
    ("select ?", [2**130], {}, False),
    ("select ?, ?, ?", [float("inf"), float("-inf"), float("nan")], {}, False),
    ("select ?, ?, ?", [True, None, b"\x00\xff"], {}, False),
    ("select ?", ["é中\U0001f600 \x00 nul"], {}, False),
    ("select ?", [decimal.Decimal("1.50")], {}, False),
    ("select ?", [decimal.Decimal("-12345678901234567890.000000001")], {}, False),
    ("select ?", [decimal.Decimal("0.001")], {}, False),
    ("select ?", [decimal.Decimal("1E+5")], {}, False),
    ("select ?", [decimal.Decimal("1E-40")], {}, False),
    ("select ?", [dt.datetime(2020, 1, 2, 3, 4, 5, 6)], {}, False),
    ("select ?", [dt.datetime(2020, 1, 2, 3, 4, 5, tzinfo=dt.timezone.utc)], {}, False),
    ("select ?", [dt.datetime(2020, 6, 1, 12, 0, tzinfo=ZoneInfo("Europe/London"))], {}, False),
    ("select ?", [dt.date(2020, 2, 29)], {}, False),
    ("select ?", [dt.time(1, 2, 3, 4)], {}, False),
    ("select ?", [dt.time(1, 2, 3, tzinfo=dt.timezone(dt.timedelta(hours=-5)))], {}, False),
    ("select ?", [dt.timedelta(days=40, seconds=5, microseconds=7)], {}, False),
    ("select ?", [dt.timedelta(microseconds=-1)], {}, False),
    ("select ?", [uuid.UUID(int=5)], {}, False),
    ("select ?", [[1, 2]], {}, False),
    ("select ?", [[1, "a", 2.5]], {}, False),
    ("select ?", [[True, 1]], {}, False),
    ("select ?", [[[1], ["a"]]], {}, False),
    ("select ?", [[]], {}, False),
    ("select ?", [{"a": 1, "b": None}], {}, False),
    ("select ?", [{}], {}, False),
    ("select ?", [[{"a": dt.date(2024, 1, 1)}, {"a": None}]], {}, False),
    ("select ?::int", ["x"], {}, False),
    (DUCKDB_VALUES_SQL, None, {}, False),
    ("select 'a'::enum('a', 'it''s') e, union_value(k := 1)::union(k int, s varchar) u, '1010'::bit b, "
     "123456789012345678901234567890::bignum v, -5::bignum nv, 2::uhugeint uh, 255::utinyint ut, "
     "{'a b': 1, 'Select': 2} q, [[1]] nested, '-infinity'::timestamp ninf, 'infinity'::date dinf, "
     "timestamp_s '2024-01-01 00:00:01' ts_s, timestamp_ms '2024-01-01 00:00:01.5' ts_ms, "
     "time_ns '01:02:03.123456789' tns, interval '-1 day 3 microseconds' iv, 0.0000001::decimal(20,10) small, "
     "0::decimal(5,3) z, -0.5::decimal(4,1) neg, map {} em", None, {}, False),
    ("select timestamptz '2024-07-01 12:00:00+00' summer, [timestamptz '2024-01-01 00:00:00+00'] l", None, {}, False),
    ("select nope", None, {}, False),
    ("select * from missing", None, {}, False),
    ("selec 1", None, {}, False),
    ("select 1/0, 1 // 0", None, {}, False),
    ("select 2147483647::int + 1", None, {}, False),
    ("select [1, 2][1:2:0]", None, {}, False),
    ("select error('boom')", None, {}, False),
    ("select nextval('seq')", None, {}, False),
    ("insert into t (id) values (10)", None, {}, False),
    ("insert into missing values (1)", None, {}, False),
    ("create table x (a int)", None, {}, False),
    ("begin", None, {}, False),
    ("commit", None, {}, True),
    ("set threads = 4", None, {}, True),
    ("pragma version", None, {}, False),
    ("call pragma_version()", None, {}, False),
    ("checkpoint", None, {}, False),
    ("describe t", None, {}, False),
    ("show tables", None, {}, False),
    ("explain select 1", None, {}, False),
    ("values (1, 'a'), (2, 'b')", None, {}, False),
    ("from t select id order by id", None, {}, False),
    ("with x as (select 1 as v) select * from x", None, {}, False),
    ("insert into t (id) values (1)", None, {}, True),
    ("insert into t (id, name) values (4, 'd')", None, {}, True),
    ("insert into t (id, name) values (5, 'e'), (6, 'f') returning id, name", None, {}, True),
    ("insert into t (id) values (7), (8), (9) returning id", None, {"max_rows": 1}, True),
    ("update t set f = 2 where id > 100", None, {}, True),
    ("update t set f = 3 where id >= 7", None, {}, True),
    ("delete from t where id = 9", None, {}, True),
    ("insert into t (id, name) values (1, 'z') on conflict (id) do update set name = 'z'", None, {}, True),
    ("insert into t (id, name) values (1, 'z') on conflict do nothing", None, {}, True),
    ("create table u (x int)", None, {}, True),
    ("create table u (x int)", None, {}, True),
    ("drop table u", None, {}, True),
    ("select nextval('seq')", None, {}, True),
    ("pivot t on name using count(*)", None, {}, False),
    ("select * from read_csv('/etc/hosts')", None, {}, False),
    ("attach '/tmp/other.duckdb' as o", None, {}, True),
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
    """(class, message, code, driver class) of an error, local or remote."""
    if hasattr(conn, "_cur"):  # in-process: the raw driver error, mapped as the core does
        mapped = conn._backend.dialect.map_exception(e)
        cause = type(e).__name__ if mapped is not None else None
        e = mapped or e
    else:
        c = e.__cause__
        cause = str(c).split(":", 1)[0].removeprefix("_duckdb.") if c is not None else None
    return ("error", type(e).__name__, str(e), getattr(e, "code", None), cause)


async def _run(conn: Any, sql: str, params: Any, kw: dict[str, Any]) -> Any:
    try:
        r = await conn.execute(sql, params if params is not None else (), time_limit_ms=None, **kw)
    except Exception as e:  # noqa: BLE001
        return _error(conn, e)
    return (
        tuple(str(c) for c in r.columns),
        tuple(c.type for c in r.columns),
        [_norm(tuple(row.as_tuple())) for row in r.rows],
        r.truncated,
        r.rowcount,
    )


def _same_result(a: Any, b: Any) -> bool:
    if a[0] == "error" or b[0] == "error":
        return a == b
    if a[:2] != b[:2] or a[3:] != b[3:] or len(a[2]) != len(b[2]):
        return False
    return all(_same(list(x), list(y)) for x, y in zip(a[2], b[2]))


async def _pair(tmp_path: Path, codec: str) -> tuple[Any, Any, Any, Any, Any, Any]:
    _duckdb_or_skip()
    from dblayer.backends.duckdb import DuckDB

    (tmp_path / "l").mkdir()
    (tmp_path / "r").mkdir()
    # The same file name: DuckDB names the database after it in messages.
    local = DuckDB(tmp_path / "l" / "db.duckdb")
    remote = DuckDBRustWorker(tmp_path / "r" / "db.duckdb", codec=codec)
    lw, rw = await local.open(write=True), await remote.open(write=True)
    lr, rr = await local.open(write=False), await remote.open(write=False)
    return local, remote, lw, rw, lr, rr


async def _close(local: Any, remote: Any, *conns: Any) -> None:
    for c in conns:
        await c.close()
    await local.close()
    await remote.close()


@pytest.mark.parametrize("codec", CODECS)
async def test_differential_against_in_process(tmp_path, codec):
    local, remote, lw, rw, lr, rr = await _pair(tmp_path, codec)
    try:
        for sql in SETUP:
            await lw.execute(sql, (), time_limit_ms=None, max_rows=None)
            await rw.execute(sql, (), time_limit_ms=None, max_rows=None)
        mismatches = []
        for sql, params, kw, write in CASES:
            kw = {"max_rows": None, **kw}
            a = await _run(lw if write else lr, sql, params, kw)
            b = await _run(rw if write else rr, sql, params, kw)
            if not _same_result(a, b):
                mismatches.append((sql, params, kw, write, a, b))
        assert not mismatches, "\n\n".join(map(repr, mismatches))
    finally:
        await _close(local, remote, lw, rw, lr, rr)


@pytest.mark.parametrize("codec", CODECS)
async def test_row_values_match_the_python_worker(tmp_path, codec):
    """An unnamed struct is a Python tuple in-process; the protocol marks
    tuples only in columns whose type can hold an array or struct, so the
    Python worker sends a TUPLE column's values as lists, and so must this
    one."""
    _duckdb_or_skip()
    from dblayer.backends.duckdb_worker import DuckDBWorker

    sql = "select (1, 'x') r, [(1, 'x')] lr, ((1,),) rr, {'a': (1, 2)} sr"
    got = []
    for b in (DuckDBWorker(tmp_path / "p.duckdb", codec=codec), DuckDBRustWorker(tmp_path / "r.duckdb", codec=codec)):
        db = Database(b)
        try:
            r = await db.execute(sql)
            got.append(([c.type for c in r.columns], r.rows[0].as_tuple()))
        finally:
            await db.close()
    assert got[0] == got[1]
    assert all(type(a) is type(b) for a, b in zip(got[0][1], got[1][1]))
    assert type(got[1][1][1][0]) is tuple


@pytest.mark.parametrize("codec", CODECS)
async def test_execute_many_batch_and_schema_match(tmp_path, codec):
    local, remote, lw, rw, lr, rr = await _pair(tmp_path, codec)
    try:
        ddl = [
            "create table p (id integer primary key, code varchar unique, v double default 1.5)",
            "create table c (id integer primary key, p integer references p (id), q varchar, "
            "g integer generated always as (id * 2) virtual, unique (q, p))",
            "create index ix on c (q, \"p\", lower(q))",
            "create view vv as select id, code from p",
        ]
        for conn in (lw, rw):
            for sql in ddl:
                await conn.execute(sql, (), time_limit_ms=None, max_rows=None)
        assert await lr.schema() == await rr.schema()
        assert await lr.schema_version() == await rr.schema_version()
        out = []
        for conn in (lw, rw):
            res = []
            for sql, seq in [
                ("insert into p (id, code) values (?, ?)", [[1, "a"], [2, "b"], [3, "c"]]),
                ("update p set v = v + 1 where id >= ?", [[1], [2]]),
                ("select ?", [[1], [2]]),
                ("insert into p (id) values (?)", []),
                ("insert into p (id) values (?)", [[1]]),
                ("create table if not exists v (x int)", [[]]),
                ("select 1; select 2", [[]]),
            ]:
                try:
                    res.append((await conn.execute_many(sql, seq)).rowcount)
                except Exception as e:  # noqa: BLE001
                    res.append(_error(conn, e)[:4])
            out.append(res)
        assert out[0] == out[1]
        out = []
        for conn in (lr, rr):
            try:
                rs = await conn.execute_batch([("select id from p order by id", ()), ("select :a", {"a": 2})], time_limit_ms=None, max_rows=None)
                out.append([r.rows for r in rs])
            except Exception as e:  # noqa: BLE001
                out.append(_error(conn, e)[:4])
        assert out[0] == out[1]
        assert await lr.schema() == await rr.schema()
    finally:
        await _close(local, remote, lw, rw, lr, rr)


@pytest.mark.parametrize("codec", CODECS)
async def test_streams_match(tmp_path, codec):
    local, remote, lw, rw, lr, rr = await _pair(tmp_path, codec)
    try:
        for conn in (lw, rw):
            await conn.execute("create table t as select range x, range::varchar s from range(25)", (), time_limit_ms=None, max_rows=None)
        got = []
        for conn in (lr, rr):
            chunks = [c async for c in conn.stream("select x, s, [x] l from t order by x", (), chunk_rows=7)]
            got.append(([len(c) for c in chunks], [tuple(r) for c in chunks for r in c]))
            s = conn.stream("select x from t order by x", (), chunk_rows=2)
            first = await s.__anext__()
            assert list(map(tuple, first)) == [(0,), (1,)]
            await s.aclose()  # abandoned: the worker closes the result and its transaction
            r = await conn.execute("select count(*) from t", (), time_limit_ms=None, max_rows=None)
            assert r.rows == [(25,)]
            empty = [c async for c in conn.stream("select x from t where x < 0", (), chunk_rows=5)]
            assert empty == []
        assert got[0] == got[1]
        assert got[0][0] == [7, 7, 7, 4]
    finally:
        await _close(local, remote, lw, rw, lr, rr)


async def test_aborted_transaction_cannot_commit(tmp_path):
    """DuckDB turns COMMIT of an aborted transaction into a silent rollback;
    the worker refuses it, as the in-process backend does."""
    db = Database(DuckDBRustWorker(tmp_path / "a.duckdb"))
    try:
        await db.execute_write("create table t (id integer primary key)")
        with pytest.raises(QueryError) as e:
            async with db.transaction() as tx:
                await tx.execute("insert into t values (1)")
                with pytest.raises(QueryError):
                    await tx.execute("insert into t values (1)")
        assert e.value.code == "TRANSACTION_ABORTED"
        assert (await db.execute("select count(*) from t")).rows == [(0,)]
        async with db.transaction() as tx:
            await tx.execute("insert into t values (2)")
        assert (await db.execute("select id from t")).rows == [(2,)]
    finally:
        await db.close()


# -- the appender --------------------------------------------------------------------------


async def test_insert_rows_uses_the_appender_and_matches_the_default_path(tmp_path):
    rows = [
        {"id": 1, "name": "a", "when": dt.datetime(2024, 1, 2, 3, 4, 5), "price": decimal.Decimal("1.50")},
        {"id": 2, "name": None, "when": None, "price": None},
        {"id": 3, "name": "c", "extra": True},
    ]
    results = []
    for native in (False, True):
        db = Database(DuckDBRustWorker(tmp_path / f"n{native}.duckdb"), native_operations=native)
        try:
            WorkerProcess.record_new = rec = []
            try:
                r = await db.insert_rows("t", rows, pk="id", alter=True)
            finally:
                WorkerProcess.record_new = None
            ops = [m.get("name") for d, m in rec if d == "->" and m.get("type") == "op"]
            assert ops == (["append", "append"] if native else [])
            got = await db.execute("select * from t order by id")
            results.append((r.rowcount, r.table_created, r.columns_added, [c.type for c in got.columns], got.rows))
        finally:
            await db.close()
    assert results[0] == results[1]


async def test_insert_rows_into_an_existing_table_is_one_round_trip_per_batch(tmp_path):
    db = Database(DuckDBRustWorker(tmp_path / "rt.duckdb"), native_operations=True)
    try:
        WorkerProcess.record_new = rec = []  # messages of processes started from now on
        try:
            await db.execute_write("create table t (id bigint, name varchar)")
            mark = len(rec)
            r = await db.insert_rows("t", [{"id": i, "name": f"n{i}"} for i in range(250)], batch_size=100)
        finally:
            WorkerProcess.record_new = None
        skip = ("hello", "open", "begin", "commit", "rollback", "close")
        sent = [m for d, m in rec[mark:] if d == "->" and m.get("type") not in skip]
        assert [m.get("type") for m in sent] == ["op", "op", "op"], sent
        assert all(m["args"].get("check") for m in sent)
        assert r.rowcount == 250 and not r.table_created
        # A missing column: the worker declines and the default path adds it.
        r = await db.insert_rows("t", [{"id": 1, "name": "x", "extra": 2.5}], alter=True)
        assert r.columns_added == ("extra",)
        assert (await db.execute("select count(*), sum(extra) from t")).rows == [(251, 2.5)]
    finally:
        await db.close()


async def test_append_errors_and_rollback(tmp_path):
    db = Database(DuckDBRustWorker(tmp_path / "e.duckdb"), native_operations=True)
    try:
        await db.execute_write("create table t (id integer primary key, v integer)")
        with pytest.raises(QueryError):
            await db.insert_rows("t", [{"id": 1, "v": 1}, {"id": 1, "v": 2}])
        assert (await db.execute("select count(*) from t")).rows == [(0,)]
        with pytest.raises(QueryError):
            await db.insert_rows("t", [{"id": 5, "v": "not a number"}])
        assert (await db.execute("select count(*) from t")).rows == [(0,)]
    finally:
        await db.close()


# -- malformed input -------------------------------------------------------------------------


def _raw_session(codec: str, payloads: list[bytes], *, raw_tail: bytes = b"") -> tuple[int, bytes, bytes]:
    p = subprocess.Popen([find_binary()], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    hello = json.dumps({"type": "hello", "protocol": 1, "codecs": [codec], "id": 1}).encode()
    data = struct.pack(">I", len(hello)) + hello
    for payload in payloads:
        data += struct.pack(">I", len(payload)) + payload
    data += raw_tail
    try:
        out, err = p.communicate(data, timeout=30)
    except subprocess.TimeoutExpired:
        p.kill()
        p.communicate()
        raise
    return p.returncode, out, err


@pytest.mark.parametrize("payload", MALFORMED_MSGPACK, ids=[str(i) for i in range(len(MALFORMED_MSGPACK))])
def test_malformed_msgpack_drops_the_connection_cleanly(payload):
    code, _out, err = _raw_session("msgpack", [payload])
    assert code == 0, err.decode(errors="replace")[-2000:]


@pytest.mark.parametrize("payload", MALFORMED_JSON, ids=[str(i) for i in range(len(MALFORMED_JSON))])
def test_malformed_json_drops_the_connection_cleanly(payload):
    code, _out, err = _raw_session("json", [payload])
    assert code == 0, err.decode(errors="replace")[-2000:]


def test_oversized_and_truncated_frames():
    code, _out, err = _raw_session("json", [], raw_tail=struct.pack(">I", 2**31) + b"x" * 8)
    assert code == 0 and b"frame size cap" in err
    code, _out, _err = _raw_session("json", [], raw_tail=struct.pack(">I", 100) + b'{"id":')
    assert code == 0


@pytest.mark.parametrize("codec", ["json", "msgpack"])
def test_random_payloads_never_crash_the_worker(codec, tmp_path):
    """A seeded fuzz of the decoders and request handlers: mutated valid
    requests, a few per process. Exit code 0 means the worker answered or
    dropped the connection; a panic that escaped, or a crash, is not 0."""
    import random

    if codec == "msgpack":
        msgpack = pytest.importorskip("msgpack")

        def enc(m: dict[str, Any]) -> bytes:
            return msgpack.packb(m, use_bin_type=True)
    else:

        def enc(m: dict[str, Any]) -> bytes:
            return json.dumps(m).encode()

    rng = random.Random(1234)
    seeds: list[dict[str, Any]] = [
        {"type": "open", "path": str(tmp_path / "f.duckdb"), "mode": "write", "id": 2},
        {"type": "execute", "sql": "select ?, $2", "params": [1, "x", 2.5], "id": 3},
        {"type": "execute", "sql": "select $a", "params": {"a": [1, {"b": None}]}, "id": 3},
        {"type": "execute", "sql": "select ?", "params": [{"$x": ["date", "2024-01-01"]}], "id": 3},
        {"type": "execute_many", "sql": "select ?", "params_seq": [[1], [2]], "id": 4},
        {"type": "batch", "statements": [["select 1", None], ["select ?", [1]]], "id": 5},
        {"type": "stream", "sql": "select range from range(10)", "chunk_rows": 1, "credit": 2, "id": 6},
        {"type": "begin", "mode": "read", "deadline_ms": 10, "id": 7},
        {"type": "schema", "tx": "t1", "id": 8},
        {"type": "rollback", "tx": "t99999999999", "id": 9},
        {"type": "op", "name": "append", "args": {"table": "t", "columns": ["a"], "rows": [[1]]}, "id": 10},
        {"type": "cancel", "target": 3},
        {"type": "more", "stream": 6, "credit": 1},
    ]
    failures = []
    for i in range(int(os.environ.get("DBLAYER_RSWORKER_FUZZ", "60"))):
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
        code, _out, err = _raw_session(codec, payloads)
        if code != 0 or b"panicked" in err:
            failures.append((i, code, err.decode(errors="replace")[-1500:]))
    assert not failures, failures[:3]


# -- end of input ------------------------------------------------------------------------------


async def test_end_of_input_cancels_rolls_back_and_exits(tmp_path):
    _duckdb_or_skip()
    import duckdb

    path = tmp_path / "eof.duckdb"
    db = Database(DuckDBRustWorker(path))
    try:
        await db.execute_write("create table t (x int)")
        async with db.transaction() as tx:
            await tx.execute("insert into t values (1)")
            proc = db._backend.placement_group.process  # type: ignore[attr-defined]
            assert proc is not None
            proc.close()  # the parent goes away: stdin EOF
            for _ in range(500):
                if proc.popen is not None and proc.popen.poll() is not None:
                    break
                await asyncio.sleep(0.01)
            assert proc.popen is not None and proc.popen.poll() == 0
            raise asyncio.CancelledError  # leave without touching the dead worker
    except (asyncio.CancelledError, Exception):
        pass
    finally:
        await db.close()
    con = duckdb.connect(str(path))  # the lock was released; nothing was committed
    try:
        assert con.execute("select count(*) from t").fetchone() == (0,)
    finally:
        con.close()
