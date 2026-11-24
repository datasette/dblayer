"""The C worker (``cworker/``, ``SqliteCWorker``) beyond the conformance
suite: finding the binary, what it refuses, both codecs, and a differential
test that runs awkward statements on in-process SQLite and on the C worker
and requires the same results and the same errors.

Skipped when the binary is not built (``make -C cworker``)."""

from __future__ import annotations

import asyncio
import math
import os
import sqlite3
import subprocess
import time
from pathlib import Path
from typing import Any

import pytest

from dblayer import Database, NotSupported, QueryError, ResourceLimitExceeded
from dblayer.backends.sqlite import SqliteInProcess
from dblayer.backends.sqlite_cworker import (
    BUILD_COMMAND,
    ENV_VAR,
    SqliteCWorker,
    binary_unavailable,
    c_shared_workers,
    find_binary,
)
from dblayer.backends.sqlite_worker import WorkerConnection, WorkerProcess
from dblayer.worker.protocol import available_codecs

UNAVAILABLE = binary_unavailable()
pytestmark = pytest.mark.skipif(UNAVAILABLE is not None, reason=UNAVAILABLE or "")

CODECS = ["json"] + (["msgpack"] if "msgpack" in available_codecs() else [])


# -- the binary --------------------------------------------------------------------


def test_binary_lookup_order(tmp_path, monkeypatch):
    real = find_binary()  # from $DBLAYER_CWORKER if set, else the default
    monkeypatch.delenv(ENV_VAR, raising=False)
    if binary_unavailable() is None:
        assert find_binary().endswith("cworker/build/dblayer-cworker")  # default location
    fake = tmp_path / "nope"
    monkeypatch.setenv(ENV_VAR, str(fake))
    with pytest.raises(NotSupported) as e:
        find_binary()
    assert BUILD_COMMAND in str(e.value) and ENV_VAR in str(e.value)
    assert find_binary(real) == real  # an explicit path beats the variable
    monkeypatch.setenv(ENV_VAR, real)
    assert find_binary() == real
    with pytest.raises(NotSupported, match="binary not built"):
        SqliteCWorker(tmp_path / "x.db", binary=fake)


def test_init_hooks_are_refused(tmp_path):
    with pytest.raises(NotSupported, match="init hooks"):
        SqliteCWorker(tmp_path / "x.db", init=["tests.worker.hooks:hog"])


def test_version_flag():
    out = subprocess.run([find_binary(), "--version"], capture_output=True, text=True, check=True).stdout
    assert out.startswith("dblayer-cworker protocol 1, SQLite 3.")


# -- through Database --------------------------------------------------------------


@pytest.mark.parametrize("codec", CODECS)
async def test_hello_and_round_trip(tmp_path, codec):
    db = Database(SqliteCWorker(tmp_path / "a.db", codec=codec))
    try:
        await db.execute_write("create table t (id integer primary key, v)")
        w = await db.execute_write("insert into t (v) values (:v)", {"v": b"\x00\xff"})
        assert (w.rowcount, w.last_pk) == (1, 1)
        r = await db.execute("select id, v from t")
        assert r.rows == [(1, b"\x00\xff")]
        conn = db._pool._idle_read[0].conn  # type: ignore[union-attr]
        hello = conn.process.hello
        assert hello["implementation"] == "c" and hello["ops"] is False
        assert hello["codec"] == codec
        assert hello["sqlite_version"].startswith("3.")
    finally:
        await db.close()


async def test_named_operations_decline_without_a_round_trip(tmp_path):
    db = Database(SqliteCWorker(tmp_path / "o.db"), native_operations=True)
    try:
        await db.execute_write("create table t (id integer primary key)")
        WorkerProcess.record_new = rec = []
        try:
            await db.insert_rows("t", [{"id": 1, "name": "a"}], alter=True)
        finally:
            WorkerProcess.record_new = None
        sent = [m.get("type") for d, m in rec if d == "->"]
        assert "op" not in sent  # the core ran insert_rows itself
        assert (await db.execute("select id, name from t")).rows == [(1, "a")]
    finally:
        await db.close()


async def test_op_message_gets_not_supported(tmp_path):
    db = Database(SqliteCWorker(tmp_path / "op.db"))
    try:
        await db.execute("select 1")
        conn = db._pool._idle_read[0].conn  # type: ignore[union-attr]
        with pytest.raises(NotSupported, match="not implemented by the C worker"):
            await conn.process.call({"type": "op", "name": "insert_rows", "args": {}}, owner=conn)
        with pytest.raises(NotSupported, match="SQLite only"):
            await conn.process.call(
                {"type": "open", "path": str(tmp_path / "d.duckdb"), "engine": "dblayer.backends.duckdb:DuckDB"},
                owner=conn,
            )
    finally:
        await db.close()


async def test_sql_limits_and_extensions(tmp_path):
    db = Database(SqliteCWorker(tmp_path / "l.db", sql_limits={"length": 1000}))
    try:
        with pytest.raises(ResourceLimitExceeded, match="too big"):
            await db.execute("select length(randomblob(2000) || 'x')")
        assert (await db.execute("select length(randomblob(500))")).rows == [(500,)]
    finally:
        await db.close()
    with pytest.raises(ValueError, match="unknown SQL limit"):
        SqliteCWorker(tmp_path / "l2.db", sql_limits={"nope": 1})
    db = Database(SqliteCWorker(tmp_path / "e.db", extensions=[str(tmp_path / "missing_ext")]))
    try:
        with pytest.raises((QueryError, NotSupported)) as e:
            await db.execute("select 1")
        print(f"\n[cworker] missing extension: {type(e.value).__name__}: {e.value}")
    finally:
        await db.close()


async def test_shared_placement(tmp_path):
    group = c_shared_workers(2, linger=0)
    dbs = [Database(SqliteCWorker(tmp_path / f"s{i}.db", workers=group)) for i in range(5)]
    try:
        for i, db in enumerate(dbs):
            await db.execute_write("create table t (x)")
            await db.execute_write("insert into t values (?)", [i])
        for i, db in enumerate(dbs):
            assert (await db.execute("select x from t")).rows == [(i,)]
        assert len({p.pid for p in group.procs}) <= 2
    finally:
        for db in dbs:
            await db.close()
        group.close()


async def test_end_of_input_rolls_back_and_exits(tmp_path):
    path = tmp_path / "eof.db"
    db = Database(SqliteCWorker(path))
    try:
        await db.execute_write("create table t (x)")
        async with db.transaction() as tx:
            await tx.execute("insert into t values (1)")
            conn = tx._conn
            assert isinstance(conn, WorkerConnection)
            proc = conn.process
            proc.close()  # the parent goes away: stdin EOF
            for _ in range(200):
                if proc.popen is not None and proc.popen.poll() is not None:
                    break
                await asyncio.sleep(0.01)
            assert proc.popen is not None and proc.popen.poll() == 0
            other = sqlite3.connect(path, timeout=0)
            other.execute("begin immediate")  # the lock was released
            other.execute("rollback")
            other.close()
            raise asyncio.CancelledError  # leave the transaction without touching the dead worker
    except (asyncio.CancelledError, Exception):
        pass
    finally:
        await db.close()
    assert sqlite3.connect(path).execute("select count(*) from t").fetchone() == (0,)


async def test_start_up_time_is_reported(tmp_path):
    """Open-to-first-result for a new database, C worker vs in-process
    (reported, not asserted beyond a generous bound)."""
    times = []
    for i in range(5):
        db = Database(SqliteCWorker(tmp_path / f"st{i}.db"))
        t0 = time.perf_counter()
        await db.execute("select 1")
        times.append(time.perf_counter() - t0)
        await db.close()
    times.sort()
    print(f"\n[cworker] open-to-first-result: median {times[2] * 1000:.1f} ms, max {times[-1] * 1000:.1f} ms")
    assert times[2] < 2.0


# -- differential: in-process sqlite3 vs the C worker -------------------------------

SETUP = [
    "create table t (id integer primary key, name text, b blob, f real)",
    "create table nr (k text primary key, v) without rowid",
    "insert into t (name, b, f) values ('a', x'00ff', 1.5), ('b', null, -0.0), ('c', x'', 1e308)",
]

CASES: list[tuple[str, Any, dict[str, Any]]] = [
    ("select * from t order by id", None, {}),
    ("select * from t order by id", None, {"max_rows": 1}),
    ("select * from t order by id", None, {"max_rows": 0}),
    ("select * from t", None, {"max_result_bytes": 5}),
    ("select 1; select 2", None, {}),
    ("select 1; -- trailing comment", None, {}),
    ("", None, {}),
    ("-- nothing", None, {}),
    # Not here: ("select :a", {}) - the parent leaves empty params out of
    # the message, so every worker sees a sequence (as SqliteWorker does).
    ("select ?", [], {}),
    ("select ?, ?", [1], {}),
    ("select ?", {"a": 1}, {}),
    ("select :a, :b", {"a": 1, "b": "x"}, {}),
    ("select ?", [2**63], {}),
    ("select ?", [-(2**63)], {}),
    ("select ?", [2**63 - 1], {}),
    ("select ?", [float("inf")], {}),
    ("select ?", [float("-inf")], {}),
    ("select ?", [float("nan")], {}),
    ("select ?, ?, ?", [True, None, b""], {}),
    ("select ?", ["é中\U0001f600 \x00 nul"], {}),
    # Not here: ("select ?", [{"$b": "x"}]) - with JSON the parent escapes
    # wrapper-like dicts only in dict params, so this arrives as bytes in
    # any worker (see docs/worker-protocol.md, Codecs).
    ("select ?", [[1, 2]], {}),
    ("select cast(x'ff' as text) as c", None, {}),
    ("select 1/0, zeroblob(3), 1e300 * 1e300, -1e300 * 1e300", None, {}),
    ("select nope", None, {}),
    ("select * from missing", None, {}),
    ("insert into t (id) values (1)", None, {}),
    ("insert into t (name) values ('d')", None, {}),
    ("insert into t (name) values ('e') returning id, name", None, {}),
    ("insert into t (name) values ('f'), ('g') returning id", None, {"max_rows": 1}),
    ("with x as (select 1) insert into t (name) select 'h' from x", None, {}),
    ("insert into t (id, name) values (1, 'z') on conflict (id) do update set name = 'z'", None, {}),
    ("insert into t (id, name) values (1, 'z') on conflict do nothing", None, {}),
    ("insert into nr values ('k', 1)", None, {}),
    ("update t set f = 2 where id > 100", None, {}),
    ("delete from t where name = 'h'", None, {}),
    ("/* c */ update t set f = f", None, {}),
    ("create table u (x)", None, {}),
    ("pragma user_version", None, {}),
    ("select x'' as e, '' as s, 9223372036854775807 as big", None, {}),
    ("select 0.1, 1e-310, 123456789.123456789, 2.0", None, {}),
    ("select group_concat(name, ',') from t", None, {}),
    ("values (1, 'a'), (2, 'b')", None, {}),
    ("select printf('%.*c', 3000, 'x')", None, {"max_result_bytes": 2000}),
]


async def _run(conn: Any, sql: str, params: Any, kw: dict[str, Any]) -> Any:
    try:
        r = await conn.execute(sql, params if params is not None else (), time_limit_ms=None, **kw)
    except Exception as e:  # noqa: BLE001
        # In-process connections raise sqlite3 errors; the core maps them.
        mapped = conn._backend.dialect.map_exception(e) if hasattr(conn, "_raw") else None
        e = mapped or e
        return ("error", type(e).__name__, str(e), getattr(e, "code", None))
    rows = [tuple("nan" if isinstance(v, float) and math.isnan(v) else v for v in row.as_tuple()) for row in r.rows]
    return (tuple(str(c) for c in r.columns), rows, r.truncated, r.rowcount, r.last_pk)


@pytest.mark.parametrize("codec", CODECS)
async def test_differential_against_in_process(tmp_path, codec):
    local = SqliteInProcess(tmp_path / "local.db")
    remote = SqliteCWorker(tmp_path / "remote.db", codec=codec)
    lc = await local.open(write=True)
    rc = await remote.open(write=True)
    try:
        for sql in SETUP:
            await lc.execute(sql, (), time_limit_ms=None, max_rows=None)
            await rc.execute(sql, (), time_limit_ms=None, max_rows=None)
        mismatches = []
        for sql, params, kw in CASES:
            kw = {"max_rows": None, **kw}
            a = await _run(lc, sql, params, kw)
            b = await _run(rc, sql, params, kw)
            if a != b:
                mismatches.append((sql, params, kw, a, b))
        assert not mismatches, "\n".join(map(repr, mismatches))
        # executemany, schema and schema_version
        for conn in (lc, rc):
            w = await conn.execute_many("insert into t (name) values (?)", [["m1"], ["m2"]])
            assert w.rowcount == 2
        for bad in ("select ?", "create table if not exists v (x)"):
            ra = await _many(lc, bad)
            rb = await _many(rc, bad)
            assert ra == rb, (bad, ra, rb)
        await lc.execute("create index ix on t (name)", (), time_limit_ms=None, max_rows=None)
        await rc.execute("create index ix on t (name)", (), time_limit_ms=None, max_rows=None)
        for sql in (
            "create table child (id integer primary key, p integer references t, q text references nr (k) "
            "on delete cascade, unique (q, p))",
            "create view vv as select id, name from t",
            "create trigger tr after insert on t begin select 1; end",
            "create table g (a int, b int generated always as (a * 2) virtual, c text default 'x' not null)",
            "create virtual table ft using fts5(body)",
        ):
            await lc.execute(sql, (), time_limit_ms=None, max_rows=None)
            await rc.execute(sql, (), time_limit_ms=None, max_rows=None)
        assert await lc.schema() == await rc.schema()
        assert await lc.schema_version() == await rc.schema_version()
    finally:
        await lc.close()
        await rc.close()
        await local.close()


async def _many(conn: Any, sql: str) -> Any:
    try:
        return (await conn.execute_many(sql, [[1]] if "?" in sql else [[]])).rowcount
    except Exception as e:  # noqa: BLE001
        mapped = conn._backend.dialect.map_exception(e) if hasattr(conn, "_raw") else None
        e = mapped or e
        return (type(e).__name__, str(e))


@pytest.mark.parametrize("codec", CODECS)
async def test_streams_match(tmp_path, codec):
    path = tmp_path / "s.db"
    setup = sqlite3.connect(path)
    setup.execute("create table t (x)")
    setup.executemany("insert into t values (?)", [[i] for i in range(25)])
    setup.commit()
    setup.close()
    for backend in (SqliteInProcess(path), SqliteCWorker(path, codec=codec)):
        conn = await backend.open(write=False)
        try:
            chunks = [c async for c in conn.stream("select x, x * 2 from t", (), chunk_rows=7)]
            assert [len(c) for c in chunks] == [7, 7, 7, 4]
            assert [tuple(r) for c in chunks for r in c][-1] == (24, 48)
            s = conn.stream("select x from t", (), chunk_rows=2)
            first = await s.__anext__()
            assert list(map(tuple, first)) == [(0,), (1,)]
            await s.aclose()  # abandoned: the worker closes its cursor
            assert (await conn.execute("select count(*) from t", (), time_limit_ms=None, max_rows=None)).rows == [(25,)]
        finally:
            await conn.close()
            await backend.close()


def test_no_python_in_the_worker(tmp_path):
    """The point of the exercise: the worker process is not a Python
    interpreter."""
    exe = Path(find_binary())
    head = exe.read_bytes()[:4]
    assert head in (b"\x7fELF", b"\xcf\xfa\xed\xfe", b"\xca\xfe\xba\xbe")
    assert os.access(exe, os.X_OK)


# -- malformed input ---------------------------------------------------------------

MALFORMED_MSGPACK = [
    b"",  # empty payload
    b"\xc1",  # never-used byte
    b"\x81",  # map of one pair, nothing after
    b"\xdf\xff\xff\xff\xff",  # map32 claiming 4 billion pairs
    b"\xdd\x7f\xff\xff\xff",  # array32 claiming 2 billion items
    b"\x81\xa4type\xdb\xff\xff\xff\xff",  # str32 longer than the frame
    b"\x81\xa4type\xc7\x05\x02abc",  # ext declared longer than the frame
    b"\x81\xa4type\xd4\x05x",  # unknown ext type
    b"\x91" * 5000 + b"\xc0",  # 5000 nested arrays
    b"\x81\xa2id\xcf\xff\xff\xff\xff\xff\xff\xff\xff",  # id: uint64 max, no type
    b"\x80\x80",  # extra data after the message
    b"\x2a",  # not a map
]
MALFORMED_JSON = [
    b"",
    b"{",
    b'{"type": "execute", "sql": "select 1"',
    b'{"type": "x\\u12"}',
    b'{"type": "\\uD800\\uD800"}',
    b"[" * 5000 + b"]" * 5000,
    b'{"a": 1e999999}',
    b'{"a": -}',
    b'{"$b": 5}',
    b'{"$d": [1]}',
    b'{"$x": ["date", "2020-01-01"]}',
    b'{"a": "unterminated}',
    b"null",
    b'{"a": 1} x',
]


def _raw_session(codec: str, payloads: list[bytes]) -> tuple[int, bytes]:
    """Start the binary, agree `codec`, then send raw payloads and close
    stdin. Returns the exit code and stderr."""
    import json
    import struct

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


@pytest.mark.parametrize("codec", ["json", "msgpack"])
def test_random_payloads_never_crash_the_worker(codec, tmp_path):
    """A small seeded fuzz of the decoders and request handlers: mutated
    valid requests, a few per process. Exit code 0 means the worker
    answered or dropped the connection; a crash (or a sanitizer report,
    with the ASan build) is a non-zero exit."""
    import json
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

    rng = random.Random(1234)
    seeds: list[dict[str, Any]] = [
        {"type": "open", "path": str(tmp_path / "f.db"), "mode": "write", "id": 2},
        {"type": "execute", "sql": "select ?, :a", "params": [1, "x", 2.5], "id": 3},
        {"type": "execute", "sql": "select ?", "params": {"a": [1, {"b": None}]}, "id": 3},
        {"type": "execute_many", "sql": "select ?", "params_seq": [[1], [2]], "id": 4},
        {"type": "batch", "statements": [["select 1", None], ["select 2", [1]]], "id": 5},
        {"type": "stream", "sql": "select 1", "chunk_rows": 1, "credit": 2, "id": 6},
        {"type": "begin", "mode": "read", "deadline_ms": 10, "id": 7},
        {"type": "schema", "tx": "t1", "id": 8},
        {"type": "rollback", "tx": "t99999999999", "id": 9},
        {"type": "cancel", "target": 3},
        {"type": "more", "stream": 6, "credit": 1},
    ]
    failures = []
    for i in range(int(os.environ.get("DBLAYER_CWORKER_FUZZ", "60"))):
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
