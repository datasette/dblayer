"""The Node worker (``nodeworker/``, ``SqliteNodeWorker``) beyond the
conformance suite: finding Node and the script, what it refuses, both
codecs (64-bit integers and int/float fidelity through JavaScript), a
differential test against in-process SQLite, streams, connections running
concurrently in one process, and malformed input.

Skipped when Node or the npm packages are missing (``npm ci --prefix
nodeworker``)."""

from __future__ import annotations

import asyncio
import json
import math
import os
import sqlite3
import struct
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest

from dblayer import Database, NotSupported, QueryError, ResourceLimitExceeded
from dblayer.backends.sqlite import SqliteInProcess
from dblayer.backends.sqlite_nodeworker import (
    NODE_ENV_VAR,
    SCRIPT_ENV_VAR,
    SETUP_COMMAND,
    SqliteNodeWorker,
    find_node,
    find_script,
    unavailable,
)
from dblayer.worker.protocol import available_codecs

from .test_cworker import CASES, MALFORMED_JSON, MALFORMED_MSGPACK, SETUP, _many, _run

UNAVAILABLE = unavailable()
pytestmark = pytest.mark.skipif(UNAVAILABLE is not None, reason=UNAVAILABLE or "")

CODECS = ["json"] + (["msgpack"] if "msgpack" in available_codecs() else [])


# -- finding Node and the script ----------------------------------------------------


def test_lookup_order(tmp_path, monkeypatch):
    node = find_node()
    script = find_script()
    assert script.endswith("nodeworker/src/main.mjs") or os.environ.get(SCRIPT_ENV_VAR)
    monkeypatch.setenv(NODE_ENV_VAR, str(tmp_path / "no-node"))
    with pytest.raises(NotSupported, match="Node.js not found"):
        find_node()
    assert find_node(node) == node  # an explicit path beats the variable
    monkeypatch.delenv(NODE_ENV_VAR)
    monkeypatch.setenv(SCRIPT_ENV_VAR, str(tmp_path / "missing.mjs"))
    with pytest.raises(NotSupported, match="script not found"):
        find_script()
    # a script with no node_modules next to it
    fake = tmp_path / "nw" / "src" / "main.mjs"
    fake.parent.mkdir(parents=True)
    fake.write_text("")
    with pytest.raises(NotSupported) as e:
        find_script(fake)
    assert SETUP_COMMAND in str(e.value)
    assert "npm" in (unavailable(script=fake) or "")
    with pytest.raises(NotSupported, match="npm packages"):
        SqliteNodeWorker(tmp_path / "x.db", script=fake)


def test_refusals(tmp_path):
    with pytest.raises(NotSupported, match="init hooks"):
        SqliteNodeWorker(tmp_path / "x.db", init=["tests.worker.hooks:hog"])
    with pytest.raises(NotSupported, match="extensions"):
        SqliteNodeWorker(tmp_path / "x.db", extensions=["/nope"])
    with pytest.raises(NotSupported, match="one process per database"):
        SqliteNodeWorker(tmp_path / "x.db", placement="dedicated")
    first = SqliteNodeWorker(tmp_path / "y.db", codec="json")
    with pytest.raises(NotSupported, match="other settings"):
        SqliteNodeWorker(tmp_path / "y.db", codec="json", max_frame_bytes=1000)
    assert SqliteNodeWorker(tmp_path / "y.db", codec="json").workers is first.workers


def test_version_flag():
    out = subprocess.run([find_node(), find_script(), "--version"], capture_output=True, text=True, check=True).stdout
    assert out.startswith("dblayer-nodeworker protocol 1, SQLite 3.")
    assert "WebAssembly" in out


# -- through Database -----------------------------------------------------------------


@pytest.mark.parametrize("codec", CODECS)
async def test_hello_and_round_trip(tmp_path, codec):
    db = Database(SqliteNodeWorker(tmp_path / "a.db", codec=codec))
    try:
        await db.execute_write("create table t (id integer primary key, v)")
        w = await db.execute_write("insert into t (v) values (:v)", {"v": b"\x00\xff"})
        assert (w.rowcount, w.last_pk) == (1, 1)
        assert (await db.execute("select id, v from t")).rows == [(1, b"\x00\xff")]
        hello = db._pool._idle_read[0].conn.process.hello  # type: ignore[union-attr]
        assert hello["implementation"] == "node" and hello["ops"] is False
        assert hello["concurrent"] is True and hello["guard"] is True
        assert hello["codec"] == codec and hello["sqlite_version"].startswith("3.")
    finally:
        await db.close()
    # a real file: native SQLite reads it
    assert sqlite3.connect(tmp_path / "a.db").execute("select v from t").fetchall() == [(b"\x00\xff",)]


BIG = [
    2**63 - 1,
    -(2**63),
    2**53,
    2**53 + 1,  # not a double: JavaScript must keep it as a BigInt
    -(2**53) - 1,
    9007199254740993,
    1,
    -1,
    0,
]


@pytest.mark.parametrize("codec", CODECS)
async def test_64_bit_integers_survive_javascript(tmp_path, codec):
    """SQLite INTEGER is 64-bit and JavaScript numbers are doubles: every
    value must cross both codecs, in parameters and in results, exactly."""
    db = Database(SqliteNodeWorker(tmp_path / "big.db", codec=codec))
    try:
        await db.execute_write("create table t (i integer)")
        for v in BIG:
            await db.execute_write("insert into t values (?)", [v])
        r = await db.execute("select i, typeof(i), cast(i as text) from t order by rowid")
        assert [row[0] for row in r.rows] == BIG
        assert {row[1] for row in r.rows} == {"integer"}
        assert [int(row[2]) for row in r.rows] == BIG  # SQLite stored the exact value
        r = await db.execute("select 9223372036854775807, -9223372036854775808, 9007199254740993")
        assert r.rows == [(2**63 - 1, -(2**63), 9007199254740993)]
        # last_pk beyond 2**53
        await db.execute_write("create table k (id integer primary key)")
        w = await db.execute_write("insert into k values (?)", [2**62 + 3])
        assert w.last_pk == 2**62 + 3
        # outside 64 bits: Python's sqlite3 raises OverflowError, so must we
        with pytest.raises(OverflowError):
            await db.execute("select ?", [2**63])
        with pytest.raises(OverflowError):
            await db.execute("select ?", [-(2**63) - 1])
    finally:
        await db.close()


@pytest.mark.parametrize("codec", CODECS)
async def test_int_and_float_stay_apart(tmp_path, codec):
    db = Database(SqliteNodeWorker(tmp_path / "f.db", codec=codec))
    try:
        r = await db.execute(
            "select ?, typeof(?), ?, typeof(?), ?, ?, 1.0, 2, -0.0, 1e308 * 10",
            [1.0, 1.0, 1, 1, -0.0, 0.5],
        )
        row = r.rows[0]
        assert [type(v) for v in row] == [float, str, int, str, float, float, float, int, float, float]
        assert (row[1], row[3]) == ("real", "integer")
        assert math.copysign(1, row[4]) == -1  # -0.0 kept
        assert row[9] == math.inf
    finally:
        await db.close()


async def test_named_operations_decline_without_a_round_trip(tmp_path):
    from dblayer.backends.sqlite_worker import WorkerProcess

    db = Database(SqliteNodeWorker(tmp_path / "o.db"), native_operations=True)
    try:
        await db.execute_write("create table t (id integer primary key)")
        WorkerProcess.record_new = rec = []
        try:
            await db.insert_rows("t", [{"id": 1, "name": "a"}], alter=True)
        finally:
            WorkerProcess.record_new = None
        assert "op" not in [m.get("type") for d, m in rec if d == "->"]
        assert (await db.execute("select id, name from t")).rows == [(1, "a")]
    finally:
        await db.close()


async def test_sql_limits_and_heap_limit(tmp_path):
    db = Database(SqliteNodeWorker(tmp_path / "l.db", sql_limits={"length": 1000}))
    try:
        with pytest.raises(ResourceLimitExceeded, match="too big"):
            await db.execute("select length(randomblob(2000) || 'x')")
        assert (await db.execute("select length(randomblob(500))")).rows == [(500,)]
    finally:
        await db.close()
    db = Database(SqliteNodeWorker(tmp_path / "h.db", memory_limit_mb=32))
    try:
        with pytest.raises(ResourceLimitExceeded):
            await db.execute("select length(randomblob(100000000))")
        assert (await db.execute("select 1")).rows == [(1,)]  # the worker carries on
    finally:
        await db.close()


async def test_one_process_per_database_connections_in_parallel(tmp_path):
    """Every connection of a database is in one process (one thread
    each), and a writer waiting for a reader's lock does not block the
    reader: the reader's transaction ends while the commit waits."""
    backend = SqliteNodeWorker(tmp_path / "p.db")
    db = Database(backend)
    try:
        await db.execute_write("create table t (x)")
        async with db.transaction("read") as tx:
            await tx.execute("select count(*) from t")  # SHARED held
            task = asyncio.create_task(db.execute_write("insert into t values (1)"))
            await asyncio.sleep(0.1)
            assert not task.done()  # the commit waits for the reader
        await asyncio.wait_for(task, 5)
        pids = {e.conn.pid for e in db._pool._idle_read}  # type: ignore[union-attr]
        assert pids == {backend.process.pid}
        # two backends on one path share the process
        other_backend = SqliteNodeWorker(tmp_path / "p.db")
        other = Database(other_backend)
        try:
            assert (await other.execute("select x from t")).rows == [(1,)]
            assert other_backend.process.pid == backend.process.pid
        finally:
            await other.close()
    finally:
        await db.close()


async def test_end_of_input_rolls_back_and_exits(tmp_path):
    from dblayer.backends.sqlite_worker import WorkerConnection

    path = tmp_path / "eof.db"
    db = Database(SqliteNodeWorker(path))
    try:
        await db.execute_write("create table t (x)")
        async with db.transaction() as tx:
            await tx.execute("insert into t values (1)")
            conn = tx._conn
            assert isinstance(conn, WorkerConnection)
            proc = conn.process
            proc.close()  # the parent goes away: stdin EOF
            for _ in range(500):
                if proc.popen is not None and proc.popen.poll() is not None:
                    break
                await asyncio.sleep(0.01)
            assert proc.popen is not None and proc.popen.poll() == 0
            other = sqlite3.connect(path, timeout=0)
            other.execute("begin immediate")
            other.execute("rollback")
            other.close()
            raise asyncio.CancelledError
    except (asyncio.CancelledError, Exception):
        pass
    finally:
        await db.close()
    assert sqlite3.connect(path).execute("select count(*) from t").fetchone() == (0,)
    assert not (tmp_path / "eof.db-journal").exists()


async def test_start_up_time_is_reported(tmp_path):
    times = []
    for i in range(5):
        db = Database(SqliteNodeWorker(tmp_path / f"st{i}.db"))
        t0 = time.perf_counter()
        await db.execute("select 1")
        times.append(time.perf_counter() - t0)
        await db.close()
    times.sort()
    print(f"\n[nodeworker] open-to-first-result: median {times[2] * 1000:.1f} ms, max {times[-1] * 1000:.1f} ms")
    assert times[2] < 10.0


# -- differential: in-process sqlite3 vs the Node worker -------------------------------

# Where the WebAssembly build differs from CPython's on purpose, a case is
# left out of the shared list (see docs/worker-protocol.md, Implementations).
NODE_CASES = CASES


@pytest.mark.parametrize("codec", CODECS)
async def test_differential_against_in_process(tmp_path, codec):
    local = SqliteInProcess(tmp_path / "local.db")
    remote = SqliteNodeWorker(tmp_path / "remote.db", codec=codec)
    lc = await local.open(write=True)
    rc = await remote.open(write=True)
    try:
        for sql in SETUP:
            await lc.execute(sql, (), time_limit_ms=None, max_rows=None)
            await rc.execute(sql, (), time_limit_ms=None, max_rows=None)
        mismatches = []
        for sql, params, kw in NODE_CASES:
            kw = {"max_rows": None, **kw}
            a = await _run(lc, sql, params, kw)
            b = await _run(rc, sql, params, kw)
            if a != b:
                mismatches.append((sql, params, kw, a, b))
        assert not mismatches, "\n".join(map(repr, mismatches))
        for conn in (lc, rc):
            w = await conn.execute_many("insert into t (name) values (?)", [["m1"], ["m2"]])
            assert w.rowcount == 2
        for bad in ("select ?", "create table if not exists v (x)"):
            assert await _many(lc, bad) == await _many(rc, bad), bad
        for sql in (
            "create index ix on t (name)",
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


@pytest.mark.parametrize("codec", CODECS)
async def test_streams_match(tmp_path, codec):
    path = tmp_path / "s.db"
    setup = sqlite3.connect(path)
    setup.execute("create table t (x)")
    setup.executemany("insert into t values (?)", [[i] for i in range(25)])
    setup.commit()
    setup.close()
    for backend in (SqliteInProcess(path), SqliteNodeWorker(path, codec=codec)):
        conn = await backend.open(write=False)
        try:
            chunks = [c async for c in conn.stream("select x, x * 2 from t", (), chunk_rows=7)]
            assert [len(c) for c in chunks] == [7, 7, 7, 4]
            assert [tuple(r) for c in chunks for r in c][-1] == (24, 48)
            s = conn.stream("select x from t", (), chunk_rows=2)
            first = await s.__anext__()
            assert list(map(tuple, first)) == [(0,), (1,)]
            await s.aclose()
            assert (await conn.execute("select count(*) from t", (), time_limit_ms=None, max_rows=None)).rows == [(25,)]
        finally:
            await conn.close()
            await backend.close()


@pytest.mark.parametrize("codec", CODECS)
async def test_value_type_params_match_the_python_worker(tmp_path, codec):
    import datetime as dt
    import decimal
    import uuid

    from dblayer.backends.sqlite_worker import SqliteWorker

    values = [
        dt.datetime(2020, 1, 2, 3, 4, 5),
        dt.datetime(2020, 1, 2, 3, 4, 5, 123456, tzinfo=dt.timezone.utc),
        dt.date(2020, 1, 2),
        dt.time(1, 2, 3),
        decimal.Decimal("1.50"),
        uuid.UUID(int=5),
        dt.timedelta(days=1, seconds=2),
        (1, 2),
    ]
    out = {}
    for name, backend in (("py", SqliteWorker(tmp_path / "p.db", codec=codec)), ("node", SqliteNodeWorker(tmp_path / "n.db", codec=codec))):
        conn = await backend.open(write=False)
        try:
            res = []
            for v in values:
                try:
                    res.append((await conn.execute("select ?", [v], time_limit_ms=None, max_rows=None)).rows[0][0])
                except Exception as e:  # noqa: BLE001
                    res.append((type(e).__name__, str(e)))
            out[name] = res
        finally:
            await conn.close()
    assert out["py"] == out["node"]


def test_no_native_sqlite_in_the_worker():
    """The point of the exercise: SQLite runs as WebAssembly. The worker's
    package has no native addon, and the module it loads is wasm."""
    pkg = Path(find_script()).parent.parent / "node_modules"
    assert not list(pkg.rglob("*.node"))
    wasm = next(pkg.rglob("sqlite3.wasm"))
    assert wasm.read_bytes()[:4] == b"\x00asm"


# -- malformed input -------------------------------------------------------------------


def _raw_session(codec: str, payloads: list[bytes]) -> tuple[int, bytes]:
    p = subprocess.Popen([find_node(), find_script()], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    hello = json.dumps({"type": "hello", "protocol": 1, "codecs": [codec], "id": 1}).encode()
    data = struct.pack(">I", len(hello)) + hello
    for payload in payloads:
        data += struct.pack(">I", len(payload)) + payload
    try:
        _out, err = p.communicate(data, timeout=30)
    except subprocess.TimeoutExpired:
        p.kill()
        p.communicate()
        raise
    return p.returncode, err


@pytest.mark.parametrize("payload", MALFORMED_MSGPACK, ids=[str(i) for i in range(len(MALFORMED_MSGPACK))])
def test_malformed_msgpack_drops_the_connection_cleanly(payload):
    code, err = _raw_session("msgpack", [payload])
    assert code == 0, err.decode(errors="replace")[-2000:]


@pytest.mark.parametrize("payload", MALFORMED_JSON, ids=[str(i) for i in range(len(MALFORMED_JSON))])
def test_malformed_json_drops_the_connection_cleanly(payload):
    code, err = _raw_session("json", [payload])
    assert code == 0, err.decode(errors="replace")[-2000:]


@pytest.mark.parametrize("codec", ["json", "msgpack"])
def test_random_payloads_never_crash_the_worker(codec, tmp_path):
    """A small seeded fuzz of the decoders and handlers, as for the C
    worker: exit code 0 means the worker answered or dropped the
    connection."""
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
    for i in range(int(os.environ.get("DBLAYER_NODEWORKER_FUZZ", "20"))):
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


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="RLIMIT_AS is enforced only on Linux")
@pytest.mark.parametrize("gb", [1, 2, 4, 8, 16, 32])
def test_rlimit_as_against_v8(tmp_path, gb):
    """Why the Node worker does not set RLIMIT_AS: V8 reserves large
    virtual regions (a WebAssembly memory reserves its whole 32-bit range
    plus guard pages, per instance; every thread gets a heap reservation).
    Start the worker under an address-space limit and run one query: report
    which limits it survives."""
    import resource

    def limit() -> None:
        resource.setrlimit(resource.RLIMIT_AS, (gb << 30, gb << 30))

    p = subprocess.Popen([find_node(), find_script()], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, preexec_fn=limit)
    msgs = [
        {"type": "hello", "protocol": 1, "codecs": ["json"], "id": 1},
        {"type": "open", "path": str(tmp_path / "r.db"), "mode": "write", "id": 2},
        {"type": "execute", "sql": "select 40 + 2", "id": 3},
    ]
    data = b"".join(struct.pack(">I", len(b)) + b for b in (json.dumps(m).encode() for m in msgs))
    out, err = p.communicate(data, timeout=60)
    ok = b"42" in out
    print(f"\n[nodeworker] RLIMIT_AS {gb} GB: {'query ran' if ok else 'failed'} (exit {p.returncode}); "
          f"{err.decode(errors='replace').strip().splitlines()[-1:] if not ok else ''}")


async def test_errors_name_the_query(tmp_path):
    db = Database(SqliteNodeWorker(tmp_path / "e.db"))
    try:
        with pytest.raises(QueryError, match="no such table"):
            await db.execute("select * from missing")
    finally:
        await db.close()
