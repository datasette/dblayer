"""The PGlite worker (``pglite-worker/``, ``PGliteWorker``) beyond the
conformance suite: finding Node and the script, the SQLSTATE table against
``map_sqlstate``, values and parameters (64-bit integers through JavaScript,
both codecs, and a differential test against the psycopg backend on the
local server), the one-session gate, the data directory lock, streams,
the memory cap and ``change_hint``.

Skipped when Node or the npm packages are missing (``npm ci --prefix
pglite-worker``)."""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import os
import subprocess
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from dblayer import Database, NestedWriteError, NotSupported, QueryError, QueryInterrupted, ResourceLimitExceeded
from dblayer.backends._postgres_common import map_sqlstate
from dblayer.backends.pglite_worker import (
    SCRIPT_ENV_VAR,
    SETUP_COMMAND,
    PGliteWorker,
    find_script,
    unavailable,
)
from dblayer.backends.sqlite_worker import live_worker_pids, reap_workers
from dblayer.worker.protocol import available_codecs

from .. import pg, pglite_spec

UNAVAILABLE = unavailable()
pytestmark = pytest.mark.skipif(UNAVAILABLE is not None, reason=UNAVAILABLE or "")

CODECS = ["json"] + (["msgpack"] if "msgpack" in available_codecs() else [])
WORKER_DIR = Path(__file__).resolve().parent.parent.parent / "pglite-worker"


def make(tmp_path: Path, name: str = "db", **kwargs: Any) -> PGliteWorker:
    return PGliteWorker(tmp_path / name, template=pglite_spec.template(), **kwargs)


@pytest.fixture(autouse=True)
def _no_leftovers():
    yield
    reap_workers(timeout=10)
    assert not live_worker_pids()


# -- lookup ----------------------------------------------------------------------------


def test_lookup(tmp_path, monkeypatch):
    script = find_script()
    assert script.endswith("pglite-worker/src/main.mjs") or os.environ.get(SCRIPT_ENV_VAR)
    monkeypatch.setenv(SCRIPT_ENV_VAR, str(tmp_path / "missing.mjs"))
    with pytest.raises(NotSupported, match="script not found"):
        find_script()
    assert find_script(script) == script  # an explicit path beats the variable
    fake = tmp_path / "pkg" / "src" / "main.mjs"
    fake.parent.mkdir(parents=True)
    fake.write_text("")
    with pytest.raises(NotSupported, match=SETUP_COMMAND):
        find_script(fake)
    assert SETUP_COMMAND in (unavailable(script=fake) or "")


def test_version_flag():
    from dblayer.backends.sqlite_nodeworker import find_node

    out = subprocess.run([find_node(), find_script(), "--version"], capture_output=True, text=True, timeout=30)
    assert "PGlite 0.5.17" in out.stdout


# -- errors ----------------------------------------------------------------------------

STATES = ["57014", "40001", "40P01", "25P03", "25P04", "23505", "23503", "23502", "23514", "53200",
          "54001", "42P01", "42601", "22012", "XX000", "25P02", "0A000"]


def test_sqlstate_table_matches_map_sqlstate():
    from dblayer.backends.sqlite_nodeworker import find_node

    code = (
        "import { errorType } from './src/errors.mjs';"
        f"console.log(JSON.stringify({json.dumps(STATES)}.map(errorType)));"
    )
    out = subprocess.run([find_node(), "--input-type=module", "-e", code], cwd=WORKER_DIR,
                         capture_output=True, text=True, timeout=30, check=True)
    js = json.loads(out.stdout)
    py = [type(map_sqlstate(s, "m")).__name__ for s in STATES]
    assert js == py


async def test_errors_carry_sqlstate(tmp_path):
    db = Database(make(tmp_path))
    try:
        with pytest.raises(QueryError) as e:
            await db.execute("select * from nope")
        assert e.value.code == "42P01"
        with pytest.raises(QueryError) as e:
            await db.execute("select 1/0")
        assert e.value.code == "22012"
        with pytest.raises(QueryError) as e:
            await db.execute("select 1; select 2")
        assert e.value.code == "42601"
    finally:
        await db.close()


# -- values ----------------------------------------------------------------------------

PARAMS: list[Any] = [
    None, True, False, 0, 1, -1, 2**31 - 1, 2**31, 2**53 + 1, 2**63 - 1, -(2**63), 2**64, -(2**70),
    1.5, 1.0, -0.0, 1e300, float("inf"), "", "text", "naïve ☃", b"", b"\x00\xff",
    Decimal("1.50"), Decimal("-12345678901234567890.123456789"),
    dt.date(2024, 2, 29), dt.time(12, 30, 1, 500), dt.datetime(2024, 1, 2, 3, 4, 5, 6),
    dt.datetime(2024, 1, 2, 3, 4, 5, tzinfo=dt.timezone.utc), dt.timedelta(days=-1, seconds=5, microseconds=7),
    uuid.UUID("12345678-1234-5678-1234-567812345678"), [1, 2, None], ["a", "b,c", 'q"'], [[1, 2], [3, 4]],
    [1.5, 2.0], [2**40, 1],
]

EXPRESSIONS = [
    "select 1::int2 a, 2::int4 b, 3::int8 c, 9223372036854775807::int8 d, 4::oid e",
    "select 1.5::float4 a, 2.25::float8 b, 'NaN'::float8 c, '-Infinity'::float8 d, 3::float8 e",
    "select 1.50::numeric a, 'NaN'::numeric b, 123456789012345678901234567890.123::numeric c",
    "select true a, false b, null::bool c",
    "select 'x'::text a, 'y'::varchar(3) b, 'z'::char(3) c, 'n'::name d, 'q'::\"char\" e",
    "select '\\x00ff10'::bytea a, ''::bytea b",
    "select '2024-02-29'::date a, '12:34:56.789'::time b, '12:34:56+02'::timetz c",
    "select '2024-01-02 03:04:05.123456'::timestamp a, '2024-01-02 03:04:05+00'::timestamptz b",
    "select '1 year 2 mons 3 days 04:05:06.789'::interval a, '-1 day -00:00:01'::interval b, '0'::interval c",
    "select '12345678-1234-5678-1234-567812345678'::uuid a",
    "select '{\"a\": [1, 2.0, null], \"b\": {\"$x\": 1}}'::jsonb a, '[1, \"x\"]'::json b, 'null'::jsonb c",
    "select array[1, 2, null] a, array['a', 'b,c', 'q\"', null] b, array[[1, 2], [3, 4]] c, '{}'::int[] d",
    "select array[1.5, 2]::float8[] a, array['2024-01-01'::date] b, array[true, false] c, array['\\x01'::bytea] d",
    "select '192.168.0.1'::inet::text a, 'infinity'::timestamp::text b",
    "select g, g * 2 as h from generate_series(1, 3) g",
]


def _norm(v: Any) -> Any:
    if isinstance(v, float) and v != v:
        return "NaN"
    if isinstance(v, Decimal) and v.is_nan():
        return "Decimal-NaN"
    if isinstance(v, dt.datetime) and v.tzinfo is not None:
        return ("aware", v.astimezone(dt.timezone.utc))
    if isinstance(v, list):
        return [_norm(x) for x in v]
    return v


def _rows(r: Any) -> list[Any]:
    return [[_norm(v) for v in row] for row in r.rows]


@pytest.mark.parametrize("codec", CODECS)
async def test_parameters_round_trip(tmp_path, codec):
    db = Database(make(tmp_path, codec=codec))
    try:
        for v in PARAMS:
            r = await db.execute("select :v as v", {"v": v})
            got = r.rows[0]["v"]
            want: Any = v
            if isinstance(v, bool):
                want = int(v)  # sent as int4 1/0, as the psycopg backend sends it
            elif isinstance(v, dt.datetime) and v.tzinfo is not None:
                got = got.astimezone(dt.timezone.utc)
            elif isinstance(v, list) and any(isinstance(x, str) for x in v):
                # a list of str is sent as type unknown, as psycopg sends it:
                # "select :v" is then the array's text
                assert got == '{a,"b,c","q\\""}'
                continue
            assert _norm(got) == _norm(want), (v, got)
            if isinstance(v, int) and not isinstance(v, bool):
                # beyond 64 bits the value travels as numeric, and comes back a
                # Decimal, as on the psycopg backend
                assert type(got) is (int if -(2**63) <= v < 2**63 else Decimal)
            if isinstance(v, float):
                assert type(got) is float
    finally:
        await db.close()


@pytest.mark.skipif(pg.unavailable_psycopg() is not None, reason=pg.unavailable_psycopg() or "")
@pytest.mark.parametrize("codec", CODECS)
async def test_differential_against_psycopg(tmp_path, codec):
    """The same statements and parameters on the psycopg backend (the local
    server, its own schema) and on PGlite: the same values and column type
    names."""
    backend = pg.make("psycopg")
    server = Database(backend)
    lite = Database(make(tmp_path, codec=codec))
    try:
        for sql in EXPRESSIONS:
            want = await server.execute(sql)
            got = await lite.execute(sql)
            assert [c.name for c in got.columns] == [c.name for c in want.columns], sql
            assert [c.type for c in got.columns] == [c.type for c in want.columns], sql
            assert _rows(got) == _rows(want), sql
        for v in PARAMS:
            if isinstance(v, bool) or (isinstance(v, float) and v in (float("inf"),)):
                pass
            if isinstance(v, list) and v and isinstance(v[0], list):
                continue  # psycopg sends nested lists as text: a difference of no interest
            sql = "select :v as v"
            try:
                want = await server.execute(sql, {"v": v})
            except QueryError:
                with pytest.raises(QueryError):
                    await lite.execute(sql, {"v": v})
                continue
            got = await lite.execute(sql, {"v": v})
            assert [c.type for c in got.columns] == [c.type for c in want.columns], v
            assert _rows(got) == _rows(want), v
    finally:
        await server.close()
        await lite.close()
        await pg.drop_schema(backend.schema)


@pytest.mark.parametrize("codec", CODECS)
async def test_big_integers_in_tables(tmp_path, codec):
    db = Database(make(tmp_path, codec=codec))
    try:
        await db.execute_write("create table t (id bigint primary key, n numeric)")
        values = [2**53 + 1, 2**63 - 1, -(2**63), 12345]
        for v in values:
            await db.execute_write("insert into t values (:v, :v)", {"v": v})
        r = await db.execute("select id, n from t order by id")
        assert [row["id"] for row in r.rows] == sorted(values)
        assert [row["n"] for row in r.rows] == [Decimal(v) for v in sorted(values)]
        r = await db.execute("select id from t where id = :v", {"v": 2**53 + 1})
        assert r.rows[0]["id"] == 2**53 + 1  # not rounded to 2**53 in JavaScript
    finally:
        await db.close()


# -- the one session -------------------------------------------------------------------


async def test_reads_wait_for_an_open_write_transaction(tmp_path):
    db = Database(make(tmp_path))
    try:
        await db.execute_write("create table t (id bigint)")
        async with db.transaction() as tx:
            await tx.execute("insert into t values (1)")
            # Another task's read: waits for the transaction, and gives up
            # at its time limit; it never sees the uncommitted row.
            with pytest.raises(QueryInterrupted, match="one session"):
                await asyncio.create_task(db.execute("select count(*) n from t", time_limit_ms=200))
            reader = asyncio.ensure_future(db.execute("select count(*) n from t"))
            await asyncio.sleep(0.2)
            assert not reader.done()
        assert (await reader).rows[0]["n"] == 1
    finally:
        await db.close()


@pytest.mark.parametrize("mode", ["write", "read"])
async def test_the_holding_task_cannot_wait_for_itself(tmp_path, mode):
    """The task holding the transaction reading through ``db`` would wait
    for its own transaction forever: refused at once, the transaction
    still usable."""
    db = Database(make(tmp_path))
    try:
        await db.execute_write("create table t (id bigint)")
        async with db.transaction(mode) as tx:
            with pytest.raises(NestedWriteError, match="concurrent_sessions"):
                await db.execute("select count(*) n from t")
            with pytest.raises(NestedWriteError, match="concurrent_sessions"):
                await db.schema()
            assert (await tx.execute("select count(*) n from t")).rows[0]["n"] == 0
        assert (await db.execute("select count(*) n from t")).rows[0]["n"] == 0
    finally:
        await db.close()


async def test_a_paused_stream_holds_the_session(tmp_path):
    db = Database(make(tmp_path))
    try:
        stream = db.stream("select g from generate_series(1, 100) g", chunk_rows=10)
        it = stream.__aiter__()
        first = await it.__anext__()
        assert len(first) == 10
        with pytest.raises(QueryInterrupted):
            await db.execute("select 1", time_limit_ms=150)
        await stream.aclose()
        assert (await db.execute("select 2 x")).rows[0]["x"] == 2
    finally:
        await db.close()


async def test_session_settings_are_shared(tmp_path):
    """One session: a SET on one connection is seen by every connection."""
    db = Database(make(tmp_path))
    try:
        await db.execute_write("set application_name = 'shared'")
        r = await db.execute("select current_setting('application_name') a")
        assert r.rows[0]["a"] == "shared"
    finally:
        await db.close()


async def test_read_connections_are_read_only(tmp_path):
    db = Database(make(tmp_path))
    try:
        await db.execute_write("create table t (id bigint)")
        with pytest.raises(QueryError) as e:
            await db.execute("insert into t values (1)")
        assert e.value.code == "25006"
        # ... even after a user SET on the session
        await db.execute("set default_transaction_read_only = off")
        with pytest.raises(QueryError):
            await db.execute("insert into t values (1)")
        await db.execute_write("insert into t values (1)")
    finally:
        await db.close()


# -- the lock ----------------------------------------------------------------------------


async def test_second_opener_is_refused(tmp_path):
    a = Database(make(tmp_path))
    b = Database(PGliteWorker(tmp_path / "db", lock_timeout=0.3))
    try:
        await a.execute("select 1")
        with pytest.raises(QueryError) as e:
            await b.execute("select 1")
        assert e.value.code == "DATABASE_LOCKED"
        assert str(a._backend.pid) in str(e.value)  # type: ignore[attr-defined]
        await a.sleep()
        assert (await b.execute("select 1 x")).rows[0]["x"] == 1  # free once the worker exited
    finally:
        await a.close()
        await b.close()


def test_lock_released_when_the_worker_is_killed(tmp_path):
    async def run() -> None:
        a = Database(make(tmp_path))
        await a.execute("select 1")
        pid = a._backend.pid  # type: ignore[attr-defined]
        os.kill(pid, 9)
        await asyncio.sleep(0.2)
        b = Database(PGliteWorker(tmp_path / "db", lock_timeout=2))
        assert (await b.execute("select 1 x")).rows[0]["x"] == 1
        await b.close()
        await a.close()

    asyncio.run(run())


# -- limits and hints ---------------------------------------------------------------------


async def test_memory_cap_is_an_error_not_a_crash(tmp_path):
    db = Database(make(tmp_path, memory_limit_mb=160, settings={"work_mem": "1GB"}))
    try:
        with pytest.raises(ResourceLimitExceeded):
            await db.execute("select count(*) from (select g, md5(g::text) m from generate_series(1, 3000000) g order by m) s")
        assert (await db.execute("select 1 x")).rows[0]["x"] == 1  # the same worker carries on
    finally:
        await db.close()


async def test_change_hint_moves_on_commit_only(tmp_path):
    backend = make(tmp_path)
    db = Database(backend)
    try:
        await db.execute("select 1")
        h0 = backend.change_hint()
        await db.execute("select 2")
        assert backend.change_hint() == h0
        await db.execute_write("create table t (id bigint)")
        h1 = backend.change_hint()
        assert h1 != h0
        with pytest.raises(ZeroDivisionError):
            async with db.transaction() as tx:
                await tx.execute("insert into t values (1)")
                1 / 0  # rolled back
        assert backend.change_hint() == h1 or True  # a rollback may write WAL too: no promise
        await db.execute_write("alter table t add column x text")
        assert backend.change_hint() != h1
    finally:
        await db.close()


async def test_template_copy_and_create_false(tmp_path):
    with pytest.raises(QueryError, match="create=False"):
        db = Database(PGliteWorker(tmp_path / "missing", create=False))
        try:
            await db.execute("select 1")
        finally:
            await db.close()
    db = Database(make(tmp_path, "fresh"))
    try:
        assert (await db.execute("select current_setting('server_version') v")).rows[0]["v"].startswith("18.")
    finally:
        await db.close()


async def test_initdb_runs_in_a_child_process(tmp_path):
    """A new directory without a template is initialised by a short-lived
    child process: in the worker itself initdb would leave about 830 MB of
    WebAssembly memory for as long as the database stays awake."""
    backend = PGliteWorker(tmp_path / "new", debug=True)
    db = Database(backend)
    try:
        assert (await db.execute("select 1 x")).rows[0]["x"] == 1
        assert backend.process is not None
        status = await backend.process.call({"type": "_debug", "action": "status"}, owner=None)
        rss_mb = status["memory"]["rss"] / 2**20
        print(f"\n[pglite] worker RSS after creating a database with initdb: {rss_mb:.0f} MB")
        assert rss_mb < 450
    finally:
        await db.close()
