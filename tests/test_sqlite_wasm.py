"""``SqliteWasm`` (SQLite in WebAssembly, in-process) beyond the conformance
suite: finding the module, what it refuses, both codecs, the C worker's
differential cases against in-process SQLite, streams, transaction
deadlines with no thread inside the instance, and instance start-up.

Skipped when the module is not built (``make -C cworker wasm``) or the
``wasmtime`` package is missing (the ``wasm`` extra)."""

from __future__ import annotations

import asyncio
import time

import pytest

from dblayer import Database, NotSupported, TransactionExpired
from dblayer.backends.sqlite import SqliteInProcess
from dblayer.backends.sqlite_wasm import (
    BUILD_COMMAND,
    ENV_VAR,
    find_module,
    module_unavailable,
    runtime_stats,
)
from dblayer.worker.protocol import available_codecs

from .worker.test_cworker import CASES, SETUP, _many, _run

UNAVAILABLE = module_unavailable()
pytestmark = pytest.mark.skipif(UNAVAILABLE is not None, reason=UNAVAILABLE or "")

CODECS = ["json"] + (["msgpack"] if "msgpack" in available_codecs() else [])


def _wasm(path, **kwargs):
    from dblayer.backends.sqlite_wasm import SqliteWasm

    return SqliteWasm(path, **kwargs)


def test_module_lookup_order(tmp_path, monkeypatch):
    real = find_module()
    monkeypatch.delenv(ENV_VAR, raising=False)
    fake = tmp_path / "nope.wasm"
    monkeypatch.setenv(ENV_VAR, str(fake))
    with pytest.raises(NotSupported) as e:
        find_module()
    assert BUILD_COMMAND in str(e.value) and ENV_VAR in str(e.value)
    assert find_module(real) == real
    with pytest.raises(NotSupported, match="not built"):
        _wasm(tmp_path / "x.db", module=fake)


def test_refusals(tmp_path):
    with pytest.raises(NotSupported, match="init hooks"):
        _wasm(tmp_path / "x.db", init=["a:b"])
    with pytest.raises(NotSupported, match="dedicated"):
        _wasm(tmp_path / "x.db", placement="shared")
    with pytest.raises(ValueError):
        _wasm(tmp_path / "x.db", memory_limit_mb=1)


def test_capabilities(tmp_path):
    caps = _wasm(tmp_path / "x.db").capabilities
    local = SqliteInProcess(tmp_path / "x.db").capabilities
    assert caps.hard_kill and caps.memory_limit
    assert {k: v for k, v in caps.as_dict().items() if k not in ("hard_kill", "memory_limit")} == {
        k: v for k, v in local.as_dict().items() if k not in ("hard_kill", "memory_limit")
    }


@pytest.mark.parametrize("codec", CODECS)
async def test_differential_against_in_process(tmp_path, codec):
    """The C worker's awkward cases, on the same C code compiled to wasm32:
    results, errors, codes and messages must equal in-process SQLite's."""
    local = SqliteInProcess(tmp_path / "local.db")
    remote = _wasm(tmp_path / "remote.db", codec=codec)
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
            "create virtual table rt using rtree(id, x0, x1)",
        ):
            await lc.execute(sql, (), time_limit_ms=None, max_rows=None)
            await rc.execute(sql, (), time_limit_ms=None, max_rows=None)
        assert await lc.schema() == await rc.schema()
        assert await lc.schema_version() == await rc.schema_version()
        # Floating point and the math functions, computed inside wasm32.
        for sql in (
            "select sqrt(2), exp(1), ln(10), pow(2, 0.5), sin(1), atan2(1, 2), 1.0 / 3, 0.1 + 0.2",
            "select printf('%.20f %e %g', 1.0 / 3, 1e-300, 123456789.0), round(2.5), round(-2.5), 1e308 * 10",
            "select cast('9223372036854775808' as integer), cast(1e19 as integer), -9223372036854775808 / -1",
            "select percentile(value, 50), median(value) from json_each('[1, 2, 3, 10]')",
            "select json_object('a', 1.5, 'b', x'00'), jsonb('[1]'), unhex('ff00'), format('%q', 'it''s')",
        ):
            assert await _run(lc, sql, None, {"max_rows": None}) == await _run(rc, sql, None, {"max_rows": None}), sql
    finally:
        await lc.close()
        await rc.close()
        await local.close()


async def test_streams(tmp_path):
    db = Database(_wasm(tmp_path / "s.db"), name="s")
    try:
        await db.execute_write("create table t (x)")
        await db.execute_write("insert into t with r(i) as (select 0 union all select i + 1 from r where i < 24) select i from r")
        chunks = [c async for c in db.stream("select x, x * 2 as y from t", chunk_rows=7)]
        assert [len(c) for c in chunks] == [7, 7, 7, 4]
    finally:
        await db.close()
    backend = _wasm(tmp_path / "s.db")
    conn = await backend.open(write=False)
    try:
        s = conn.stream("select x from t", (), chunk_rows=2)
        first = await s.__anext__()
        assert list(map(tuple, first)) == [(0,), (1,)]
        await s.aclose()  # abandoned: the instance closes its cursor
        assert (await conn.execute("select count(*) from t", (), time_limit_ms=None, max_rows=None)).rows == [(25,)]
    finally:
        await conn.close()


async def test_transaction_deadline_rolls_back_an_idle_transaction(tmp_path):
    """There is no thread inside an instance: the host pokes it when a
    deadline passes, and it rolls back and sends its ``expired`` notice."""
    db = Database(_wasm(tmp_path / "d.db"), name="d")
    try:
        await db.execute_write("create table t (x)")
        with pytest.raises(TransactionExpired):
            async with db.transaction(deadline_ms=100) as tx:
                await tx.execute("insert into t values (1)")
                await asyncio.sleep(0.4)
                await tx.execute("insert into t values (2)")
        assert (await db.execute("select count(*) from t")).rows == [(0,)]
        # The write lock was released at the deadline: writing goes on.
        await db.execute_write("insert into t values (3)")
    finally:
        await db.close()


async def test_instances_start_fast_and_the_module_is_compiled_once(tmp_path):
    db = Database(_wasm(tmp_path / "w.db"), name="w")
    try:
        await db.execute("select 1")  # compile (or load the cached compilation)
    finally:
        await db.close()
    stats = runtime_stats()
    times = []
    for i in range(20):
        d = Database(_wasm(tmp_path / f"n{i}.db"), name=f"n{i}")
        t0 = time.perf_counter()
        assert (await d.execute("select 1")).rows == [(1,)]
        times.append(time.perf_counter() - t0)
        await d.close()
    times.sort()
    print(
        f"\n[wasm] module {stats['module_source']} in {stats['compile_s'] * 1000:.0f} ms; "
        f"new database to first result: median {times[10] * 1000:.2f} ms, max {times[-1] * 1000:.2f} ms"
    )
    assert runtime_stats()["instances_created"] >= stats["instances_created"] + 20
    assert times[10] < 1.0
