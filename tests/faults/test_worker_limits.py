"""Memory limits and frame caps on the worker backend.

``PRAGMA hard_heap_limit`` (SQLite's own allocations) works on every
platform. ``RLIMIT_AS`` (the whole worker process) is enforced only on
Linux; those tests skip elsewhere and run in CI."""

from __future__ import annotations

import gc
import time

import pytest

from dblayer import Database, ResourceLimitExceeded, WorkerCrashed
from dblayer.backends.sqlite_worker import WorkerConnection

from ..scale.harness import current_rss
from .conftest import LINUX, linux_only

# 30 rows of 100 MB each, concatenated: a 3 GB value if nothing stops it.
MULTI_GB = (
    "select length(group_concat(randomblob(100000000))) from "
    "(with recursive c(i) as (select 1 union all select i + 1 from c where i < 30) select i from c)"
)


def _conn(db: Database, write: bool = False) -> WorkerConnection:
    pool = db._pool
    assert pool is not None
    idle = pool._idle_write if write else pool._idle_read
    return idle[0].conn  # type: ignore[return-value]


async def test_multi_gigabyte_value_stopped_by_heap_limit_parent_memory_unchanged(tmp_path, make_worker):
    db = Database(make_worker(tmp_path / "m.db", memory_limit_mb=256))
    try:
        await db.execute("select 1")
        gc.collect()
        rss_before = current_rss()
        t0 = time.perf_counter()
        with pytest.raises(ResourceLimitExceeded):
            await db.execute(MULTI_GB)
        elapsed = time.perf_counter() - t0
        gc.collect()
        rss_after = current_rss()
        grew = None if rss_before is None or rss_after is None else rss_after - rss_before
        print(f"\n[worker] multi-GB value: ResourceLimitExceeded after {elapsed * 1000:.0f} ms; "
              f"parent RSS change {grew} bytes")
        if grew is not None:
            assert grew < 50 * 1024 * 1024
        assert (await db.execute("select 1")).rows == [(1,)]
    finally:
        await db.close()


async def test_huge_single_allocation_stopped_by_heap_limit(tmp_path, make_worker):
    db = Database(make_worker(tmp_path / "h.db", memory_limit_mb=128))
    try:
        with pytest.raises(ResourceLimitExceeded):
            await db.execute("select length(randomblob(500000000))")
        assert (await db.execute("select length(randomblob(1000))")).rows == [(1000,)]
    finally:
        await db.close()


async def test_max_result_bytes_stops_fetching_in_the_worker(tmp_path, make_worker):
    db = Database(make_worker(tmp_path / "b.db"))
    try:
        sql = "select randomblob(10000) from (with recursive c(i) as (select 1 union all select i + 1 from c where i < 10000) select i from c)"
        with pytest.raises(ResourceLimitExceeded, match="max_result_bytes"):
            await db.execute(sql, max_result_bytes=1_000_000)
        assert (await db.execute("select 1")).rows == [(1,)]
    finally:
        await db.close()


def test_memory_limit_capability_is_linux_only(tmp_path, make_worker):
    caps = make_worker(tmp_path / "c.db").capabilities
    assert caps.hard_kill
    assert caps.memory_limit is LINUX


@linux_only
async def test_rlimit_as_stops_python_side_allocation(tmp_path, make_worker):
    """Rows of 200 MB each pass SQLite's heap limit one at a time, but the
    Python copies pile up in the worker: RLIMIT_AS makes that a
    MemoryError there, reported as ResourceLimitExceeded; the worker exits
    and is replaced. The parent is untouched."""
    db = Database(make_worker(tmp_path / "r.db", memory_limit_mb=512))
    try:
        await db.execute("select 1")
        assert _conn(db).process.hello.get("rlimit_as") is True
        rss_before = current_rss()
        sql = "select randomblob(200000000) from (with recursive c(i) as (select 1 union all select i + 1 from c where i < 20) select i from c)"
        with pytest.raises((ResourceLimitExceeded, WorkerCrashed)) as e:
            await db.execute(sql)
        print(f"\n[worker] RLIMIT_AS: {type(e.value).__name__}: {e.value}")
        assert isinstance(e.value, ResourceLimitExceeded)
        rss_after = current_rss()
        if rss_before is not None and rss_after is not None:
            assert rss_after - rss_before < 50 * 1024 * 1024
        assert (await db.execute("select 2")).rows == [(2,)]
    finally:
        await db.close()


@linux_only
async def test_rlimit_as_in_init_hook_function(tmp_path, make_worker):
    """A plugin SQL function allocating without bound runs inside the
    worker, under its limit."""
    db = Database(
        make_worker(tmp_path / "i.db", memory_limit_mb=256, init=["tests.worker.hooks:hog"])
    )
    try:
        assert (await db.execute("select hog(10)")).rows == [(10,)]
        with pytest.raises(Exception) as e:
            await db.execute("select hog(4000)")
        print(f"\n[worker] init-hook function over RLIMIT_AS: {type(e.value).__name__}: {e.value}")
        assert (await db.execute("select 3")).rows == [(3,)]
    finally:
        await db.close()


# -- frames -------------------------------------------------------------------------


async def _debug(db: Database, action: str) -> None:
    await db.execute("select 1")
    conn = _conn(db)
    await conn.process.call({"type": "_debug", "action": action}, owner=conn, limit_ms=2000)


@pytest.mark.parametrize(
    "action,error",
    [("oversized_frame", ResourceLimitExceeded), ("truncated_frame", WorkerCrashed), ("garbage", WorkerCrashed)],
)
async def test_bad_frame_from_worker_drops_it_and_the_pool_replaces_it(tmp_path, action, error, make_worker):
    db = Database(make_worker(tmp_path / "f.db", debug=True))
    try:
        await db.execute("select 1")
        old = _conn(db).process
        with pytest.raises(error) as e:
            await _debug(db, action)
        print(f"\n[worker] {action}: {type(e.value).__name__}: {e.value}")
        assert not old.alive
        assert (await db.execute("select 7")).rows == [(7,)]
        assert _conn(db).pid != old.pid  # the connection got a new worker
    finally:
        await db.close()


async def test_reply_over_frame_cap_is_refused_by_the_worker(tmp_path, make_worker):
    db = Database(make_worker(tmp_path / "c.db", max_frame_bytes=1_000_000))
    try:
        await db.execute("select 1")
        pid = _conn(db).pid
        with pytest.raises(ResourceLimitExceeded, match="too large"):
            await db.execute("select randomblob(2000000)")
        assert (await db.execute("select 1")).rows == [(1,)]
        assert _conn(db).pid == pid  # soft: the worker refused to send it
    finally:
        await db.close()


async def test_request_over_frame_cap_is_refused_before_sending(tmp_path, make_worker):
    db = Database(make_worker(tmp_path / "q.db", max_frame_bytes=1_000_000))
    try:
        await db.execute_write("create table t (b blob)")
        with pytest.raises(ResourceLimitExceeded, match="too large"):
            await db.execute_write("insert into t values (:b)", {"b": b"x" * 2_000_000})
        await db.execute_write("insert into t values (:b)", {"b": b"x"})
        assert (await db.execute("select count(*) from t")).rows == [(1,)]
    finally:
        await db.close()
