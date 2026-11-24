"""Fault-injection tests for the worker backend. No worker process may
outlive the test that started it."""

from __future__ import annotations

import sys
from collections.abc import Callable
from typing import Any

import pytest

from dblayer.backends.sqlite_worker import SqliteWorker, live_worker_pids, reap_workers

LINUX = sys.platform.startswith("linux")
linux_only = pytest.mark.skipif(
    not LINUX,
    reason="RLIMIT_AS is enforced only on Linux (macOS accepts but ignores it); "
    "these memory-limit tests run in CI on Linux",
)


@pytest.fixture(autouse=True)
def no_orphan_workers():
    yield
    killed = reap_workers(timeout=5.0)
    assert not killed, f"worker processes still running after the test: {killed}"
    assert live_worker_pids() == []


@pytest.fixture(params=["python", "c", "cpool", "rspool", "node"],
                ids=["py-worker", "c-worker", "c-pool", "rust-pool", "node-worker"])
def make_worker(request: pytest.FixtureRequest):
    """``SqliteWorker``, the C worker (``SqliteCWorker``) or the
    multi-tenant C pool (``SqliteCPool``, one process per test); the C ones
    are skipped when the binary is not built."""
    if request.param == "python":
        yield SqliteWorker
        return
    if request.param == "node":  # SQLite-WASM in Node (sqlite_nodeworker)
        from dblayer.backends.sqlite_nodeworker import SqliteNodeWorker, unavailable

        if reason := unavailable():
            pytest.skip(reason)
        if request.node.name.startswith("test_rlimit_as"):
            pytest.skip("the Node worker does not use RLIMIT_AS (V8 reserves more address space than any useful "
                        "limit; its WebAssembly memory maximum caps SQLite instead)")
        if request.node.name.startswith("test_idle_readers_recover_a_hot_journal"):
            # a native writer in another process beside the worker: its
            # cross-process guard locks that writer out, so the test runs
            # the worker's own recovery path with the guard off
            request.getfixturevalue("monkeypatch").setenv("DBLAYER_NODEWORKER_GUARD", "0")

        def make_node(path: Any, **kwargs: Any) -> SqliteWorker:
            if kwargs.get("init"):
                pytest.skip("init hooks need the Python worker")
            return SqliteNodeWorker(path, **kwargs)

        yield make_node
        return
    from dblayer.backends.sqlite_cworker import SqliteCWorker, binary_unavailable

    if request.param == "rspool":  # the Rust port of the multi-tenant worker
        from dblayer.backends.sqlite_rspool import binary_unavailable as rs_unavailable

        reason = rs_unavailable()
    else:
        reason = binary_unavailable()
    if reason:
        pytest.skip(reason)
    pools: list[Any] = []

    def make(path: Any, **kwargs: Any) -> SqliteWorker:
        if kwargs.get("init"):
            pytest.skip("init hooks need the Python worker")
        if request.param == "c":
            return SqliteCWorker(path, **kwargs)
        from dblayer.backends.sqlite_cpool import CPool, SqliteCPool
        from dblayer.backends.sqlite_rspool import RustPool

        process_options = {k: kwargs.pop(k) for k in ("kill_grace_ms", "memory_limit_mb", "debug", "max_frame_bytes")
                           if k in kwargs}
        if process_options.get("memory_limit_mb"):  # the per-database heap limit is per process here
            process_options["heap_limit_mb"] = process_options["memory_limit_mb"]
        pool_class = RustPool if request.param == "rspool" else CPool
        pools.append(pool := pool_class(1, linger=0, **process_options))
        return SqliteCPool(path, pool=pool, **kwargs)  # type: ignore[return-value]

    yield make
    for pool in pools:
        pool.close()
