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


@pytest.fixture(params=["python", "c"], ids=["py-worker", "c-worker"])
def make_worker(request: pytest.FixtureRequest) -> Callable[..., SqliteWorker]:
    """``SqliteWorker`` or the C worker (``SqliteCWorker``, skipped when
    the binary is not built): the fault tests run against both."""
    if request.param == "python":
        return SqliteWorker
    from dblayer.backends.sqlite_cworker import SqliteCWorker, binary_unavailable

    reason = binary_unavailable()
    if reason:
        pytest.skip(reason)

    def make(path: Any, **kwargs: Any) -> SqliteWorker:
        if kwargs.get("init"):
            pytest.skip("init hooks need the Python worker")
        return SqliteCWorker(path, **kwargs)

    return make
