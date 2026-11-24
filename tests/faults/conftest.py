"""Fault-injection tests for the worker backend. No worker process may
outlive the test that started it."""

from __future__ import annotations

import sys

import pytest

from dblayer.backends.sqlite_worker import live_worker_pids, reap_workers

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
