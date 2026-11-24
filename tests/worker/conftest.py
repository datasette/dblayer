"""Worker tests: no worker process may outlive the test that started it."""

from __future__ import annotations

import pytest

from dblayer.backends.sqlite_worker import live_worker_pids, reap_workers


@pytest.fixture(autouse=True)
def no_orphan_workers():
    yield
    # Closed workers exit on their own (stdin EOF); give them a moment.
    killed = reap_workers(timeout=5.0)
    assert not killed, f"worker processes still running after the test: {killed}"
    assert live_worker_pids() == []
