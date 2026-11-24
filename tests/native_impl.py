"""Run the C worker's tests against both native SQLite workers.

``native_impl`` is a fixture parametrised over ``"c"`` (``cworker/``) and
``"rust"`` (``sqlite-rs-worker/``). It points ``$DBLAYER_CWORKER`` at the
chosen binary for the test, so everything that finds the C worker by the
default lookup (``SqliteCWorker``, ``CPool``, ``find_binary()``) runs that
implementation, and skips the test when it is not built. Import it into a
test module (``from ..native_impl import native_impl``), with
``autouse_native_impl`` for every test of the module."""

from __future__ import annotations

import pytest

IMPLEMENTATIONS = ["c", "rust"]


def binary(impl: str) -> tuple[str | None, str | None]:
    """(path, None) when ``impl``'s binary is built, else (None, reason)."""
    if impl == "c":
        from dblayer.backends import sqlite_cworker as mod
    else:
        from dblayer.backends import sqlite_rspool as mod  # type: ignore[no-redef]
    reason = mod.binary_unavailable()
    if reason is not None:
        return None, reason
    return mod.find_binary(), None


@pytest.fixture(params=IMPLEMENTATIONS)
def native_impl(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> str:
    path, reason = binary(request.param)
    if path is None:
        pytest.skip(reason or "")
    monkeypatch.setenv("DBLAYER_CWORKER", path)
    return request.param


@pytest.fixture(autouse=True)
def autouse_native_impl(native_impl: str) -> str:
    return native_impl
