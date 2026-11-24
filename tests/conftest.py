"""Shared fixtures. The conformance suite is parametrised by backend through
the single ``backend_spec`` fixture below.

Adding a backend
----------------
Append one ``BackendSpec`` to ``BACKENDS``. ``make(tmp_path)`` must return a
fresh backend for an empty database each time it is called (a new file under
``tmp_path``, a new schema on a server, ...). Set ``available`` to a callable
that returns ``None`` when the backend can run here, or a skip reason.

Skipping by capability
----------------------
``@pytest.mark.requires("returning", "implicit_rowid")`` skips a test unless
every named flag is true on the backend; ``@pytest.mark.requires_not(...)``
skips if any named flag is true. Unknown flag names fail loudly.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from pathlib import Path

import pytest

from dblayer import Database
from dblayer.backends.base import Backend, Capabilities
from dblayer.backends.sqlite import SqliteInProcess


@dataclass(frozen=True)
class BackendSpec:
    id: str
    make: Callable[[Path], Backend]
    available: Callable[[], str | None] = lambda: None


_counter = 0


def _sqlite_file(tmp_path: Path) -> Backend:
    global _counter
    _counter += 1
    return SqliteInProcess(tmp_path / f"db{_counter}.db")


def _sqlite_memory(tmp_path: Path) -> Backend:
    return SqliteInProcess.memory()


BACKENDS: list[BackendSpec] = [
    BackendSpec("sqlite", _sqlite_file),
    BackendSpec("sqlite-memory", _sqlite_memory),
]


def _check_marks(request: pytest.FixtureRequest, caps: Capabilities) -> None:
    for mark in request.node.iter_markers("requires"):
        missing = [f for f in mark.args if not caps.has(f)]
        if missing:
            pytest.skip(f"backend lacks capability: {', '.join(missing)}")
    for mark in request.node.iter_markers("requires_not"):
        present = [f for f in mark.args if caps.has(f)]
        if present:
            pytest.skip(f"backend has capability: {', '.join(present)}")


@pytest.fixture(params=BACKENDS, ids=lambda s: s.id)
def backend_spec(request: pytest.FixtureRequest) -> BackendSpec:
    spec: BackendSpec = request.param
    reason = spec.available()
    if reason:
        pytest.skip(reason)
    return spec


@pytest.fixture
def make_backend(
    backend_spec: BackendSpec, tmp_path: Path, request: pytest.FixtureRequest
) -> Callable[[], Backend]:
    """Factory for fresh backends of the current kind. Applies the
    capability marks using the first backend it builds."""
    checked = False

    def make() -> Backend:
        nonlocal checked
        b = backend_spec.make(tmp_path)
        if not checked:
            checked = True
            _check_marks(request, b.capabilities)
        return b

    return make


@pytest.fixture
def backend(make_backend: Callable[[], Backend]) -> Backend:
    return make_backend()


@pytest.fixture
async def make_db(make_backend: Callable[..., Backend]) -> AsyncIterator[Callable[..., Database]]:
    """Factory: ``make_db(**database_kwargs)`` -> a new Database on a fresh
    backend; all are closed at teardown."""
    made: list[Database] = []

    def make(**kwargs: object) -> Database:
        db = Database(make_backend(), **kwargs)  # type: ignore[arg-type]
        made.append(db)
        return db

    yield make
    for db in made:
        await db.close()


@pytest.fixture
async def db(make_db: Callable[..., Database]) -> Database:
    return make_db(name="test")
