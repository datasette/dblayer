"""Shared fixtures. The conformance suite is parametrised by backend through
the single ``backend_spec`` fixture below.

Adding a backend
----------------
Append one ``BackendSpec`` to ``BACKENDS``. ``make(tmp_path)`` must return a
fresh backend for an empty database each time it is called (a new file under
``tmp_path``, a new schema on a server, ...). Set ``available`` to a callable
that returns ``None`` when the backend can run here, or a skip reason.

Named operations, both paths
----------------------------
``ops_db`` is a Database parametrised over ``default`` (``native_operations
=False``: always the core's default implementations) and ``override``
(the backend's own ``run_operation`` overrides, or the test-only
``OverrideBackend`` for backends without any). Operation tests use it and
assert the same results on both.

Skipping by capability
----------------------
``@pytest.mark.requires("returning", "implicit_rowid")`` skips a test unless
every named flag is true on the backend; ``@pytest.mark.requires_not(...)``
skips if any named flag is true. Unknown flag names fail loudly.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path

import pytest

from dblayer import Database
from dblayer.backends.base import Backend, Capabilities
from dblayer.backends.sqlite import SqliteInProcess

from . import pg
from .override_backend import OverrideBackend


@dataclass(frozen=True)
class BackendSpec:
    id: str
    make: Callable[[Path], Backend]
    available: Callable[[], str | None] = lambda: None
    native_operations: bool = False
    """True if the backend's connections implement ``run_operation``
    overrides. If false, the "override" path of ``ops_db`` wraps the backend
    in the test-only ``OverrideBackend``."""
    cleanup: Callable[[Backend], Awaitable[None]] | None = None
    """Called at teardown for every backend ``make`` returned, after its
    databases were closed (drops a server-side schema, ...)."""


_counter = 0


def _sqlite_file(tmp_path: Path) -> Backend:
    global _counter
    _counter += 1
    return SqliteInProcess(tmp_path / f"db{_counter}.db")


def _sqlite_memory(tmp_path: Path) -> Backend:
    return SqliteInProcess.memory()


def _duckdb_available() -> str | None:
    try:
        import duckdb  # noqa: F401
    except ImportError:
        return "duckdb is not installed (uv run --extra duckdb pytest)"
    return None


def _duckdb_file(tmp_path: Path) -> Backend:
    from dblayer.backends.duckdb import DuckDB

    global _counter
    _counter += 1
    import os

    # DBLAYER_DUCKDB_WRITE_QUEUE=0 runs the suite with concurrent writers
    # (experimental). Two tests then fail by design: autocommit updates of
    # one hot row conflict on DuckDB where PostgreSQL waits for the lock.
    return DuckDB(
        tmp_path / f"db{_counter}.duckdb",
        write_queue=os.environ.get("DBLAYER_DUCKDB_WRITE_QUEUE", "1") != "0",
    )


def _postgres(tmp_path: Path) -> Backend:
    return pg.make()


async def _postgres_cleanup(backend: Backend) -> None:
    await pg.drop_schema(backend.schema)  # type: ignore[attr-defined]


BACKENDS: list[BackendSpec] = [
    BackendSpec("sqlite", _sqlite_file),
    BackendSpec("sqlite-memory", _sqlite_memory),
    BackendSpec("duckdb", _duckdb_file, available=_duckdb_available, native_operations=True),
    BackendSpec(
        "postgres",
        _postgres,
        available=pg.unavailable,
        native_operations=True,
        cleanup=_postgres_cleanup,
    ),
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
async def make_backend(
    backend_spec: BackendSpec, tmp_path: Path, request: pytest.FixtureRequest
) -> AsyncIterator[Callable[[], Backend]]:
    """Factory for fresh backends of the current kind. Applies the
    capability marks using the first backend it builds, and runs the spec's
    ``cleanup`` for each at teardown."""
    checked = False
    made: list[Backend] = []

    def make() -> Backend:
        nonlocal checked
        b = backend_spec.make(tmp_path)
        made.append(b)
        if not checked:
            checked = True
            _check_marks(request, b.capabilities)
        return b

    yield make
    if backend_spec.cleanup is not None:
        for b in made:
            await backend_spec.cleanup(b)


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


class _OpsDatabase(Database):
    """``Database`` uses ``__slots__``; this subclass can carry ``op_path``."""

    op_path: str


@pytest.fixture(params=["default", "override"])
async def make_ops_db(
    request: pytest.FixtureRequest,
    backend_spec: BackendSpec,
    make_backend: Callable[[], Backend],
) -> AsyncIterator[Callable[..., Database]]:
    made: list[Database] = []
    path = request.param

    def make(**kwargs: object) -> Database:
        backend = make_backend()
        if path == "default":
            db = _OpsDatabase(backend, native_operations=False, **kwargs)  # type: ignore[arg-type]
        else:
            if not backend_spec.native_operations:
                backend = OverrideBackend(backend)  # type: ignore[assignment]
            db = _OpsDatabase(backend, native_operations=True, **kwargs)  # type: ignore[arg-type]
        db.op_path = path
        made.append(db)
        return db

    yield make
    for db in made:
        await db.close()


@pytest.fixture
async def ops_db(make_ops_db: Callable[..., Database]) -> Database:
    return make_ops_db(name="ops")
