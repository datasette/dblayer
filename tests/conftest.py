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

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path

import pytest

from dblayer import Database
from dblayer.backends.base import Backend, Capabilities
from dblayer.backends.sqlite import SqliteInProcess

from . import clickhouse as ch
from . import pg, pglite_spec
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


def _sqlite_apsw(tmp_path: Path) -> Backend:
    from dblayer.backends.sqlite_apsw import SqliteApsw

    global _counter
    _counter += 1
    import os

    # DBLAYER_APSW_INLINE=1 runs the suite with inline_reads on.
    inline = os.environ.get("DBLAYER_APSW_INLINE") == "1"
    return SqliteApsw(tmp_path / f"db{_counter}.db", inline_reads=inline)  # type: ignore[return-value]


def _apsw_available() -> str | None:
    from dblayer.backends.sqlite_apsw import apsw_unavailable

    return apsw_unavailable()


def _sqlite_worker(codec: str) -> Callable[[Path], Backend]:
    def make(tmp_path: Path) -> Backend:
        from dblayer.backends.sqlite_worker import SqliteWorker

        global _counter
        _counter += 1
        return SqliteWorker(tmp_path / f"db{_counter}.db", codec=codec)  # type: ignore[return-value]

    return make


def _sqlite_cworker(codec: str | None) -> Callable[[Path], Backend]:
    def make(tmp_path: Path) -> Backend:
        from dblayer.backends.sqlite_cworker import SqliteCWorker

        global _counter
        _counter += 1
        return SqliteCWorker(tmp_path / f"db{_counter}.db", codec=codec)  # type: ignore[return-value]

    return make


_cpool: list = []  # one CPool for the session: every sqlite-cpool test database shares its processes


def _sqlite_cpool(tmp_path: Path) -> Backend:
    from dblayer.backends.sqlite_cpool import CPool, SqliteCPool

    global _counter
    _counter += 1
    if not _cpool:
        _cpool.append(CPool(2, linger=0))
    return SqliteCPool(tmp_path / f"db{_counter}.db", pool=_cpool[0])  # type: ignore[return-value]


async def _cpool_cleanup(backend: Backend) -> None:
    backend.pool.close()  # type: ignore[attr-defined]
    await _reap_workers(backend)


def _cworker_available() -> str | None:
    from dblayer.backends.sqlite_cworker import binary_unavailable

    return binary_unavailable()


def _msgpack_available() -> str | None:
    try:
        import msgpack  # noqa: F401
    except ImportError:
        return "msgpack is not installed (uv run --extra msgpack pytest)"
    return None


async def _reap_workers(backend: Backend) -> None:
    from dblayer.backends.sqlite_worker import live_worker_pids, reap_workers

    if live_worker_pids():
        await asyncio.sleep(0)  # let closed workers see EOF
    reap_workers(timeout=5.0)


def _sqlite_wasm(tmp_path: Path) -> Backend:
    from dblayer.backends.sqlite_wasm import SqliteWasm

    global _counter
    _counter += 1
    return SqliteWasm(tmp_path / f"db{_counter}.db")  # type: ignore[return-value]


def _wasm_available() -> str | None:
    from dblayer.backends.sqlite_wasm import module_unavailable

    return module_unavailable()


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


def _postgres_psycopg(tmp_path: Path) -> Backend:
    import os

    # DBLAYER_PSYCOPG_PREPARE_THRESHOLD=0 runs the suite with server-side
    # prepared statements (psycopg's statement cache), off by default.
    threshold = os.environ.get("DBLAYER_PSYCOPG_PREPARE_THRESHOLD")
    return pg.make("psycopg", prepare_threshold=None if threshold is None else int(threshold))


def _duckdb_worker(codec: str) -> Callable[[Path], Backend]:
    def make(tmp_path: Path) -> Backend:
        from dblayer.backends.duckdb_worker import DuckDBWorker

        global _counter
        _counter += 1
        return DuckDBWorker(tmp_path / f"db{_counter}.duckdb", codec=codec)  # type: ignore[return-value]

    return make


def _duckdb_cppworker(tmp_path: Path) -> Backend:
    from dblayer.backends.duckdb_cppworker import DuckDBCppWorker

    global _counter
    _counter += 1
    return DuckDBCppWorker(tmp_path / f"db{_counter}.duckdb")  # type: ignore[return-value]


def _duckdb_cppworker_available() -> str | None:
    from dblayer.backends.duckdb_cppworker import binary_unavailable

    return binary_unavailable()


def _duckdb_rsworker(tmp_path: Path) -> Backend:
    from dblayer.backends.duckdb_rsworker import DuckDBRustWorker

    global _counter
    _counter += 1
    return DuckDBRustWorker(tmp_path / f"db{_counter}.duckdb")  # type: ignore[return-value]


def _duckdb_rsworker_available() -> str | None:
    from dblayer.backends.duckdb_rsworker import binary_unavailable

    return binary_unavailable()


def _duckdb_msgpack_available() -> str | None:
    return _duckdb_available() or _msgpack_available()


def _sqlite_nodeworker(tmp_path: Path) -> Backend:
    from dblayer.backends.sqlite_nodeworker import SqliteNodeWorker

    global _counter
    _counter += 1
    return SqliteNodeWorker(tmp_path / f"db{_counter}.db")  # type: ignore[return-value]


def _nodeworker_available() -> str | None:
    from dblayer.backends.sqlite_nodeworker import unavailable

    return unavailable()


async def _postgres_cleanup(backend: Backend) -> None:
    await pg.drop_schema(backend.schema)  # type: ignore[attr-defined]


BACKENDS: list[BackendSpec] = [
    BackendSpec("sqlite", _sqlite_file),
    BackendSpec("sqlite-memory", _sqlite_memory),
    # In-process SQLite on apsw (experimental, extra "apsw").
    BackendSpec("sqlite-apsw", _sqlite_apsw, available=_apsw_available),
    # The worker backend runs the default named operations inside the
    # worker ("op" message): native_operations=True.
    BackendSpec("sqlite-worker", _sqlite_worker("json"), native_operations=True, cleanup=_reap_workers),
    BackendSpec(
        "sqlite-worker-msgpack",
        _sqlite_worker("msgpack"),
        available=_msgpack_available,
        native_operations=True,
        cleanup=_reap_workers,
    ),
    # The C worker (cworker/, experimental): MessagePack when installed,
    # else JSON; and JSON explicitly. Named operations decline in the
    # worker, so the "override" path is the core's default path.
    BackendSpec(
        "sqlite-cworker",
        _sqlite_cworker(None),
        available=_cworker_available,
        native_operations=True,
        cleanup=_reap_workers,
    ),
    BackendSpec(
        "sqlite-cworker-json",
        _sqlite_cworker("json"),
        available=_cworker_available,
        native_operations=True,
        cleanup=_reap_workers,
    ),
    # Multi-tenant C worker processes shared by every database (sqlite_cpool).
    BackendSpec("sqlite-cpool", _sqlite_cpool, available=_cworker_available, native_operations=True,
                cleanup=_cpool_cleanup),
    # SQLite compiled to WebAssembly, in-process under wasmtime
    # (experimental; `make -C cworker wasm`). Operations run in the parent.
    BackendSpec("sqlite-wasm", _sqlite_wasm, available=_wasm_available, native_operations=True),
    # SQLite compiled to WebAssembly in a Node.js process, one per database
    # (experimental; `npm ci --prefix nodeworker`). Operations run in the parent.
    BackendSpec("sqlite-nodeworker", _sqlite_nodeworker, available=_nodeworker_available,
                native_operations=True, cleanup=_reap_workers),
    BackendSpec("duckdb", _duckdb_file, available=_duckdb_available, native_operations=True),
    # DuckDB in a worker process the database owns; JSON codec. The
    # MessagePack variant runs with DBLAYER_DUCKDB_WORKER_MSGPACK=1.
    BackendSpec(
        "duckdb-worker",
        _duckdb_worker("json"),
        available=_duckdb_available,
        native_operations=True,
        cleanup=_reap_workers,
    ),
    # DuckDB in a C++ worker process (duckdb-cpp-worker/, experimental).
    BackendSpec("duckdb-cppworker", _duckdb_cppworker, available=_duckdb_cppworker_available,
                native_operations=True, cleanup=_reap_workers),
    # DuckDB in a Rust worker process (duckdb-rs-worker/, experimental).
    BackendSpec("duckdb-rsworker", _duckdb_rsworker, available=_duckdb_rsworker_available,
                native_operations=True, cleanup=_reap_workers),
    # PostgreSQL in WebAssembly (PGlite) in a Node.js process, one data
    # directory per database (experimental; `npm ci --prefix pglite-worker`).
    BackendSpec("pglite", pglite_spec.make, available=pglite_spec.available, native_operations=True,
                cleanup=_reap_workers),
    BackendSpec(
        "postgres",
        _postgres,
        available=pg.unavailable,
        native_operations=True,
        cleanup=_postgres_cleanup,
    ),
    BackendSpec(
        "postgres-psycopg",
        _postgres_psycopg,
        available=pg.unavailable_psycopg,
        native_operations=True,
        cleanup=_postgres_cleanup,
    ),
    # ClickHouse embedded in-process through chDB (experimental, extra
    # "clickhouse", Python <= 3.14): one ClickHouse database per test.
    BackendSpec("clickhouse", lambda tmp_path: ch.make(), available=ch.unavailable, native_operations=True,
                cleanup=ch.cleanup),
]


def pytest_terminal_summary(terminalreporter, exitstatus, config) -> None:  # type: ignore[no-untyped-def]
    """List the spec latency figures ``helpers.check_latency`` measured."""
    from .helpers import STRICT_TIMING, latencies

    if not latencies:
        return
    mode = "asserted" if STRICT_TIMING else "reported only; DBLAYER_STRICT_TIMING=1 asserts them"
    terminalreporter.section(f"spec latencies ({mode})")
    by_name: dict[str, list[float]] = {}
    bounds: dict[str, float] = {}
    for name, seconds, bound in latencies:
        by_name.setdefault(name, []).append(seconds)
        bounds[name] = bound
    for name, values in by_name.items():
        values.sort()
        over = sum(1 for v in values if v >= bounds[name])
        terminalreporter.write_line(
            f"{name}: n={len(values)} median {values[len(values) // 2] * 1000:.1f} ms, "
            f"max {values[-1] * 1000:.1f} ms, spec {bounds[name] * 1000:.0f} ms, over spec {over}"
        )


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
