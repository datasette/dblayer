"""The demo databases: one declarative list, opened and seeded at startup.

Each ``DemoSpec`` says how to build its backend. Building it may fail (a
backend module not written yet, a driver not installed, a server not
reachable); the database is then still listed, marked unavailable with the
reason, and the app carries on.
"""

from __future__ import annotations

import asyncio
import urllib.parse
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path

from dblayer import Database
from dblayer.backends.base import Backend

from .config import POSTGRES_DEFAULT_DSN, POSTGRES_DSN_ENV, DemoConfig
from .seed import seed

__all__ = ["DemoSpec", "DemoDatabase", "Unavailable", "DEMO_SPECS", "open_all", "close_all"]

SEED_TIMEOUT_S = 60.0


@dataclass(frozen=True)
class DemoSpec:
    slug: str
    """URL segment: ``/<slug>``."""
    title: str
    backend: str
    """The dblayer backend class, for display."""
    make: Callable[[DemoConfig], Backend]
    """Build the backend from the run's settings. May raise."""
    check: Callable[[Database], Awaitable[None]] | None = None
    """Run after the first connection, before seeding: raise to refuse."""
    after: str | None = None
    """Open only once the spec with this slug is open (two backends sharing
    one PostgreSQL schema must not seed it at the same time)."""


@dataclass
class DemoDatabase:
    spec: DemoSpec
    db: Database | None = None
    error: str | None = None
    """Why the database is unavailable, or ``None``."""
    seeded: bool = False
    """True if this run wrote the demo data (it was not there yet)."""

    @property
    def available(self) -> bool:
        return self.db is not None


class Unavailable(Exception):
    """Raised by a spec's ``make`` or ``check``: the message is the whole
    reason shown on the homepage."""


# -- the backends ------------------------------------------------------------------


def _sqlite(config: DemoConfig) -> Backend:
    from dblayer.backends.sqlite import SqliteInProcess

    return SqliteInProcess(config.data_dir / "sqlite.db")


POSTGRES_DATABASE = "dblayer_test"
"""The only PostgreSQL database the demo will connect to."""
POSTGRES_SCHEMA = "dblayer_demo"
"""The only schema the demo creates or writes to."""


def _dsn_database(dsn: str) -> str | None:
    parts = urllib.parse.urlsplit(dsn)
    query = urllib.parse.parse_qs(parts.query)
    if "dbname" in query:
        return query["dbname"][0]
    return urllib.parse.unquote(parts.path.lstrip("/")) or None


POSTGRES_HINT = (
    f"Set the server with --postgres-dsn or ${POSTGRES_DSN_ENV}, naming the "
    f"{POSTGRES_DATABASE} database, e.g. postgresql://USER@localhost:5432/{POSTGRES_DATABASE} "
    f"(the default, {POSTGRES_DEFAULT_DSN}, connects as the current OS user)."
)


def check_postgres_dsn(dsn: str) -> None:
    """Refuse, before connecting, any DSN not naming ``dblayer_test``: the
    server may hold other, real databases."""
    if _dsn_database(dsn) != POSTGRES_DATABASE:
        raise Unavailable(
            f"the PostgreSQL DSN must name the {POSTGRES_DATABASE!r} database; refusing to "
            f"connect. {POSTGRES_HINT}"
        )


def postgres_backend(dsn: str, schema: str, application_name: str) -> Backend:
    check_postgres_dsn(dsn)
    from dblayer.backends.postgres import Postgres

    return Postgres(dsn, schema=schema, application_name=application_name, connect_timeout=5.0)


def _postgres(config: DemoConfig) -> Backend:
    return postgres_backend(config.postgres_dsn, POSTGRES_SCHEMA, "dblayer-demo")


async def check_postgres_connection(db: Database, schema: str) -> None:
    """The first query on a PostgreSQL database: confirm where it landed.
    A connection failure is reported with how to set the DSN."""
    try:
        r = await db.execute("select current_database(), current_schema()")
    except Exception as e:
        raise Unavailable(f"{_reason(e)}. {POSTGRES_HINT}") from e
    database, current = r.rows[0]
    if database != POSTGRES_DATABASE or current != schema:
        raise Unavailable(
            f"connected to {database}.{current}, expected {POSTGRES_DATABASE}.{schema}; refusing"
        )


async def _postgres_check(db: Database) -> None:
    await check_postgres_connection(db, POSTGRES_SCHEMA)


def postgres_psycopg_backend(dsn: str, schema: str, application_name: str) -> Backend:
    check_postgres_dsn(dsn)
    try:
        import psycopg  # noqa: F401
    except ImportError as e:
        raise Unavailable(f"psycopg is not installed (the psycopg extra): {e}") from e
    from dblayer.backends.postgres_psycopg import PostgresPsycopg

    return PostgresPsycopg(
        dsn, schema=schema, application_name=application_name, connect_timeout=5.0
    )


def _postgres_psycopg(config: DemoConfig) -> Backend:
    # The same schema as the asyncpg entry: one data set, two drivers.
    return postgres_psycopg_backend(config.postgres_dsn, POSTGRES_SCHEMA, "dblayer-demo-psycopg")


def _sqlite_worker(config: DemoConfig) -> Backend:
    # The worker backend (spec: "Backend: SQLite in a separate process") is
    # not written yet. This is the one place to change once it lands:
    # confirm the module path and constructor below.
    return sqlite_worker_backend(config.data_dir / "sqlite-worker.db")


def sqlite_worker_backend(path: Path) -> Backend:
    """Shared by the featured worker database and the fleet's."""
    try:
        from dblayer.backends.sqlite_worker import SqliteWorker  # type: ignore[import-not-found]
    except ImportError as e:
        raise Unavailable(f"the SqliteWorker backend is not available yet ({e})") from e
    return SqliteWorker(path)


def _sqlite_apsw(config: DemoConfig) -> Backend:
    # Experimental: in-process SQLite on apsw (extra "apsw"), with the user's
    # SQL locked down (untrusted) and cheap reads run without a thread hop.
    try:
        from dblayer.backends.sqlite_apsw import SqliteApsw, apsw_unavailable
    except ImportError as e:
        raise Unavailable(f"the SqliteApsw backend is not available here ({e})") from e
    reason = apsw_unavailable()
    if reason:
        raise Unavailable(reason)
    return SqliteApsw(config.data_dir / "sqlite-apsw.db", untrusted=True, inline_reads=True)


def _sqlite_cworker(config: DemoConfig) -> Backend:
    # Experimental: the same worker protocol, served by a C program.
    return sqlite_cworker_backend(config.data_dir / "sqlite-cworker.db")


def sqlite_cworker_backend(path: Path) -> Backend:
    """Shared by the featured C worker database and the fleet's. Raises
    ``Unavailable`` naming the build command until the binary is built."""
    try:
        from dblayer.backends.sqlite_cworker import SqliteCWorker, binary_unavailable
    except ImportError as e:
        raise Unavailable(f"the SqliteCWorker backend is not available here ({e})") from e
    reason = binary_unavailable()
    if reason:
        raise Unavailable(reason)
    return SqliteCWorker(path)


def _sqlite_cpool(config: DemoConfig) -> Backend:
    # Experimental: a thread per connection inside a shared C process.
    sqlite_cworker_backend(config.data_dir / "sqlite-cpool.db")  # Unavailable until built
    from dblayer.backends.sqlite_cpool import SqliteCPool

    return SqliteCPool(config.data_dir / "sqlite-cpool.db")


def _sqlite_rspool(config: DemoConfig) -> Backend:
    # Experimental: the multi-tenant worker's Rust port, on the same pool code.
    try:
        from dblayer.backends.sqlite_rspool import SqliteRustPool, binary_unavailable
    except ImportError as e:
        raise Unavailable(f"the SqliteRustPool backend is not available here ({e})") from e
    reason = binary_unavailable()
    if reason:
        raise Unavailable(reason)
    return SqliteRustPool(config.data_dir / "sqlite-rspool.db")


def _sqlite_wasm(config: DemoConfig) -> Backend:
    # Experimental: SQLite compiled to WebAssembly, in process (extra
    # "wasm"; build with `make -C cworker wasm`). Capped at 256 MB.
    try:
        from dblayer.backends.sqlite_wasm import SqliteWasm, module_unavailable
    except ImportError as e:
        raise Unavailable(f"the SqliteWasm backend is not available here ({e})") from e
    reason = module_unavailable()
    if reason:
        raise Unavailable(reason)
    return SqliteWasm(config.data_dir / "sqlite-wasm.db", memory_limit_mb=256)


def _sqlite_nodeworker(config: DemoConfig) -> Backend:
    # Experimental: SQLite compiled to WebAssembly in a Node.js process, one
    # per database (`npm ci --prefix nodeworker`). Capped at 256 MB.
    try:
        from dblayer.backends.sqlite_nodeworker import SqliteNodeWorker, unavailable
    except ImportError as e:
        raise Unavailable(f"the SqliteNodeWorker backend is not available here ({e})") from e
    reason = unavailable()
    if reason:
        raise Unavailable(reason)
    return SqliteNodeWorker(config.data_dir / "sqlite-nodeworker.db", memory_limit_mb=256)


def _duckdb(config: DemoConfig) -> Backend:
    from dblayer.backends.duckdb import DuckDB  # ImportError without the duckdb extra

    return DuckDB(config.data_dir / "duckdb.duckdb")


def _duckdb_worker(config: DemoConfig) -> Backend:
    return duckdb_worker_backend(config.data_dir / "duckdb-worker.duckdb")


def duckdb_worker_backend(path: Path, **options: object) -> Backend:
    """Shared by the featured DuckDB worker database and the fleet's: one
    worker process per awake database, exiting when it sleeps."""
    try:
        import duckdb  # noqa: F401  (the worker imports it; fail here if it cannot)

        from dblayer.backends.duckdb_worker import DuckDBWorker
    except ImportError as e:
        raise Unavailable(f"the DuckDBWorker backend is not available here ({e})") from e
    return DuckDBWorker(path, **options)  # type: ignore[arg-type]


def _duckdb_rsworker(config: DemoConfig) -> Backend:
    # Experimental: the DuckDB worker protocol served by a Rust program
    # (build with `make -C duckdb-rs-worker`); needs no duckdb package here.
    try:
        from dblayer.backends.duckdb_rsworker import DuckDBRustWorker, binary_unavailable
    except ImportError as e:
        raise Unavailable(f"the DuckDBRustWorker backend is not available here ({e})") from e
    reason = binary_unavailable()
    if reason:
        raise Unavailable(reason)
    return DuckDBRustWorker(config.data_dir / "duckdb-rsworker.duckdb")


def _duckdb_cppworker(config: DemoConfig) -> Backend:
    # Experimental: DuckDB in a C++ worker process (build with
    # `make -C duckdb-cpp-worker`); needs no duckdb package here.
    try:
        from dblayer.backends.duckdb_cppworker import DuckDBCppWorker, binary_unavailable
    except ImportError as e:
        raise Unavailable(f"the DuckDBCppWorker backend is not available here ({e})") from e
    reason = binary_unavailable()
    if reason:
        raise Unavailable(reason)
    return DuckDBCppWorker(config.data_dir / "duckdb-cppworker.duckdb")


def _pglite(config: DemoConfig) -> Backend:
    # Experimental: PostgreSQL compiled to WebAssembly (PGlite) in a Node.js
    # process, one data directory per database (`npm ci --prefix pglite-worker`).
    return pglite_backend(config.data_dir / "pglite")


def pglite_backend(path: Path, **options: object) -> Backend:
    """A PGlite data directory, opened by a Node.js worker on wake and
    released on sleep. Raises ``Unavailable`` naming the setup command until
    the npm packages are installed."""
    try:
        from dblayer.backends.pglite_worker import PGliteWorker, unavailable
    except ImportError as e:
        raise Unavailable(f"the PGliteWorker backend is not available here ({e})") from e
    reason = unavailable()
    if reason:
        raise Unavailable(reason)
    return PGliteWorker(path, memory_limit_mb=512, **options)  # type: ignore[arg-type]


def _clickhouse(config: DemoConfig) -> Backend:
    # Experimental: ClickHouse embedded in-process (chDB, the 'clickhouse'
    # extra; wheels for Python 3.12-3.14 only). One chDB data directory per
    # process; this database is one ClickHouse database in it.
    try:
        from dblayer.backends.clickhouse import ClickHouse, chdb_unavailable
    except ImportError as e:
        raise Unavailable(f"the ClickHouse backend is not available here ({e})") from e
    if chdb_unavailable():
        raise Unavailable(
            "chdb is not installed: run the demo with "
            "uv run --python 3.14 --all-extras --group demo python -m dblayer_demo "
            "(chDB has no wheels for Python 3.15 yet)"
        )
    return ClickHouse(config.data_dir / "clickhouse", database="demo")


DEMO_SPECS: list[DemoSpec] = [
    DemoSpec("sqlite", "SQLite, in process", "SqliteInProcess", _sqlite),
    DemoSpec("sqlite-apsw", "SQLite, in process (apsw)", "SqliteApsw", _sqlite_apsw),
    DemoSpec("sqlite-worker", "SQLite, in a worker process", "SqliteWorker", _sqlite_worker),
    DemoSpec("sqlite-cworker", "SQLite, in a C worker process", "SqliteCWorker", _sqlite_cworker),
    DemoSpec("sqlite-cpool", "SQLite, multi-tenant C worker", "SqliteCPool", _sqlite_cpool),
    DemoSpec("sqlite-rspool", "SQLite, multi-tenant Rust worker", "SqliteRustPool", _sqlite_rspool),
    DemoSpec("sqlite-wasm", "SQLite, in WebAssembly (wasmtime)", "SqliteWasm", _sqlite_wasm),
    DemoSpec("sqlite-nodeworker", "SQLite, WebAssembly in a Node.js process", "SqliteNodeWorker", _sqlite_nodeworker),
    DemoSpec("postgres", "PostgreSQL", "Postgres", _postgres, check=_postgres_check),
    DemoSpec("duckdb", "DuckDB", "DuckDB", _duckdb),
    DemoSpec("duckdb-worker", "DuckDB, in a worker process", "DuckDBWorker", _duckdb_worker),
    DemoSpec("duckdb-rsworker", "DuckDB, in a Rust worker process", "DuckDBRustWorker", _duckdb_rsworker),
    DemoSpec("duckdb-cppworker", "DuckDB, in a C++ worker process", "DuckDBCppWorker", _duckdb_cppworker),
    DemoSpec(
        "pglite",
        "PostgreSQL in WebAssembly (PGlite, Node.js process)",
        "PGliteWorker",
        _pglite,
    ),
    DemoSpec(
        "postgres-psycopg",
        "PostgreSQL (psycopg 3)",
        "PostgresPsycopg",
        _postgres_psycopg,
        check=_postgres_check,
        after="postgres",
    ),
    DemoSpec("clickhouse", "ClickHouse, in process (chDB)", "ClickHouse", _clickhouse),
]


# -- opening and closing ---------------------------------------------------------


def _reason(e: BaseException) -> str:
    msg = str(e).strip()
    if isinstance(e, Unavailable):
        return msg
    return f"{type(e).__name__}: {msg}" if msg else type(e).__name__


async def open_one(spec: DemoSpec, config: DemoConfig) -> DemoDatabase:
    demo = DemoDatabase(spec)
    db: Database | None = None
    try:
        db = Database(spec.make(config), name=spec.slug)
        if spec.check is not None:
            await spec.check(db)
        demo.seeded = await asyncio.wait_for(seed(db), SEED_TIMEOUT_S)
    except Exception as e:  # noqa: BLE001 - any failure means "unavailable"
        demo.error = _reason(e)
        if db is not None:
            try:
                await db.close()
            except Exception:  # noqa: BLE001
                pass
        return demo
    demo.db = db
    return demo


async def open_all(config: DemoConfig, specs: list[DemoSpec] | None = None) -> list[DemoDatabase]:
    config.data_dir.mkdir(parents=True, exist_ok=True)
    specs = DEMO_SPECS if specs is None else specs
    slugs = {s.slug for s in specs}
    first = [s for s in specs if s.after is None or s.after not in slugs]
    later = [s for s in specs if s not in first]
    opened = list(await asyncio.gather(*(open_one(s, config) for s in first)))
    opened += await asyncio.gather(*(open_one(s, config) for s in later))
    order = {s.slug: i for i, s in enumerate(specs)}
    return sorted(opened, key=lambda d: order[d.spec.slug])


async def close_all(demos: list[DemoDatabase]) -> None:
    for demo in demos:
        if demo.db is not None:
            await demo.db.close()
    if any(d.spec.backend == "SqliteCPool" for d in demos):
        # The featured database's pool processes would otherwise outlive
        # the app's event loop (their linger timer dies with it).
        from dblayer.backends.sqlite_cpool import close_default_pool

        close_default_pool()
    if any(d.spec.backend == "SqliteRustPool" for d in demos):
        from dblayer.backends.sqlite_rspool import close_default_pool as close_rust_pool

        close_rust_pool()
