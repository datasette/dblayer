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


def _duckdb(config: DemoConfig) -> Backend:
    from dblayer.backends.duckdb import DuckDB  # ImportError without the duckdb extra

    return DuckDB(config.data_dir / "duckdb.duckdb")


DEMO_SPECS: list[DemoSpec] = [
    DemoSpec("sqlite", "SQLite, in process", "SqliteInProcess", _sqlite),
    DemoSpec("sqlite-worker", "SQLite, in a worker process", "SqliteWorker", _sqlite_worker),
    DemoSpec("postgres", "PostgreSQL", "Postgres", _postgres, check=_postgres_check),
    DemoSpec("duckdb", "DuckDB", "DuckDB", _duckdb),
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
    return list(await asyncio.gather(*(open_one(s, config) for s in specs)))


async def close_all(demos: list[DemoDatabase]) -> None:
    for demo in demos:
        if demo.db is not None:
            await demo.db.close()
