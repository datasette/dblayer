"""The demo databases: one declarative list, opened and seeded at startup.

Each ``DemoSpec`` says how to build its backend. Building it may fail (a
backend module not written yet, a driver not installed, a server not
reachable); the database is then still listed, marked unavailable with the
reason, and the app carries on.
"""

from __future__ import annotations

import asyncio
import os
import urllib.parse
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path

from dblayer import Database
from dblayer.backends.base import Backend

from .seed import seed

__all__ = ["DemoSpec", "DemoDatabase", "DEMO_SPECS", "open_all", "close_all"]

SEED_TIMEOUT_S = 60.0


@dataclass(frozen=True)
class DemoSpec:
    slug: str
    """URL segment: ``/<slug>``."""
    title: str
    backend: str
    """The dblayer backend class, for display."""
    make: Callable[[Path], Backend]
    """Build the backend, given the demo data directory. May raise."""
    check: Callable[[Database], Awaitable[None]] | None = None
    """Run after the first connection, before seeding: raise to refuse."""


@dataclass
class DemoDatabase:
    spec: DemoSpec
    db: Database | None = None
    error: str | None = None
    """Why the database is unavailable, or ``None``."""
    seeded: bool = False
    table_count: int | None = None
    extra: dict[str, str] = field(default_factory=dict)

    @property
    def available(self) -> bool:
        return self.db is not None


# -- the backends ------------------------------------------------------------------


def _sqlite(data_dir: Path) -> Backend:
    from dblayer.backends.sqlite import SqliteInProcess

    return SqliteInProcess(data_dir / "sqlite.db")


POSTGRES_DSN_ENV = "DBLAYER_TEST_POSTGRES"
POSTGRES_DEFAULT_DSN = "postgresql:///dblayer_test?host=/tmp"
POSTGRES_DATABASE = "dblayer_test"
"""The only PostgreSQL database the demo will connect to."""
POSTGRES_SCHEMA = "dblayer_demo"
"""The only schema the demo creates or writes to."""


def postgres_dsn() -> str:
    return os.environ.get(POSTGRES_DSN_ENV, POSTGRES_DEFAULT_DSN)


def _dsn_database(dsn: str) -> str | None:
    parts = urllib.parse.urlsplit(dsn)
    query = urllib.parse.parse_qs(parts.query)
    if "dbname" in query:
        return query["dbname"][0]
    return urllib.parse.unquote(parts.path.lstrip("/")) or None


def _postgres(data_dir: Path) -> Backend:
    dsn = postgres_dsn()
    # Refuse before connecting: the server may hold other, real databases.
    if _dsn_database(dsn) != POSTGRES_DATABASE:
        raise ValueError(
            f"${POSTGRES_DSN_ENV} must name the {POSTGRES_DATABASE!r} database; refusing to connect"
        )
    from dblayer.backends.postgres import Postgres

    return Postgres(
        dsn, schema=POSTGRES_SCHEMA, application_name="dblayer-demo", connect_timeout=5.0
    )


async def _postgres_check(db: Database) -> None:
    r = await db.execute("select current_database(), current_schema()")
    database, schema = r.rows[0]
    if database != POSTGRES_DATABASE or schema != POSTGRES_SCHEMA:
        raise RuntimeError(f"connected to {database}.{schema}, expected "
                           f"{POSTGRES_DATABASE}.{POSTGRES_SCHEMA}; refusing to seed")


DEMO_SPECS: list[DemoSpec] = [
    DemoSpec("sqlite", "SQLite, in process", "SqliteInProcess", _sqlite),
    DemoSpec("postgres", "PostgreSQL", "Postgres", _postgres, check=_postgres_check),
]


# -- opening and closing ---------------------------------------------------------


def _reason(e: BaseException) -> str:
    msg = str(e).strip()
    return f"{type(e).__name__}: {msg}" if msg else type(e).__name__


async def open_one(spec: DemoSpec, data_dir: Path) -> DemoDatabase:
    demo = DemoDatabase(spec)
    db: Database | None = None
    try:
        db = Database(spec.make(data_dir), name=spec.slug)
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


async def open_all(data_dir: Path, specs: list[DemoSpec] | None = None) -> list[DemoDatabase]:
    data_dir.mkdir(parents=True, exist_ok=True)
    return list(await asyncio.gather(*(open_one(s, data_dir) for s in specs or DEMO_SPECS)))


async def close_all(demos: list[DemoDatabase]) -> None:
    for demo in demos:
        if demo.db is not None:
            await demo.db.close()
