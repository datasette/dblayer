"""The fleet: many databases behind one ``Registry``.

Every database is registered at startup (a few small objects each, no I/O
apart from one ``stat`` per file). The registry owns the shared thread
pool, a global connection cap far below the fleet size and an idle timeout,
so most of the fleet is asleep or cold at any moment.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path

from dblayer import Database, Registry
from dblayer.backends.base import Backend

from .config import DemoConfig
from .databases import (
    DemoDatabase,
    DemoSpec,
    Unavailable,
    _reason,
    postgres_backend,
    sqlite_worker_backend,
)

__all__ = ["Fleet", "FleetMember", "fleet_mix", "KINDS"]

KINDS = ("sqlite", "duckdb", "postgres", "sqlite-worker")
BACKEND_CLASS = {
    "sqlite": "SqliteInProcess",
    "duckdb": "DuckDB",
    "postgres": "Postgres",
    "sqlite-worker": "SqliteWorker",
}
POSTGRES_FLEET_PREFIX = "dblayer_demo_fleet_"
"""Fleet schemas in ``dblayer_test``: ``dblayer_demo_fleet_001`` and up."""

_TOWNS = (
    "Ashford Bramley Calder Dunmore Elmstead Fernhill Glenrock Harrow Ivybridge Juniper "
    "Kestrel Larkfield Millbrook Northcote Oakham Pendle Quarry Rookwood Saltash Thornbury "
    "Upton Vale Westcliff Yarrow"
).split()
_KIND_LABEL = {
    "sqlite": "SQLite",
    "duckdb": "DuckDB",
    "postgres": "PostgreSQL",
    "sqlite-worker": "SQLite worker",
}


def fleet_mix(size: int) -> dict[str, int]:
    """How many of each backend: mostly in-process SQLite (cheapest asleep
    and awake), 15% DuckDB (about 5 MB per awake instance), 3% PostgreSQL
    schemas (each awake one holds server connections) and 2% worker SQLite."""
    if size <= 0:
        return {k: 0 for k in KINDS}
    duckdb = round(size * 0.15)
    postgres = max(1, round(size * 0.03)) if size >= 10 else 0
    worker = max(1, round(size * 0.02)) if size >= 10 else 0
    return {
        "sqlite": size - duckdb - postgres - worker,
        "duckdb": duckdb,
        "postgres": postgres,
        "sqlite-worker": worker,
    }


@dataclass
class FleetMember:
    name: str
    kind: str
    number: int
    """1-based within its kind."""
    branch: str
    """A per-database label: the library branch this database belongs to."""
    path: Path | None
    """Local file (SQLite, DuckDB), or ``None`` (PostgreSQL schema)."""
    demo: DemoDatabase
    """The same shape the featured databases have, so every page works."""
    last_wake: float | None = None
    last_sleep: float | None = None

    @property
    def db(self) -> Database | None:
        return self.demo.db

    @property
    def state(self) -> str:
        if self.demo.db is None:
            return "unavailable"
        return self.demo.db.state

    @property
    def schema(self) -> str:
        return f"{POSTGRES_FLEET_PREFIX}{self.number:03d}"


@dataclass
class Fleet:
    config: DemoConfig
    registry: Registry = field(init=False)
    members: dict[str, FleetMember] = field(default_factory=dict)
    mix: dict[str, int] = field(default_factory=dict)
    started_at: float = 0.0
    startup_seconds: float = 0.0

    def __post_init__(self) -> None:
        c = self.config
        self.registry = Registry(
            max_open_connections=c.fleet_max_open_connections,
            max_threads=c.fleet_max_threads,
            idle_timeout=c.fleet_idle_timeout,
            on_wake=self._on_wake,
            on_sleep=self._on_sleep,
        )

    @property
    def dir(self) -> Path:
        return self.config.data_dir / "fleet"

    # -- startup ---------------------------------------------------------------

    async def start(self, postgres_error: str | None = None) -> None:
        """Register every member. ``postgres_error``: why PostgreSQL is
        unavailable (from the featured database), if it is."""
        t0 = time.perf_counter()
        self.started_at = time.time()
        self.dir.mkdir(parents=True, exist_ok=True)
        self.mix = fleet_mix(self.config.fleet_size)
        n = 0
        for kind in KINDS:
            for i in range(1, self.mix[kind] + 1):
                n += 1
                name = f"{kind}-{i:04d}" if kind != "postgres" else f"postgres-{i:03d}"
                branch = f"{_TOWNS[(n * 7) % len(_TOWNS)]} branch no. {n}"
                path = None
                if kind == "sqlite":
                    path = self.dir / f"{name}.db"
                elif kind == "duckdb":
                    path = self.dir / f"{name}.duckdb"
                elif kind == "sqlite-worker":
                    path = self.dir / f"{name}.db"
                spec = DemoSpec(
                    slug=name,
                    title=f"{branch} ({_KIND_LABEL[kind]})",
                    backend=BACKEND_CLASS[kind],
                    make=lambda config: None,  # type: ignore[arg-type,return-value]
                )
                member = FleetMember(name, kind, i, branch, path, DemoDatabase(spec))
                self.members[name] = member
                if kind == "postgres" and postgres_error:
                    member.demo.error = postgres_error
                    continue
                try:
                    backend = self._backend(member)
                except Exception as e:  # noqa: BLE001 - unavailable here
                    member.demo.error = _reason(e)
                    continue
                db = Database(backend, name=name)
                self.registry.add(name, db)
                member.demo.db = db
        self.startup_seconds = time.perf_counter() - t0

    def _backend(self, member: FleetMember) -> Backend:
        if member.kind == "sqlite":
            from dblayer.backends.sqlite import SqliteInProcess

            assert member.path is not None
            return SqliteInProcess(member.path)
        if member.kind == "duckdb":
            from dblayer.backends.duckdb import DuckDB

            assert member.path is not None
            # Small instances: up to the cap of them can be awake at once.
            return DuckDB(member.path, memory_limit="64MB", threads=1)
        if member.kind == "postgres":
            return postgres_backend(
                self.config.postgres_dsn, member.schema, "dblayer-demo-fleet"
            )
        if member.kind == "sqlite-worker":
            assert member.path is not None
            return sqlite_worker_backend(member.path)
        raise Unavailable(f"unknown kind {member.kind!r}")

    # -- hooks -----------------------------------------------------------------

    async def _on_wake(self, db: Database) -> None:
        member = self.members[db.name or ""]
        member.last_wake = time.time()

    async def _on_sleep(self, db: Database) -> None:
        member = self.members.get(db.name or "")
        if member is not None:
            member.last_sleep = time.time()

    # -- shutdown --------------------------------------------------------------

    async def close(self) -> None:
        await self.registry.close()
