"""The fleet: many databases behind one ``Registry``.

Every database is registered at startup (a few small objects each, no I/O
apart from one ``stat`` per file). The registry owns the shared thread
pool, a global connection cap far below the fleet size and an idle timeout,
so most of the fleet is asleep or cold at any moment.
"""

from __future__ import annotations

import asyncio
import os
import shutil
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
    check_postgres_connection,
    postgres_backend,
    sqlite_worker_backend,
)
from .seed import VIEW_NAME, seed

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
    materialised: int = 0
    """Times this run built its data from the template."""
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
            on_evict=self._on_evict,
        )

    @property
    def dir(self) -> Path:
        return self.config.data_dir / "fleet"

    # -- startup ---------------------------------------------------------------

    async def start(
        self, postgres_error: str | None = None, postgres_db: Database | None = None
    ) -> None:
        """Register every member: cold unless its data is already local
        (a file from an earlier run, or a seeded schema). ``postgres_error``
        is why PostgreSQL is unavailable, if it is; ``postgres_db`` (the
        featured one) is used to list the fleet schemas that exist."""
        t0 = time.perf_counter()
        self.started_at = time.time()
        self.dir.mkdir(parents=True, exist_ok=True)
        self.mix = fleet_mix(self.config.fleet_size)
        await self._ensure_templates()
        seeded_schemas: set[str] = set()
        if postgres_db is not None and not postgres_error and self.mix["postgres"]:
            r = await postgres_db.execute(
                "select schemaname from pg_views where viewname = :v "
                "and starts_with(schemaname, :p)",
                {"v": VIEW_NAME, "p": POSTGRES_FLEET_PREFIX},
            )
            seeded_schemas = {row[0] for row in r.rows}
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
                if member.path is not None:
                    local = member.path.exists()
                else:
                    local = member.schema in seeded_schemas
                self.registry.add(name, db, cold=not local)
                member.demo.db = db
        self.startup_seconds = time.perf_counter() - t0

    def _backend(self, member: FleetMember) -> Backend:
        if member.kind == "sqlite":
            from dblayer.backends.sqlite import SqliteInProcess

            assert member.path is not None
            return SqliteInProcess(member.path, executor=self.registry.executor)
        if member.kind == "duckdb":
            from dblayer.backends.duckdb import DuckDB

            assert member.path is not None
            # Small instances: up to the cap of them can be awake at once.
            return _duckdb(member.path, self.registry.executor)
        if member.kind == "postgres":
            return postgres_backend(
                self.config.postgres_dsn, member.schema, "dblayer-demo-fleet"
            )
        if member.kind == "sqlite-worker":
            assert member.path is not None
            return sqlite_worker_backend(member.path)
        raise Unavailable(f"unknown kind {member.kind!r}")

    # -- cold storage: templates stand in for object storage ---------------------

    def template_path(self, kind: str) -> Path:
        return self.dir / ("_template.db" if kind == "sqlite" else "_template.duckdb")

    async def _ensure_templates(self) -> None:
        """The seeded dataset, once per kind: what "fetching from object
        storage" copies. Seeded through the named operations on first run;
        later runs find them already there."""
        kinds = [k for k in ("sqlite", "duckdb") if self.mix.get(k)]
        for kind in kinds:
            path = self.template_path(kind)
            if path.exists():
                continue
            tmp = path.with_name(path.name + ".tmp")
            _remove(tmp)
            db = Database(self._file_backend(kind, tmp))
            try:
                await seed(db)
            finally:
                await db.close()
            os.replace(tmp, path)

    def _file_backend(self, kind: str, path: Path) -> Backend:
        if kind == "sqlite":
            from dblayer.backends.sqlite import SqliteInProcess

            return SqliteInProcess(path, executor=self.registry.executor)
        return _duckdb(path, self.registry.executor)

    async def _materialise(self, member: FleetMember) -> None:
        """Build a cold member's data: copy the template file into place
        (SQLite, DuckDB) or create and seed its schema (PostgreSQL), then
        make it this branch's own."""
        if member.kind == "postgres":
            db = Database(
                postgres_backend(self.config.postgres_dsn, member.schema, "dblayer-demo-fleet-seed")
            )
            try:
                await check_postgres_connection(db, member.schema)
                await seed(db)
                await _personalise(db, member)
            finally:
                await db.close()
            return
        assert member.path is not None
        tmp = member.path.with_name(member.path.name + ".tmp")
        _remove(tmp)
        await asyncio.to_thread(shutil.copyfile, self.template_path(member.kind), tmp)
        db = Database(self._file_backend(member.kind, tmp))
        try:
            await _personalise(db, member)
        finally:
            await db.close()
        os.replace(tmp, member.path)

    # -- hooks -----------------------------------------------------------------

    async def _on_wake(self, db: Database) -> None:
        member = self.members[db.name or ""]
        if db.state == "cold":
            await self._materialise(member)
            member.materialised += 1
        member.last_wake = time.time()

    async def _on_evict(self, db: Database) -> bool:
        """Drop the local file: the data can be rebuilt from the template,
        so there is nothing to copy out first. PostgreSQL schemas are never
        dropped by the demo."""
        member = self.members[db.name or ""]
        if member.path is None:
            return False
        await asyncio.to_thread(_remove, member.path)
        return True

    async def _on_sleep(self, db: Database) -> None:
        member = self.members.get(db.name or "")
        if member is not None:
            member.last_sleep = time.time()

    # -- shutdown --------------------------------------------------------------

    async def close(self) -> None:
        await self.registry.close()


def _duckdb(path: Path, executor: object) -> Backend:
    from dblayer.backends.duckdb import DuckDB

    # Small instances: up to the connection cap of them can be awake at once.
    return DuckDB(path, memory_limit="64MB", threads=1, executor=executor)  # type: ignore[arg-type]


def _remove(path: Path) -> None:
    for p in (path, path.with_name(path.name + ".wal"), path.with_name(path.name + "-journal")):
        try:
            p.unlink()
        except FileNotFoundError:
            pass


async def _personalise(db: Database, member: FleetMember) -> None:
    """Make a copy of the template this branch's own: a one-row ``branch``
    table, and a different three quarters of the reviews."""
    await db.create_table(
        "branch",
        {"number": "integer", "name": "text", "kind": "text", "backend": "text"},
        pk="number",
        if_not_exists=True,
    )
    await db.insert_rows(
        "branch",
        [{"number": member.number, "name": member.branch, "kind": member.kind,
          "backend": BACKEND_CLASS[member.kind]}],
        pk="number",
        replace=True,
    )
    await db.execute_write(
        "delete from reviews where (id + :n) % 4 = 0", {"n": member.number}
    )
