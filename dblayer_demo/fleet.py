"""The fleet: many databases behind one ``Registry``.

Every database is registered at startup (a few small objects each, no I/O
apart from one ``stat`` per file). The registry owns the shared thread
pool, a global connection cap far below the fleet size and an idle timeout,
so most of the fleet is asleep or cold at any moment.
"""

from __future__ import annotations

import asyncio
import os
import random
import shutil
import statistics
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from dblayer import Database, Registry
from dblayer.backends.base import Backend

from .config import DemoConfig
from .databases import (
    DemoDatabase,
    DemoSpec,
    Unavailable,
    _reason,
    check_postgres_connection,
    duckdb_worker_backend,
    postgres_backend,
    sqlite_cworker_backend,
    sqlite_worker_backend,
)
from .seed import VIEW_NAME, seed

__all__ = ["Fleet", "FleetMember", "FanoutBusy", "fleet_mix", "KINDS", "FANOUT_MAX"]

FANOUT_MAX = 500
"""The most databases one fan-out request may touch."""
FANOUT_ACTIONS = ("query", "evict")
FANOUT_TIMEOUT_S = 60.0
FANOUT_SQL = "select (select name from branch) as branch, count(*) as reviews from reviews"
ZIPF_S = 1.1
"""Skew of the fan-out draw: rank r is chosen with weight 1 / r**s."""


MAX_CONCURRENT_MATERIALISE = 8
"""Cold databases being built at once (template copy plus personalisation)."""


class FanoutBusy(Exception):
    pass

KINDS = ("sqlite", "sqlite-cworker", "duckdb", "duckdb-worker", "postgres", "sqlite-worker")
BACKEND_CLASS = {
    "sqlite": "SqliteInProcess",
    "sqlite-cworker": "SqliteCWorker",
    "duckdb": "DuckDB",
    "duckdb-worker": "DuckDBWorker",
    "postgres": "Postgres",
    "sqlite-worker": "SqliteWorker",
}
TEMPLATE_KIND = {
    "sqlite": "sqlite",
    "sqlite-worker": "sqlite",
    "sqlite-cworker": "sqlite",
    "duckdb": "duckdb",
    "duckdb-worker": "duckdb",
}
"""Which template file a kind's members are copied from: a worker-process
database is an ordinary file of its engine."""
POSTGRES_FLEET_PREFIX = "dblayer_demo_fleet_"
"""Fleet schemas in ``dblayer_test``: ``dblayer_demo_fleet_001`` and up."""

_TOWNS = (
    "Ashford Bramley Calder Dunmore Elmstead Fernhill Glenrock Harrow Ivybridge Juniper "
    "Kestrel Larkfield Millbrook Northcote Oakham Pendle Quarry Rookwood Saltash Thornbury "
    "Upton Vale Westcliff Yarrow"
).split()
_KIND_LABEL = {
    "sqlite": "SQLite",
    "sqlite-cworker": "SQLite C worker",
    "duckdb": "DuckDB",
    "duckdb-worker": "DuckDB worker",
    "postgres": "PostgreSQL",
    "sqlite-worker": "SQLite worker",
}


def fleet_mix(size: int) -> dict[str, int]:
    """How many of each backend: mostly in-process SQLite (cheapest asleep
    and awake), 20% SQLite in a C worker process (about 3 ms and 3 MB per
    awake database, so a process each is affordable), 10% in-process DuckDB
    (about 5 MB per awake instance, not all of it returned on sleep), 5%
    DuckDB in a worker process (a process per awake database, gone on
    sleep), 3% PostgreSQL schemas (each awake one holds server connections)
    and 2% SQLite in a Python worker (about 60 ms and 38 MB per process).
    1,000 gives 600 / 200 / 100 / 50 / 30 / 20."""
    if size <= 0:
        return {k: 0 for k in KINDS}
    duckdb = round(size * 0.10)
    small = size >= 10
    cworker = max(1, round(size * 0.20)) if small else 0
    duckdb_worker = max(1, round(size * 0.05)) if small else 0
    postgres = max(1, round(size * 0.03)) if small else 0
    worker = max(1, round(size * 0.02)) if small else 0
    return {
        "sqlite": size - cworker - duckdb - duckdb_worker - postgres - worker,
        "sqlite-cworker": cworker,
        "duckdb": duckdb,
        "duckdb-worker": duckdb_worker,
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
    _fanout_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    _materialising: asyncio.Semaphore = field(
        default_factory=lambda: asyncio.Semaphore(MAX_CONCURRENT_MATERIALISE)
    )
    _hot: list[Database] | None = None

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
        template_errors = await self._ensure_templates()
        if self.mix["postgres"] and postgres_db is None and not postgres_error:
            postgres_error = await self._probe_postgres()
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
                elif kind in ("duckdb", "duckdb-worker"):
                    path = self.dir / f"{name}.duckdb"
                elif kind in ("sqlite-worker", "sqlite-cworker"):
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
                if TEMPLATE_KIND.get(kind) in template_errors:
                    member.demo.error = template_errors[TEMPLATE_KIND[kind]]
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
        if member.kind == "sqlite-cworker":
            assert member.path is not None
            # Raises Unavailable (the build command) until the binary exists.
            return sqlite_cworker_backend(member.path)
        if member.kind == "duckdb-worker":
            assert member.path is not None
            # The same small settings as in-process members; the worker
            # process exits when the database sleeps.
            return duckdb_worker_backend(member.path, memory_limit="64MB", threads=1)
        raise Unavailable(f"unknown kind {member.kind!r}")

    # -- cold storage: templates stand in for object storage ---------------------

    def template_path(self, kind: str) -> Path:
        # A worker-process database is an ordinary file of its engine.
        sqlite_file = TEMPLATE_KIND[kind] == "sqlite"
        return self.dir / ("_template.db" if sqlite_file else "_template.duckdb")

    async def _ensure_templates(self) -> dict[str, str]:
        """The seeded dataset, once per kind: what "fetching from object
        storage" copies. Seeded through the named operations on first run;
        later runs find them already there. Returns ``{kind: reason}`` for
        kinds that cannot run here (DuckDB not installed)."""
        errors: dict[str, str] = {}
        kinds = []
        if self.mix.get("sqlite") or self.mix.get("sqlite-worker") or self.mix.get("sqlite-cworker"):
            kinds.append("sqlite")
        if self.mix.get("duckdb") or self.mix.get("duckdb-worker"):
            kinds.append("duckdb")
        for kind in kinds:
            path = self.template_path(kind)
            if path.exists():
                continue
            tmp = path.with_name(path.name + ".tmp")
            _remove(tmp)
            try:
                db = Database(self._file_backend(kind, tmp))
            except ImportError as e:
                errors[kind] = _reason(e)
                continue
            try:
                await seed(db)
            finally:
                await db.close()
            os.replace(tmp, path)
        return errors

    async def _probe_postgres(self) -> str | None:
        """Without the featured PostgreSQL database to ask: one connection
        to the first fleet schema, so an unreachable server marks the
        PostgreSQL members unavailable at startup rather than failing on
        every wake. Returns the reason, or ``None``."""
        schema = f"{POSTGRES_FLEET_PREFIX}001"
        try:
            db = Database(postgres_backend(self.config.postgres_dsn, schema, "dblayer-demo-fleet"))
        except Exception as e:  # noqa: BLE001
            return _reason(e)
        try:
            await asyncio.wait_for(check_postgres_connection(db, schema), 10)
        except Exception as e:  # noqa: BLE001
            return _reason(e)
        finally:
            await db.close()
        return None

    def _file_backend(self, kind: str, path: Path) -> Backend:
        if TEMPLATE_KIND[kind] == "sqlite":
            # Building the file needs no worker: it is plain SQLite on disk.
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
        await self._in_pool(shutil.copyfile, self.template_path(member.kind), tmp)
        db = Database(self._file_backend(member.kind, tmp))
        try:
            await _personalise(db, member)
        finally:
            await db.close()
        os.replace(tmp, member.path)

    async def _in_pool(self, fn: Any, *args: Any) -> Any:
        """Blocking file work on the registry's thread pool, so the thread
        count stays fixed (``asyncio.to_thread`` would grow the loop's own
        default pool)."""
        return await asyncio.get_running_loop().run_in_executor(
            self.registry.executor, fn, *args
        )

    # -- hooks -----------------------------------------------------------------

    async def _on_wake(self, db: Database) -> None:
        member = self.members[db.name or ""]
        if db.state == "cold":
            # on_wake runs before the database has a place under the
            # connection cap, so without this a fan-out would build every
            # cold database it touches at once (each with a temporary
            # connection the cap does not count).
            async with self._materialising:
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
        await self._in_pool(_remove, member.path)
        return True

    async def _on_sleep(self, db: Database) -> None:
        member = self.members.get(db.name or "")
        if member is not None:
            member.last_sleep = time.time()

    # -- load ------------------------------------------------------------------

    async def fanout(self, n: int, action: str = "query") -> dict[str, Any]:
        """Run ``action`` on ``n`` fleet databases at once and report.

        ``query``: ``n`` concurrent reads, drawn with a Zipf-like skew (a
        few hot databases, a long tail), all launched together so the
        connection cap does the queueing. ``evict``: make up to ``n``
        asleep file-backed databases cold. ``n`` is capped at
        ``FANOUT_MAX``; one fan-out runs at a time."""
        if action not in FANOUT_ACTIONS:
            raise ValueError(f"action must be one of {', '.join(FANOUT_ACTIONS)}")
        n = max(1, min(int(n), FANOUT_MAX))
        if self._fanout_lock.locked():
            raise FanoutBusy("another fan-out is running; try again when it finishes")
        async with self._fanout_lock:
            before = self.registry.stats()
            peaks = {"awake": before.awake, "open_connections": before.open_connections,
                     "waiting": before.waiting}
            stop = asyncio.Event()

            async def sample() -> None:
                while not stop.is_set():
                    s = self.registry.stats()
                    peaks["awake"] = max(peaks["awake"], s.awake)
                    peaks["open_connections"] = max(peaks["open_connections"], s.open_connections)
                    peaks["waiting"] = max(peaks["waiting"], s.waiting)
                    try:
                        await asyncio.wait_for(stop.wait(), 0.005)
                    except TimeoutError:
                        pass

            sampler = asyncio.create_task(sample())
            t0 = time.perf_counter()
            try:
                if action == "query":
                    result = await self._fanout_query(n)
                else:
                    result = await self._fanout_evict(n)
            finally:
                stop.set()
                await sampler
            seconds = time.perf_counter() - t0
            after = self.registry.stats()
        result.update(
            action=action,
            requested=n,
            seconds=round(seconds, 3),
            before=asdict(before),
            after=asdict(after),
            peak=peaks,
            cap=self.registry.max_open_connections,
            cap_held=peaks["open_connections"] <= (self.registry.max_open_connections or 1 << 30),
        )
        return result

    def _hot_order(self) -> list[Database]:
        """Available members in a fixed shuffled order: rank 1 is hottest."""
        if self._hot is None:
            dbs = [m.db for m in self.members.values() if m.db is not None]
            random.Random(1234).shuffle(dbs)
            self._hot = dbs
        return self._hot

    async def _fanout_query(self, n: int) -> dict[str, Any]:
        dbs = self._hot_order()
        if not dbs:
            return {"ran": 0, "ok": 0, "errors": 0, "distinct": 0, "error_samples": []}
        weights = [1 / (rank ** ZIPF_S) for rank in range(1, len(dbs) + 1)]
        picks = random.choices(dbs, weights=weights, k=n)
        latencies: list[float] = []
        errors: list[str] = []

        async def one(db: Database) -> None:
            t = time.perf_counter()
            try:
                await db.execute(FANOUT_SQL, time_limit_ms=5000)
            except Exception as e:  # noqa: BLE001 - reported, not raised
                errors.append(f"{db.name}: {_reason(e)}")
            else:
                latencies.append(time.perf_counter() - t)

        await asyncio.wait_for(asyncio.gather(*(one(db) for db in picks)), FANOUT_TIMEOUT_S)
        ms = sorted(x * 1000 for x in latencies)
        return {
            "ran": len(picks),
            "ok": len(latencies),
            "errors": len(errors),
            "error_samples": errors[:5],
            "distinct": len({id(db) for db in picks}),
            "latency_ms": {
                "p50": round(statistics.median(ms), 1) if ms else None,
                "p95": round(ms[int(len(ms) * 0.95) - 1], 1) if ms else None,
                "max": round(ms[-1], 1) if ms else None,
            },
        }

    async def _fanout_evict(self, n: int) -> dict[str, Any]:
        asleep = [
            m for m in self.members.values()
            if m.db is not None and m.path is not None and m.db.state == "asleep"
        ]
        random.shuffle(asleep)
        evicted = 0
        for m in asleep[:n]:
            if await self.registry.evict(m.name):
                evicted += 1
        return {"ran": min(n, len(asleep)), "ok": evicted, "errors": 0, "error_samples": [],
                "distinct": evicted}

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
