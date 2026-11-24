"""DuckDB in a worker process: the parent side.

``DuckDBWorker(path)`` runs the in-process DuckDB backend
(``dblayer.backends.duckdb.DuckDB``) inside a child process started with
``python -m dblayer.worker``, using the generic ``WorkerBackend``.

Why a worker for DuckDB
-----------------------
An awake in-process DuckDB instance costs about 5 MB of resident memory and
closing it returns little of that to the operating system (the allocator
keeps it). A process that exits returns everything. The worker also gives
DuckDB what it gives SQLite: a hard kill for a query that ignores
``interrupt()``, and ``RLIMIT_AS`` on Linux, on top of DuckDB's own
``memory_limit``.

Placement: one process owns one database
-----------------------------------------
DuckDB locks its file per process, so a database's read and write
connections cannot live in separate processes (dedicated placement fails),
and two databases in one process would share that process's fate on a kill.
``DatabaseWorker`` is a one-process placement owned by one backend:

* every connection of the database (read and write) is a connection inside
  the same worker, which holds one DuckDB instance;
* the process starts on the first ``open()`` after a wake and exits when the
  last connection closes, which is what the core does when the database
  sleeps;
* ``Backend.close()`` waits until the process has exited, so the file lock
  is free when ``db.sleep()`` returns, as it is for in-process DuckDB; a new
  process is never started while the previous one is still exiting.

The worker runs one request at a time, so a database's connections are
served in turn: a long analytical query delays other queries *on the same
database* (never on other databases, which have their own processes).

Value types
-----------
DuckDB returns dates, times, timestamps (with time zone: ``ZoneInfo``),
``Decimal``, ``UUID``, ``timedelta`` intervals, lists, structs, maps and
fixed-size arrays; ``dblayer.worker.protocol`` carries all of them intact on
both codecs.
"""

from __future__ import annotations

import asyncio
import os
import time
from collections.abc import Mapping
from typing import Any

from ..errors import NotSupported
from ..worker.protocol import DEFAULT_MAX_FRAME_BYTES
from .sqlite_worker import (
    DEFAULT_KILL_GRACE_MS,
    SharedWorkers,
    WorkerBackend,
    WorkerConfig,
    WorkerConnection,
    WorkerProcess,
)

__all__ = ["DuckDBWorker", "DatabaseWorker", "DuckDBWorkerPool", "SpareWorkers", "DUCKDB_ENGINE"]

DUCKDB_ENGINE = "dblayer.backends.duckdb:DuckDB"
DUCKDB_PRELOAD = ("duckdb", "dblayer.backends.duckdb")
"""Imported by every DuckDB worker at start-up (``WorkerConfig.preload``)."""


class SpareWorkers:
    """Experimental: keep ``size`` worker processes started (interpreter
    up, ``preload`` modules imported, ``hello`` done) so a waking database
    takes one instead of paying the ~100 ms start. Each spare is a process
    that costs memory while it waits (~45 MB footprint for DuckDB), and a
    taken spare is replaced in the background. Only processes started with
    the same ``WorkerConfig`` are handed out. Belongs to one event loop at
    a time; ``close()`` stops the spares."""

    def __init__(self, size: int = 1) -> None:
        if size < 0:
            raise ValueError("size must be at least 0")
        self.size = size
        self._spares: list[WorkerProcess] = []
        self._filling: dict[WorkerConfig, int] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        self._tasks: set[asyncio.Task[None]] = set()
        self.taken = 0
        self.missed = 0

    def __repr__(self) -> str:
        return f"<SpareWorkers size={self.size} ready={len(self._spares)}>"

    def _check_loop(self) -> asyncio.AbstractEventLoop:
        loop = asyncio.get_running_loop()
        if self._loop is not loop:
            self.close()
            self._loop = loop
        return loop

    def take(self, config: WorkerConfig) -> WorkerProcess | None:
        """A ready spare for ``config`` (and a replacement starting), or
        None (and spares starting for next time)."""
        self._check_loop()
        self._spares = [p for p in self._spares if p.alive]
        proc = next((p for p in self._spares if p.config == config), None)
        if proc is not None:
            self._spares.remove(proc)
            self.taken += 1
        else:
            self.missed += 1
        self.fill(config)
        return proc

    def fill(self, config: WorkerConfig) -> None:
        """Start spares for ``config`` in the background, up to ``size``."""
        loop = self._check_loop()
        ready = sum(1 for p in self._spares if p.config == config and p.alive)
        for _ in range(self.size - ready - self._filling.get(config, 0)):
            self._filling[config] = self._filling.get(config, 0) + 1
            task = loop.create_task(self._start(config))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    async def _start(self, config: WorkerConfig) -> None:
        try:
            proc = WorkerProcess(config)
            await proc.start()
        except Exception:
            return
        finally:
            self._filling[config] -= 1
        if self._loop is asyncio.get_running_loop():
            self._spares.append(proc)
        else:
            proc.close()

    @property
    def ready(self) -> int:
        return sum(1 for p in self._spares if p.alive)

    def close(self) -> None:
        for t in self._tasks:
            t.cancel()
        spares, self._spares = self._spares, []
        for p in spares:
            p.close()

    async def aclose(self) -> None:
        self.close()
        await asyncio.sleep(0)


class DatabaseWorker(SharedWorkers):
    """Placement where one process serves every connection of one database
    and exits when the last of them closes.

    A ``SharedWorkers`` of size one, with two differences that DuckDB's
    per-process file lock needs: concurrent first opens wait for the one
    process being started instead of starting a second, and a new process
    is started only once the previous one has exited."""

    def __init__(
        self,
        *,
        codec: str = "json",
        memory_limit_mb: int | None = None,
        kill_grace_ms: int = DEFAULT_KILL_GRACE_MS,
        max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
        exit_timeout: float = 10.0,
        debug: bool = False,
        preload: tuple[str, ...] = (),
        spares: SpareWorkers | None = None,
    ) -> None:
        super().__init__(
            1,
            codec=codec,
            memory_limit_mb=memory_limit_mb,
            kill_grace_ms=kill_grace_ms,
            max_frame_bytes=max_frame_bytes,
            linger=0.0,
            debug=debug,
        )
        if preload:
            from dataclasses import replace

            self.config = replace(self.config, preload=tuple(preload))
        self.spares = spares
        self.exit_timeout = exit_timeout
        self._starting_proc: asyncio.Future[WorkerProcess] | None = None
        self._exiting: list[WorkerProcess] = []
        self.last_start_ms: float | None = None
        """Wall time of the most recent process start (spawn to ``hello``)."""

    @property
    def process(self) -> WorkerProcess | None:
        """The live process, if any."""
        for p in self.procs:
            if p.alive:
                return p
        return None

    async def acquire(self) -> WorkerProcess:
        loop = asyncio.get_running_loop()
        if self._loop is not loop:
            self.close()
            self._loop = loop
            self._starting_proc = None
        while True:
            proc = self.process
            if proc is not None:
                break
            pending = self._starting_proc
            if pending is not None:
                # Another open is starting the process: share it (or, if
                # that start failed, try again here).
                await asyncio.wait([pending])
                continue
            fut: asyncio.Future[WorkerProcess] = loop.create_future()
            self._starting_proc = fut
            try:
                await self.wait_exited()
                t0 = time.perf_counter()
                spare = self.spares.take(self.config) if self.spares is not None else None
                if spare is not None:
                    proc = spare
                else:
                    proc = WorkerProcess(self.config)
                    await proc.start()
                self.last_start_ms = (time.perf_counter() - t0) * 1000
            except BaseException:
                fut.cancel()
                raise
            finally:
                self._starting_proc = None
            self.started += 1
            self.procs = [proc]
            fut.set_result(proc)
            break
        proc.handles += 1
        if proc.idle_timer is not None:
            proc.idle_timer.cancel()
            proc.idle_timer = None
        return proc

    def _retire(self, proc: WorkerProcess) -> None:
        if proc.handles == 0 and proc.popen is not None and proc.popen.poll() is None:
            self._exiting.append(proc)
        super()._retire(proc)

    def close(self) -> None:
        for p in self.procs:
            if p.popen is not None and p.popen.poll() is None:
                self._exiting.append(p)
        super().close()

    async def wait_exited(self) -> None:
        """Wait until every process this placement closed has exited
        (SIGKILL after ``exit_timeout``), so its DuckDB file lock is free."""
        delay = 0.001
        end = time.monotonic() + self.exit_timeout
        while self._exiting:
            self._exiting = [p for p in self._exiting if p.popen is not None and p.popen.poll() is None]
            if not self._exiting:
                break
            if time.monotonic() >= end:
                for p in self._exiting:
                    p.kill()
                end = time.monotonic() + 1.0
            await asyncio.sleep(delay)
            delay = min(delay * 2, 0.02)

    async def aclose(self) -> None:
        self.close()
        await self.wait_exited()


class _Host:
    """One process of a ``DuckDBWorkerPool`` and the databases on it."""

    __slots__ = ("proc", "dbs", "wakes", "accepting")

    def __init__(self, proc: WorkerProcess) -> None:
        self.proc = proc
        self.dbs: dict[str, int] = {}
        """Database path -> connections open on this process."""
        self.wakes = 0
        self.accepting = True


class DuckDBWorkerPool:
    """Experimental placement shared by many ``DuckDBWorker`` backends: one
    process hosts up to ``databases_per_process`` awake databases (every
    connection of each, as DuckDB's file lock needs), so the ~45 MB a
    worker costs before it opens anything (interpreter, imports, the duckdb
    library) is paid once per group, not once per database.

    Memory is still returned: a process exits when its last database
    sleeps, and stops taking new databases after ``wakes_per_process``
    wakes, so one that never empties is replaced once its databases sleep
    (whatever DuckDB kept after earlier sleeps goes with it). A database
    reopened on another process is safe: a connection's close is
    acknowledged by the worker only after the DuckDB instance has closed.

    The cost: a kill (time limit overrun, memory) takes every database on
    that process down, and their requests share one worker, one at a
    time."""

    def __init__(
        self,
        *,
        databases_per_process: int = 8,
        wakes_per_process: int = 64,
        codec: str = "json",
        memory_limit_mb: int | None = None,
        kill_grace_ms: int = DEFAULT_KILL_GRACE_MS,
        max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
        debug: bool = False,
    ) -> None:
        if databases_per_process < 1 or wakes_per_process < 1:
            raise ValueError("databases_per_process and wakes_per_process must be at least 1")
        self.databases_per_process = databases_per_process
        self.wakes_per_process = wakes_per_process
        self.config = WorkerConfig(
            codec=codec,
            memory_limit_mb=memory_limit_mb,
            kill_grace_ms=kill_grace_ms,
            max_frame_bytes=max_frame_bytes,
            debug=debug,
            preload=DUCKDB_PRELOAD,
        )
        self.hosts: list[_Host] = []
        self._where: dict[str, _Host] = {}
        self._placing: dict[str, asyncio.Future[_Host]] = {}
        self._starting = 0
        self._loop: asyncio.AbstractEventLoop | None = None
        self.started = 0

    def __repr__(self) -> str:
        return f"<DuckDBWorkerPool processes={self.processes} databases={len(self._where)}>"

    def slot(self, path: str) -> "_PoolSlot":
        return _PoolSlot(self, path)

    def _host_of(self, proc: WorkerProcess) -> _Host | None:
        for h in self.hosts:
            if h.proc is proc:
                return h
        return None

    async def acquire(self, path: str) -> WorkerProcess:
        loop = asyncio.get_running_loop()
        if self._loop is not loop:
            self.close()
            self._loop = loop
        while True:
            host = self._where.get(path)
            if host is not None and host.proc.alive:
                break
            pending = self._placing.get(path)
            if pending is not None:
                await asyncio.wait([pending])
                continue
            fut: asyncio.Future[_Host] = loop.create_future()
            self._placing[path] = fut
            try:
                host = await self._place(path)
            except BaseException:
                fut.cancel()
                raise
            finally:
                del self._placing[path]
            fut.set_result(host)
            break
        host.dbs[path] = host.dbs.get(path, 0) + 1
        host.proc.handles += 1
        return host.proc

    async def _place(self, path: str) -> _Host:
        self.hosts = [h for h in self.hosts if h.proc.alive]
        room = [h for h in self.hosts if h.accepting and len(h.dbs) < self.databases_per_process]
        if room:
            host = max(room, key=lambda h: len(h.dbs))  # pack: fewer processes
        else:
            proc = WorkerProcess(self.config)
            await proc.start()
            self.started += 1
            host = _Host(proc)
            self.hosts.append(host)
        host.wakes += 1
        if host.wakes >= self.wakes_per_process:
            host.accepting = False
        self._where[path] = host
        host.dbs.setdefault(path, 0)
        return host

    def release(self, path: str, proc: WorkerProcess) -> None:
        proc.handles -= 1
        host = self._host_of(proc)
        if host is None:
            return
        n = host.dbs.get(path, 0) - 1
        if n > 0:
            host.dbs[path] = n
            return
        host.dbs.pop(path, None)
        if self._where.get(path) is host:
            del self._where[path]
        if not host.dbs:
            # The last database on this process slept: exit, returning
            # everything (the next wake starts or picks another process).
            self.hosts.remove(host)
            host.proc.close()

    def close(self) -> None:
        hosts, self.hosts = self.hosts, []
        self._where.clear()
        for h in hosts:
            h.proc.close()

    @property
    def processes(self) -> int:
        return sum(1 for h in self.hosts if h.proc.alive)


class _PoolSlot:
    """A ``DuckDBWorkerPool`` seen from one database: the placement
    interface ``WorkerBackend`` uses (``acquire``, ``release``,
    ``config``)."""

    __slots__ = ("pool", "path")

    def __init__(self, pool: DuckDBWorkerPool, path: str) -> None:
        self.pool = pool
        self.path = path

    @property
    def config(self) -> WorkerConfig:
        return self.pool.config

    @property
    def process(self) -> WorkerProcess | None:
        host = self.pool._where.get(self.path)
        return host.proc if host is not None and host.proc.alive else None

    async def acquire(self) -> WorkerProcess:
        return await self.pool.acquire(self.path)

    def release(self, proc: WorkerProcess) -> None:
        self.pool.release(self.path, proc)

    def close(self) -> None:
        pass

    async def aclose(self) -> None:
        pass


class DuckDBWorker(WorkerBackend):
    """DuckDB in a worker process that the database owns: started on wake,
    exited on sleep. See the module docstring.

    DuckDB options, applied inside the worker exactly as by ``DuckDB``:
    ``memory_limit`` (default ``"256MB"``), ``threads`` (2), ``read_only``,
    ``allow_external_access`` (False), ``config`` (other DuckDB settings),
    ``create``.

    Process options: ``codec`` (``"json"`` or ``"msgpack"``),
    ``memory_limit_mb`` (``RLIMIT_AS`` for the whole worker, Linux only),
    ``kill_grace_ms``, ``max_frame_bytes``. Or ``pool=DuckDBWorkerPool(...)``
    (experimental): share processes with other databases, a few awake
    databases per process; the pool's process options then apply.
    ``spares=SpareWorkers(n)`` (experimental): take a pre-started process
    on wake instead of starting one; share one ``SpareWorkers`` between
    databases with the same process options.

    Capability flags: as in-process DuckDB, plus ``hard_kill``."""

    __slots__ = ()

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        memory_limit: str | None = "256MB",
        threads: int | None = 2,
        read_only: bool = False,
        allow_external_access: bool = False,
        config: Mapping[str, Any] | None = None,
        create: bool = True,
        codec: str = "json",
        memory_limit_mb: int | None = None,
        kill_grace_ms: int = DEFAULT_KILL_GRACE_MS,
        max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
        debug: bool = False,
        pool: DuckDBWorkerPool | None = None,
        spares: SpareWorkers | None = None,
    ) -> None:
        engine_config: dict[str, Any] = {"memory_limit": memory_limit, "threads": threads}
        if read_only:
            engine_config["read_only"] = True
        if allow_external_access:
            engine_config["allow_external_access"] = True
        if config:
            engine_config["config"] = dict(config)
        if not create:
            engine_config["create"] = False
        workers: Any
        if pool is not None:
            workers = pool.slot(os.path.abspath(os.fspath(path)))
            codec = pool.config.codec
        else:
            workers = DatabaseWorker(
                codec=codec,
                memory_limit_mb=memory_limit_mb,
                kill_grace_ms=kill_grace_ms,
                max_frame_bytes=max_frame_bytes,
                debug=debug,
                preload=DUCKDB_PRELOAD,
                spares=spares,
            )
        super().__init__(
            path,
            engine=DUCKDB_ENGINE,
            engine_config=engine_config,
            workers=workers,
            codec=codec,
            memory_limit_mb=memory_limit_mb,
            kill_grace_ms=kill_grace_ms,
            max_frame_bytes=max_frame_bytes,
            debug=debug,
        )
        self.placement = "pool" if pool is not None else "database"  # type: ignore[assignment]

    @property
    def placement_group(self) -> Any:
        """The ``DatabaseWorker`` (or pool slot) this database uses."""
        return self.workers

    @property
    def pid(self) -> int | None:
        """Pid of the running worker process, or None while asleep."""
        p = self.workers.process  # type: ignore[union-attr]
        return p.pid if p is not None else None

    async def open(self, *, write: bool) -> WorkerConnection:
        if write and self.engine_config.get("read_only"):
            raise NotSupported("database is read-only; writes are not allowed")
        return await super().open(write=write)

    async def close(self) -> None:
        """Called by the core on sleep, after every connection closed: wait
        for the worker to exit, so the file lock is released (as closing
        an in-process instance releases it)."""
        await self.placement_group.aclose()
