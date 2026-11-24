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
    WorkerConnection,
    WorkerProcess,
)

__all__ = ["DuckDBWorker", "DatabaseWorker", "DUCKDB_ENGINE"]

DUCKDB_ENGINE = "dblayer.backends.duckdb:DuckDB"


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
                proc = WorkerProcess(self.config)
                t0 = time.perf_counter()
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


class DuckDBWorker(WorkerBackend):
    """DuckDB in a worker process that the database owns: started on wake,
    exited on sleep. See the module docstring.

    DuckDB options, applied inside the worker exactly as by ``DuckDB``:
    ``memory_limit`` (default ``"256MB"``), ``threads`` (2), ``read_only``,
    ``allow_external_access`` (False), ``config`` (other DuckDB settings),
    ``create``.

    Process options: ``codec`` (``"json"`` or ``"msgpack"``),
    ``memory_limit_mb`` (``RLIMIT_AS`` for the whole worker, Linux only),
    ``kill_grace_ms``, ``max_frame_bytes``.

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
        workers = DatabaseWorker(
            codec=codec,
            memory_limit_mb=memory_limit_mb,
            kill_grace_ms=kill_grace_ms,
            max_frame_bytes=max_frame_bytes,
            debug=debug,
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
        self.placement = "database"  # type: ignore[assignment]

    @property
    def placement_group(self) -> DatabaseWorker:
        assert isinstance(self.workers, DatabaseWorker)
        return self.workers

    @property
    def pid(self) -> int | None:
        """Pid of the running worker process, or None while asleep."""
        p = self.placement_group.process
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
