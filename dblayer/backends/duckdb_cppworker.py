"""Experimental: DuckDB in a worker process written in C++.

``DuckDBCppWorker(path)`` is ``DuckDBWorker`` with the child process
swapped: instead of ``python -m dblayer.worker`` running the in-process
DuckDB backend (a 45 MB process before it opens anything, 160 to 190 ms to
wake), it runs ``duckdb-cpp-worker/build/dblayer-duckdb-worker``, a C++
program that speaks the same protocol (``docs/worker-protocol.md``) and
links the official prebuilt ``libduckdb`` (the same version as the
``duckdb`` Python package, so files are interchangeable) through its C API.

Build the binary first::

    make -C duckdb-cpp-worker

The binary is found through, in order: the ``binary=`` argument, the
``DBLAYER_DUCKDB_CPP_WORKER`` environment variable, then
``duckdb-cpp-worker/build/dblayer-duckdb-worker`` in the source tree.
Without one, constructing a backend raises ``NotSupported`` naming the
build command.

Placement is ``DatabaseWorker``'s: one process owns every connection of
one database (DuckDB locks its file per process) and exits when the
database sleeps. What differs from the Python worker:

* **Concurrency.** The worker runs each connection on a thread of its own,
  so requests on different connections of one database run at the same
  time (a point read is not held up by a long analytical query); its
  ``hello`` says ``"concurrent": true`` and the parent multiplexes requests
  over the pipes (``ConcurrentWorkerProcess``), each with its own kill
  timer. Requests on one connection still run in order.
* **Named operations.** ``hello`` lists the operations the worker runs
  itself (``"ops": ["insert_rows"]``, through DuckDB's appender); any other
  operation, and an ``insert_rows`` the worker declines (a table to create
  or alter, ``ignore``/``replace``), runs in the parent as the core's
  default implementation.
* **No Python in the worker:** the parent never imports ``duckdb``; this
  module describes the engine itself (``LocalDuckDB``).

Codec: MessagePack when the ``msgpack`` package is installed, else JSON.
The worker implements both, with every value type of
``dblayer.worker.protocol.STANDARD_VALUE_TYPES``.
"""

from __future__ import annotations

import asyncio
import os
import weakref
from collections.abc import Callable, Mapping
from dataclasses import replace
from pathlib import Path
from typing import Any

from ..errors import NotSupported, QueryInterrupted, WorkerCrashed
from ..worker.protocol import DEFAULT_MAX_FRAME_BYTES, available_codecs
from .base import Capabilities
from .duckdb import DuckDBDialect, _stat
from .duckdb_worker import DatabaseWorker, DuckDBWorker
from .sqlite_worker import (
    DEFAULT_KILL_GRACE_MS,
    WorkerBackend,
    WorkerConfig,
    WorkerConnection,
    WorkerProcess,
    _intern,
    _unwrap,
    _Waiter,
)

__all__ = [
    "DuckDBCppWorker",
    "LocalDuckDB",
    "ConcurrentWorkerProcess",
    "find_binary",
    "binary_unavailable",
    "BUILD_COMMAND",
    "ENV_VAR",
]

ENV_VAR = "DBLAYER_DUCKDB_CPP_WORKER"
BUILD_COMMAND = "make -C duckdb-cpp-worker"
DEFAULT_BINARY = (
    Path(__file__).resolve().parent.parent.parent / "duckdb-cpp-worker" / "build" / "dblayer-duckdb-worker"
)
LOCAL_ENGINE = "dblayer.backends.duckdb_cppworker:LocalDuckDB"

_found: dict[tuple[str | None, str | None], str] = {}


def find_binary(binary: str | os.PathLike[str] | None = None) -> str:
    """The worker executable: ``binary`` if given, else ``$DBLAYER_DUCKDB_CPP_WORKER``,
    else the default build location. Raises ``NotSupported`` if it is not
    an executable file. Successful lookups are cached."""
    key = (None if binary is None else os.fspath(binary), os.environ.get(ENV_VAR))
    hit = _found.get(key)
    if hit is not None:
        return hit
    if binary is not None:
        candidate, source = Path(binary), "binary="
    elif os.environ.get(ENV_VAR):
        candidate, source = Path(os.environ[ENV_VAR]), f"${ENV_VAR}"
    else:
        candidate, source = DEFAULT_BINARY, "default location"
    if not (candidate.is_file() and os.access(candidate, os.X_OK)):
        raise NotSupported(
            f"DuckDB C++ worker binary not built: run `{BUILD_COMMAND}` "
            f"(looked for {candidate}, from {source})"
        )
    _found[key] = found = str(candidate.resolve())
    return found


def binary_unavailable(binary: str | os.PathLike[str] | None = None) -> str | None:
    """``None`` if the C++ worker can run here, else the reason (for skips)."""
    try:
        find_binary(binary)
    except NotSupported as e:
        return str(e)
    return None


def _default_codec() -> str:
    return "msgpack" if "msgpack" in available_codecs() else "json"


class LocalDuckDB:
    """The engine as the parent sees it: dialect, capabilities and
    ``change_hint()`` of ``dblayer.backends.duckdb.DuckDB`` with the same
    options, without the ``duckdb`` package (the worker is not Python).
    Never opened; ``WorkerBackend`` calls it once to describe the engine,
    and its name travels in ``open`` (the worker accepts DuckDB engines
    only)."""

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
    ) -> None:
        self.path = Path(path).absolute()
        self.dialect = DuckDBDialect()
        # As DuckDB(path, write_queue=True).capabilities.
        self.capabilities = Capabilities(
            concurrent_writers=False,
            transactional_ddl=True,
            implicit_rowid=False,
            cheap_change_hint=True,
            hard_kill=False,
            memory_limit=memory_limit is not None,
            table_definition_sql=True,
            full_text_search=False,
            sleep_releases_everything=True,
            returning=True,
            snapshot_reads=True,
            statement_errors_abort_transaction=True,
        )

    def change_hint(self) -> object:
        return (_stat(self.path), _stat(self.path.with_name(self.path.name + ".wal")))


class ConcurrentWorkerProcess(WorkerProcess):
    """A worker that runs several requests at once (one per connection;
    each connection's requests in order), matched by ``id``.
    ``WorkerProcess`` sends one request at a time; here each owner (a
    connection) has its own lock, and each request its own kill timer, so
    a slow query on one connection does not hold up another."""

    def __init__(self, config: WorkerConfig) -> None:
        super().__init__(config)
        self._owner_locks: weakref.WeakKeyDictionary[Any, asyncio.Lock] = weakref.WeakKeyDictionary()
        self._none_lock = asyncio.Lock()
        self._by_owner: dict[int, tuple[int, object]] = {}

    def _lock_for(self, owner: object) -> asyncio.Lock:
        if owner is None:
            return self._none_lock
        lock = self._owner_locks.get(owner)
        if lock is None:
            lock = self._owner_locks[owner] = asyncio.Lock()
        return lock

    async def call(
        self,
        msg: dict[str, Any],
        *,
        owner: object,
        limit_ms: float | None = None,
        expect_frames: int | None = None,
        on_sent: Callable[[int], None] | None = None,
    ) -> Any:
        async with self._lock_for(owner):
            self._check()
            mid = msg["id"] = msg.get("id") or next(self._ids)
            if expect_frames is not None and msg.get("type") == "more":
                del msg["id"]  # a ``more`` has no id of its own: frames carry the stream's
                mid = msg["stream"]
            fut: asyncio.Future[Any] = self.loop.create_future()
            self._waiters[mid] = _Waiter(fut, owner, expect_frames)
            try:
                self.send(msg)
            except BaseException:
                self._waiters.pop(mid, None)
                raise
            if on_sent is not None:
                on_sent(mid)
            timer = None
            if limit_ms is not None:
                timer = self.loop.call_later(
                    max(limit_ms, 0) / 1000 + self.config.kill_grace_ms / 1000,
                    self._kill_for_time, mid, limit_ms,
                )
            key = id(owner)
            self._by_owner[key] = (mid, owner)
            self.inflight = (mid, owner)
            try:
                reply = await self._wait(fut, mid)
            finally:
                if self._by_owner.get(key, (None,))[0] == mid:
                    del self._by_owner[key]
                if self.inflight is not None and self.inflight[0] == mid:
                    self.inflight = None
                if timer is not None:
                    timer.cancel()
        if expect_frames is None:
            return _unwrap(reply, self)
        return reply

    def inflight_of(self, owner: object) -> int | None:
        """The id of ``owner``'s request in flight, if any."""
        cur = self._by_owner.get(id(owner))
        return cur[0] if cur is not None and cur[1] is owner else None

    def cancel(self, owner: object) -> None:
        mid = self.inflight_of(owner)
        if mid is not None and self.dead is None:
            try:
                self.send({"type": "cancel", "target": mid})
            except Exception:
                pass


class _CppDatabaseWorker(DatabaseWorker):
    process_class = ConcurrentWorkerProcess


class CppWorkerConnection(WorkerConnection):
    """``WorkerConnection`` for the C++ worker: named operations by the
    worker's list, and deadline kills that find this connection's request
    among several in flight."""

    async def run_operation(self, name: str, args: dict[str, Any]) -> object:
        await self._ensure_alive()
        ops = self._proc.hello.get("ops")
        if not isinstance(ops, list) or name not in ops:
            return NotImplemented
        reply = await self._call(self._tx_field({"type": "op", "name": name, "args": args}))
        result = reply["result"]
        if isinstance(result, dict) and result.get("declined"):
            return NotImplemented
        return result

    def _on_deadline_overrun(self, tx: str, deadline_ms: int) -> None:
        self._deadline_timer = None
        if self._tx != tx or self._tx_done:
            return
        proc = self._proc
        for_ids: dict[int, BaseException] = {}
        mid = proc.inflight_of(self) if isinstance(proc, ConcurrentWorkerProcess) else None
        if mid is not None:
            for_ids[mid] = QueryInterrupted(
                f"transaction passed its deadline ({deadline_ms} ms) and the worker did not "
                f"confirm rollback; worker {proc.pid} was killed"
            )
        proc.kill(
            WorkerCrashed(f"worker {proc.pid} was killed: a transaction on it overran its deadline"),
            for_ids=for_ids,
        )


class DuckDBCppWorker(DuckDBWorker):
    """DuckDB in a C++ worker process that the database owns: started on
    wake, exited on sleep. See the module docstring.

    DuckDB options, applied inside the worker as by ``DuckDB``:
    ``memory_limit`` (default ``"256MB"``), ``threads`` (2), ``read_only``,
    ``allow_external_access`` (False), ``config`` (other DuckDB settings),
    ``create``. Process options: ``binary``, ``codec`` (default MessagePack
    when installed), ``memory_limit_mb`` (``RLIMIT_AS``, Linux only),
    ``kill_grace_ms``, ``max_frame_bytes``, ``debug``.

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
        binary: str | os.PathLike[str] | None = None,
        codec: str | None = None,
        memory_limit_mb: int | None = None,
        kill_grace_ms: int = DEFAULT_KILL_GRACE_MS,
        max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
        debug: bool = False,
    ) -> None:
        executable = find_binary(binary)
        codec = codec or _default_codec()
        engine_config: dict[str, Any] = {"memory_limit": memory_limit, "threads": threads}
        if read_only:
            engine_config["read_only"] = True
        if allow_external_access:
            engine_config["allow_external_access"] = True
        if config:
            engine_config["config"] = dict(config)
        if not create:
            engine_config["create"] = False
        workers = _CppDatabaseWorker(
            codec=codec,
            memory_limit_mb=memory_limit_mb,
            kill_grace_ms=kill_grace_ms,
            max_frame_bytes=max_frame_bytes,
            debug=debug,
        )
        workers.config = replace(workers.config, executable=executable)
        WorkerBackend.__init__(
            self,
            path,
            engine=LOCAL_ENGINE,
            engine_config=engine_config,
            workers=workers,  # type: ignore[arg-type]
            codec=codec,
            memory_limit_mb=memory_limit_mb,
            kill_grace_ms=kill_grace_ms,
            max_frame_bytes=max_frame_bytes,
            debug=debug,
        )
        self.config = _intern(replace(self.config, executable=executable))
        self.placement = "database"  # type: ignore[assignment]

    async def open(self, *, write: bool) -> WorkerConnection:
        if write and self.engine_config.get("read_only"):
            raise NotSupported("database is read-only; writes are not allowed")
        assert self.workers is not None
        proc = await self.workers.acquire()
        conn = CppWorkerConnection(self, proc, write=write, shared=True)
        try:
            await conn._open()
        except BaseException:
            await conn.close()
            raise
        return conn
