"""Experimental: DuckDB in a worker process written in Rust.

``DuckDBRustWorker(path)`` is ``DuckDBWorker`` with the child process
swapped: instead of ``python -m dblayer.worker`` with the DuckDB engine (a
45 MB footprint before it opens anything, ~160 ms to start) it runs
``duckdb-rs-worker/target/release/dblayer-duckdb-rsworker``, a Rust program
linked against libduckdb that speaks the same protocol
(``docs/worker-protocol.md``). The parent side is the ``WorkerBackend``
machinery with ``DatabaseWorker`` placement (one process owns every
connection of a database, because DuckDB locks its file per process).

Build the binary first::

    make -C duckdb-rs-worker

The binary is found through, in order: the ``binary=`` argument, the
``DBLAYER_DUCKDB_RS_WORKER`` environment variable, then
``duckdb-rs-worker/target/release/dblayer-duckdb-rsworker`` in the source
tree. Without one, constructing a backend raises ``NotSupported`` naming
the build command.

Differences from ``DuckDBWorker``:

* **Concurrent requests.** The worker runs each connection on its own
  thread and says ``"concurrent": true`` in ``hello``; the parent then sends
  requests for different connections without waiting for earlier replies,
  so a point read is not held up by a long analytical query on another
  connection of the same database. (The Python worker serves one request at
  a time.)
* **Named operations** (``op``) are Python functions the worker cannot run
  (``hello`` says ``"ops": false``), so the core runs them with one round
  trip per statement, except ``insert_rows``, which goes through DuckDB's
  appender in the worker (the ``append`` operation), as in-process DuckDB
  does.
* The parent never imports ``duckdb``: the dialect and capability flags
  come from ``dblayer.backends.duckdb`` without the driver.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Mapping
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
    "DuckDBRustWorker",
    "find_binary",
    "binary_unavailable",
    "BUILD_COMMAND",
    "ENV_VAR",
    "RUST_ENGINE",
]

ENV_VAR = "DBLAYER_DUCKDB_RS_WORKER"
BUILD_COMMAND = "make -C duckdb-rs-worker"
DEFAULT_BINARY = (
    Path(__file__).resolve().parent.parent.parent
    / "duckdb-rs-worker" / "target" / "release" / "dblayer-duckdb-rsworker"
)
RUST_ENGINE = "dblayer.backends.duckdb_rsworker:engine"

_found: dict[tuple[str | None, str | None], str] = {}


def find_binary(binary: str | os.PathLike[str] | None = None) -> str:
    """The Rust worker executable: ``binary`` if given, else
    ``$DBLAYER_DUCKDB_RS_WORKER``, else the default build location. Raises
    ``NotSupported`` (naming the build command) if it is not an executable
    file. Successful lookups are cached."""
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
            f"Rust DuckDB worker binary not built: run `{BUILD_COMMAND}` "
            f"(looked for {candidate}, from {source})"
        )
    _found[key] = found = str(candidate.resolve())
    return found


def binary_unavailable(binary: str | os.PathLike[str] | None = None) -> str | None:
    """``None`` if the Rust worker can run here, else the reason (for skips)."""
    try:
        find_binary(binary)
    except NotSupported as e:
        return str(e)
    return None


# -- the engine, as the parent sees it -------------------------------------------------


class _Engine:
    """What ``WorkerBackend`` learns from an engine without opening it:
    dialect, capability flags and ``change_hint()``, exactly as
    ``dblayer.backends.duckdb.DuckDB`` reports them, but without needing the
    ``duckdb`` package in the parent."""

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        memory_limit: str | None = "256MB",
        threads: int | None = 2,
        read_only: bool = False,
        write_queue: bool = True,
        allow_external_access: bool = False,
        config: Mapping[str, Any] | None = None,
        create: bool = True,
    ) -> None:
        self.path = Path(path).absolute()
        self.dialect = DuckDBDialect()
        self.capabilities = Capabilities(
            concurrent_writers=not write_queue,
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


def engine(path: str | os.PathLike[str], **config: Any) -> _Engine:
    """The ``engine`` named in ``open`` messages (``RUST_ENGINE``)."""
    return _Engine(path, **config)


# -- concurrent requests on one process ---------------------------------------------------


class ConcurrentWorkerProcess(WorkerProcess):
    """A ``WorkerProcess`` whose requests are not serialised when the worker
    says ``"concurrent": true`` in ``hello``: each connection in the worker
    has its own thread, so requests for different connections run at the
    same time. Every in-flight request is tracked by its owner, for
    ``cancel``."""

    def __init__(self, config: WorkerConfig) -> None:
        super().__init__(config)
        self.calls: dict[int, object] = {}
        """In-flight request id -> owner (the connection that sent it)."""

    @property
    def concurrent(self) -> bool:
        return bool(self.hello.get("concurrent"))

    def inflight_of(self, owner: object) -> int | None:
        for mid, o in self.calls.items():
            if o is owner:
                return mid
        return None

    async def call(
        self,
        msg: dict[str, Any],
        *,
        owner: object,
        limit_ms: float | None = None,
        expect_frames: int | None = None,
        on_sent: Any = None,
    ) -> Any:
        if not self.concurrent:
            return await super().call(msg, owner=owner, limit_ms=limit_ms, expect_frames=expect_frames, on_sent=on_sent)
        self._check()
        mid = msg["id"] = msg.get("id") or next(self._ids)
        if expect_frames is not None and msg.get("type") == "more":
            del msg["id"]
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
        self.calls[mid] = owner
        self.inflight = (mid, owner)
        try:
            reply = await self._wait(fut, mid)
        finally:
            self.calls.pop(mid, None)
            if self.inflight is not None and self.inflight[0] == mid:
                self.inflight = None
            if timer is not None:
                timer.cancel()
        if expect_frames is None:
            return _unwrap(reply, self)
        return reply

    def cancel(self, owner: object) -> None:
        mid = self.inflight_of(owner)
        if mid is None or self.dead is not None:
            return
        try:
            self.send({"type": "cancel", "target": mid})
        except Exception:
            pass


class RustDatabaseWorker(DatabaseWorker):
    """``DatabaseWorker`` placement whose processes are
    ``ConcurrentWorkerProcess`` running the Rust binary."""

    process_class = ConcurrentWorkerProcess


class RustWorkerConnection(WorkerConnection):
    """A connection inside the Rust worker: as ``WorkerConnection``, with
    the deadline kill aimed at this connection's own request, and
    ``insert_rows`` through DuckDB's appender."""

    def _on_deadline_overrun(self, tx: str, deadline_ms: int) -> None:
        self._deadline_timer = None
        if self._tx != tx or self._tx_done:
            return
        proc = self._proc
        mid = proc.inflight_of(self) if isinstance(proc, ConcurrentWorkerProcess) else None
        for_ids = {}
        if mid is not None:
            for_ids[mid] = QueryInterrupted(
                f"transaction passed its deadline ({deadline_ms} ms) and the worker did not "
                f"confirm rollback; worker {proc.pid} was killed"
            )
        proc.kill(
            WorkerCrashed(f"worker {proc.pid} was killed: a transaction on it overran its deadline"),
            for_ids=for_ids,
        )

    async def run_operation(self, name: str, args: dict[str, Any]) -> object:
        """``insert_rows`` through the appender (``append`` in the worker),
        as ``DuckDBConnection.run_operation`` does in-process; everything
        else declines, so the core runs the default implementation."""
        await self._ensure_alive()
        if name != "insert_rows" or "append" not in (self._proc.hello.get("native_ops") or ()):
            return NotImplemented
        if args.get("ignore") or args.get("replace") or not args.get("rows"):
            return NotImplemented
        return await _insert_rows(self, args["table"], args["rows"], args.get("pk"), bool(args.get("alter")))


async def _insert_rows(conn: RustWorkerConnection, table: str, rows: list[dict[str, Any]], pk: Any, alter: bool) -> object:
    from ..core import operations as ops
    from .duckdb import _HERE, _ConnectionTx

    existing = await conn.execute(
        "SELECT c.column_name FROM duckdb_columns() c JOIN duckdb_tables() t "
        "ON c.table_oid = t.table_oid "
        f"WHERE t.{_HERE.replace(' AND ', ' AND t.')} AND t.table_name = $name "
        "ORDER BY c.column_index",
        {"name": table},
        time_limit_ms=None,
        max_rows=None,
    )
    have_cols = [r[0] for r in existing.rows]
    added: tuple[str, ...] = ()
    if not have_cols:
        created, added = await ops._ensure_table(
            _ConnectionTx(conn), table, rows, ops._pk_tuple(pk), alter  # type: ignore[arg-type]
        )
    else:
        created = False
        if alter:
            have = {c.casefold() for c in have_cols}
            types = ops._column_types(rows)
            missing = [c for c in types if c.casefold() not in have]
            for c in missing:
                sql = conn._backend.dialect.add_column_sql(table, ops.ColumnDef(c, types[c]))
                await conn.execute(sql, [], time_limit_ms=None, max_rows=0)
            added = tuple(missing)
    total = 0
    for cols, group in ops._groups(rows):
        msg = {
            "type": "op",
            "name": "append",
            "args": {"table": table, "columns": list(cols), "rows": [[r[c] for c in cols] for r in group]},
        }
        reply = await conn._call(conn._tx_field(msg))
        total += int(reply["result"]["rowcount"])
    return ops.OperationResult(rowcount=total, table_created=created, columns_added=added)


# -- the backend ------------------------------------------------------------------------------


def _default_codec() -> str:
    return "msgpack" if "msgpack" in available_codecs() else "json"


class DuckDBRustWorker(DuckDBWorker):
    """DuckDB in a Rust worker process that the database owns: started on
    wake, exited on sleep. Options as ``DuckDBWorker`` (``memory_limit``,
    ``threads``, ``read_only``, ``allow_external_access``, ``config``,
    ``create``, ``codec``, ``memory_limit_mb``, ``kill_grace_ms``,
    ``max_frame_bytes``), plus ``binary`` (see ``find_binary``). The codec
    defaults to MessagePack when available. ``pool`` and ``spares`` are not
    supported (a Rust worker starts in a few milliseconds)."""

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
        codec: str | None = None,
        memory_limit_mb: int | None = None,
        kill_grace_ms: int = DEFAULT_KILL_GRACE_MS,
        max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
        debug: bool = False,
        binary: str | os.PathLike[str] | None = None,
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
        workers = RustDatabaseWorker(
            codec=codec,
            memory_limit_mb=memory_limit_mb,
            kill_grace_ms=kill_grace_ms,
            max_frame_bytes=max_frame_bytes,
            debug=debug,
        )
        workers.config = _intern(replace(workers.config, executable=executable))
        # WorkerBackend's constructor, not DuckDBWorker's: the engine is
        # described by ``engine()`` here, so the parent needs no duckdb.
        WorkerBackend.__init__(
            self,
            path,
            engine=RUST_ENGINE,
            engine_config=engine_config,
            workers=workers,  # type: ignore[arg-type]
            codec=codec,
            memory_limit_mb=memory_limit_mb,
            kill_grace_ms=kill_grace_ms,
            max_frame_bytes=max_frame_bytes,
            debug=debug,
        )
        self.placement = "database"  # type: ignore[assignment]
        self.config = _intern(replace(self.config, executable=executable))

    async def open(self, *, write: bool) -> WorkerConnection:
        if write and self.engine_config.get("read_only"):
            raise NotSupported("database is read-only; writes are not allowed")
        proc = await self.workers.acquire()  # type: ignore[union-attr]
        conn = RustWorkerConnection(self, proc, write=write, shared=True)
        try:
            await conn._open()
        except BaseException:
            await conn.close()
            raise
        return conn
