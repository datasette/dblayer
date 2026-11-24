"""Experimental: PostgreSQL compiled to WebAssembly (PGlite), in a Node.js
worker process, one data directory per database.

``PGliteWorker(path)`` is a real PostgreSQL (PGlite embeds PostgreSQL 18)
with no server: ``path`` is a directory holding one PostgreSQL cluster,
opened by ``node pglite-worker/src/main.mjs`` when the database wakes and
released when it sleeps, so many small isolated PostgreSQL databases can sit
under one ``Registry`` the way SQLite files do. The SQL dialect, error table
and catalog queries are those of the server backends
(``dblayer.backends._postgres_common``).

Set it up once::

    npm ci --prefix pglite-worker

Lookup, in order: ``node=`` / ``script=`` arguments, the ``DBLAYER_NODE`` /
``DBLAYER_PGLITE_WORKER`` environment variables, then ``node`` on ``PATH``
and ``pglite-worker/src/main.mjs`` in the source tree. Without Node, the
script or its ``node_modules``, constructing a backend raises
``NotSupported`` naming the setup command.

One session per database
------------------------
A PGlite instance is one PostgreSQL backend in single-user mode: one
session, one statement at a time. Every pooled connection of a database (the
writer and up to ``max_read_connections`` readers) is a *name* for that
session inside one Node process (``DatabaseWorker`` placement), with a mode:

* **Requests run one at a time**, in the order the parent sends them.
* **An open transaction holds the session.** From ``begin`` until
  ``commit``/``rollback`` (or the worker's own deadline rollback), requests
  from every other connection wait *in the parent* (``PGliteProcess``),
  before anything is sent: a read on another connection waits for the write
  transaction to end, and never sees its uncommitted rows. A waiting request
  with a time limit gives up with ``QueryInterrupted`` when the limit passes;
  the wait counts against it. A paused ``stream`` holds the session the same
  way (it runs in a transaction block of its own).
* **A read transaction's snapshot** is REPEATABLE READ as on a server, but
  trivially so: nothing else can write while it is open, so it sees exactly
  the last committed state, and a writer waits for it to end.
* Read connections run with ``default_transaction_read_only = on``; inside a
  transaction each statement runs in a savepoint (a failed statement does not
  abort the transaction), as on the psycopg backend.
* Session state is shared: a ``SET`` on one connection is seen by all, as are
  temporary tables and prepared statements.

Capability flags: ``concurrent_writers`` false; ``concurrent_sessions``
false (the flag added for this backend: other connections' statements wait
while a transaction is open); ``snapshot_reads`` true (see above);
``statement_errors_abort_transaction`` false; ``hard_kill`` true; ``memory_limit``
true (the WebAssembly memory maximum, on every platform);
``cheap_change_hint`` true (see below).

Stopping a query
----------------
A query runs inside WebAssembly on Node's main thread, which blocks the
event loop: the worker cannot read a ``cancel`` while it runs, and
``statement_timeout`` never fires (PostgreSQL's timers are JavaScript
timers here). The only stop is the parent's SIGKILL, at the time limit plus
``kill_grace_ms``; it loses the session, so the open transaction (there is
at most one) and any paused stream with it. The data directory survives a
kill: PostgreSQL's WAL recovery runs on the next open (see
``tests/faults/test_pglite_faults.py``).

Persistence
-----------
PGlite writes through Emscripten's NODEFS straight to files under ``path``.
It runs with ``fsync=off`` and NODEFS has no fsync: a committed transaction
survives the process being killed (it is in the operating system's page
cache), not a power loss or kernel crash. Two processes opening one
directory would corrupt it, and PGlite does not prevent it (its
``postmaster.pid`` holds a fake pid): the parent takes an ``flock`` on
``path/dblayer.lock`` and hands the locked descriptor to the worker, so the
lock lasts exactly as long as the worker process.

``change_hint()`` is the modification times and names of the WAL segment
files and ``global/pg_control``: every committed change writes WAL, so a
schema change always moves it (as does any write; the core then asks for
``schema_version``). It reads no database and wakes nothing.

Memory
------
V8 compiles the 9 MB PostgreSQL module lazily with Liftoff and tiers hot
functions up with TurboFan; the TurboFan compiles alone cost the process
about 600 MB. The worker therefore runs with ``--liftoff-only`` (reads are
as fast, CPU-heavy queries about 1.5x slower); ``optimize=True`` turns
TurboFan back on. ``shared_buffers`` defaults to 16 MB here (PGlite: 128 MB).

What the worker cannot do: named operations run in the parent (``hello``
says ``"ops": false``), except ``insert_rows``, done with ``COPY ... FROM
STDIN`` as on the psycopg backend; ``schema()`` is read in the parent with
the shared catalog queries (one ``batch`` message, one implicit
transaction).
"""

from __future__ import annotations

import asyncio
import errno
import fcntl
import functools
import json
import os
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from pathlib import Path
from typing import Any

from ..core.results import Results, WriteResult
from ..core.schema import Schema
from ..errors import NotSupported, QueryError, QueryInterrupted, WorkerCrashed
from ..worker.protocol import DEFAULT_MAX_FRAME_BYTES, available_codecs, results_from_wire
from ._postgres_common import VERSION_SQL, PostgresDialectBase, read_schema
from .base import Capabilities, NativeParams
from .duckdb_worker import DatabaseWorker
from .sqlite_nodeworker import NODE_ENV_VAR, find_node
from .sqlite_worker import (
    DEFAULT_KILL_GRACE_MS,
    WorkerBackend,
    WorkerConfig,
    WorkerConnection,
    WorkerProcess,
    WorkerStream,
    _intern,
)

__all__ = [
    "PGliteWorker",
    "PGliteDialect",
    "PGliteProcess",
    "find_script",
    "unavailable",
    "SETUP_COMMAND",
    "SCRIPT_ENV_VAR",
    "NODE_ENV_VAR",
    "LOCK_FILE",
]

SCRIPT_ENV_VAR = "DBLAYER_PGLITE_WORKER"
SETUP_COMMAND = "npm ci --prefix pglite-worker"
DEFAULT_SCRIPT = Path(__file__).resolve().parent.parent.parent / "pglite-worker" / "src" / "main.mjs"
LOCAL_ENGINE = "dblayer.backends.pglite_worker:LocalPGlite"
LOCK_FILE = "dblayer.lock"
SCHEMA = "public"

_found: dict[tuple[str | None, str | None], str] = {}


def find_script(script: str | os.PathLike[str] | None = None) -> str:
    """The worker script: ``script`` if given, else ``$DBLAYER_PGLITE_WORKER``,
    else ``pglite-worker/src/main.mjs`` in the source tree. Raises
    ``NotSupported`` if it, or PGlite in ``node_modules`` next to it, is
    missing. Cached."""
    key = (None if script is None else os.fspath(script), os.environ.get(SCRIPT_ENV_VAR))
    hit = _found.get(key)
    if hit is not None:
        return hit
    if script is not None:
        candidate, source = Path(script), "script="
    elif os.environ.get(SCRIPT_ENV_VAR):
        candidate, source = Path(os.environ[SCRIPT_ENV_VAR]), f"${SCRIPT_ENV_VAR}"
    else:
        candidate, source = DEFAULT_SCRIPT, "default location"
    if not candidate.is_file():
        raise NotSupported(f"PGlite worker script not found: {candidate} (from {source})")
    package = candidate.resolve().parent.parent
    if not (package / "node_modules" / "@electric-sql" / "pglite" / "package.json").is_file():
        raise NotSupported(
            f"the PGlite worker's npm packages are not installed: run `{SETUP_COMMAND}` "
            f"(looked in {package / 'node_modules'})"
        )
    _found[key] = found = str(candidate.resolve())
    return found


def unavailable(node: str | os.PathLike[str] | None = None, script: str | os.PathLike[str] | None = None) -> str | None:
    """``None`` if the PGlite worker can run here, else the reason (for skips)."""
    try:
        find_node(node)
        find_script(script)
    except NotSupported as e:
        return str(e)
    return None


def _default_codec() -> str:
    return "msgpack" if "msgpack" in available_codecs() else "json"


# -- the engine, as the parent sees it --------------------------------------------------


class PGliteDialect(PostgresDialectBase):
    """The PostgreSQL dialect. Errors arrive from the worker already mapped
    (by SQLSTATE, the table of ``map_sqlstate``), so there is no driver
    exception to map here."""


_DIALECT = PGliteDialect()

CAPABILITIES = Capabilities(
    concurrent_writers=False,
    transactional_ddl=True,
    implicit_rowid=False,
    cheap_change_hint=True,
    hard_kill=True,
    memory_limit=True,
    table_definition_sql=False,
    full_text_search=False,
    sleep_releases_everything=True,
    returning=True,
    snapshot_reads=True,
    statement_errors_abort_transaction=False,
    replace_keeps_views=False,
)


def _stat_token(p: str) -> tuple[int, int] | None:
    try:
        st = os.stat(p)
    except OSError:
        return None
    return (st.st_mtime_ns, st.st_size)


def data_dir_hint(path: str) -> object:
    """A token that moves whenever the cluster in ``path`` commits anything:
    the WAL segment files' names and modification times, and
    ``global/pg_control`` (rewritten at checkpoints). No database access."""
    wal = os.path.join(path, "pg_wal")
    try:
        names = sorted(n for n in os.listdir(wal) if len(n) == 24)
    except OSError:
        return None
    return (
        tuple((n, _stat_token(os.path.join(wal, n))) for n in names),
        _stat_token(os.path.join(path, "global", "pg_control")),
    )


class LocalPGlite:
    """Dialect, capabilities and ``change_hint()`` of a PGlite database, for
    ``WorkerBackend`` (the worker is not Python). Never opened."""

    def __init__(self, path: str | os.PathLike[str], **config: Any) -> None:
        self.path = os.path.abspath(os.fspath(path))
        self.dialect = _DIALECT
        self.capabilities = CAPABILITIES

    def change_hint(self) -> object:
        return data_dir_hint(self.path)


# -- the process: one session, held by one connection at a time --------------------------


class PGliteProcess(WorkerProcess):
    """A PGlite worker process: one request in flight (the worker runs them
    one at a time anyway, so a time limit counts from when it is sent), and
    a gate in front of it. While one owner (a connection) holds the session
    (an open transaction, a stream paused between chunks) another owner's
    request waits here, before it is sent; with a time limit it waits at
    most that long and raises ``QueryInterrupted``.

    ``start()`` takes an exclusive ``flock`` on ``lock_path`` and passes the
    descriptor to the worker, so the lock is held exactly as long as the
    worker process lives."""

    def __init__(self, config: WorkerConfig, *, lock_path: str | None = None, lock_timeout: float = 5.0) -> None:
        super().__init__(config)
        self.lock_path = lock_path
        self.lock_timeout = lock_timeout
        self._lock_fd: int | None = None
        self.holder: object | None = None
        self._released = asyncio.Event()
        self._released.set()
        self._cancel_timers: dict[int, asyncio.TimerHandle] = {}
        self.timings: dict[str, float] = {}
        """The worker's start-up breakdown (ms), from its first ``open``:
        ``boot_ms`` (Node and modules), ``compile_ms`` (WebAssembly),
        ``open_ms`` (PGlite start, recovery, initdb when new),
        ``first_query_ms``."""
        self.waited_ms = 0.0
        """Total time requests spent waiting for another connection's hold."""

    # -- the lock ---------------------------------------------------------------

    def popen_options(self) -> dict[str, Any]:
        if self._lock_fd is None:
            return {}
        return {"pass_fds": (self._lock_fd,)}

    async def _take_lock(self) -> None:
        if self.lock_path is None:
            return
        os.makedirs(os.path.dirname(self.lock_path), exist_ok=True)
        fd = os.open(self.lock_path, os.O_RDWR | os.O_CREAT, 0o644)
        end = time.monotonic() + self.lock_timeout
        delay = 0.005
        try:
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except OSError as e:
                    if e.errno not in (errno.EAGAIN, errno.EWOULDBLOCK, errno.EACCES):
                        raise
                    if time.monotonic() >= end:
                        holder = ""
                        try:
                            holder = os.pread(fd, 64, 0).decode("ascii", "replace").strip()
                        except OSError:
                            pass
                        raise QueryError(
                            f"PGlite database {os.path.dirname(self.lock_path)} is in use by another process"
                            + (f" (pid {holder})" if holder else ""),
                            "DATABASE_LOCKED",
                        ) from None
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 0.1)
        except BaseException:
            os.close(fd)
            raise
        self._lock_fd = fd

    async def start(self) -> None:
        await self._take_lock()
        try:
            await super().start()
        finally:
            fd, self._lock_fd = self._lock_fd, None
            if fd is not None:
                if self.pid is not None:
                    try:
                        os.ftruncate(fd, 0)
                        os.pwrite(fd, f"{self.pid}\n".encode(), 0)
                    except OSError:
                        pass
                os.close(fd)  # the worker holds its copy (and the lock) until it exits

    # -- the gate -----------------------------------------------------------------

    def _hold(self, owner: object) -> None:
        if self.holder is not owner:
            self.holder = owner
            self._released.clear()

    def _release(self, owner: object | None = None) -> None:
        if owner is None or self.holder is owner:
            self.holder = None
            self._released.set()

    async def _wait_turn(self, owner: object, limit_ms: float | None) -> float | None:
        """Wait until no other owner holds the session. Returns what is
        left of ``limit_ms``."""
        if self.holder is None or self.holder is owner or owner is None:
            return limit_ms
        t0 = time.perf_counter()
        end = None if limit_ms is None else t0 + limit_ms / 1000
        while self.holder is not None and self.holder is not owner and self.dead is None:
            remaining = None if end is None else end - time.perf_counter()
            if remaining is not None and remaining <= 0:
                waited = (time.perf_counter() - t0) * 1000
                self.waited_ms += waited
                raise QueryInterrupted(
                    f"query interrupted (time limit exceeded): waited {waited:.0f} ms for another "
                    "connection's transaction or stream on this database to end (PGlite has one session)"
                )
            try:
                await asyncio.wait_for(self._released.wait(), remaining)
            except TimeoutError:
                continue
        waited = (time.perf_counter() - t0) * 1000
        self.waited_ms += waited
        if limit_ms is None:
            return None
        return max(limit_ms - waited, 0)

    async def call(
        self,
        msg: dict[str, Any],
        *,
        owner: object,
        limit_ms: float | None = None,
        expect_frames: int | None = None,
        on_sent: Callable[[int], None] | None = None,
    ) -> Any:
        if msg.get("type") not in ("open", "more", "cancel", "close", "rollback"):
            limit_ms = await self._wait_turn(owner, limit_ms)
        return await super().call(msg, owner=owner, limit_ms=limit_ms, expect_frames=expect_frames, on_sent=on_sent)

    def _dispatch(self, msg: dict[str, Any]) -> None:
        mid = msg.get("id")
        if mid is None:
            if msg.get("type") == "expired":
                self._release()
        else:
            w = self._waiters.get(mid)
            if w is not None and w.owner is not None:
                if msg.get("in_tx"):
                    self._hold(w.owner)
                elif w.expect is not None and "rows" in msg and not msg.get("end"):
                    self._hold(w.owner)  # a stream: open until its end frame
                elif "result" in msg or "ok" in msg or "error" in msg or msg.get("end"):
                    self._release(w.owner)
        super()._dispatch(msg)
        if msg.get("fatal") and self.dead is None:
            self.kill(WorkerCrashed(f"worker {self.pid} exited after a fatal error: {msg['error'].get('message')}"))

    async def _wait(self, fut: asyncio.Future[Any], mid: int) -> Any:
        """As ``WorkerProcess._wait``, except that a cancelled request still
        running ``kill_grace_ms`` after the ``cancel`` was sent is stopped by
        SIGKILL before the cancellation propagates (the worker cannot hear
        the ``cancel``; ``WorkerProcess._wait`` gives up waiting at the grace
        without killing, which would leave the query running and the next
        request queued behind it)."""
        try:
            return await asyncio.shield(fut)
        except asyncio.CancelledError:
            if fut.done() or self.dead is not None:
                raise
            try:
                self.send({"type": "cancel", "target": mid})
            except Exception:
                raise asyncio.CancelledError() from None
            end = time.monotonic() + self.config.kill_grace_ms / 1000
            while not fut.done() and self.dead is None:
                remaining = end - time.monotonic()
                if remaining <= 0:
                    self.kill(WorkerCrashed(f"worker {self.pid} was killed to stop a cancelled query"))
                    break
                try:
                    await asyncio.wait({fut}, timeout=remaining)
                except asyncio.CancelledError:
                    continue
            if fut.done() and not fut.cancelled():
                fut.exception()
            raise

    def cancel(self, owner: object) -> None:
        """``interrupt()``: the worker cannot read a ``cancel`` while a query
        runs (it blocks the event loop), so a request still in flight
        ``kill_grace_ms`` after the cancel was sent is stopped by SIGKILL;
        the call raises ``QueryInterrupted``. Losing the process loses the
        session, which only the cancelled connection could be using."""
        cur = self.inflight
        if cur is None or cur[1] is not owner or self.dead is not None:
            return
        mid = cur[0]
        if mid in self._cancel_timers:
            return
        try:
            self.send({"type": "cancel", "target": mid})
        except Exception:
            return

        def stop() -> None:
            self._cancel_timers.pop(mid, None)
            if self.dead is None and mid in self._waiters:
                interrupted = QueryInterrupted(
                    "query interrupted (cancelled): a running PGlite query cannot be reached, "
                    f"so worker {self.pid} was killed"
                )
                self.kill(WorkerCrashed(f"worker {self.pid} was killed to stop a cancelled query"),
                          for_ids={mid: interrupted})

        self._cancel_timers[mid] = self.loop.call_later(self.config.kill_grace_ms / 1000, stop)

    def _teardown(self, exc: BaseException, for_ids: Mapping[int, BaseException]) -> None:
        for t in self._cancel_timers.values():
            t.cancel()
        self._cancel_timers.clear()
        super()._teardown(exc, for_ids)
        self._release()


class PGliteDatabaseWorker(DatabaseWorker):
    """``DatabaseWorker`` placement whose processes are ``PGliteProcess``
    holding the data directory's lock."""

    def __init__(self, *, lock_path: str, lock_timeout: float, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.process_class = functools.partial(  # type: ignore[assignment]
            PGliteProcess, lock_path=lock_path, lock_timeout=lock_timeout
        )


# -- connections ------------------------------------------------------------------------


def _json_columns(types: Sequence[Any] | None) -> list[int]:
    if not types:
        return []
    return [i for i, t in enumerate(types) if t in ("json", "jsonb")]


def _load_json(result: dict[str, Any]) -> dict[str, Any]:
    """json/jsonb values arrive as their text; parse them as psycopg does
    (``json.loads``), arrays of them included."""
    cols = _json_columns(result.get("types"))
    rows = result.get("rows")
    if cols and rows:
        for row in rows:
            for i in cols:
                v = row[i]
                if isinstance(v, str):
                    row[i] = json.loads(v)
    return result


class PGliteStream(WorkerStream):
    async def _exchange(self) -> None:
        before = len(self._buffer)
        await super()._exchange()
        cols = _json_columns([c.type for c in self.columns] if self.columns else None)
        if cols:
            for k in range(before, len(self._buffer)):
                chunk = self._buffer[k]
                self._buffer[k] = [
                    tuple(json.loads(v) if i in cols and isinstance(v, str) else v for i, v in enumerate(r))
                    for r in chunk
                ]


class PGliteConnection(WorkerConnection):
    """A dblayer connection: a name for the database's one PGlite session,
    with a mode. ``schema()`` and ``schema_version()`` run the shared
    catalog queries through the worker."""

    async def _open(self) -> None:
        reply = await self._proc.call(self._backend._open_message(self.write), owner=self)
        self._conn_id = reply["conn"]
        self._proc.notice_handlers[self._conn_id] = self._on_notice
        if isinstance(self._proc, PGliteProcess) and "timings" in reply:
            self._proc.timings = dict(reply["timings"])

    async def execute(
        self,
        sql: str,
        params: NativeParams,
        *,
        time_limit_ms: int | None,
        max_rows: int | None,
        max_result_bytes: int | None = None,
    ) -> Results:
        msg: dict[str, Any] = {"type": "execute", "sql": sql}
        if params:
            msg["params"] = list(params) if not isinstance(params, list) else params
        if time_limit_ms is not None:
            msg["time_limit_ms"] = time_limit_ms
        if max_rows is not None:
            msg["max_rows"] = max_rows
        if max_result_bytes is not None:
            msg["max_result_bytes"] = max_result_bytes
        reply = await self._call(self._tx_field(msg), limit_ms=time_limit_ms)
        return results_from_wire(_load_json(reply["result"]))

    async def execute_batch(
        self,
        statements: Sequence[tuple[str, NativeParams]],
        *,
        time_limit_ms: int | None,
        max_rows: int | None,
    ) -> list[Results]:
        msg: dict[str, Any] = {
            "type": "batch",
            "statements": [[s, list(p) if p else []] for s, p in statements],
        }
        if time_limit_ms is not None:
            msg["time_limit_ms"] = time_limit_ms
        if max_rows is not None:
            msg["max_rows"] = max_rows
        limit = None if time_limit_ms is None else time_limit_ms * max(len(statements), 1)
        reply = await self._call(self._tx_field(msg), limit_ms=limit)
        return [results_from_wire(_load_json(r)) for r in reply["result"]]

    def stream(
        self,
        sql: str,
        params: NativeParams,
        *,
        chunk_rows: int,
        time_limit_ms: int | None = None,
        max_result_bytes: int | None = None,
    ) -> WorkerStream:
        if chunk_rows < 1:
            raise ValueError("chunk_rows must be at least 1")
        return PGliteStream(self, sql, list(params) if params else [], chunk_rows, time_limit_ms, max_result_bytes)

    async def _fetch_many(self, queries: Sequence[str], schema: str) -> list[list[Any]]:
        """Every catalog query in one ``batch`` message: one round trip, and
        (outside a transaction) one implicit transaction, so one snapshot."""
        msg: dict[str, Any] = {"type": "batch", "statements": [[q, [schema]] for q in queries]}
        reply = await self._call(self._tx_field(msg))
        return [r.get("rows", []) for r in reply["result"]]

    async def schema(self) -> Schema:
        return await read_schema(self._fetch_many, SCHEMA)

    async def schema_version(self) -> object:
        reply = await self._call(self._tx_field({"type": "execute", "sql": VERSION_SQL, "params": [SCHEMA]}))
        rows = reply["result"].get("rows") or [[None]]
        return rows[0][0]

    async def run_operation(self, name: str, args: dict[str, Any]) -> object:
        """``insert_rows`` by ``COPY ... FROM STDIN`` (the worker's ``copy``
        message), for the cases the psycopg backend copies; everything else
        declines, and the core runs the default implementation here."""
        if name != "insert_rows" or not args.get("rows") or args.get("ignore") or args.get("replace"):
            return NotImplemented
        if self._tx is None and not self._in_tx:
            return NotImplemented
        from ..core.operations import OperationResult, _ensure_table, _groups
        from ._postgres_common import ConnTx
        from .postgres_psycopg import _copyable

        rows: list[dict[str, Any]] = args["rows"]
        tx = ConnTx(self)  # type: ignore[arg-type]
        created, added = await _ensure_table(tx, args["table"], rows, tuple(args.get("pk") or ()), bool(args.get("alter")))
        info = (await tx.schema()).tables.get(args["table"])
        types = {c.name: c.type for c in info.columns} if info else {}
        d = self._backend.dialect
        total = 0
        for cols, group in _groups(rows):
            records = [[r[c] for c in cols] for r in group]
            if all(_copyable(types.get(c), [rec[i] for rec in records]) for i, c in enumerate(cols)):
                statement = (
                    f"COPY {d.quote_identifier(args['table'])} ({d.quote_identifiers(cols)}) FROM STDIN"
                )
                reply = await self._call(self._tx_field({"type": "copy", "statement": statement, "rows": records}))
                total += max(reply["result"]["rowcount"], 0)
            else:
                r = await tx.execute_many(d.insert_sql(args["table"], cols), records)
                total += max(r.rowcount, 0)
        return OperationResult(rowcount=total, table_created=created, columns_added=added)

    # ConnTx needs these three (``_postgres_common._NativeConn``).

    @property
    def backend(self) -> Any:
        return self._backend

    async def run_native(self, sql: str, args: list[Any]) -> Results:
        reply = await self._call(self._tx_field({"type": "execute", "sql": sql, "params": args}))
        return results_from_wire(_load_json(reply["result"]))

    async def run_native_many(self, sql: str, seq: list[list[Any]]) -> WriteResult:
        reply = await self._call(self._tx_field({"type": "execute_many", "sql": sql, "params_seq": seq}))
        return WriteResult(rowcount=reply["result"]["rowcount"])

    async def cached_schema(self) -> Schema:
        version = await self.schema_version()
        b = self._backend
        memo = getattr(b, "_schema_memo", None)
        if memo is not None and memo.version == version:
            return memo
        schema = await read_schema(self._fetch_many, SCHEMA)
        b._schema_memo = schema
        return schema


# -- the backend --------------------------------------------------------------------------


class PGliteWorker(WorkerBackend):
    """PostgreSQL (PGlite, WebAssembly) in a Node.js worker process that the
    database owns: started on wake, exited on sleep. ``path`` is the data
    directory (created on first open unless ``create=False``). See the
    module docstring.

    Options:

    * ``node``, ``script``: see ``find_node`` / ``find_script``.
    * ``shared_buffers``: PostgreSQL's (default ``"16MB"``).
    * ``settings``: other PostgreSQL settings, ``{"work_mem": "4MB"}``,
      passed as ``-c name=value`` at start-up.
    * ``template``: a data directory copied into ``path`` on first open
      instead of running initdb (create one with a first
      ``PGliteWorker(template)`` open and sleep).
    * ``optimize``: let V8 tier hot WebAssembly up to TurboFan (faster
      CPU-heavy queries, about 600 MB more memory per process).
    * ``memory_limit_mb``: the WebAssembly memory maximum (at least 128 MB,
      PGlite's initial size); past it PostgreSQL reports "out of memory"
      (``ResourceLimitExceeded``) and carries on.
    * ``codec`` (default MessagePack when installed), ``kill_grace_ms``,
      ``max_frame_bytes``, ``debug``; ``lock_timeout``: seconds to wait for
      another process's lock on ``path``.
    """

    __slots__ = ("pglite_options", "_schema_memo")

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        node: str | os.PathLike[str] | None = None,
        script: str | os.PathLike[str] | None = None,
        shared_buffers: str = "16MB",
        settings: Mapping[str, Any] | None = None,
        template: str | os.PathLike[str] | None = None,
        create: bool = True,
        optimize: bool = False,
        codec: str | None = None,
        memory_limit_mb: int | None = None,
        kill_grace_ms: int = DEFAULT_KILL_GRACE_MS,
        max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
        lock_timeout: float = 5.0,
        debug: bool = False,
    ) -> None:
        node_exe = find_node(node)
        script_path = find_script(script)
        codec = codec or _default_codec()
        abspath = os.path.abspath(os.fspath(path))
        config: dict[str, Any] = {"shared_buffers": shared_buffers}
        if settings:
            config["settings"] = {str(k): str(v) for k, v in settings.items()}
        if template is not None:
            config["template"] = os.path.abspath(os.fspath(template))
        if not create:
            config["create"] = False
        workers = PGliteDatabaseWorker(
            lock_path=os.path.join(abspath, LOCK_FILE),
            lock_timeout=lock_timeout,
            codec=codec,
            memory_limit_mb=memory_limit_mb,
            kill_grace_ms=kill_grace_ms,
            max_frame_bytes=max_frame_bytes,
            debug=debug,
        )
        node_args = () if optimize else ("--liftoff-only",)
        workers.config = _intern(replace(workers.config, executable=node_exe, executable_args=(*node_args, script_path)))
        super().__init__(
            abspath,
            engine=LOCAL_ENGINE,
            engine_config=config,
            workers=workers,  # type: ignore[arg-type]
            codec=codec,
            memory_limit_mb=memory_limit_mb,
            kill_grace_ms=kill_grace_ms,
            max_frame_bytes=max_frame_bytes,
            debug=debug,
        )
        self.config = workers.config
        self.capabilities = CAPABILITIES
        self.placement = "database"  # type: ignore[assignment]
        self.pglite_options = config
        self._schema_memo: Schema | None = None

    def __repr__(self) -> str:
        return f"PGliteWorker({self._path!r})"

    @property
    def placement_group(self) -> PGliteDatabaseWorker:
        assert isinstance(self.workers, PGliteDatabaseWorker)
        return self.workers

    @property
    def process(self) -> PGliteProcess | None:
        """The live worker process, if the database is awake."""
        p = self.placement_group.process
        return p if isinstance(p, PGliteProcess) else None

    @property
    def pid(self) -> int | None:
        p = self.process
        return p.pid if p is not None else None

    async def open(self, *, write: bool) -> WorkerConnection:
        proc = await self.placement_group.acquire()
        conn = PGliteConnection(self, proc, write=write, shared=True)
        try:
            await conn._open()
        except BaseException:
            await conn.close()
            raise
        return conn

    def change_hint(self) -> object:
        return data_dir_hint(self._path)

    async def close(self) -> None:
        """On sleep, after every connection closed: wait for the worker to
        exit (PGlite has closed the cluster, and the lock is free)."""
        self._schema_memo = None
        await self.placement_group.aclose()
