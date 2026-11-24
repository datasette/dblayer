"""Experimental in-process SQLite backend on apsw (``dblayer[apsw]``), an
alternative to ``SqliteInProcess`` (the standard library ``sqlite3``).

apsw wheels bundle their own current SQLite, so behaviour no longer depends
on which library Python was linked against (Apple's system SQLite, Ubuntu's
3.46, ...). The backend behaves like ``SqliteInProcess`` (same dialect,
schema-reading SQL and error mapping, from ``_sqlite_common``) except where
the docstrings below say why apsw does better.

* **Connections.** Read connections open ``SQLITE_OPEN_READONLY`` with
  ``mode=ro`` (``immutable=1`` for immutable databases); the write
  connection ``mode=rwc``. apsw has no implicit transaction handling to
  switch off: the backend issues BEGIN/COMMIT/ROLLBACK itself.
* **Named in-memory databases** use the ``memdb`` VFS
  (``file:/name?vfs=memdb``): apsw's SQLite is built with
  ``SQLITE_OMIT_SHARED_CACHE``, so the shared-cache URIs ``SqliteInProcess``
  uses do not share anything. memdb has real locks, but coarse ones: a
  write transaction (RESERVED) shuts readers out and an open read
  transaction makes the writer wait, each up to ``busy_timeout``. Readers
  therefore never see uncommitted data (no ``read_uncommitted``), but a
  read issued while a write transaction is open waits for its commit, and
  fails with ``SQLITE_BUSY`` after ``busy_timeout``. That breaks the
  core's contract that reads proceed alongside a write transaction, so
  memory databases here are for single-task use, and the conformance suite
  runs the file variant only. An anchor connection keeps the database
  alive, as on ``SqliteInProcess``.
* **One statement per call,** as the standard module enforces: apsw would
  run every ``;``-separated statement; the backend refuses before running
  any.
* **Exact counts without extra statements.** ``rowcount`` for a write is
  ``Connection.changes()`` (``WITH ... INSERT`` included, no ``select
  changes()``); ``last_pk`` uses ``last_insert_rowid()`` and
  ``set_last_insert_rowid()`` around the insert (no ``select
  last_insert_rowid()`` first), and is right in a case the before/after
  comparison gets wrong (see ``_LASTROWID_SENTINEL``).
* **Result column types.** ``Column.type`` is the declared type of the
  column a result column comes from (``description_full``), ``None`` for
  expressions; the standard module gives ``None`` always.
* **Cancellation and time limits** use one progress handler per connection,
  installed for every statement: it stops the statement when the call's
  deadline passes or when the call was cancelled. A cancel that arrives
  before the statement starts is therefore not lost (the flag is read once
  the statement runs), with no need to repeat ``interrupt()``.
* ``untrusted=True``: read connections get an authorizer that denies
  ATTACH/DETACH, pragma writes and extension loading,
  ``SQLITE_DBCONFIG_DEFENSIVE``, ``trusted_schema`` off, no attached
  databases, and refuse any statement SQLite does not report read-only
  (``sqlite3_stmt_readonly``), such as writes to the temp schema.
* ``memory_limit_mb``: sets SQLite's *process-wide* hard heap limit. See
  ``SqliteApsw`` for what that does and does not promise.
* ``inline_reads=True``: reads on read connections first run on the event
  loop thread under a small opcode budget, and move to the executor when
  they exceed it. Off by default. See ``SqliteApswConnection.execute``.

The ``init(conn)`` hook receives an ``apsw.Connection``. The backend owns
the connection's exec tracer and progress handler; a hook that replaces
them breaks one-statement checks, column types, time limits and cancels.
"""

from __future__ import annotations

import functools
import itertools
import os
import threading
import time
import weakref
from collections.abc import Callable, Sequence
from concurrent.futures import Executor
from typing import TYPE_CHECKING, Any

from ..core.executor import run_blocking
from ..core.params import leading_keyword, statement_count
from ..core.results import Column, Results, WriteResult
from ..core.schema import Schema
from ..errors import NotSupported, QueryError, ResourceLimitExceeded
from ._sqlite_common import (
    FETCH_BATCH,
    WRITE_VERBS,
    SqliteDialectBase,
    SqliteTarget,
    has_fts,
    make_capabilities,
    read_schema,
    row_bytes,
)
from .base import Capabilities, Dialect, NativeParams

if TYPE_CHECKING:
    import apsw

__all__ = ["SqliteApsw", "SqliteApswConnection", "ApswDialect", "apsw_unavailable"]

type InitHook = Callable[["apsw.Connection"], None]


def apsw_unavailable() -> str | None:
    """``None`` when apsw can be imported, else a skip/unavailable reason."""
    try:
        import apsw  # noqa: F401
    except ImportError:
        return "apsw is not installed (uv run --extra apsw ...)"
    return None


def _apsw() -> Any:
    import apsw

    return apsw


@functools.cache
def _version() -> tuple[int, ...]:
    return tuple(int(p) for p in _apsw().sqlite_lib_version().split("."))


class ApswDialect(SqliteDialectBase):
    """The SQLite dialect, with errors read from apsw exceptions."""

    def __init__(self) -> None:
        self.native_alter = self.native_alter_for_version(_version())

    def error_info(self, exc: BaseException) -> tuple[str | None, str, bool] | None:
        apsw = _apsw()
        if not isinstance(exc, apsw.Error):
            return None
        extended = getattr(exc, "extendedresult", None)
        result = getattr(exc, "result", None)
        code = apsw.mapping_extended_result_codes.get(extended) or apsw.mapping_result_codes.get(
            result
        )
        return code, str(exc), isinstance(exc, apsw.ConstraintError)


@functools.cache
def _dialect() -> ApswDialect:
    return ApswDialect()


@functools.cache
def _capabilities(is_memory: bool, memory_limit: bool) -> Capabilities:
    caps = make_capabilities(
        is_memory=is_memory,
        fts=has_fts(_apsw().compile_options),
        version=_version(),
        memory_limit=memory_limit,
    )
    return caps


_ANCHOR_LOCK = threading.Lock()

# Statements the untrusted authorizer lets through with an argument:
# introspection pragmas whose argument names a table or index.
_READ_PRAGMAS_WITH_ARG = frozenset(
    {
        "table_info",
        "table_xinfo",
        "table_list",
        "index_list",
        "index_info",
        "index_xinfo",
        "foreign_key_list",
        "foreign_key_check",
    }
)
_DENIED_FUNCTIONS = frozenset({"load_extension", "fts3_tokenizer"})


def set_process_heap_limit(mb: int | None) -> int:
    """Set SQLite's process-wide hard heap limit (``sqlite3_hard_heap_limit64``)
    for apsw's SQLite; ``None`` or 0 removes it. Returns the previous limit
    in bytes. It covers every apsw connection in the process, not one
    database, and not the standard library ``sqlite3`` module's SQLite."""
    return _apsw().hard_heap_limit(int(mb or 0) * 1024 * 1024)


class SqliteApsw(SqliteTarget):
    """In-process SQLite backend on apsw. Experimental.

    Options beyond ``SqliteInProcess``'s (``path``, ``memory_name``,
    ``immutable``, ``init``, ``executor``, ``busy_timeout``, ``create``):

    * ``untrusted``: lock read connections down for SQL typed by users
      (module docstring). Write connections are unaffected: dblayer's own
      operations need pragmas there.
    * ``memory_limit_mb``: SQLite's hard heap limit, which is **process
      wide**: it bounds the memory every apsw connection in the process
      uses together, so one database's large query can make a query on
      another database fail with ``ResourceLimitExceeded`` (SQLITE_NOMEM).
      It cannot promise any one database a share, and it counts only
      SQLite's own allocations (page cache, sorting, values being built),
      not the Python objects rows become. Setting it on several backends
      sets one limit: the last value wins. ``capabilities.memory_limit`` is
      true only for a backend given one. Per connection, what *is*
      enforceable is ``max_value_bytes`` (``SQLITE_LIMIT_LENGTH``: the
      largest string or blob a statement may build) and the page cache
      size (``cache_size_kb``, ``PRAGMA cache_size``).
    * ``inline_reads`` / ``inline_budget``: see ``SqliteApswConnection.execute``.
    * ``stream_prefetch_rows``: rows a stream fetches per executor hop when
      that is more than the caller's ``chunk_rows`` (chunks are then served
      from the buffer without a hop).
    * ``statement_cache_size``: apsw's prepared-statement cache (default 100).
    * ``trace_sql``: keep ``sqlite3_expanded_sql`` of the last statement on
      each connection (``last_expanded_sql``), for debugging.

    Capability rounding as ``SqliteInProcess``, except ``memory_limit``
    (above).
    """

    __slots__ = (
        "init",
        "executor",
        "busy_timeout",
        "capabilities",
        "untrusted",
        "memory_limit_mb",
        "max_value_bytes",
        "cache_size_kb",
        "inline_reads",
        "inline_budget",
        "stream_prefetch_rows",
        "statement_cache_size",
        "trace_sql",
        "_anchor",
        "_conns",
    )

    def __init__(
        self,
        path: str | os.PathLike[str] | None = None,
        *,
        memory_name: str | None = None,
        immutable: bool = False,
        init: InitHook | None = None,
        executor: Executor | None = None,
        busy_timeout: float = 5.0,
        create: bool = True,
        untrusted: bool = False,
        memory_limit_mb: int | None = None,
        max_value_bytes: int | None = None,
        cache_size_kb: int | None = None,
        inline_reads: bool = False,
        inline_budget: int = 10_000,
        stream_prefetch_rows: int | None = None,
        statement_cache_size: int = 100,
        trace_sql: bool = False,
    ) -> None:
        reason = apsw_unavailable()
        if reason:
            raise NotSupported(reason)
        self._init_target(path, memory_name, immutable, create)
        self.init = init
        self.executor = executor
        self.busy_timeout = busy_timeout
        self.untrusted = untrusted
        self.memory_limit_mb = memory_limit_mb
        self.max_value_bytes = max_value_bytes
        self.cache_size_kb = cache_size_kb
        self.inline_reads = inline_reads
        self.inline_budget = inline_budget
        self.stream_prefetch_rows = stream_prefetch_rows
        self.statement_cache_size = statement_cache_size
        self.trace_sql = trace_sql
        self.capabilities = _capabilities(memory_name is not None, bool(memory_limit_mb))
        self._anchor: Any = None
        self._conns: weakref.WeakSet[SqliteApswConnection] | None = None

    dialect: Dialect

    @classmethod
    def memory(cls, name: str | None = None, **kwargs: Any) -> "SqliteApsw":
        """A named in-memory database (memdb VFS). Without a name, a unique
        one is generated, so the database is private to this backend."""
        return cls(memory_name=name or cls.unique_memory_name(), **kwargs)

    def __repr__(self) -> str:
        return f"SqliteApsw({self._target_repr()})"

    @staticmethod
    def sqlite_version() -> str:
        """The SQLite version apsw bundles (or was built against)."""
        return _apsw().sqlite_lib_version()

    # -- Backend protocol ----------------------------------------------------

    async def open(self, *, write: bool) -> "SqliteApswConnection":
        if write and self.immutable:
            raise NotSupported("database is immutable; writes are not allowed")
        raw = await run_blocking(self.executor, self._connect, write)
        conn = SqliteApswConnection(self, raw, write=write)
        if self._conns is None:
            self._conns = weakref.WeakSet()
        self._conns.add(conn)
        return conn

    async def close(self) -> None:
        with _ANCHOR_LOCK:
            anchor, self._anchor = self._anchor, None
        if anchor is not None:
            anchor.close()

    def memory_status(self) -> dict[str, int]:
        """Memory held by this backend's open connections, summed
        (``sqlite3_db_status``): page cache, schema and prepared
        statements, in bytes, plus the connection count. No connection is
        opened. Process-wide figures are in ``process_memory_status()``."""
        out = {"connections": 0, "cache_used": 0, "schema_used": 0, "stmt_used": 0}
        for conn in list(self._conns or ()):
            s = conn.memory_status()
            if s is None:
                continue
            out["connections"] += 1
            for k in ("cache_used", "schema_used", "stmt_used"):
                out[k] += s[k]
        return out

    # -- internals -----------------------------------------------------------

    def _apsw_uri(self, write: bool) -> str:
        if self.memory_name is not None:
            import urllib.parse

            return f"file:/{urllib.parse.quote(self.memory_name)}?vfs=memdb"
        return self._uri(write)

    def _flags(self, write: bool) -> int:
        apsw = _apsw()
        flags = apsw.SQLITE_OPEN_URI
        if write or (self.memory_name is not None and self._anchor is None):
            flags |= apsw.SQLITE_OPEN_READWRITE
            if self.create or self.memory_name is not None:
                flags |= apsw.SQLITE_OPEN_CREATE
        else:
            flags |= apsw.SQLITE_OPEN_READONLY
        return flags

    def _connect(self, write: bool) -> Any:
        apsw = _apsw()
        if self.memory_limit_mb:
            set_process_heap_limit(self.memory_limit_mb)
        if self.memory_name is not None:
            with _ANCHOR_LOCK:
                if self._anchor is None:
                    self._anchor = apsw.Connection(
                        self._apsw_uri(True),
                        flags=apsw.SQLITE_OPEN_URI
                        | apsw.SQLITE_OPEN_READWRITE
                        | apsw.SQLITE_OPEN_CREATE,
                    )
        elif self._prepare_file(write):
            self._recover_hot_journal()
        raw = apsw.Connection(
            self._apsw_uri(write),
            flags=self._flags(write),
            statementcachesize=self.statement_cache_size,
        )
        try:
            raw.set_busy_timeout(int(self.busy_timeout * 1000))
            if self.memory_name is not None and not write:
                raw.execute("pragma query_only = 1")
            if self.max_value_bytes is not None:
                raw.limit(apsw.SQLITE_LIMIT_LENGTH, int(self.max_value_bytes))
            if self.cache_size_kb is not None:
                raw.execute(f"pragma cache_size = {-int(self.cache_size_kb)}")
            if self.init is not None:
                self.init(raw)
            if self.untrusted and not write:
                _lock_down(raw)  # after init, whose own pragmas it would deny
        except BaseException:
            raw.close()
            raise
        return raw

    def _recover_hot_journal(self) -> None:
        """As ``SqliteInProcess._recover_hot_journal``: read once through a
        read-write connection so SQLite rolls a hot journal back."""
        if not self.has_journal():
            return
        apsw = _apsw()
        try:
            rw = apsw.Connection(
                self._rw_uri(), flags=apsw.SQLITE_OPEN_URI | apsw.SQLITE_OPEN_READWRITE
            )
        except apsw.Error:
            return  # read-only file or directory: nothing we can do
        try:
            rw.set_busy_timeout(int(self.busy_timeout * 1000))
            rw.execute("select count(*) from sqlite_master").fetchall()
        except apsw.Error:
            pass
        finally:
            rw.close()


SqliteApsw.dialect = property(lambda self: _dialect())  # type: ignore[assignment]


def _lock_down(raw: Any) -> None:
    """``untrusted=True`` on a read connection."""
    apsw = _apsw()
    raw.config(apsw.SQLITE_DBCONFIG_DEFENSIVE, 1)
    raw.config(apsw.SQLITE_DBCONFIG_TRUSTED_SCHEMA, 0)
    raw.config(apsw.SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION, 0)
    raw.limit(apsw.SQLITE_LIMIT_ATTACHED, 0)
    ok, deny = apsw.SQLITE_OK, apsw.SQLITE_DENY
    op_pragma, op_attach, op_detach = apsw.SQLITE_PRAGMA, apsw.SQLITE_ATTACH, apsw.SQLITE_DETACH
    op_function = apsw.SQLITE_FUNCTION

    def authorizer(op: int, p1: str | None, p2: str | None, db: str | None, trig: str | None) -> int:
        if op == op_pragma:
            if p2 is None or (p1 or "").lower() in _READ_PRAGMAS_WITH_ARG:
                return ok
            return deny
        if op == op_attach or op == op_detach:
            return deny
        if op == op_function and (p2 or "").lower() in _DENIED_FUNCTIONS:
            return deny
        return ok

    raw.authorizer = authorizer


# last_pk: before an INSERT the connection's last_insert_rowid is set to this
# value and read back afterwards; a change means this statement inserted a
# rowid row. SqliteInProcess compares the value before and after instead,
# which misses an insert that reuses the previous insert's rowid (insert id
# 5, delete it, insert id 5 again: last_pk None). Only an insert of rowid
# -2**63 itself is missed here. The previous value is restored when nothing
# was inserted, so SQL's last_insert_rowid() is unchanged.
_LASTROWID_SENTINEL = -(2**63)

_HANDLER_STEPS = 1000  # opcodes between progress-handler calls
_INLINE_STEPS = 100  # ... while a statement runs inline on the event loop
_INLINE_WALL_S = 0.002  # an inline statement never holds the loop longer


class _Budget(Exception):
    """Inline execution went over its budget; finish it on the executor."""


class SqliteApswConnection:
    """One apsw connection. Used by one task at a time.

    Per statement the connection's exec tracer records the result columns
    (``description_full``) and whether SQLite says the statement is
    read-only, and the progress handler enforces the deadline, the cancel
    flag and the inline budget."""

    def __init__(self, backend: SqliteApsw, raw: Any, *, write: bool) -> None:
        self._backend = backend
        self._raw = raw
        self.write = write
        self._closed = False
        self._cancelled = False
        self._deadline: float | None = None
        self._inline_ticks: int | None = None  # handler calls left inline
        self._inline_until = 0.0
        self._over_budget = False
        self._description: tuple[tuple[Any, ...], ...] = ()
        self._readonly = True
        self._require_readonly = backend.untrusted and not write
        self._many: list[int] | None = None
        self._trace = backend.trace_sql
        self.last_expanded_sql: str | None = None
        self.inline_runs = 0
        self.inline_fallbacks = 0
        raw.exec_trace = self._exec_trace
        raw.set_progress_handler(self._progress, _HANDLER_STEPS)

    # -- hooks (run on whichever thread runs the statement) ------------------

    def _exec_trace(self, cursor: Any, sql: str, bindings: Any) -> bool:
        many = self._many
        if many is not None:
            # executemany: called before each item; changes() is the
            # previous item's count.
            if many[1]:
                many[0] += self._raw.changes()
            many[1] = 1
            return True
        self._description = cursor.description_full
        self._readonly = cursor.is_readonly
        if self._trace:
            self.last_expanded_sql = cursor.expanded_sql
        if self._require_readonly and not self._readonly:
            raise QueryError("only read-only statements are allowed here", "SQLITE_READONLY")
        if self._inline_ticks is not None and not self._readonly:
            raise _Budget()
        return True

    def _progress(self) -> bool:
        if self._cancelled:
            return True
        d = self._deadline
        if d is not None and time.perf_counter() >= d:
            return True
        ticks = self._inline_ticks
        if ticks is not None:
            if ticks <= 0 or time.perf_counter() >= self._inline_until:
                self._over_budget = True
                return True
            self._inline_ticks = ticks - 1
        return False

    # -- plumbing --------------------------------------------------------------

    @property
    def in_transaction(self) -> bool:
        return not self._closed and self._raw.in_transaction

    async def _run[T](self, fn: Callable[..., T], *args: Any) -> T:
        self._cancelled = False
        return await run_blocking(self._backend.executor, fn, *args, on_cancel=self._on_cancel)

    def _on_cancel(self) -> None:
        """The awaiting task was cancelled (event loop thread). The flag
        stops the statement at the progress handler's next call whether or
        not it has started yet; ``interrupt()`` stops one that is running
        straight away. Neither touches statements of later calls: the flag
        is cleared when the next call starts, and ``sqlite3_interrupt`` only
        affects statements running at the time."""
        self._cancelled = True
        self._raw.interrupt()

    def _set_limit(self, time_limit_ms: int | None) -> None:
        if time_limit_ms is None:
            self._deadline = None
            return
        self._deadline = time.perf_counter() + time_limit_ms / 1000
        if time_limit_ms <= 20:
            # ~1000 VM instructions take about 0.08ms (Datasette #1679).
            self._raw.set_progress_handler(self._progress, 1)

    def _clear_limit(self, time_limit_ms: int | None) -> None:
        self._deadline = None
        if time_limit_ms is not None and time_limit_ms <= 20:
            self._raw.set_progress_handler(self._progress, _HANDLER_STEPS)

    def memory_status(self) -> dict[str, int] | None:
        """``sqlite3_db_status`` for this connection: bytes of page cache,
        schema and prepared statements, and the page cache hit and miss
        counters. ``None`` once closed. Safe from any thread: SQLite reads
        them under the connection's mutex."""
        if self._closed:
            return None
        apsw = _apsw()
        raw = self._raw
        return {
            "cache_used": raw.status(apsw.SQLITE_DBSTATUS_CACHE_USED)[0],
            "schema_used": raw.status(apsw.SQLITE_DBSTATUS_SCHEMA_USED)[0],
            "stmt_used": raw.status(apsw.SQLITE_DBSTATUS_STMT_USED)[0],
            "cache_hit": raw.status(apsw.SQLITE_DBSTATUS_CACHE_HIT)[0],
            "cache_miss": raw.status(apsw.SQLITE_DBSTATUS_CACHE_MISS)[0],
        }

    # -- execute ---------------------------------------------------------------

    async def execute(
        self,
        sql: str,
        params: NativeParams,
        *,
        time_limit_ms: int | None,
        max_rows: int | None,
        max_result_bytes: int | None = None,
    ) -> Results:
        """Run one statement on the executor.

        With ``inline_reads`` on a read connection, first on the event loop
        thread: SQLite must report the prepared statement read-only, the
        busy timeout is 0, and the progress handler stops it after
        ``inline_budget`` opcodes (counted in steps of 100) or 2 ms,
        whichever comes first. If it is stopped, it is re-run from the start
        on the executor, which is safe because it was read-only. If it
        produced rows inline but went over budget between rows, the same
        statement is continued on the executor from where it was: apsw
        allows a cursor to move between threads as long as two never use it
        at once, which the pool guarantees."""
        if self._backend.inline_reads and not self.write:
            done = self._execute_inline(sql, params, time_limit_ms, max_rows, max_result_bytes)
            if isinstance(done, Results):
                return done
            if done is not None:
                return await self._run(done)
        return await self._run(
            self._execute_recovering, sql, params, time_limit_ms, max_rows, max_result_bytes
        )

    def _execute_inline(
        self,
        sql: str,
        params: NativeParams,
        time_limit_ms: int | None,
        max_rows: int | None,
        max_result_bytes: int | None,
    ) -> Results | Callable[[], Results] | None:
        """Results, or a continuation to run on the executor, or ``None`` to
        re-run from the start there."""
        apsw = _apsw()
        raw = self._raw
        self._cancelled = False
        self._over_budget = False
        hard = max(2, self._backend.inline_budget // _INLINE_STEPS)
        self._inline_ticks = hard
        now = time.perf_counter()
        self._inline_until = now + _INLINE_WALL_S
        # Between row batches, hand off once half the budget is used, so
        # the next batch is unlikely to be cut off mid-step (and re-run).
        soft_ticks = hard // 2
        soft_until = now + _INLINE_WALL_S / 2
        raw.set_progress_handler(self._progress, _INLINE_STEPS)
        raw.set_busy_timeout(0)
        handed_off = False
        try:
            run = _Run(self, sql, params, time_limit_ms, max_rows, max_result_bytes)
            try:
                result = run.start()
                while result is None:
                    ticks = self._inline_ticks
                    if (ticks is not None and ticks < soft_ticks) or (
                        time.perf_counter() >= soft_until
                    ):
                        # Over budget between rows: continue on a thread.
                        handed_off = True
                        self.inline_fallbacks += 1
                        return functools.partial(self._continue, run)
                    result = run.step()
                self.inline_runs += 1
                return result
            except _Budget:
                run.abandon()
            except apsw.InterruptError:
                run.abandon()
                if not self._over_budget:
                    raise  # the time limit, not the budget
            except apsw.BusyError:
                run.abandon()
            except BaseException:
                run.abandon()
                raise
            self.inline_fallbacks += 1
            return None
        finally:
            self._inline_ticks = None
            if not handed_off:
                self._clear_limit(time_limit_ms)
            raw.set_progress_handler(self._progress, _HANDLER_STEPS)
            raw.set_busy_timeout(int(self._backend.busy_timeout * 1000))

    def _continue(self, run: "_Run") -> Results:
        try:
            result = None
            while result is None:
                result = run.step()
            return result
        except BaseException:
            run.abandon()
            raise
        finally:
            self._clear_limit(run.time_limit_ms)

    def _execute_recovering(self, *args: Any) -> Results:
        """``_execute``; on a read connection that meets a hot journal left
        by a killed writer (``SQLITE_READONLY_ROLLBACK``), recover through a
        read-write connection and retry once."""
        apsw = _apsw()
        try:
            return self._execute(*args)
        except apsw.ReadOnlyError as e:
            if self.write or getattr(e, "extendedresult", None) != apsw.SQLITE_READONLY_ROLLBACK:
                raise
            self._backend._recover_hot_journal()
            return self._execute(*args)

    def _execute(
        self,
        sql: str,
        params: NativeParams,
        time_limit_ms: int | None,
        max_rows: int | None,
        max_result_bytes: int | None,
    ) -> Results:
        run = _Run(self, sql, params, time_limit_ms, max_rows, max_result_bytes)
        try:
            result = run.start()
            while result is None:
                result = run.step()
            return result
        except BaseException:
            run.abandon()
            raise
        finally:
            self._clear_limit(time_limit_ms)

    async def execute_batch(
        self,
        statements: Sequence[tuple[str, NativeParams]],
        *,
        time_limit_ms: int | None,
        max_rows: int | None,
    ) -> list[Results]:
        """Every statement in one executor hop (``SupportsBatch``)."""

        def run() -> list[Results]:
            return [
                self._execute(sql, params, time_limit_ms, max_rows, None)
                for sql, params in statements
            ]

        return await self._run(run)

    async def execute_many(self, sql: str, params_seq: Sequence[NativeParams]) -> WriteResult:
        def run() -> WriteResult:
            _check_one_statement(sql)
            is_write = leading_keyword(sql) in WRITE_VERBS
            if not params_seq:
                return WriteResult(rowcount=0 if is_write else -1)
            many = [0, 0]
            self._many = many
            cur = self._raw.cursor()
            try:
                cur.executemany(sql, params_seq)
                many[0] += self._raw.changes()
            finally:
                self._many = None
                cur.close()
            return WriteResult(rowcount=many[0] if is_write else -1)

        return await self._run(run)

    def stream(
        self,
        sql: str,
        params: NativeParams,
        *,
        chunk_rows: int,
        time_limit_ms: int | None = None,
        max_result_bytes: int | None = None,
    ) -> "_ApswStream":
        return _ApswStream(self, sql, params, chunk_rows, time_limit_ms, max_result_bytes)

    # -- transactions ------------------------------------------------------------

    def _exec(self, sql: str) -> None:
        self._raw.execute(sql).fetchall()

    async def begin(self, mode: str) -> None:
        if mode == "write":
            sql = "BEGIN IMMEDIATE"
        elif mode == "read":
            sql = "BEGIN"
        else:
            raise ValueError(f"unknown transaction mode {mode!r}")
        await self._run(self._exec, sql)

    async def commit(self) -> None:
        if self._raw.in_transaction:
            await self._run(self._exec, "COMMIT")

    async def rollback(self) -> None:
        # Checked without a thread hop: no statement is running on this
        # connection while the pool or the owning task calls rollback().
        if not self._closed and self._raw.in_transaction:
            await self._run(self._exec, "ROLLBACK")

    # -- schema --------------------------------------------------------------

    async def schema_version(self) -> object:
        def run() -> int:
            return self._raw.execute("pragma schema_version").fetchall()[0][0]

        return await self._run(run)

    async def schema(self) -> Schema:
        return await self._run(self._schema)

    def _schema(self) -> Schema:
        raw = self._raw
        own_tx = not raw.in_transaction
        if own_tx:
            self._exec("BEGIN")  # one consistent snapshot for every query
        try:
            return read_schema(raw, _apsw().SQLError)
        finally:
            if own_tx and raw.in_transaction:
                self._exec("ROLLBACK")

    # -- misc ----------------------------------------------------------------

    async def interrupt(self) -> None:
        self._raw.interrupt()  # thread-safe; interrupts whatever is running

    async def close(self) -> None:
        if not self._closed:
            self._closed = True
            await run_blocking(self._backend.executor, self._raw.close)


def _check_one_statement(sql: str) -> None:
    """apsw runs every statement in ``sql``; the standard module refuses
    more than one. Refuse before anything runs. Scans only SQL that has a
    semicolon."""
    if ";" in sql and statement_count(sql) > 1:
        raise QueryError("You can only execute one statement at a time.", None)


class _Run:
    """One ``execute`` call's statement: start it, fetch rows in batches,
    finish. Kept as an object so that inline execution can hand it to the
    executor between batches."""

    __slots__ = (
        "conn", "sql", "params", "time_limit_ms", "max_rows", "max_bytes", "cur",
        "rows", "size", "limit", "is_write", "is_insert", "rowid_before", "columns",
    )

    def __init__(
        self,
        conn: SqliteApswConnection,
        sql: str,
        params: NativeParams,
        time_limit_ms: int | None,
        max_rows: int | None,
        max_result_bytes: int | None,
    ) -> None:
        self.conn = conn
        self.sql = sql
        self.params = params
        self.time_limit_ms = time_limit_ms
        self.max_rows = max_rows
        self.max_bytes = max_result_bytes
        self.cur: Any = None
        self.rows: list[tuple[Any, ...]] = []
        self.size = 0
        self.limit = None if max_rows is None else max_rows + 1
        self.columns: tuple[Column, ...] = ()

    def start(self) -> Results | None:
        _check_one_statement(self.sql)
        conn = self.conn
        raw = conn._raw
        verb = leading_keyword(self.sql)
        self.is_write = verb in WRITE_VERBS
        self.is_insert = verb in ("INSERT", "REPLACE")
        if conn._cancelled:
            raise _apsw().InterruptError("interrupted")
        conn._set_limit(self.time_limit_ms)
        if self.is_insert:
            self.rowid_before = raw.last_insert_rowid()
            raw.set_last_insert_rowid(_LASTROWID_SENTINEL)
        conn._description = ()
        cur = raw.cursor()
        self.cur = cur
        cur.execute(self.sql, self.params)
        self.columns = tuple(Column(d[0], d[1]) for d in conn._description)
        if not self.columns:
            return self.finish()
        return self.step()

    def step(self) -> Results | None:
        """Fetch one batch; the Results when done, else ``None``."""
        rows = self.rows
        limit = self.limit
        n = FETCH_BATCH if limit is None else min(FETCH_BATCH, limit - len(rows))
        batch = list(itertools.islice(self.cur, n)) if n > 0 else []
        if self.max_bytes is not None:
            for r in batch:
                self.size += row_bytes(r)
            if self.size > self.max_bytes:
                raise ResourceLimitExceeded(f"result exceeded max_result_bytes={self.max_bytes}")
        rows.extend(batch)
        if len(batch) == n and n > 0:
            return None
        return self.finish()

    def finish(self) -> Results:
        conn = self.conn
        raw = conn._raw
        rows = self.rows
        truncated = False
        if self.max_rows is not None and len(rows) > self.max_rows:
            truncated = True
            del rows[self.max_rows :]
            if self.is_write:
                for _ in self.cur:  # a write must run to completion
                    pass
        cur, self.cur = self.cur, None
        cur.close()
        rowcount = raw.changes() if self.is_write else -1
        last_pk = None
        if self.is_insert:
            after = raw.last_insert_rowid()
            if after == _LASTROWID_SENTINEL:
                raw.set_last_insert_rowid(self.rowid_before)
            elif rowcount > 0:
                last_pk = after
        return Results.from_tuples(
            self.columns, rows, truncated=truncated, rowcount=rowcount, last_pk=last_pk
        )

    def abandon(self) -> None:
        cur, self.cur = self.cur, None
        if cur is not None:
            try:
                cur.close(True)
            except Exception:
                pass
        if getattr(self, "is_insert", False):
            raw = self.conn._raw
            if raw.last_insert_rowid() == _LASTROWID_SENTINEL:
                raw.set_last_insert_rowid(self.rowid_before)


class _ApswStream:
    """Lazily fetched chunks; one executor hop per chunk (or per
    ``stream_prefetch_rows`` rows), so a slow consumer applies backpressure.
    ``time_limit_ms`` is a budget of time spent inside SQLite across all
    hops, as on ``SqliteInProcess``."""

    def __init__(
        self,
        conn: SqliteApswConnection,
        sql: str,
        params: NativeParams,
        chunk_rows: int,
        time_limit_ms: int | None,
        max_result_bytes: int | None,
    ) -> None:
        if chunk_rows < 1:
            raise ValueError("chunk_rows must be at least 1")
        self._conn = conn
        self._sql = sql
        self._params = params
        self._chunk_rows = chunk_rows
        self._fetch_rows = max(chunk_rows, conn._backend.stream_prefetch_rows or 0)
        self._remaining_ms: float | None = time_limit_ms
        self._max_bytes = max_result_bytes
        self._bytes = 0
        self._cur: Any = None
        self._buffer: list[tuple[Any, ...]] = []
        self._exhausted = False
        self._done = False
        self.columns: tuple[Column, ...] | None = None

    def __aiter__(self) -> "_ApswStream":
        return self

    async def __anext__(self) -> list[tuple[Any, ...]]:
        if self._done:
            raise StopAsyncIteration
        if len(self._buffer) < self._chunk_rows and not self._exhausted:
            try:
                await self._conn._run(self._fill)
            except BaseException:
                await self.aclose()
                raise
        if not self._buffer:
            await self.aclose()
            raise StopAsyncIteration
        chunk = self._buffer[: self._chunk_rows]
        del self._buffer[: self._chunk_rows]
        return chunk

    def _fill(self) -> None:
        conn = self._conn
        start = time.perf_counter()
        ms = None if self._remaining_ms is None else max(int(self._remaining_ms), 0)
        conn._set_limit(ms)
        try:
            if self._remaining_ms is not None and self._remaining_ms <= 0:
                raise _apsw().InterruptError("interrupted")
            if conn._cancelled:
                raise _apsw().InterruptError("interrupted")
            if self._cur is None:
                _check_one_statement(self._sql)
                conn._description = ()
                self._cur = conn._raw.cursor()
                self._cur.execute(self._sql, self._params)
                self.columns = tuple(Column(d[0], d[1]) for d in conn._description)
                if not self.columns:
                    self._exhausted = True
                    return
            want = self._fetch_rows - len(self._buffer)
            chunk = list(itertools.islice(self._cur, want))
        finally:
            conn._clear_limit(ms)
            if self._remaining_ms is not None:
                self._remaining_ms -= (time.perf_counter() - start) * 1000
        if len(chunk) < want:
            self._exhausted = True
        if self._max_bytes is not None:
            for r in chunk:
                self._bytes += row_bytes(r)
            if self._bytes > self._max_bytes:
                raise ResourceLimitExceeded(f"result exceeded max_result_bytes={self._max_bytes}")
        self._buffer.extend(chunk)

    async def aclose(self) -> None:
        if self._done:
            return
        self._done = True
        self._buffer = []
        cur, self._cur = self._cur, None
        if cur is not None:
            await run_blocking(self._conn._backend.executor, cur.close, True)


def process_memory_status() -> dict[str, int]:
    """Process-wide SQLite memory (apsw's SQLite only), in bytes:
    ``sqlite3_status(SQLITE_STATUS_MEMORY_USED)`` now and its high-water
    mark, and the current soft and hard heap limits (0: none)."""
    apsw = _apsw()
    used, high = apsw.status(apsw.SQLITE_STATUS_MEMORY_USED)
    return {
        "memory_used": used,
        "memory_high_water": high,
        "soft_heap_limit": apsw.soft_heap_limit(-1),
        "hard_heap_limit": apsw.hard_heap_limit(-1),
    }

