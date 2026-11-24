"""DuckDB backend, using the ``duckdb`` package on the shared thread pool.

Facts this module relies on were checked against DuckDB 2.0.1 by
``scripts/duckdb_verify.py``.

* **One instance per file**, opened on the first ``open()`` after a wake
  and closed by ``Backend.close()`` (which the core calls on sleep). Closing
  the instance closes every cursor made from it and releases the file lock,
  so another process can then open the file read-write. Each pooled
  "connection" is a cursor of that instance (DuckDB: its own transaction and
  session), used by one thread at a time; cursors of one instance run
  concurrently on different threads.
* **Read-only at the API level.** One process cannot hold a file open
  read-only and read-write at once ("different configuration"), so a
  writable database opens read-write and read connections are restricted
  here: only ``SELECT``/``EXPLAIN``/``PRAGMA``/``CALL`` statements, each run
  inside ``BEGIN TRANSACTION READ ONLY`` (the engine then refuses writes,
  ``nextval()`` included). ``DuckDB(path, read_only=True)`` opens the file
  read-only so other processes can read it too.
* **One statement per call.** DuckDB's ``execute`` would run every
  statement in ``"select 1; drop table t"``; more than one is refused.
* **Writers** are serialised by the core's single write slot by default.
  ``write_queue=False`` sets ``concurrent_writers``; DuckDB then resolves
  conflicts optimistically (``WriteConflict``).
* **Time limit**: no progress handler exists; a shared timer thread calls
  ``interrupt()`` on the cursor (it lands within ~5 ms). Interrupting, and
  any other error, aborts the whole open transaction in DuckDB; see
  ``commit()``.
* **Settings** ``memory_limit`` and ``threads`` are applied when the
  instance opens and then locked (``lock_configuration``), so SQL cannot
  raise them. Defaults are low (256MB, 2 threads) because a registry may
  have many instances awake at once. ``allow_external_access=False``
  (default) stops SQL from reading or writing other files (``read_csv``,
  ``COPY``, ``ATTACH``), which matters when callers can run arbitrary SQL.
* **Parameters** arrive as ``$name`` (dict) or ``?`` (list).
* **Introspection** reads ``duckdb_tables()``, ``duckdb_columns()``,
  ``duckdb_constraints()``, ``duckdb_indexes()`` and ``duckdb_views()``.
  PRIMARY KEY and UNIQUE constraints are reported as indexes with origin
  ``"pk"`` / ``"u"``, as SQLite reports its automatic indexes.
  ``schema_version()`` is an md5 of the CREATE statements of every table,
  view and index (DuckDB exposes no catalog version, and table oids do not
  change on ALTER). ``change_hint()`` is size and mtime of the file and its
  ``.wal``.

Capability rounding: ``concurrent_writers`` is false by default (the spec's
"Experimental": true only with ``write_queue=False``); ``full_text_search``
is false (the ``fts`` extension has a different syntax); ``hard_kill`` is
false; ``memory_limit`` is true.
"""

from __future__ import annotations

import contextlib
import heapq
import importlib.util
import itertools
import os
import re
import sys
import threading
import time
from collections import Counter
from collections.abc import Callable, Iterator, Mapping, Sequence
from concurrent.futures import Executor
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ..core.executor import run_blocking
from ..core.params import DUCKDB_FLAVOR, ParamStyle
from ..core.results import Column, Results, WriteResult
from ..core.schema import ColumnInfo, ForeignKey, Index, Schema, TableInfo
from ..errors import (
    DblayerError,
    IntegrityError,
    NotSupported,
    QueryError,
    QueryInterrupted,
    ResourceLimitExceeded,
    WriteConflict,
)
from .base import Capabilities, Dialect, NativeParams

if TYPE_CHECKING:
    import duckdb as _duckdb

__all__ = ["DuckDB", "DuckDBDialect", "DuckDBConnection"]

# "Conflict on update!", "Transaction conflict: attempting to insert into
# table "t" but it has been altered by a different transaction".
_CONFLICT = re.compile(r"conflict", re.IGNORECASE)


class DuckDBDialect(Dialect):
    name = "duckdb"
    paramstyle = ParamStyle.NAMED_DOLLAR
    flavor = DUCKDB_FLAVOR
    type_map = {
        # SQLite integers are 64-bit; DuckDB INTEGER is 32-bit. REAL would
        # be a 4-byte FLOAT.
        "integer": "BIGINT",
        "float": "DOUBLE",
        "text": "TEXT",
        "blob": "BLOB",
    }
    native_alter = frozenset({"rename", "drop", "types"})
    # A table other tables reference can be neither ALTERed (dependency
    # error) nor dropped for a rebuild ("main key table").
    alter_referenced_tables = False
    rowid_column = None  # DuckDB's rowid pseudo-column is not stable
    # Dropping a table other tables reference fails ("main key table"), and
    # foreign keys have no ON DELETE actions, so nothing is touched silently.
    foreign_keys_enforced_sql = None

    def native_alter_for(
        self,
        info: TableInfo,
        *,
        rename: Mapping[str, str],
        drop: Sequence[str],
        types: Mapping[str, str],
    ) -> frozenset[str]:
        """DuckDB refuses every ALTER except ADD COLUMN / SET DEFAULT on a
        table that has a ``CREATE INDEX`` index (DependencyException), even
        for unindexed columns. It also refuses to drop or retype a PRIMARY
        KEY or UNIQUE column. Those cases rebuild the table."""
        if any(i.origin == "c" for i in info.indexes):
            return frozenset()
        keyed = set(info.primary_keys)
        for i in info.indexes:
            if i.origin == "u":
                keyed.update(c for c in i.columns if c is not None)
        native = set(self.native_alter)
        if keyed & set(drop):
            native.discard("drop")
        if keyed & set(types):
            native.discard("types")
        return frozenset(native)

    def literal(self, value: Any) -> str:
        # Portable columns have no boolean type (bool maps to integer), so
        # render booleans as numbers, as SQLite stores them.
        if isinstance(value, bool):
            return "1" if value else "0"
        return super().literal(value)

    def blob_literal(self, value: bytes) -> str:
        return "'" + "".join(f"\\x{b:02x}" for b in value) + "'::BLOB"

    def map_exception(self, exc: BaseException) -> DblayerError | None:
        # A process that never imported duckdb (the parent of a DuckDB
        # worker, which uses this dialect) cannot hold a duckdb error.
        duckdb = sys.modules.get("duckdb")
        if duckdb is None or not isinstance(exc, duckdb.Error):
            return None
        message = str(exc)
        code = type(exc).__name__.removesuffix("Exception") or type(exc).__name__
        if isinstance(exc, duckdb.InterruptException):
            return QueryInterrupted("query interrupted (time limit exceeded or cancelled)")
        if isinstance(exc, duckdb.ConstraintException):
            return IntegrityError(message, code)
        if isinstance(exc, duckdb.OutOfMemoryException):
            return ResourceLimitExceeded(message)
        if isinstance(exc, duckdb.ConnectionException):
            # The instance was closed under the connection: not an SQL
            # error, so the pool discards the connection.
            return None
        if isinstance(exc, duckdb.TransactionException):
            if "Failed to commit" in message and "constraint" in message.lower():
                # Two transactions inserted the same key; the second learns
                # at COMMIT. Retrying would give an ordinary IntegrityError.
                return IntegrityError(message, "Transaction")
            if _CONFLICT.search(message):
                return WriteConflict(message)
            if "transaction is aborted" in message:
                return QueryError(message, "TRANSACTION_ABORTED")
        return QueryError(message, code)


# -- time limits -----------------------------------------------------------------


class _Interrupter:
    """One daemon thread that calls ``interrupt()`` callbacks at deadlines.

    A ``threading.Timer`` per statement would start a thread per call. The
    callback runs while the lock is held, so once ``cancel()`` returns the
    callback is not running and never will: a late interrupt cannot hit the
    next statement on the same cursor."""

    def __init__(self) -> None:
        self._cv = threading.Condition()
        self._heap: list[tuple[float, int]] = []
        self._live: dict[int, Callable[[], None]] = {}
        self._seq = itertools.count()
        self._thread: threading.Thread | None = None

    def schedule(self, delay_s: float, fn: Callable[[], None]) -> int:
        with self._cv:
            token = next(self._seq)
            heapq.heappush(self._heap, (time.monotonic() + delay_s, token))
            self._live[token] = fn
            if self._thread is None or not self._thread.is_alive():
                self._thread = threading.Thread(
                    target=self._loop, name="dblayer-duckdb-interrupter", daemon=True
                )
                self._thread.start()
            self._cv.notify()
            return token

    def cancel(self, token: int) -> None:
        with self._cv:
            self._live.pop(token, None)

    def _loop(self) -> None:
        with self._cv:
            while True:
                while self._heap and self._heap[0][1] not in self._live:
                    heapq.heappop(self._heap)
                if not self._heap:
                    self._cv.wait()
                    continue
                wait = self._heap[0][0] - time.monotonic()
                if wait > 0:
                    self._cv.wait(wait)
                    continue
                _, token = heapq.heappop(self._heap)
                fn = self._live.pop(token)
                try:
                    fn()
                except Exception:
                    pass


_interrupter = _Interrupter()


# -- backend -----------------------------------------------------------------------

_DML = frozenset({"INSERT", "UPDATE", "DELETE", "MERGE_INTO"})
_RESULT_TYPES = frozenset({"SELECT", "EXPLAIN", "PRAGMA", "CALL"})
_READ_TYPES = _RESULT_TYPES
_FETCH_BATCH = 1024
_REINTERRUPT_S = 0.005
_PARSE_CACHE_MAX = 4096


def _row_bytes(row: Sequence[Any]) -> int:
    size = 0
    for v in row:
        if v is None:
            size += 1
        elif isinstance(v, (str, bytes)):
            size += len(v)
        else:
            size += 8
    return size


def _stat(path: Path) -> tuple[int, int] | None:
    try:
        st = os.stat(path)
    except FileNotFoundError:
        return None
    return (st.st_size, st.st_mtime_ns)


class DuckDB:
    """DuckDB backend for one database file. See the module docstring."""

    dialect: Dialect

    export_formats = frozenset({"csv", "tsv", "json"})
    """Serialised by DuckDB itself in ``db.export()`` (``_duckdb_export``)."""

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
        executor: Executor | None = None,
        max_write_connections: int | None = None,
    ) -> None:
        # Fail early when the extra is missing, without importing duckdb:
        # a DuckDB worker's parent builds one of these only for its dialect
        # and capabilities, and should not pay for the library.
        if importlib.util.find_spec("duckdb") is None:
            raise ImportError("DuckDB backend needs the duckdb package (the 'duckdb' extra)")

        self.path = Path(path).absolute()
        self.max_write_connections = max_write_connections
        """With ``write_queue=False``: how many write transactions the core
        pool lets in at once (default: ``max_read_connections``)."""
        self.memory_limit = memory_limit
        self.threads = threads
        self.read_only = read_only
        self.write_queue = write_queue
        self.allow_external_access = allow_external_access
        self.extra_config = dict(config or {})
        self.create = create and not read_only
        self.executor = executor
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
        self._instance: _duckdb.DuckDBPyConnection | None = None
        self._lock = threading.Lock()
        self._parse_cache: dict[str, tuple[str, ...]] = {}
        self.instances_opened = 0
        """How many times the instance was opened (once per wake)."""
        self.native_calls: Counter[str] = Counter()
        """Named operations run by the native override, by name."""

    def __repr__(self) -> str:
        return f"DuckDB({str(self.path)!r})"

    @property
    def wal_path(self) -> Path:
        return self.path.with_name(self.path.name + ".wal")

    @property
    def is_open(self) -> bool:
        """True while the instance (and the file lock) is held."""
        return self._instance is not None

    # -- Backend protocol ----------------------------------------------------

    async def open(self, *, write: bool) -> "DuckDBConnection":
        if write and self.read_only:
            raise NotSupported("database is read-only; writes are not allowed")
        cur = await run_blocking(self.executor, self._cursor)
        return DuckDBConnection(self, cur, write=write)

    def change_hint(self) -> object:
        """Size and mtime of the file and its ``.wal``; ``os.stat`` only."""
        return (_stat(self.path), _stat(self.wal_path))

    async def close(self) -> None:
        with self._lock:
            inst, self._instance = self._instance, None
        if inst is not None:
            # Closing checkpoints the WAL into the file: blocking work.
            await run_blocking(self.executor, inst.close)

    # -- internals -----------------------------------------------------------

    def _config(self) -> dict[str, Any]:
        cfg: dict[str, Any] = {}
        if self.memory_limit is not None:
            cfg["memory_limit"] = self.memory_limit
        if self.threads is not None:
            cfg["threads"] = self.threads
        if not self.allow_external_access:
            cfg["enable_external_access"] = False
        cfg.update(self.extra_config)
        return cfg

    def _cursor(self) -> "_duckdb.DuckDBPyConnection":
        import duckdb

        with self._lock:
            inst = self._instance
            if inst is None:
                if not self.path.exists() and not self.create:
                    raise QueryError(
                        f"unable to open database file {str(self.path)!r}", "CANTOPEN"
                    )
                inst = duckdb.connect(
                    str(self.path), read_only=self.read_only, config=self._config()
                )
                try:
                    inst.execute("SET lock_configuration = true")
                except BaseException:
                    inst.close()
                    raise
                self._instance = inst
                self.instances_opened += 1
            return inst.cursor()

    def _statement_types(self, cur: "_duckdb.DuckDBPyConnection", sql: str) -> tuple[str, ...]:
        types = self._parse_cache.get(sql)
        if types is None:
            types = tuple(str(s.type).rsplit(".", 1)[-1] for s in cur.extract_statements(sql))
            if len(self._parse_cache) >= _PARSE_CACHE_MAX:
                self._parse_cache.clear()
            self._parse_cache[sql] = types
        return types


class DuckDBConnection:
    """One pooled cursor of the backend's instance. Used by one task at a
    time."""

    def __init__(
        self, backend: DuckDB, cur: "_duckdb.DuckDBPyConnection", *, write: bool
    ) -> None:
        self._backend = backend
        self._cur = cur
        self.write = write
        self._closed = False
        self._in_tx = False
        self._tx_failed = False
        self._cancelled = False
        # Interrupt bookkeeping, shared with the interrupter thread and the
        # event loop: ``_active`` while a statement runs on this cursor,
        # ``_stmt`` numbers statements so a late interrupt cannot hit the
        # next one.
        self._lock = threading.Lock()
        self._active = False
        self._stmt = 0

    def __repr__(self) -> str:
        return f"<DuckDBConnection {'write' if self.write else 'read'} {self._backend.path.name}>"

    async def _run[T](self, fn: Callable[..., T], *args: Any) -> T:
        self._cancelled = False
        return await run_blocking(
            self._backend.executor, self._guarded, fn, args, on_cancel=self._on_cancel
        )

    def _on_cancel(self) -> None:
        with self._lock:
            self._cancelled = True
            stmt = self._stmt
        self._interrupt_while_active(stmt)

    def _interrupt_while_active(self, stmt: int) -> None:
        """Interrupt statement number ``stmt``, again every few ms until it
        returns. DuckDB drops an ``interrupt()`` that arrives before the
        statement has really started (the flag is reset when a query
        begins), so a single interrupt sent just after the thread marked the
        statement active would be lost and the statement would run on:
        forever, for an unbounded query. A statement that has not started
        yet sees ``_cancelled`` instead; one that has finished is left
        alone, so the cleanup (ROLLBACK of the read wrapper) is never
        interrupted."""
        with self._lock:
            if not self._active or self._stmt != stmt:
                return
            self._cur.interrupt()
        _interrupter.schedule(_REINTERRUPT_S, lambda: self._interrupt_while_active(stmt))

    @contextlib.contextmanager
    def _statement(self, time_limit_ms: float | None) -> Iterator[None]:
        """Worker-thread side of a statement: refuse to start if already
        cancelled, mark it active for ``_interrupt_while_active`` and arm
        its time limit."""
        with self._lock:
            if self._cancelled:
                raise QueryInterrupted("query interrupted (cancelled)")
            self._stmt += 1
            stmt = self._stmt
            self._active = True
        token = None
        if time_limit_ms is not None:
            token = _interrupter.schedule(
                max(time_limit_ms, 0) / 1000, lambda: self._interrupt_while_active(stmt)
            )
        try:
            yield
        finally:
            if token is not None:
                _interrupter.cancel(token)
            with self._lock:
                self._active = False

    def _guarded[T](self, fn: Callable[..., T], args: tuple[Any, ...]) -> T:
        import duckdb

        if self._cancelled:
            # Cancelled before the thread started: interrupting an idle
            # cursor is a no-op, so check here instead.
            raise QueryInterrupted("query interrupted (cancelled)")
        try:
            return fn(*args)
        except duckdb.Error:
            if self._in_tx:
                # Any error, even a parse error, aborts a DuckDB transaction.
                self._tx_failed = True
            raise

    # -- statements -------------------------------------------------------------

    def _check(self, sql: str) -> str | None:
        """Parse ``sql`` (cached) and enforce one statement per call and,
        on read connections, read-only statement types. Returns the
        statement type, or ``None`` for an empty statement."""
        types = self._backend._statement_types(self._cur, sql)
        if not types:
            return None
        if len(types) > 1:
            raise QueryError(
                "only one statement can be executed at a time", "MULTIPLE_STATEMENTS"
            )
        kind = types[0]
        if kind == "TRANSACTION":
            raise QueryError(
                "BEGIN/COMMIT/ROLLBACK are managed by dblayer; use db.transaction()",
                "TRANSACTION_STATEMENT",
            )
        if not self.write and kind not in _READ_TYPES:
            raise QueryError(
                f"attempt to write a read-only connection ({kind} statement)", "READ_ONLY"
            )
        return kind

    def _read_only_wrapper(self) -> bool:
        """Read connections run statements outside a transaction inside a
        read-only one, so the engine refuses writes too."""
        if self.write or self._in_tx:
            return False
        self._cur.execute("BEGIN TRANSACTION READ ONLY")
        return True

    def _end_wrapper(self, ok: bool) -> None:
        try:
            self._cur.execute("COMMIT" if ok else "ROLLBACK")
        except Exception:
            if ok:
                raise

    async def execute(
        self,
        sql: str,
        params: NativeParams,
        *,
        time_limit_ms: int | None,
        max_rows: int | None,
        max_result_bytes: int | None = None,
    ) -> Results:
        return await self._run(self._execute, sql, params, time_limit_ms, max_rows, max_result_bytes)

    def _execute(
        self,
        sql: str,
        params: NativeParams,
        time_limit_ms: int | None,
        max_rows: int | None,
        max_result_bytes: int | None,
    ) -> Results:
        kind = self._check(sql)
        if kind is None:
            return Results.from_tuples((), [])
        wrapped = self._read_only_wrapper()
        ok = False
        try:
            with self._statement(time_limit_ms):
                result = self._run_statement(kind, sql, params, max_rows, max_result_bytes)
            ok = True
            return result
        finally:
            if wrapped:
                self._end_wrapper(ok)

    def _run_statement(
        self,
        kind: str,
        sql: str,
        params: NativeParams,
        max_rows: int | None,
        max_result_bytes: int | None,
    ) -> Results:
        cur = self._cur
        cur.execute(sql, params)
        description = cur.description
        rowcount = cur.rowcount
        if kind in _DML and rowcount >= 0:
            # No RETURNING: the result is DuckDB's one-row "Count".
            return Results.from_tuples((), [], rowcount=rowcount)
        if description is None or (kind not in _DML and kind not in _RESULT_TYPES):
            # DDL and the like also report a "Count" description.
            return Results.from_tuples((), [], rowcount=-1)
        rows: list[tuple[Any, ...]] = []
        limit = None if max_rows is None else max_rows + 1
        size = 0
        while True:
            n = _FETCH_BATCH if limit is None else min(_FETCH_BATCH, limit - len(rows))
            if n <= 0:
                break
            batch = cur.fetchmany(n)
            if not batch:
                break
            if max_result_bytes is not None:
                for r in batch:
                    size += _row_bytes(r)
                if size > max_result_bytes:
                    raise ResourceLimitExceeded(
                        f"result exceeded max_result_bytes={max_result_bytes}"
                    )
            rows.extend(batch)
        truncated = max_rows is not None and len(rows) > max_rows
        total = len(rows)
        if truncated:
            if kind in _DML:
                # RETURNING: count every returned row, as SQLite reports.
                while more := cur.fetchmany(_FETCH_BATCH):
                    total += len(more)
            del rows[max_rows:]
        if kind in _DML:
            rowcount = total
        columns = tuple(Column(d[0], str(d[1])) for d in description)
        return Results.from_tuples(columns, rows, truncated=truncated, rowcount=rowcount)

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
            kind = self._check(sql)
            if kind is None or not params_seq:
                return WriteResult(rowcount=0)
            wrapped = self._read_only_wrapper()
            ok = False
            try:
                with self._statement(None):
                    cur = self._cur.executemany(sql, params_seq)
                ok = True
                return WriteResult(rowcount=cur.rowcount)
            finally:
                if wrapped:
                    self._end_wrapper(ok)

        return await self._run(run)

    def stream(
        self,
        sql: str,
        params: NativeParams,
        *,
        chunk_rows: int,
        time_limit_ms: int | None = None,
        max_result_bytes: int | None = None,
    ) -> "_DuckDBStream":
        return _DuckDBStream(self, sql, params, chunk_rows, time_limit_ms, max_result_bytes)

    export_formats = frozenset({"csv", "tsv", "json"})

    def export(self, sql: str, params: NativeParams, *, encoding: str, max_rows: int | None,
               time_limit_ms: int | None, chunk_bytes: int) -> Any:
        """``SupportsExport``: DuckDB builds each line (``_duckdb_export``)."""
        from ._duckdb_export import DuckDBExport

        return DuckDBExport(self, sql, params, encoding=encoding, max_rows=max_rows,
                            time_limit_ms=time_limit_ms, chunk_bytes=chunk_bytes)

    # -- transactions ------------------------------------------------------------

    async def begin(self, mode: str) -> None:
        if mode == "write":
            sql = "BEGIN TRANSACTION"
        elif mode == "read":
            sql = "BEGIN TRANSACTION READ ONLY"
        else:
            raise ValueError(f"unknown transaction mode {mode!r}")

        def run() -> None:
            self._cur.execute(sql)
            self._in_tx = True
            self._tx_failed = False

        await self._run(run)

    async def commit(self) -> None:
        """COMMIT. DuckDB aborts a transaction on any error and then
        *silently* turns COMMIT into a rollback. If a statement failed in
        this transaction (even one the caller caught) and the transaction
        is aborted, it is rolled back and ``QueryError`` raised instead, so
        lost writes are never reported as committed."""
        if not self._in_tx:
            return

        def run() -> None:
            import duckdb

            try:
                if self._tx_failed:
                    try:
                        self._cur.execute("SELECT 1")
                    except duckdb.TransactionException:
                        self._cur.execute("ROLLBACK")
                        raise QueryError(
                            "the transaction was aborted by an earlier error and has been "
                            "rolled back (DuckDB cannot continue a transaction after a "
                            "failed statement)",
                            "TRANSACTION_ABORTED",
                        ) from None
                self._cur.execute("COMMIT")
            finally:
                self._in_tx = False
                self._tx_failed = False

        await run_blocking(self._backend.executor, run)

    async def rollback(self) -> None:
        if self._closed or not self._in_tx:
            return

        def run() -> None:
            try:
                self._cur.execute("ROLLBACK")
            finally:
                self._in_tx = False
                self._tx_failed = False

        await run_blocking(self._backend.executor, run)

    # -- schema --------------------------------------------------------------

    async def schema_version(self) -> object:
        def run() -> str:
            return self._cur.execute(_VERSION_SQL).fetchone()[0]  # type: ignore[index]

        return await self._run(run)

    async def schema(self) -> Schema:
        def run() -> Schema:
            own = not self._in_tx
            if own:
                self._cur.execute("BEGIN TRANSACTION READ ONLY")
            try:
                return _read_schema(self._cur)
            finally:
                if own:
                    self._cur.execute("ROLLBACK")

        return await self._run(run)

    # -- native operations ------------------------------------------------------

    async def run_operation(self, name: str, args: dict[str, Any]) -> object:
        """``insert_rows`` through DuckDB's appender (``SupportsOperations``).
        executemany costs ~0.2 ms a row here; the appender inserts 100 rows
        in under 1 ms and 100,000 in ~80 ms (scripts/bench_duckdb_insert.py).
        It converts values as executemany parameters are converted, so
        results match the default path. ``ignore`` / ``replace`` decline."""
        if name != "insert_rows":
            return NotImplemented
        if args.get("ignore") or args.get("replace") or not args.get("rows"):
            return NotImplemented
        self._backend.native_calls[name] += 1
        return await self._insert_rows(
            args["table"], args["rows"], args.get("pk"), bool(args.get("alter"))
        )

    async def _insert_rows(
        self, table: str, rows: list[dict[str, Any]], pk: Any, alter: bool
    ) -> object:
        from ..core import operations as ops

        existing = await self._run(self._table_columns, table)
        if existing is None:
            # Missing table (or a view): the default code creates it, or
            # lets the engine refuse the view, exactly as it would.
            created, added = await ops._ensure_table(
                _ConnectionTx(self), table, rows, ops._pk_tuple(pk), alter
            )
        else:
            created = False
            added: tuple[str, ...] = ()
            if alter:
                have = {c.casefold() for c in existing}
                types = ops._column_types(rows)
                missing = [c for c in types if c.casefold() not in have]
                for c in missing:
                    sql = self._backend.dialect.add_column_sql(table, ops.ColumnDef(c, types[c]))
                    await self.execute(sql, [], time_limit_ms=None, max_rows=0)
                added = tuple(missing)
        groups = [(list(cols), [[r[c] for c in cols] for r in group]) for cols, group in ops._groups(rows)]

        def append() -> int:
            total = 0
            for cols, values in groups:
                app = self._cur.appender(table, columns=cols)
                try:
                    app.append_rows(values)
                    app.flush()
                except BaseException:
                    app.clear()  # never let buffered rows be flushed later
                    raise
                total += len(values)
            return total

        total = await self._run(append)
        return ops.OperationResult(rowcount=total, table_created=created, columns_added=added)

    def _table_columns(self, table: str) -> list[str] | None:
        rows = self._cur.execute(
            "SELECT c.column_name FROM duckdb_columns() c JOIN duckdb_tables() t "
            "ON c.table_oid = t.table_oid "
            f"WHERE t.{_HERE.replace(' AND ', ' AND t.')} AND t.table_name = $name "
            "ORDER BY c.column_index",
            {"name": table},
        ).fetchall()
        return [r[0] for r in rows] or None

    # -- misc ----------------------------------------------------------------

    async def interrupt(self) -> None:
        self._cur.interrupt()  # thread-safe

    async def close(self) -> None:
        if not self._closed:
            self._closed = True
            await run_blocking(self._backend.executor, self._cur.close)


class _ConnectionTx:
    """The ``TxLike`` view of a connection inside an open transaction, so a
    native override can reuse default helpers (table creation)."""

    def __init__(self, conn: DuckDBConnection) -> None:
        self._conn = conn
        self.dialect = conn._backend.dialect
        self.capabilities = conn._backend.capabilities

    def _native(self, sql: str, params: Any) -> tuple[str, Any]:
        from ..core.params import rewrite

        return rewrite(sql, params, self.dialect.paramstyle, self.dialect.flavor)

    async def execute(self, sql: str, params: Any = None) -> Results:
        native, p = self._native(sql, params)
        return await self._conn.execute(native, p, time_limit_ms=None, max_rows=None)

    async def execute_many(self, sql: str, params_seq: Any) -> WriteResult:
        rewritten = [self._native(sql, p) for p in params_seq]
        if not rewritten:
            return WriteResult(rowcount=0)
        return await self._conn.execute_many(rewritten[0][0], [p for _, p in rewritten])

    async def schema(self) -> Schema:
        return await self._conn.schema()


class _DuckDBStream:
    """Lazily fetched chunks, one executor hop per chunk. ``time_limit_ms``
    is a budget of time spent inside DuckDB across hops, as on SQLite."""

    def __init__(
        self,
        conn: DuckDBConnection,
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
        self._remaining_ms: float | None = time_limit_ms
        self._max_bytes = max_result_bytes
        self._bytes = 0
        self._started = False
        self._executed = False
        self._kind: str | None = None
        self._wrapped = False
        self._done = False
        self.columns: tuple[Column, ...] | None = None

    def __aiter__(self) -> "_DuckDBStream":
        return self

    async def __anext__(self) -> list[tuple[Any, ...]]:
        if self._done:
            raise StopAsyncIteration
        try:
            chunk = await self._conn._run(self._next_chunk)
        except BaseException:
            await self.aclose(ok=False)
            raise
        if chunk is None:
            await self.aclose()
            raise StopAsyncIteration
        return chunk

    def _next_chunk(self) -> list[tuple[Any, ...]] | None:
        conn = self._conn
        cur = conn._cur
        start = time.perf_counter()
        try:
            if self._remaining_ms is not None and self._remaining_ms <= 0:
                raise QueryInterrupted("query interrupted (time limit exceeded)")
            if not self._started:
                self._kind = conn._check(self._sql)
                self._started = True
                if self._kind is None:
                    self.columns = ()
                    return None
                # Outside the statement block: an interrupt must never
                # land on the wrapper's BEGIN.
                self._wrapped = conn._read_only_wrapper()
            # ``_statement`` refuses to start once cancelled and repeats the
            # interrupt (cancel or time limit) until the hop returns: one
            # ``interrupt()`` sent before DuckDB has really started is lost.
            with conn._statement(self._remaining_ms):
                if not self._executed:
                    self._executed = True
                    kind = self._kind
                    cur.execute(self._sql, self._params)
                    desc = cur.description or ()
                    if kind not in _RESULT_TYPES and not (kind in _DML and cur.rowcount < 0):
                        desc = ()
                    self.columns = tuple(Column(d[0], str(d[1])) for d in desc)
                    if not desc:
                        return None
                chunk = cur.fetchmany(self._chunk_rows)
        finally:
            if self._remaining_ms is not None:
                self._remaining_ms -= (time.perf_counter() - start) * 1000
        if not chunk:
            return None
        if self._max_bytes is not None:
            for r in chunk:
                self._bytes += _row_bytes(r)
            if self._bytes > self._max_bytes:
                raise ResourceLimitExceeded(f"result exceeded max_result_bytes={self._max_bytes}")
        return chunk

    async def aclose(self, ok: bool = True) -> None:
        if self._done:
            return
        self._done = True
        conn = self._conn
        if self._wrapped and not conn._closed:
            self._wrapped = False
            # Ends the read-only transaction, which also discards the
            # rest of an unfinished result.
            await run_blocking(conn._backend.executor, conn._end_wrapper, ok)


# -- introspection -------------------------------------------------------------

_HERE = "database_name = current_database() AND schema_name = current_schema()"

_VERSION_SQL = f"""
SELECT md5(coalesce(string_agg(x, chr(10) ORDER BY x), '')) FROM (
    SELECT 't' || table_name || chr(31) || coalesce(sql, '') AS x
        FROM duckdb_tables() WHERE {_HERE}
    UNION ALL SELECT 'v' || view_name || chr(31) || coalesce(sql, '')
        FROM duckdb_views() WHERE NOT internal AND {_HERE}
    UNION ALL SELECT 'i' || index_name || chr(31) || coalesce(sql, '')
        FROM duckdb_indexes() WHERE {_HERE}
)
"""

_IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*")


def _strip_semicolon(sql: str | None) -> str | None:
    if sql is None:
        return None
    sql = sql.strip()
    return sql[:-1].rstrip() if sql.endswith(";") else sql


def _split_top_level(s: str) -> list[str]:
    """Split on commas outside parentheses and quotes."""
    parts: list[str] = []
    depth = 0
    buf: list[str] = []
    quote: str | None = None
    for ch in s:
        if quote:
            buf.append(ch)
            if ch == quote:
                quote = None
            continue
        if ch in "'\"":
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append("".join(buf).strip())
            buf = []
            continue
        buf.append(ch)
    if buf:
        parts.append("".join(buf).strip())
    return parts


def _index_columns(sql: str | None, columns: Sequence[str]) -> tuple[str | None, ...]:
    """Column names of a ``CREATE INDEX`` statement (``None`` for an
    expression). DuckDB's ``expressions`` column is a display string, so the
    statement is parsed instead."""
    if not sql:
        return ()
    open_ = sql.find("(")
    close = sql.rfind(")")
    if open_ == -1 or close <= open_:
        return ()
    by_fold = {c.casefold(): c for c in columns}
    out: list[str | None] = []
    for part in _split_top_level(sql[open_ + 1 : close]):
        name: str | None = None
        if len(part) >= 2 and part[0] == part[-1] == '"':
            name = part[1:-1].replace('""', '"')
        elif _IDENT.fullmatch(part):
            name = part
        out.append(by_fold.get(name.casefold()) if name is not None else None)
    return tuple(out)


def _read_schema(cur: "_duckdb.DuckDBPyConnection") -> Schema:
    version = cur.execute(_VERSION_SQL).fetchone()[0]  # type: ignore[index]
    tables_rows = cur.execute(
        f"SELECT table_name, sql FROM duckdb_tables() WHERE {_HERE} ORDER BY table_oid"
    ).fetchall()
    views_rows = cur.execute(
        f"SELECT view_name, sql FROM duckdb_views() WHERE NOT internal AND {_HERE} "
        "ORDER BY view_oid"
    ).fetchall()
    cols: dict[str, list[tuple[Any, ...]]] = {}
    for tname, cname, ctype, nullable, default, generated in cur.execute(
        "SELECT table_name, column_name, data_type, is_nullable, column_default, is_generated "
        f"FROM duckdb_columns() WHERE {_HERE} ORDER BY table_oid, column_index"
    ).fetchall():
        cols.setdefault(tname, []).append((cname, ctype, nullable, default, generated))
    constraints: dict[str, list[tuple[Any, ...]]] = {}
    for tname, ctype, names, cname, ref_table, ref_cols in cur.execute(
        "SELECT table_name, constraint_type, constraint_column_names, constraint_name, "
        "referenced_table, referenced_column_names FROM duckdb_constraints() "
        f"WHERE {_HERE} AND constraint_type IN ('PRIMARY KEY', 'UNIQUE', 'FOREIGN KEY') "
        "ORDER BY table_oid, constraint_index"
    ).fetchall():
        constraints.setdefault(tname, []).append((ctype, names, cname, ref_table, ref_cols))
    index_rows: dict[str, list[tuple[Any, ...]]] = {}
    for tname, iname, unique, sql in cur.execute(
        f"SELECT table_name, index_name, is_unique, sql FROM duckdb_indexes() WHERE {_HERE} "
        "ORDER BY index_name"
    ).fetchall():
        index_rows.setdefault(tname, []).append((iname, unique, sql))

    pks: dict[str, tuple[str, ...]] = {}
    for tname, items in constraints.items():
        for ctype, names, *_ in items:
            if ctype == "PRIMARY KEY":
                pks[tname] = tuple(names)

    def columns_of(name: str) -> tuple[ColumnInfo, ...]:
        pk = pks.get(name, ())
        return tuple(
            ColumnInfo(
                name=cname,
                type=ctype or "",
                not_null=not nullable and cname not in pk,
                default=None if generated else default,
                primary_key=pk.index(cname) + 1 if cname in pk else 0,
                generated=bool(generated),
            )
            for cname, ctype, nullable, default, generated in cols.get(name, [])
        )

    tables: dict[str, TableInfo] = {}
    for name, sql in tables_rows:
        columns = columns_of(name)
        names = [c.name for c in columns]
        fks: list[ForeignKey] = []
        indexes: list[Index] = []
        for ctype, ccols, cname, ref_table, ref_cols in constraints.get(name, []):
            if ctype == "FOREIGN KEY":
                other_cols = tuple(ref_cols or ()) or pks.get(ref_table, ())
                fks.append(ForeignKey(tuple(ccols), ref_table, tuple(other_cols)))
            else:
                indexes.append(
                    Index(
                        name=cname,
                        columns=tuple(ccols),
                        unique=True,
                        origin="pk" if ctype == "PRIMARY KEY" else "u",
                    )
                )
        for iname, unique, isql in index_rows.get(name, []):
            indexes.append(
                Index(
                    name=iname,
                    columns=_index_columns(isql, names),
                    unique=bool(unique),
                    origin="c",
                    definition=_strip_semicolon(isql),
                )
            )
        tables[name] = TableInfo(
            name=name,
            kind="table",
            columns=columns,
            foreign_keys=tuple(fks),
            indexes=tuple(sorted(indexes, key=lambda i: i.name)),
            definition=_strip_semicolon(sql),
        )
    views = {
        name: TableInfo(
            name=name, kind="view", columns=columns_of(name), definition=_strip_semicolon(sql)
        )
        for name, sql in views_rows
    }
    return Schema(version=version, tables=tables, views=views, triggers={})
