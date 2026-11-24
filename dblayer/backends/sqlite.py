"""In-process SQLite backend, using the standard library ``sqlite3`` module.

* Read connections open with ``mode=ro`` (``immutable=1`` for immutable
  databases); the write connection opens ``mode=rwc``. All use
  ``check_same_thread=False`` and ``autocommit=True`` (no implicit
  transactions); the backend issues BEGIN/COMMIT/ROLLBACK itself.
* Every blocking call runs on the shared executor via ``run_blocking``, with
  context variables copied.
* Time limits use a progress handler with a deadline, as Datasette's
  ``sqlite_timelimit`` does. ``interrupt()`` calls ``Connection.interrupt()``.
* Named shared-cache in-memory databases (``SqliteInProcess.memory(name)``)
  are supported. Closing the last connection would destroy them, so the
  backend keeps a private anchor connection open from the first ``open()``
  until the backend is closed, and reports
  ``sleep_releases_everything=False`` so the core does not close the backend
  on sleep. Read connections to a memory database use ``PRAGMA
  query_only=1`` (``mode=ro`` cannot be combined with ``mode=memory``) and
  ``PRAGMA read_uncommitted=1`` (to avoid shared-cache table locks between
  readers and the writer).
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import os
import sqlite3
import threading
import time
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import Executor
from typing import Any

from ..core.executor import run_blocking
from ..core.params import leading_keyword
from ..core.results import Column, Results, WriteResult
from ..core.schema import Schema
from ..errors import NotSupported, ResourceLimitExceeded
from ._sqlite_common import (
    FETCH_BATCH as _FETCH_BATCH,
    WRITE_VERBS as _WRITE_VERBS,
    SqliteDialectBase,
    SqliteTarget,
    has_fts,
    make_capabilities,
    read_schema,
    row_bytes as _row_bytes,
    stat as _stat,  # noqa: F401 (re-exported for sqlite_worker)
)
from .base import Capabilities, Dialect, NativeParams

__all__ = ["SqliteInProcess", "SqliteDialect", "SqliteConnection"]

type InitHook = Callable[[sqlite3.Connection], None]


class SqliteDialect(SqliteDialectBase):
    """The SQLite dialect, with errors read from ``sqlite3`` exceptions."""

    native_alter = SqliteDialectBase.native_alter_for_version(sqlite3.sqlite_version_info)

    def error_info(self, exc: BaseException) -> tuple[str | None, str, bool] | None:
        if not isinstance(exc, sqlite3.Error):
            return None
        code = getattr(exc, "sqlite_errorname", None)
        return code, str(exc), isinstance(exc, sqlite3.IntegrityError)


@functools.cache
def _has_fts5() -> bool:
    # One throwaway in-memory connection per process; no file is touched.
    conn = sqlite3.connect(":memory:")
    try:
        return has_fts([row[0] for row in conn.execute("pragma compile_options")])
    finally:
        conn.close()


_REINTERRUPT_S = 0.005

_LASTROWID_SENTINEL = -(2**63)
"""``last_insert_rowid()`` is set to this before an insert on a write
connection, so an insert that reuses the previous insert's rowid (insert 5,
delete it, insert 5 again) is told apart from one that inserted nothing:
comparing the value before and after gets that case wrong. The standard
module has no ``sqlite3_set_last_insert_rowid()``, so the value is set by
writing that rowid into a one-row temp table (``_SENTINEL_TABLE``)."""
_SENTINEL_TABLE = "temp.dblayer_last_insert_rowid"


class _TimeLimit:
    """Install a progress-handler deadline for the duration of a block.
    Must be entered and exited on the thread that runs the statement."""

    __slots__ = ("conn", "ms")

    def __init__(self, conn: sqlite3.Connection, ms: int | None) -> None:
        self.conn = conn
        self.ms = ms

    def __enter__(self) -> None:
        if self.ms is None:
            return
        deadline = time.perf_counter() + self.ms / 1000
        # ~1000 VM instructions take about 0.08ms (Datasette #1679); check
        # every instruction for very short limits, as Datasette does.
        n = 1 if self.ms <= 20 else 1000

        def handler() -> int:
            return 1 if time.perf_counter() >= deadline else 0

        self.conn.set_progress_handler(handler, n)

    def __exit__(self, *exc: object) -> None:
        if self.ms is not None:
            self.conn.set_progress_handler(None, 0)


@functools.cache
def _capabilities(is_memory: bool) -> Capabilities:
    """Shared (frozen) instances: one per kind, not one per database."""
    return make_capabilities(
        is_memory=is_memory, fts=_has_fts5(), version=sqlite3.sqlite_version_info
    )


_DIALECT = SqliteDialect()  # stateless, shared by every backend instance
_ANCHOR_LOCK = threading.Lock()  # anchor creation is rare; one lock for all


class SqliteInProcess(SqliteTarget):
    """In-process SQLite backend.

    Capability rounding: none needed; every flag is a plain yes/no here.
    ``full_text_search`` reflects whether this sqlite3 build has FTS3/4/5.

    Slotted, with shared capabilities and dialect objects, so that 10,000
    registered databases cost a few hundred bytes each. The path is kept as
    a string; ``path`` builds a ``Path`` on demand.
    """

    __slots__ = ("init", "executor", "busy_timeout", "capabilities", "_anchor")
    dialect: Dialect = _DIALECT

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
    ) -> None:
        self._init_target(path, memory_name, immutable, create)
        self.init = init
        self.executor = executor
        self.busy_timeout = busy_timeout
        self.capabilities = _capabilities(memory_name is not None)
        self._anchor: sqlite3.Connection | None = None

    @classmethod
    def memory(cls, name: str | None = None, **kwargs: Any) -> "SqliteInProcess":
        """A named shared-cache in-memory database. Without a name, a unique
        one is generated, so the database is private to this backend."""
        return cls(memory_name=name or cls.unique_memory_name(), **kwargs)

    def __repr__(self) -> str:
        return f"SqliteInProcess({self._target_repr()})"

    # -- Backend protocol ----------------------------------------------------

    async def open(self, *, write: bool) -> "SqliteConnection":
        if write and self.immutable:
            raise NotSupported("database is immutable; writes are not allowed")
        raw = await run_blocking(self.executor, self._connect, write)
        return SqliteConnection(self, raw, write=write)

    async def close(self) -> None:
        with _ANCHOR_LOCK:
            anchor, self._anchor = self._anchor, None
        if anchor is not None:
            anchor.close()

    # -- internals -----------------------------------------------------------

    def _connect(self, write: bool) -> sqlite3.Connection:
        if self.memory_name is not None:
            with _ANCHOR_LOCK:
                if self._anchor is None:
                    self._anchor = sqlite3.connect(
                        self._uri(True), uri=True, check_same_thread=False, autocommit=True
                    )
        elif self._prepare_file(write):
            self._recover_hot_journal()
        raw = sqlite3.connect(
            self._uri(write),
            uri=True,
            check_same_thread=False,
            autocommit=True,
            timeout=self.busy_timeout,
        )
        try:
            if self.memory_name is not None and not write:
                raw.execute("pragma query_only = 1")
                raw.execute("pragma read_uncommitted = 1")
            if self.init is not None:
                self.init(raw)
        except BaseException:
            raw.close()
            raise
        return raw

    def _recover_hot_journal(self) -> None:
        """A writer killed mid-transaction in rollback-journal mode leaves
        a hot journal, and a ``mode=ro`` connection cannot roll it back:
        every read fails with ``SQLITE_READONLY_ROLLBACK`` until a writer
        opens the file. When a journal file exists, read through a
        read-write connection once, which lets SQLite roll it back (it does
        so only if no live writer holds the lock, so this never touches a
        transaction in progress). Costs one ``stat`` otherwise."""
        if not self.has_journal():
            return
        try:
            rw = sqlite3.connect(self._rw_uri(), uri=True, timeout=self.busy_timeout)
        except sqlite3.Error:
            return  # read-only file or directory: nothing we can do
        try:
            rw.execute("select count(*) from sqlite_master").fetchone()
        except sqlite3.Error:
            pass
        finally:
            rw.close()


class SqliteConnection:
    """One pooled sqlite3 connection. Used by one task at a time."""

    def __init__(self, backend: SqliteInProcess, raw: sqlite3.Connection, *, write: bool) -> None:
        self._backend = backend
        self._raw = raw
        self.write = write
        self._closed = False
        # Cancellation, shared between the event loop and the worker
        # thread: see _on_cancel.
        self._lock = threading.Lock()
        self._cancelled = False
        self._active = False
        self._stmt = 0

    @property
    def in_transaction(self) -> bool:
        """Whether a transaction is open. Optional attribute, read by the
        worker process so the parent can skip no-op rollbacks."""
        return not self._closed and self._raw.in_transaction

    async def _run[T](self, fn: Callable[..., T], *args: Any) -> T:
        self._cancelled = False
        return await run_blocking(self._backend.executor, fn, *args, on_cancel=self._on_cancel)

    def _on_cancel(self) -> None:
        """The awaiting task was cancelled (event loop thread).

        ``sqlite3_interrupt()`` is a no-op when no statement is running, so
        one call made just before the thread steps into the statement is
        lost and an unbounded query then runs forever. A statement that has
        not started sees ``_cancelled`` instead; one that is running (or
        about to) is interrupted again every few ms until it returns. Only
        the statement itself is interrupted, never what runs after it."""
        with self._lock:
            self._cancelled = True
            stmt = self._stmt
        self._interrupt_while_active(asyncio.get_running_loop(), stmt)

    def _interrupt_while_active(self, loop: asyncio.AbstractEventLoop, stmt: int) -> None:
        with self._lock:
            if not self._active or self._stmt != stmt:
                return
            self._raw.interrupt()
        loop.call_later(_REINTERRUPT_S, self._interrupt_while_active, loop, stmt)

    @contextlib.contextmanager
    def _statement(self) -> Iterator[None]:
        """Worker-thread side of a statement that may run for a long time:
        refuse to start once cancelled, and mark it active meanwhile."""
        with self._lock:
            if self._cancelled:
                raise sqlite3.OperationalError("interrupted")
            self._stmt += 1
            self._active = True
        try:
            yield
        finally:
            with self._lock:
                self._active = False

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
        return await self._run(
            self._execute_recovering, sql, params, time_limit_ms, max_rows, max_result_bytes
        )

    def _execute_recovering(self, *args: Any) -> Results:
        """``_execute``; on a read connection that meets a hot journal left
        by a killed writer (``SQLITE_READONLY_ROLLBACK``), recover through a
        read-write connection and retry once."""
        try:
            return self._execute(*args)
        except sqlite3.OperationalError as e:
            if self.write or getattr(e, "sqlite_errorname", None) != "SQLITE_READONLY_ROLLBACK":
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
        raw = self._raw
        verb = leading_keyword(sql)
        is_write = verb in _WRITE_VERBS
        is_insert = verb in ("INSERT", "REPLACE")
        sentinel = False
        if is_insert:
            # last_insert_rowid() is per connection and is not touched by an
            # upsert that took the UPDATE path, by OR IGNORE / DO NOTHING
            # skips or by WITHOUT ROWID tables. A sentinel value set first
            # tells us whether *this* statement inserted a rowid row; on a
            # read connection (where the insert fails anyway) comparing
            # before and after is enough.
            rowid_before = raw.execute("select last_insert_rowid()").fetchone()[0]
            if self.write:
                self._set_last_insert_rowid(_LASTROWID_SENTINEL)
                sentinel = True
        try:
            with _TimeLimit(raw, time_limit_ms), self._statement():
                cur = raw.execute(sql, params)
                try:
                    description = cur.description
                    rows: list[tuple[Any, ...]] = []
                    truncated = False
                    if description is not None:
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
                        if max_rows is not None and len(rows) > max_rows:
                            truncated = True
                            del rows[max_rows:]
                            if is_write:
                                # sqlite3 only reports rowcount for RETURNING
                                # statements once they have run to completion.
                                while cur.fetchmany(_FETCH_BATCH):
                                    pass
                    rowcount = cur.rowcount
                    lastrowid = cur.lastrowid
                finally:
                    cur.close()
                if is_write and rowcount == -1:
                    # sqlite3 only recognises DML by its first keyword, so
                    # "WITH ... INSERT" reports -1. Ask SQLite directly.
                    rowcount, lastrowid = raw.execute(
                        "select changes(), last_insert_rowid()"
                    ).fetchone()
        except BaseException:
            if sentinel:
                with contextlib.suppress(Exception):
                    self._set_last_insert_rowid(rowid_before)
            raise
        if sentinel:
            if lastrowid == _LASTROWID_SENTINEL:
                last_pk = None
                # Nothing inserted: give SQL's last_insert_rowid() back.
                self._set_last_insert_rowid(rowid_before)
            else:
                last_pk = lastrowid if rowcount > 0 else None
        else:
            last_pk = (
                lastrowid
                if is_insert and rowcount > 0 and lastrowid != rowid_before
                else None
            )
        columns = tuple(Column(d[0]) for d in description) if description else ()
        return Results.from_tuples(
            columns, rows, truncated=truncated, rowcount=rowcount, last_pk=last_pk
        )

    def _set_last_insert_rowid(self, value: int) -> None:
        """``sqlite3_set_last_insert_rowid()`` for the standard module: an
        INSERT OR REPLACE of rowid ``value`` into a one-row temp table. The
        table is created on first use; if a rollback took it away, again."""
        sql = f"insert or replace into {_SENTINEL_TABLE} (rowid, x) values (?, 0)"
        try:
            self._raw.execute(sql, (value,))
        except sqlite3.OperationalError as e:
            if "no such table" not in str(e):
                raise
            self._raw.execute(f"create temp table if not exists {_SENTINEL_TABLE[5:]} (x)")
            self._raw.execute(sql, (value,))

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
            with self._statement():
                cur = self._raw.executemany(sql, params_seq)
            try:
                return WriteResult(rowcount=cur.rowcount)
            finally:
                cur.close()

        return await self._run(run)

    def stream(
        self,
        sql: str,
        params: NativeParams,
        *,
        chunk_rows: int,
        time_limit_ms: int | None = None,
        max_result_bytes: int | None = None,
    ) -> "_SqliteStream":
        return _SqliteStream(self, sql, params, chunk_rows, time_limit_ms, max_result_bytes)

    # -- transactions ------------------------------------------------------------

    async def begin(self, mode: str) -> None:
        if mode == "write":
            sql = "BEGIN IMMEDIATE"
        elif mode == "read":
            sql = "BEGIN"
        else:
            raise ValueError(f"unknown transaction mode {mode!r}")
        await self._run(self._raw.execute, sql)

    async def commit(self) -> None:
        if self._raw.in_transaction:
            await self._run(self._raw.execute, "COMMIT")

    async def rollback(self) -> None:
        # Checked without a thread hop: no statement is running on this
        # connection while the pool or the owning task calls rollback().
        if not self._closed and self._raw.in_transaction:
            await self._run(self._raw.execute, "ROLLBACK")

    # -- schema --------------------------------------------------------------

    async def schema_version(self) -> object:
        def run() -> int:
            return self._raw.execute("pragma schema_version").fetchone()[0]

        return await self._run(run)

    async def schema(self) -> Schema:
        return await self._run(self._schema)

    def _schema(self) -> Schema:
        raw = self._raw
        own_tx = not raw.in_transaction
        if own_tx:
            raw.execute("BEGIN")  # one consistent snapshot for every query
        try:
            return _read_schema(raw)
        finally:
            if own_tx and raw.in_transaction:
                raw.execute("ROLLBACK")

    # -- misc ----------------------------------------------------------------

    async def interrupt(self) -> None:
        self._raw.interrupt()  # thread-safe; interrupts whatever is running

    async def close(self) -> None:
        if not self._closed:
            self._closed = True
            await run_blocking(self._backend.executor, self._raw.close)


class _SqliteStream:
    """Lazily fetched chunks; one executor hop per chunk, so a slow consumer
    applies backpressure. ``time_limit_ms`` is a budget of time spent inside
    SQLite across all hops, not wall-clock time, so a slow consumer does not
    use it up."""

    def __init__(
        self,
        conn: SqliteConnection,
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
        self._cur: sqlite3.Cursor | None = None
        self._done = False
        self.columns: tuple[Column, ...] | None = None

    def __aiter__(self) -> "_SqliteStream":
        return self

    async def __anext__(self) -> list[tuple[Any, ...]]:
        if self._done:
            raise StopAsyncIteration
        try:
            chunk = await self._conn._run(self._next_chunk)
        except BaseException:
            await self.aclose()
            raise
        if chunk is None:
            await self.aclose()
            raise StopAsyncIteration
        return chunk

    def _next_chunk(self) -> list[tuple[Any, ...]] | None:
        raw = self._conn._raw
        start = time.perf_counter()
        ms = None if self._remaining_ms is None else max(int(self._remaining_ms), 0)
        try:
            # ``_statement``: a cancel must interrupt the hop (repeatedly,
            # since an interrupt sent before the step begins is dropped)
            # and a hop must not start once cancelled.
            with _TimeLimit(raw, ms), self._conn._statement():
                if self._remaining_ms is not None and self._remaining_ms <= 0:
                    raise sqlite3.OperationalError("interrupted")
                if self._cur is None:
                    self._cur = raw.execute(self._sql, self._params)
                    desc = self._cur.description or ()
                    self.columns = tuple(Column(d[0]) for d in desc)
                    if not desc:
                        return None
                chunk = self._cur.fetchmany(self._chunk_rows)
        finally:
            if self._remaining_ms is not None:
                self._remaining_ms -= (time.perf_counter() - start) * 1000
        if not chunk:
            return None
        if self._max_bytes is not None:
            for r in chunk:
                self._bytes += _row_bytes(r)
            if self._bytes > self._max_bytes:
                raise ResourceLimitExceeded(
                    f"result exceeded max_result_bytes={self._max_bytes}"
                )
        return chunk

    async def aclose(self) -> None:
        if self._done:
            return
        self._done = True
        cur, self._cur = self._cur, None
        if cur is not None:
            await run_blocking(self._conn._backend.executor, cur.close)


def _read_schema(raw: sqlite3.Connection) -> Schema:
    return read_schema(raw, sqlite3.OperationalError)
