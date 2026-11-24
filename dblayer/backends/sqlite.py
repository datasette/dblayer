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
import urllib.parse
import uuid
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import Executor
from pathlib import Path
from typing import Any

from ..core.executor import run_blocking
from ..core.params import SQLITE_FLAVOR, ParamStyle, leading_keyword
from ..core.results import Column, Results, WriteResult
from ..core.schema import ColumnInfo, ForeignKey, Index, Schema, TableInfo, Trigger
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

__all__ = ["SqliteInProcess", "SqliteDialect", "SqliteConnection"]

type InitHook = Callable[[sqlite3.Connection], None]

_FETCH_BATCH = 256
_WRITE_VERBS = frozenset({"INSERT", "UPDATE", "DELETE", "REPLACE"})


class SqliteDialect(Dialect):
    name = "sqlite"
    paramstyle = ParamStyle.NAMED_COLON
    flavor = SQLITE_FLAVOR
    type_map = {
        "integer": "INTEGER",
        "float": "REAL",
        "text": "TEXT",
        "blob": "BLOB",
    }

    # Conflict handling uses the generic ON CONFLICT syntax (SQLite 3.24+),
    # not INSERT OR IGNORE / OR REPLACE: OR IGNORE also skips NOT NULL and
    # CHECK violations, and OR REPLACE deletes the old row (new rowid,
    # delete side effects), neither of which PostgreSQL or DuckDB can match.

    # Native RENAME COLUMN (3.25+) also rewrites indexes, triggers, views and
    # other tables' foreign keys. DROP COLUMN (3.35+) refuses indexed, key and
    # referenced columns, and there is no ALTER COLUMN TYPE, so those (and pk
    # changes) rebuild the table.
    native_alter = (
        frozenset({"rename"}) if sqlite3.sqlite_version_info >= (3, 25, 0) else frozenset()
    )
    rowid_column = "rowid"
    foreign_keys_enforced_sql = "PRAGMA foreign_keys"

    def blob_literal(self, value: bytes) -> str:
        return "X'" + value.hex() + "'"

    def swap_rename_sql(self, staging: str, target: str) -> list[str]:
        # Since 3.26 RENAME re-parses the whole schema and fails ("error in
        # view v: no such table: main.target") while views still name the
        # just-dropped target. Legacy mode renames without that check (and
        # without rewriting references, which already say "target").
        # Assumes legacy_alter_table is normally off on dblayer connections.
        return [
            "PRAGMA legacy_alter_table = ON",
            self.rename_table_sql(staging, target),
            "PRAGMA legacy_alter_table = OFF",
        ]

    def map_exception(self, exc: BaseException) -> DblayerError | None:
        if not isinstance(exc, sqlite3.Error):
            return None
        code = getattr(exc, "sqlite_errorname", None)
        message = str(exc)
        if code == "SQLITE_INTERRUPT" or (code is None and message == "interrupted"):
            return QueryInterrupted("query interrupted (time limit exceeded or cancelled)")
        if isinstance(exc, sqlite3.IntegrityError):
            return IntegrityError(message, code)
        if code in ("SQLITE_NOMEM", "SQLITE_TOOBIG", "SQLITE_FULL"):
            return ResourceLimitExceeded(message)
        if code == "SQLITE_BUSY_SNAPSHOT":
            return WriteConflict(message)
        return QueryError(message, code)


@functools.cache
def _has_fts5() -> bool:
    # One throwaway in-memory connection per process; no file is touched.
    conn = sqlite3.connect(":memory:")
    try:
        opts = {row[0] for row in conn.execute("pragma compile_options")}
        return "ENABLE_FTS5" in opts or "ENABLE_FTS4" in opts or "ENABLE_FTS3" in opts
    finally:
        conn.close()


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


_REINTERRUPT_S = 0.005


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
    return Capabilities(
        concurrent_writers=False,
        transactional_ddl=True,
        implicit_rowid=True,
        cheap_change_hint=not is_memory,
        hard_kill=False,
        memory_limit=False,
        table_definition_sql=True,
        full_text_search=_has_fts5(),
        sleep_releases_everything=not is_memory,
        returning=sqlite3.sqlite_version_info >= (3, 35, 0),
        snapshot_reads=not is_memory,
    )


_DIALECT = SqliteDialect()  # stateless, shared by every backend instance
_ANCHOR_LOCK = threading.Lock()  # anchor creation is rare; one lock for all


class SqliteInProcess:
    """In-process SQLite backend.

    Capability rounding: none needed; every flag is a plain yes/no here.
    ``full_text_search`` reflects whether this sqlite3 build has FTS3/4/5.

    Slotted, with shared capabilities and dialect objects, so that 10,000
    registered databases cost a few hundred bytes each. The path is kept as
    a string; ``path`` builds a ``Path`` on demand.
    """

    __slots__ = (
        "_path",
        "memory_name",
        "immutable",
        "init",
        "executor",
        "busy_timeout",
        "create",
        "capabilities",
        "_anchor",
    )
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
        if (path is None) == (memory_name is None):
            raise ValueError("pass exactly one of path or memory_name")
        if memory_name is not None and immutable:
            raise ValueError("an in-memory database cannot be immutable")
        self._path: str | None = (
            os.path.abspath(os.fspath(path)) if path is not None else None
        )
        self.memory_name = memory_name
        self.immutable = immutable
        self.init = init
        self.executor = executor
        self.busy_timeout = busy_timeout
        self.create = create and not immutable
        self.capabilities = _capabilities(memory_name is not None)
        self._anchor: sqlite3.Connection | None = None

    @classmethod
    def memory(cls, name: str | None = None, **kwargs: Any) -> "SqliteInProcess":
        """A named shared-cache in-memory database. Without a name, a unique
        one is generated, so the database is private to this backend."""
        return cls(memory_name=name or f"dblayer-{uuid.uuid4().hex}", **kwargs)

    @property
    def path(self) -> Path | None:
        return None if self._path is None else Path(self._path)

    @property
    def is_memory(self) -> bool:
        return self.memory_name is not None

    def __repr__(self) -> str:
        target = f"memory:{self.memory_name}" if self.is_memory else self._path
        return f"SqliteInProcess({target!r})"

    # -- Backend protocol ----------------------------------------------------

    async def open(self, *, write: bool) -> "SqliteConnection":
        if write and self.immutable:
            raise NotSupported("database is immutable; writes are not allowed")
        raw = await run_blocking(self.executor, self._connect, write)
        return SqliteConnection(self, raw, write=write)

    def change_hint(self) -> object:
        """Size and mtime of the database file and its ``-wal`` file. Uses
        ``os.stat`` only; never opens a connection. ``None`` for memory
        databases (no cheap hint exists)."""
        if self._path is None:
            return None
        return (_stat(self._path), _stat(self._path + "-wal"))

    async def close(self) -> None:
        with _ANCHOR_LOCK:
            anchor, self._anchor = self._anchor, None
        if anchor is not None:
            anchor.close()

    # -- internals -----------------------------------------------------------

    def _uri(self, write: bool) -> str:
        if self.memory_name is not None:
            return f"file:{urllib.parse.quote(self.memory_name)}?mode=memory&cache=shared"
        assert self._path is not None
        base = Path(self._path).as_uri()
        if write:
            return base + ("?mode=rwc" if self.create else "?mode=rw")
        if self.immutable:
            return base + "?immutable=1"
        return base + "?mode=ro"

    def _connect(self, write: bool) -> sqlite3.Connection:
        if self.memory_name is not None:
            with _ANCHOR_LOCK:
                if self._anchor is None:
                    self._anchor = sqlite3.connect(
                        self._uri(True), uri=True, check_same_thread=False, autocommit=True
                    )
        elif self.create and not write and self._path is not None:
            # A read before the first write: an empty file is a valid empty
            # database, so create one rather than fail with CANTOPEN.
            try:
                fd = os.open(self._path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
            except FileExistsError:
                pass
            else:
                os.close(fd)
        if not write and not self.immutable and self._path is not None:
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
        assert self._path is not None
        if not os.path.exists(self._path + "-journal"):
            return
        try:
            rw = sqlite3.connect(
                Path(self._path).as_uri() + "?mode=rw", uri=True, timeout=self.busy_timeout
            )
        except sqlite3.Error:
            return  # read-only file or directory: nothing we can do
        try:
            rw.execute("select count(*) from sqlite_master").fetchone()
        except sqlite3.Error:
            pass
        finally:
            rw.close()


def _stat(path: str) -> tuple[int, int] | None:
    try:
        st = os.stat(path)
    except FileNotFoundError:
        return None
    return (st.st_size, st.st_mtime_ns)


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
        if is_insert:
            # last_insert_rowid() is per connection and is not touched by an
            # upsert that took the UPDATE path, by OR IGNORE / DO NOTHING
            # skips or by WITHOUT ROWID tables. Comparing before and after
            # tells us whether *this* statement inserted a rowid row.
            rowid_before = raw.execute("select last_insert_rowid()").fetchone()[0]
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
        last_pk = (
            lastrowid
            if is_insert and rowcount > 0 and lastrowid != rowid_before
            else None
        )
        columns = tuple(Column(d[0]) for d in description) if description else ()
        return Results.from_tuples(
            columns, rows, truncated=truncated, rowcount=rowcount, last_pk=last_pk
        )

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
            with _TimeLimit(raw, ms):
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
    version = raw.execute("pragma schema_version").fetchone()[0]
    master = raw.execute(
        "select type, name, tbl_name, sql from sqlite_master order by rowid"
    ).fetchall()
    kinds: dict[str, str] = {}
    try:
        for _schema, name, ttype, *_rest in raw.execute(
            "select * from pragma_table_list where schema = 'main'"
        ):
            kinds[name] = ttype
    except sqlite3.OperationalError:
        pass  # SQLite < 3.37
    index_sql = {name: sql for type_, name, _t, sql in master if type_ == "index"}
    tables: dict[str, TableInfo] = {}
    views: dict[str, TableInfo] = {}
    triggers: dict[str, Trigger] = {}

    pk_cache: dict[str, tuple[str, ...]] = {}

    def columns_of(name: str) -> tuple[ColumnInfo, ...]:
        cols = []
        for cid, cname, ctype, notnull, dflt, pk, hidden in raw.execute(
            "select cid, name, type, \"notnull\", dflt_value, pk, hidden "
            "from pragma_table_xinfo(?)",
            (name,),
        ):
            cols.append(
                ColumnInfo(
                    name=cname,
                    type=ctype or "",
                    not_null=bool(notnull),
                    default=dflt,
                    primary_key=pk,
                    hidden=hidden == 1,
                    generated=hidden in (2, 3),
                )
            )
        return tuple(cols)

    def pks_of(name: str) -> tuple[str, ...]:
        if name not in pk_cache:
            rows = raw.execute(
                "select name, pk from pragma_table_info(?) where pk > 0 order by pk", (name,)
            ).fetchall()
            pk_cache[name] = tuple(r[0] for r in rows)
        return pk_cache[name]

    for type_, name, tbl_name, sql in master:
        if type_ == "trigger":
            triggers[name] = Trigger(name=name, table=tbl_name, definition=sql)
            continue
        if type_ not in ("table", "view") or name.startswith("sqlite_"):
            continue
        columns = columns_of(name)
        if type_ == "view":
            views[name] = TableInfo(name=name, kind="view", columns=columns, definition=sql)
            continue
        fks: dict[int, list[tuple[Any, ...]]] = {}
        for fid, seq, other, frm, to, on_update, on_delete, _match in raw.execute(
            "select id, seq, \"table\", \"from\", \"to\", on_update, on_delete, match "
            "from pragma_foreign_key_list(?) order by id, seq",
            (name,),
        ):
            fks.setdefault(fid, []).append((other, frm, to, on_update, on_delete))
        foreign_keys = []
        for parts in fks.values():
            other = parts[0][0]
            to_cols = tuple(p[2] for p in parts)
            if any(c is None for c in to_cols):
                to_cols = pks_of(other) or to_cols
            foreign_keys.append(
                ForeignKey(
                    columns=tuple(p[1] for p in parts),
                    other_table=other,
                    other_columns=tuple(to_cols),
                    on_update=parts[0][3],
                    on_delete=parts[0][4],
                )
            )
        indexes = []
        for _seq, iname, unique, origin, partial in raw.execute(
            "select seq, name, \"unique\", origin, partial from pragma_index_list(?) "
            "order by seq",
            (name,),
        ).fetchall():
            icols = tuple(
                r[0]
                for r in raw.execute(
                    "select name from pragma_index_info(?) order by seqno", (iname,)
                )
            )
            indexes.append(
                Index(
                    name=iname,
                    columns=icols,
                    unique=bool(unique),
                    origin=origin,
                    partial=bool(partial),
                    definition=index_sql.get(iname),
                )
            )
        kind = kinds.get(name, "table")
        tables[name] = TableInfo(
            name=name,
            kind="table",
            columns=columns,
            foreign_keys=tuple(foreign_keys),
            indexes=tuple(sorted(indexes, key=lambda i: i.name)),
            definition=sql,
            virtual=kind == "virtual" or (sql or "").lstrip().upper().startswith("CREATE VIRTUAL"),
            hidden=kind == "shadow",
        )
    return Schema(version=version, tables=tables, views=views, triggers=triggers)
