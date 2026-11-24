"""PostgreSQL backend, using ``asyncpg`` (natively async: no threads).

* **One ``Database`` = one schema.** Every connection is opened with
  ``search_path`` set to the configured schema, so table names in the API
  stay unqualified. ``create_schema=True`` (default) creates the schema on
  the first connection.
* **No driver pool.** The core pool is the pool: ``open()`` makes one
  ``asyncpg`` connection and the core checks it in and out, closes idle ones
  and closes everything on ``sleep()``. An ``asyncpg.Pool`` underneath would
  be a second pool with its own idle policy.
* **Parameters** arrive as ``$1..$n`` (the core rewrites ``:name`` / ``?``).
  PostgreSQL infers the type of an untyped parameter from context and falls
  back to ``text``, and ``asyncpg`` then refuses a Python ``int`` for it
  (``select :a`` with ``{"a": 1}``). Where the inferred type is a string
  type and the value is an ``int``, ``float``, ``bool`` or ``bytes``, the
  statement is re-prepared with an explicit cast on that parameter
  (``$1::int8``), which is what SQLite's dynamic typing gives callers.
* **Read connections** have ``default_transaction_read_only=on``, the
  equivalent of SQLite's ``mode=ro``. ``mode="read"`` transactions are
  ``REPEATABLE READ, READ ONLY``; write transactions use the server default
  isolation. Writers are not queued: ``capabilities.concurrent_writers`` is
  true and the core pool lets up to ``max_write_connections`` writers in.
* **Time limits**: ``statement_timeout`` (``SET LOCAL`` in a transaction,
  session ``SET`` otherwise; only re-sent when the value changes) plus the
  driver's per-call timeout, a little longer, as a backstop that cancels on
  the server.
* **Transaction deadline** is enforced by the core's timer and backed by
  ``idle_in_transaction_session_timeout`` (deadline plus
  ``server_deadline_grace_ms``), so the server ends the session even if the
  client stops running.
* **Statement savepoints**: PostgreSQL aborts a whole transaction when one
  statement fails; SQLite keeps it usable. With ``statement_savepoints=True``
  (default) each call on a transaction runs inside a savepoint and a failed
  call is rolled back to it, so ``try: await tx.execute(...) except
  IntegrityError: ...`` works the same on both. It costs one extra round
  trip per call inside a transaction (``RELEASE`` of the previous
  savepoint and the new ``SAVEPOINT`` go together in one simple query):
  measured at about 30-40 us on a local Unix socket, roughly +45% for a
  one-row statement; on a remote server, one network round trip per call.
  Calls outside transactions are unaffected. ``statement_savepoints=False``
  removes it, and ``capabilities.statement_errors_abort_transaction`` then
  says so. A named operation runs inside one savepoint, so a failed
  ``tx.insert_rows(..., alter=True)`` is undone whole, columns included.
* **Introspection** reads ``pg_catalog`` restricted to the schema.
  ``schema_version()`` is an md5 of a catalog listing; ``change_hint()`` is
  a time bucket of ``schema_ttl`` seconds plus a counter bumped by
  ``Database.refresh_schema()``.
* **Values** are passed through as ``asyncpg`` returns them (``Decimal``,
  ``datetime``, ``UUID``, lists, ...).

Capability rounding: ``hard_kill`` is false (there is a server-side cancel,
but nothing that stops a statement that ignores it); ``memory_limit`` is
false (``work_mem`` is per operation, not a limit on a query);
``full_text_search`` is false (PostgreSQL has it, with different syntax
from the SQLite FTS the flag stands for).
"""

from __future__ import annotations

import asyncio
import datetime
import time
import uuid
from collections.abc import Awaitable, Callable, Mapping, Sequence
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from ..core.params import POSTGRES_FLAVOR, dollar_parameters, leading_keyword
from ..core.results import Column, Results, WriteResult
from ..core.schema import Schema
from ..errors import DblayerError, QueryError, ResourceLimitExceeded
from ._postgres_common import DML as _DML
from ._postgres_common import NO_SCHEMA_CHANGE as _NO_SCHEMA_CHANGE
from ._postgres_common import SAVEPOINT as _SAVEPOINT
from ._postgres_common import TX_CONTROL as _TX_CONTROL
from ._postgres_common import VERSION_SQL as _VERSION_SQL
from ._postgres_common import ConnTx as _ConnTx
from ._postgres_common import PostgresBackendBase, PostgresDialectBase, map_sqlstate
from ._postgres_common import begin_sql as _begin_sql
from ._postgres_common import has_top_level_returning as _has_top_level_returning
from ._postgres_common import interrupted as _interrupted
from ._postgres_common import read_schema, upsert_rows_native
from ._postgres_common import row_bytes as _row_bytes
from ._postgres_common import rowcount_from_status as _rowcount
from .base import Dialect, NativeParams

if TYPE_CHECKING:
    import asyncpg

__all__ = ["Postgres", "PostgresDialect", "PostgresConnection"]


class PostgresDialect(PostgresDialectBase):
    """The PostgreSQL dialect with asyncpg's exceptions mapped."""

    def map_driver_exception(self, exc: BaseException) -> DblayerError | None:
        try:
            import asyncpg
        except ImportError:  # pragma: no cover - asyncpg is required to get here
            return None
        if type(exc) is asyncpg.DataError:
            # Raised by asyncpg before sending: a value its codec for the
            # parameter's type cannot encode.
            return QueryError(str(exc), "DATA_ERROR")
        if isinstance(exc, asyncpg.PostgresError):
            return map_sqlstate(exc.sqlstate or "", str(exc))
        return None


# -- values and statements ----------------------------------------------------

_STRING_TYPES = frozenset({"text", "varchar", "bpchar", "name", "unknown"})
# Types whose asyncpg codec takes a str as it is.
_ACCEPTS_STR = _STRING_TYPES | {"json", "jsonb", "xml", "uuid", "citext", "char"}
_INT8_MIN, _INT8_MAX = -(2**63), 2**63 - 1


def _cast_for(value: Any) -> str | None:
    """The cast that makes PostgreSQL accept ``value`` for a parameter it
    would otherwise type as ``text``; ``None`` if no cast is needed."""
    if value is None or isinstance(value, str):
        return None
    if isinstance(value, int):  # bool included: asyncpg encodes it as 0/1
        return "int8" if _INT8_MIN <= value <= _INT8_MAX else "numeric"
    if isinstance(value, float):
        return "float8"
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "bytea"
    if isinstance(value, Decimal):
        return "numeric"
    if isinstance(value, datetime.datetime):
        return "timestamptz" if value.tzinfo is not None else "timestamp"
    if isinstance(value, datetime.date):
        return "date"
    if isinstance(value, datetime.time):
        return "time"
    if isinstance(value, uuid.UUID):
        return "uuid"
    return None


def _with_casts(sql: str, casts: Mapping[int, str]) -> str:
    """Replace each ``$n`` in ``casts`` by ``casts[n].format(p="$n")``."""
    parts = []
    pos = 0
    for start, end, n in dollar_parameters(sql, POSTGRES_FLAVOR):
        if n in casts:
            parts.append(sql[pos:start])
            parts.append(casts[n].format(p=sql[start:end]))
            pos = end
    parts.append(sql[pos:])
    return "".join(parts)


def _as_text(value: Any) -> Any:
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float, Decimal)):
        return str(value)
    if isinstance(value, (datetime.date, datetime.time, uuid.UUID)):
        return value.isoformat() if not isinstance(value, uuid.UUID) else str(value)
    return value


def _close_awaitable(aw: Awaitable[Any]) -> None:
    close = getattr(aw, "close", None)
    if close is not None:
        close()


async def _prepare(raw: "asyncpg.Connection", sql: str) -> Any:
    """A prepared statement from asyncpg's per-connection statement cache, so
    a repeated statement costs one round trip. The public
    ``Connection.prepare()`` bypasses the cache and would add a Parse round
    trip to every call. (Private asyncpg API, checked against 0.32.)"""
    return await raw._prepare(sql, use_cache=True)


# -- backend ------------------------------------------------------------------

_DIALECT = PostgresDialect()


class Postgres(PostgresBackendBase):
    """PostgreSQL backend on asyncpg: one ``Database`` is one schema of the
    database named by ``dsn``. Settings: see ``PostgresBackendBase``;
    ``statement_savepoints``: see the module docstring."""

    dialect: Dialect = _DIALECT

    # -- Backend protocol --------------------------------------------------

    async def open(self, *, write: bool) -> "PostgresConnection":
        import asyncpg

        settings = self.session_settings(write=write)
        kwargs: dict[str, Any] = {
            # asyncpg expires cached statements with one loop timer per
            # entry; 0 turns that off (the cache is still bounded by size),
            # so an idle connection schedules nothing on the loop.
            "max_cached_statement_lifetime": 0,
            **self.connect_kwargs,
        }
        raw = await asyncpg.connect(
            self.dsn, server_settings=settings, timeout=self.connect_timeout, **kwargs
        )
        try:
            if self.create_schema and not self._schema_ready:
                await self._ensure_schema(raw)
        except BaseException:
            raw.terminate()
            raise
        return PostgresConnection(self, raw, write=write)

    # -- internals -----------------------------------------------------------

    async def _ensure_schema(self, raw: "asyncpg.Connection") -> None:
        import asyncpg

        exists = await raw.fetchval(
            "select exists(select 1 from pg_namespace where nspname = $1)", self.schema
        )
        if not exists:
            await raw.execute("BEGIN READ WRITE")
            try:
                await raw.execute(
                    f"CREATE SCHEMA IF NOT EXISTS {_DIALECT.quote_identifier(self.schema)}"
                )
                await raw.execute("COMMIT")
            except asyncpg.UniqueViolationError:
                # Another connection created it at the same moment.
                await raw.execute("ROLLBACK")
            except BaseException:
                if not raw.is_closed():
                    await raw.execute("ROLLBACK")
                raise
        self._schema_ready = True


# -- connection ---------------------------------------------------------------


class PostgresConnection:
    """One asyncpg connection, checked out by one task at a time.

    Every driver call runs as an inner task (``_call``) so ``interrupt()``
    can cancel it; asyncpg turns that cancellation into a server-side
    cancel request and keeps the connection usable.
    """

    def __init__(self, backend: Postgres, raw: "asyncpg.Connection", *, write: bool) -> None:
        self._backend = backend
        self.backend = backend
        self._raw = raw
        self.write = write
        self._closed = False
        self._session_timeout = 0
        self._local_timeout: int | None = None
        self._sp_pending_release = False
        self._inflight: asyncio.Future[Any] | None = None
        self._interrupting = False

    def __repr__(self) -> str:
        kind = "write" if self.write else "read"
        return f"<PostgresConnection {kind} pid={self._raw.get_server_pid()}>"

    @property
    def server_pid(self) -> int:
        return self._raw.get_server_pid()

    # -- plumbing ------------------------------------------------------------

    async def _call[T](self, aw: Awaitable[T]) -> T:
        task = asyncio.ensure_future(aw)
        self._inflight = task
        try:
            return await task
        except asyncio.CancelledError:
            me = asyncio.current_task()
            if self._interrupting and (me is None or me.cancelling() == 0):
                raise _interrupted() from None
            raise
        except TimeoutError:
            raise _interrupted() from None
        finally:
            self._inflight = None
            self._interrupting = False

    def _in_tx(self) -> bool:
        return self._raw.is_in_transaction()

    def _driver_timeout(self, time_limit_ms: int | None) -> float | None:
        if time_limit_ms is None:
            return None
        return (max(time_limit_ms, 1) + self._backend.time_limit_grace_ms) / 1000

    def _prelude(self, time_limit_ms: int | None, savepoint: bool) -> str:
        """Simple-query SQL to send before a statement, or ``""``: release
        the previous statement's savepoint, set ``statement_timeout`` if it
        must change, open a new savepoint. The timeout is set outside the
        savepoint so rolling back to it does not undo the SET."""
        parts = []
        in_tx = self._in_tx()
        if in_tx and self._sp_pending_release:
            parts.append(f"RELEASE SAVEPOINT {_SAVEPOINT}")
            self._sp_pending_release = False
        want = 0 if time_limit_ms is None else max(int(time_limit_ms), 1)
        if in_tx:
            current = (
                self._local_timeout if self._local_timeout is not None else self._session_timeout
            )
            if current != want:
                parts.append(f"SET LOCAL statement_timeout = {want}")
                self._local_timeout = want
        elif self._session_timeout != want:
            parts.append(f"SET statement_timeout = {want}")
            self._session_timeout = want
        if savepoint:
            parts.append(f"SAVEPOINT {_SAVEPOINT}")
        return "; ".join(parts)

    def _use_savepoint(self, sql: str | None) -> bool:
        if not (self._backend.statement_savepoints and self._in_tx()):
            return False
        return sql is None or leading_keyword(sql, POSTGRES_FLAVOR) not in _TX_CONTROL

    async def _guarded[T](
        self, sql: str | None, time_limit_ms: int | None, body: Callable[[], Awaitable[T]]
    ) -> T:
        """Run ``body()`` after the prelude. Inside a transaction a failure
        is rolled back to the statement savepoint, so the transaction stays
        usable, as it does on SQLite. ``sql=None`` means a whole named
        operation.

        A cached prepared statement that another connection's DDL made
        stale fails with ``InvalidCachedStatementError``; asyncpg retries
        that itself outside a transaction, and the savepoint lets it be
        retried (once, after dropping the statement cache) inside one."""
        import asyncpg

        savepoint = self._use_savepoint(sql)
        if sql is not None and not savepoint and self._in_tx() and self._sp_pending_release:
            # A transaction-control statement: settle our savepoint first so
            # it cannot swallow the caller's.
            await self._raw.execute(f"RELEASE SAVEPOINT {_SAVEPOINT}")
            self._sp_pending_release = False
        prelude = self._prelude(time_limit_ms, savepoint)
        if prelude:
            await self._raw.execute(prelude)
        for attempt in (1, 2):
            try:
                result = await body()
            except asyncio.CancelledError:
                raise
            except BaseException as e:
                retry = attempt == 1 and isinstance(e, asyncpg.InvalidCachedStatementError)
                if not savepoint:
                    if retry and not self._in_tx():
                        # An autocommit statement: nothing ran. (asyncpg
                        # retries this itself, but not for statements taken
                        # from its cache as PreparedStatement objects.)
                        await self._raw.reload_schema_state()
                        continue
                    raise
                if self._raw.is_closed() or not self._in_tx():
                    raise
                try:
                    if retry:
                        await self._raw.execute(f"ROLLBACK TO SAVEPOINT {_SAVEPOINT}")
                        await self._raw.reload_schema_state()
                        continue
                    await self._raw.execute(
                        f"ROLLBACK TO SAVEPOINT {_SAVEPOINT}; RELEASE SAVEPOINT {_SAVEPOINT}"
                    )
                except Exception:
                    pass
                raise
            if savepoint:
                self._sp_pending_release = True
            return result
        raise AssertionError("unreachable")  # pragma: no cover

    async def _statement(self, sql: str, rows: list[list[Any]]) -> Any:
        """A cached prepared statement for ``sql`` (an asyncpg
        ``PreparedStatement``), adapted to the parameter values in ``rows``
        (one list per execution).

        asyncpg's codecs are strict about Python types, where SQLite takes
        any value anywhere. Two adaptations, both per parameter:

        * PostgreSQL types a parameter it cannot infer as ``text`` and asyncpg
          then refuses a non-``str``. If every non-null value is of one
          non-string kind, the statement is re-prepared with a cast
          (``$1::int8``), so ``select :a`` returns the value as SQLite
          would; if strings and other values are mixed, the others are sent
          as their text (what SQLite's TEXT affinity stores).
        * A ``str`` for a parameter of another type (``cast(:x as
          integer)`` with ``"42"``) is sent as text and converted by the
          server, ``($1::text)::int4``, as an untyped literal would be.

        ``rows`` is changed in place where values are sent as text."""
        stmt = await _prepare(self._raw, sql)
        if not rows or not rows[0]:
            return stmt
        casts: dict[int, str] = {}
        for n, ptype in enumerate(stmt.get_parameters(), start=1):
            i = n - 1
            if ptype.name in _STRING_TYPES:
                kinds = {_cast_for(r[i]) for r in rows if r[i] is not None}
                if not kinds or kinds == {None}:
                    continue
                if len(kinds) == 1:
                    casts[n] = "{p}::" + kinds.pop()  # type: ignore[operator]
                    continue
            elif ptype.name in _ACCEPTS_STR or not any(isinstance(r[i], str) for r in rows):
                continue
            else:
                q = _DIALECT.quote_identifier
                casts[n] = "({p}::text)::" + f"{q(ptype.schema)}.{q(ptype.name)}"
            for r in rows:
                r[i] = _as_text(r[i])
        if casts:
            stmt = await _prepare(self._raw, _with_casts(sql, casts))
        return stmt

    # -- execute -------------------------------------------------------------

    async def execute(
        self,
        sql: str,
        params: NativeParams,
        *,
        time_limit_ms: int | None,
        max_rows: int | None,
        max_result_bytes: int | None = None,
    ) -> Results:
        args = list(params) if params else []
        return await self._call(
            self._guarded(
                sql,
                time_limit_ms,
                lambda: self._execute(sql, args, time_limit_ms, max_rows, max_result_bytes),
            )
        )

    async def _execute(
        self,
        sql: str,
        args: list[Any],
        time_limit_ms: int | None,
        max_rows: int | None,
        max_result_bytes: int | None,
    ) -> Results:
        raw = self._raw
        timeout = self._driver_timeout(time_limit_ms)
        stmt = await self._statement(sql, [args])
        attrs = stmt.get_attributes()
        verb = leading_keyword(sql, POSTGRES_FLAVOR)
        is_dml = verb in _DML
        if attrs and max_rows is not None and not is_dml:
            # Fetch only max_rows + 1 rows: an unnamed portal with a row
            # limit, which works outside a transaction. (Private asyncpg
            # API, as fetchrow() uses it.) DML always runs to the end so
            # the command tag gives the row count.
            rows, status, _done = await raw._execute(
                stmt.get_query(), args, max_rows + 1, timeout, return_status=True
            )
        else:
            rows = await stmt.fetch(*args, timeout=timeout)
            status = stmt.get_statusmsg()
        if isinstance(status, bytes):
            status = status.decode()
        truncated = False
        if max_rows is not None and len(rows) > max_rows:
            truncated = True
            rows = rows[:max_rows]
        if max_result_bytes is not None:
            size = 0
            for r in rows:
                size += _row_bytes(r)
                if size > max_result_bytes:
                    raise ResourceLimitExceeded(
                        f"result exceeded max_result_bytes={max_result_bytes}"
                    )
        if verb not in _NO_SCHEMA_CHANGE:
            # Our own DDL makes cached statements on this connection stale
            # ("cached plan must not change result type"); drop them now
            # rather than fail and retry later.
            await raw.reload_schema_state()
        columns = tuple(Column(a.name, a.type.name) for a in attrs)
        return Results.from_tuples(
            columns,
            rows,
            truncated=truncated,
            rowcount=_rowcount(status) if is_dml else -1,
        )

    async def execute_batch(
        self,
        statements: Sequence[tuple[str, NativeParams]],
        *,
        time_limit_ms: int | None,
        max_rows: int | None,
    ) -> list[Results]:
        """``SupportsBatch``: every statement in one inner task, with no
        statement savepoints (a failing batch rolls the transaction back)."""

        async def run() -> list[Results]:
            out = []
            prelude = self._prelude(time_limit_ms, False)
            if prelude:
                await self._raw.execute(prelude)
            for sql, params in statements:
                args = list(params) if params else []
                out.append(await self._execute(sql, args, time_limit_ms, max_rows, None))
            return out

        return await self._call(run())

    async def execute_many(self, sql: str, params_seq: Sequence[NativeParams]) -> WriteResult:
        """Pipelined (one round trip for the whole set) and atomic, as
        asyncpg's ``executemany`` is. asyncpg reports no row counts, so a
        plain INSERT/UPDATE/DELETE is sent as ``WITH n AS (<sql> RETURNING
        1) SELECT count(*) FROM n`` to count the rows it touched."""
        seq = [list(p) if p else [] for p in params_seq]
        if not seq:
            return WriteResult(rowcount=0)
        return await self._call(self._guarded(sql, None, lambda: self._execute_many(sql, seq)))

    async def _execute_many(self, sql: str, seq: list[list[Any]]) -> WriteResult:
        verb = leading_keyword(sql, POSTGRES_FLAVOR)
        body = sql.strip().rstrip(";").rstrip()
        first = body.split(None, 1)[0].upper() if body else ""
        if verb in ("INSERT", "UPDATE", "DELETE") and first == verb:
            returning = "" if _has_top_level_returning(body) else " RETURNING 1"
            counted = f"WITH _dblayer_n AS ({body}{returning}) SELECT count(*) FROM _dblayer_n"
            stmt = await self._statement(counted, seq)
            rows = await stmt.fetchmany(seq)
            return WriteResult(rowcount=sum(r[0] for r in rows))
        stmt = await self._statement(sql, seq)
        if verb in _DML:
            # WITH ... INSERT and MERGE cannot be nested in a CTE: one
            # round trip per parameter set, counting from command tags.
            total = 0
            for args in seq:
                await stmt.fetch(*args)
                total += max(_rowcount(stmt.get_statusmsg()), 0)
            return WriteResult(rowcount=total)
        await stmt.executemany(seq)
        return WriteResult(rowcount=-1)

    def stream(
        self,
        sql: str,
        params: NativeParams,
        *,
        chunk_rows: int,
        time_limit_ms: int | None = None,
        max_result_bytes: int | None = None,
    ) -> "_PostgresStream":
        return _PostgresStream(
            self, sql, list(params) if params else [], chunk_rows, time_limit_ms, max_result_bytes
        )

    # -- transactions ----------------------------------------------------------

    async def begin(self, mode: str) -> None:
        await self.begin_with_deadline(mode, None)

    async def begin_with_deadline(self, mode: str, deadline_ms: int | None) -> None:
        """``BEGIN`` plus, with a deadline, ``SET LOCAL
        idle_in_transaction_session_timeout`` (deadline + grace), in one
        round trip: if the client stops running, the server ends the
        session, which rolls the transaction back."""
        sql = _begin_sql(mode, deadline_ms, self._backend.server_deadline_grace_ms)
        self._end_tx()
        await self._call(self._raw.execute(sql))

    def _end_tx(self) -> None:
        self._local_timeout = None
        self._sp_pending_release = False

    async def commit(self) -> None:
        if self._closed or not self._in_tx():
            return
        try:
            status = await self._call(self._raw.execute("COMMIT"))
        finally:
            self._end_tx()
        if status == "ROLLBACK":
            # PostgreSQL answers COMMIT of a failed transaction with a
            # silent rollback.
            raise QueryError(
                "transaction was aborted by an earlier error and has been rolled back",
                "25P02",
            )

    async def rollback(self) -> None:
        # Cheap no-op without a transaction: the pool calls this on every
        # connection it gets back.
        if self._closed:
            return
        if self._raw.is_closed():
            # The server ended the session (idle_in_transaction timeout, a
            # restart...): nothing to roll back, but the pool must not
            # reuse it, and a failed reset makes the pool close it.
            self._end_tx()
            raise ConnectionError("the server closed this connection")
        if not self._in_tx():
            self._end_tx()
            return
        try:
            await self._call(self._raw.execute("ROLLBACK"))
        finally:
            self._end_tx()

    # -- schema ------------------------------------------------------------------

    async def schema_version(self) -> object:
        async def run() -> object:
            prelude = self._prelude(None, False)
            if prelude:
                await self._raw.execute(prelude)
            return await self._raw.fetchval(_VERSION_SQL, self._backend.schema)

        return await self._call(run())

    async def schema(self) -> Schema:
        return await self._call(self._schema())

    async def _schema(self) -> Schema:
        raw = self._raw
        own_tx = not self._in_tx()
        prelude = self._prelude(None, False)
        if own_tx:
            # One snapshot for every catalog query.
            prelude = "; ".join(
                p for p in (prelude, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY") if p
            )
        if prelude:
            await raw.execute(prelude)
        try:
            return await read_schema(self._fetch_many, self._backend.schema)
        finally:
            if own_tx and not raw.is_closed() and self._in_tx():
                await raw.execute("ROLLBACK")

    async def _fetch_many(self, queries: Sequence[str], schema: str) -> list[list[Any]]:
        return [await self._raw.fetch(q, schema) for q in queries]

    async def cached_schema(self) -> Schema:
        """The schema as this connection's transaction sees it: one version
        query, and the full catalog read only when the version is not the
        one last read through this backend."""
        version = await self._raw.fetchval(_VERSION_SQL, self._backend.schema)
        memo = self._backend._schema_memo
        if memo is not None and memo.version == version:
            return memo
        schema = await read_schema(self._fetch_many, self._backend.schema)
        self._backend._schema_memo = schema
        return schema

    async def run_native(self, sql: str, args: list[Any]) -> Results:
        """``ConnTx``: one statement inside the open transaction, without a
        savepoint of its own."""
        return await self._execute(sql, args, None, None, None)

    async def run_native_many(self, sql: str, seq: list[list[Any]]) -> WriteResult:
        return await self._execute_many(sql, seq)

    # -- native operations (SupportsOperations) -------------------------------------

    async def run_operation(self, name: str, args: dict[str, Any]) -> object:
        """Native ``insert_rows`` (``COPY``) and ``upsert_rows`` (one
        multi-row ``INSERT ... ON CONFLICT`` per group of rows), each inside
        one statement savepoint. Everything else, and the cases these do
        not handle, return ``NotImplemented`` before running anything."""
        if name == "insert_rows":
            op = self._op_insert_rows
        elif name == "upsert_rows":
            op = self._op_upsert_rows
        else:
            return NotImplemented
        if not _ConnTx.precheck(name, args):
            return NotImplemented
        return await self._call(self._guarded(None, None, lambda: op(**args)))

    async def _op_insert_rows(
        self,
        table: str,
        rows: list[dict[str, Any]],
        pk: list[str] | None = None,
        alter: bool = False,
        ignore: bool = False,
        replace: bool = False,
    ) -> object:
        from ..core.operations import OperationResult, _ensure_table, _groups

        tx = _ConnTx(self)
        created, added = await _ensure_table(tx, table, rows, tuple(pk or ()), alter)
        info = (await tx.schema()).tables.get(table)
        types = {c.name: c.type for c in info.columns} if info else {}
        total = 0
        for cols, group in _groups(rows):
            records = [tuple(r[c] for c in cols) for r in group]
            if all(_copyable(types.get(c), [rec[i] for rec in records]) for i, c in enumerate(cols)):
                status = await self._raw.copy_records_to_table(
                    table, records=records, columns=list(cols), schema_name=self._backend.schema
                )
                total += max(_rowcount(status), 0)
            else:
                # Values the binary COPY codecs would refuse (an int for a
                # text column...): the INSERT path adapts them.
                r = await tx.execute_many(self._backend.dialect.insert_sql(table, cols), records)
                total += max(r.rowcount, 0)
        return OperationResult(rowcount=total, table_created=created, columns_added=added)

    async def _op_upsert_rows(
        self,
        table: str,
        rows: list[dict[str, Any]],
        pk: list[str] | None = None,
        alter: bool = False,
    ) -> object:
        return await upsert_rows_native(_ConnTx(self), table, rows, pk, alter)

    # -- misc --------------------------------------------------------------------

    async def interrupt(self) -> None:
        """Cancel the statement running on this connection, if any (asyncpg
        sends a cancel request to the server). The interrupted call raises
        ``QueryInterrupted``."""
        task = self._inflight
        if task is not None and not task.done():
            self._interrupting = True
            task.cancel()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        raw = self._raw
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if getattr(raw, "_loop", running) is not running:
            # Left behind on another (usually closed) event loop: neither a
            # graceful close nor terminate() can run there, so close the
            # socket directly. The server sees end-of-file and rolls back.
            _close_socket(raw)
            return
        try:
            await asyncio.wait_for(raw.close(), 5)
        except BaseException:
            raw.terminate()
            raise



# Python types the binary COPY codecs take for a column type, by the type
# name introspection reports (without a modifier). Other column types, and
# other values, go through INSERT, which adapts values as execute() does.
_COPY_TYPES: dict[str, tuple[type, ...]] = {
    "BIGINT": (int,),
    "INTEGER": (int,),
    "SMALLINT": (int,),
    "DOUBLE PRECISION": (int, float),
    "REAL": (int, float),
    "NUMERIC": (int, float, Decimal),
    "TEXT": (str,),
    "VARCHAR": (str,),
    "CHAR": (str,),
    "BYTEA": (bytes, bytearray, memoryview),
    "BOOLEAN": (bool,),
}


def _copyable(column_type: str | None, values: Sequence[Any]) -> bool:
    if column_type is None:
        return False
    accepted = _COPY_TYPES.get(column_type.split("(", 1)[0])
    if accepted is None:
        return False
    return all(v is None or isinstance(v, accepted) for v in values)


def _close_socket(raw: "asyncpg.Connection") -> None:
    transport = getattr(raw, "_transport", None)
    sock = getattr(transport, "_sock", None)  # private asyncio attribute
    if sock is not None:
        sock.close()
    else:  # pragma: no cover - other transport implementations
        try:
            raw.terminate()
        except Exception:
            pass


class _PostgresStream:
    """A server-side cursor fetched ``chunk_rows`` at a time, one round trip
    per chunk, so a slow consumer applies backpressure. Cursors need a
    transaction: outside one, the stream opens a ``REPEATABLE READ READ
    ONLY`` transaction and ends it on close. ``time_limit_ms`` is a budget
    of time spent waiting on the server across chunks, enforced with a
    client-side timeout that cancels on the server."""

    def __init__(
        self,
        conn: PostgresConnection,
        sql: str,
        args: list[Any],
        chunk_rows: int,
        time_limit_ms: int | None,
        max_result_bytes: int | None,
    ) -> None:
        if chunk_rows < 1:
            raise ValueError("chunk_rows must be at least 1")
        self._conn = conn
        self._sql = sql
        self._args = args
        self._chunk_rows = chunk_rows
        self._remaining: float | None = None if time_limit_ms is None else time_limit_ms / 1000
        self._max_bytes = max_result_bytes
        self._bytes = 0
        self._cursor: Any = None
        self._own_tx = False
        self._started = False
        self._done = False
        self.columns: tuple[Column, ...] | None = None

    def __aiter__(self) -> "_PostgresStream":
        return self

    async def __anext__(self) -> list[tuple[Any, ...]]:
        if self._done:
            raise StopAsyncIteration
        try:
            chunk = await self._timed(self._next_chunk())
        except BaseException:
            await self.aclose()
            raise
        if not chunk:
            await self.aclose()
            raise StopAsyncIteration
        return chunk

    async def _timed[T](self, aw: Awaitable[T]) -> T:
        if self._remaining is None:
            return await self._conn._call(aw)
        if self._remaining <= 0:
            _close_awaitable(aw)
            raise _interrupted()
        start = time.perf_counter()
        try:
            return await self._conn._call(asyncio.wait_for(aw, self._remaining))
        finally:
            self._remaining -= time.perf_counter() - start

    async def _next_chunk(self) -> list[Any]:
        conn = self._conn
        raw = conn._raw
        if not self._started:
            self._started = True
            prelude = conn._prelude(None, False)
            if not conn._in_tx():
                self._own_tx = True
                prelude = "; ".join(
                    p for p in (prelude, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY") if p
                )
            if prelude:
                await raw.execute(prelude)
            stmt = await conn._statement(self._sql, [self._args])
            self.columns = tuple(Column(a.name, a.type.name) for a in stmt.get_attributes())
            if not self.columns:
                await stmt.fetch(*self._args)
                return []
            self._cursor = await stmt.cursor(*self._args)
        chunk = await self._cursor.fetch(self._chunk_rows)
        if self._max_bytes is not None:
            for r in chunk:
                self._bytes += _row_bytes(r)
            if self._bytes > self._max_bytes:
                raise ResourceLimitExceeded(f"result exceeded max_result_bytes={self._max_bytes}")
        return chunk

    async def aclose(self) -> None:
        if self._done:
            return
        self._done = True
        self._cursor = None
        conn = self._conn
        if self._own_tx and not conn._raw.is_closed() and conn._in_tx():
            try:
                await conn._call(conn._raw.execute("ROLLBACK"))
            finally:
                conn._end_tx()
