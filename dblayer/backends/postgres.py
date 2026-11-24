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
  IntegrityError: ...`` works the same on both. Costs one extra round trip
  per call inside a transaction.
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
from collections.abc import Awaitable, Mapping, Sequence
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from ..core.params import POSTGRES_FLAVOR, ParamStyle, _scan, dollar_parameters, leading_keyword
from ..core.results import Column, Results, WriteResult
from ..core.schema import ColumnInfo, ForeignKey, Index, Schema, TableInfo, Trigger
from ..errors import (
    DblayerError,
    IntegrityError,
    QueryError,
    QueryInterrupted,
    ResourceLimitExceeded,
    TransactionExpired,
    WriteConflict,
)
from .base import Capabilities, Dialect, NativeParams

if TYPE_CHECKING:
    import asyncpg

__all__ = ["Postgres", "PostgresDialect", "PostgresConnection"]

_FK_ACTIONS = {
    "a": "NO ACTION",
    "r": "RESTRICT",
    "c": "CASCADE",
    "n": "SET NULL",
    "d": "SET DEFAULT",
}

_MAX_IDENTIFIER = 63


class PostgresDialect(Dialect):
    name = "postgres"
    paramstyle = ParamStyle.NUMERIC_DOLLAR
    flavor = POSTGRES_FLAVOR
    type_map = {
        # SQLite integers are 64-bit; INTEGER here would be 32-bit.
        "integer": "BIGINT",
        "float": "DOUBLE PRECISION",
        "text": "TEXT",
        "blob": "BYTEA",
    }
    native_alter = frozenset({"rename", "drop", "types", "pk"})
    rowid_column = None
    # Dropping a table other tables reference fails (no CASCADE is ever
    # sent), so nothing is touched silently.
    foreign_keys_enforced_sql = None

    def literal(self, value: Any) -> str:
        # Portable columns have no boolean type (bool maps to integer), so
        # render booleans as numbers, as SQLite stores them.
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, Decimal):
            return str(value)
        return super().literal(value)

    def alter_column_type_sql(self, table: str, column: str, type_name: str) -> str:
        native = self.native_type(type_name)
        col = self.quote_identifier(column)
        return (
            f"ALTER TABLE {self.quote_identifier(table)} ALTER COLUMN {col} "
            f"TYPE {native} USING {col}::{native}"
        )

    def set_primary_key_sql(
        self, table: str, current_constraint: str | None, pk: Sequence[str]
    ) -> list[str]:
        """Replace the primary key in one ``ALTER TABLE``."""
        actions = []
        if current_constraint:
            actions.append(f"DROP CONSTRAINT {self.quote_identifier(current_constraint)}")
        if pk:
            actions.append(f"ADD PRIMARY KEY ({self.quote_identifiers(pk)})")
        if not actions:
            return []
        return [f"ALTER TABLE {self.quote_identifier(table)} " + ", ".join(actions)]

    def swap_rename_sql(self, staging: str, target: str) -> list[str]:
        """Rename staging into place, then rename the constraints it was
        created with (``<staging>_pkey``, ``<staging>_<col>_key``...) to the
        names PostgreSQL would have given them on ``target``. Constraint
        names are per schema, so a later import would otherwise collide or
        leave staging names behind. Views referring to the old target do
        not survive: PostgreSQL refuses to drop a table a view depends on."""
        reg = self.literal(self.quote_identifier(target))
        return [
            self.rename_table_sql(staging, target),
            "DO $dblayer$\nDECLARE r record;\nBEGIN\n"
            f"  FOR r IN SELECT conname FROM pg_constraint WHERE conrelid = {reg}::regclass\n"
            f"           AND starts_with(conname, {self.literal(staging)}) LOOP\n"
            "    EXECUTE format('ALTER TABLE %s RENAME CONSTRAINT %I TO %I',\n"
            f"      {reg}::regclass, r.conname,\n"
            f"      left({self.literal(target)} || substr(r.conname, {len(staging) + 1}), "
            f"{_MAX_IDENTIFIER}));\n"
            "  END LOOP;\nEND\n$dblayer$",
        ]

    def map_exception(self, exc: BaseException) -> DblayerError | None:
        try:
            import asyncpg
        except ImportError:  # pragma: no cover - asyncpg is required to get here
            return None
        if isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
            return QueryInterrupted("query interrupted (time limit exceeded or cancelled)")
        if type(exc) is asyncpg.DataError:
            # Raised by asyncpg before sending: a value its codec for the
            # parameter's type cannot encode.
            return QueryError(str(exc), "DATA_ERROR")
        if isinstance(exc, asyncpg.PostgresError):
            state: str = exc.sqlstate or ""
            message = str(exc)
            if state == "57014":
                return QueryInterrupted(
                    "query interrupted (time limit exceeded or cancelled)"
                )
            if state in ("40001", "40P01"):
                return WriteConflict(message)
            if state == "25P03":
                return TransactionExpired(
                    "the server ended the session: the transaction was idle past "
                    "its deadline and was rolled back"
                )
            if state.startswith("23"):
                return IntegrityError(message, state)
            if state[:2] in ("53", "54"):
                return ResourceLimitExceeded(message)
            return QueryError(message, state or None)
        return None


def _rowcount(status: str | None) -> int:
    """Rows affected from a command tag such as ``INSERT 0 3``."""
    if not status:
        return -1
    last = status.rsplit(" ", 1)[-1]
    return int(last) if last.isdigit() else -1


# -- values and statements ----------------------------------------------------

_DML = frozenset({"INSERT", "UPDATE", "DELETE", "MERGE"})
# Statements that manage the transaction themselves: never wrapped in a
# statement savepoint.
_TX_CONTROL = frozenset(
    {"BEGIN", "START", "COMMIT", "END", "ROLLBACK", "ABORT", "SAVEPOINT", "RELEASE", "PREPARE"}
)
_STRING_TYPES = frozenset({"text", "varchar", "bpchar", "name", "unknown"})
_INT8_MIN, _INT8_MAX = -(2**63), 2**63 - 1
_SAVEPOINT = "dblayer_stmt"


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
    parts = []
    pos = 0
    for _start, end, n in dollar_parameters(sql, POSTGRES_FLAVOR):
        if n in casts:
            parts.append(sql[pos:end])
            parts.append(f"::{casts[n]}")
            pos = end
    parts.append(sql[pos:])
    return "".join(parts)


def _has_top_level_returning(sql: str) -> bool:
    depth = 0
    for kind, start, end in _scan(sql, POSTGRES_FLAVOR, words=True):
        if kind == "(":
            depth += 1
        elif kind == ")":
            depth = max(0, depth - 1)
        elif kind == "word" and depth == 0 and sql[start:end].upper() == "RETURNING":
            return True
    return False


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


def _interrupted() -> QueryInterrupted:
    return QueryInterrupted("query interrupted (time limit exceeded or cancelled)")


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


class Postgres:
    """PostgreSQL backend: one ``Database`` is one schema of the database
    named by ``dsn``.

    ``schema_ttl``: seconds after which ``change_hint()`` moves on by itself,
    so schema changes made by other clients are noticed within that time
    (``Database.refresh_schema()`` notices them at once).
    ``max_write_connections``: concurrent write connections per database
    (default: the database's ``max_read_connections``).
    ``server_deadline_grace_ms``: added to a transaction's deadline for the
    server-side ``idle_in_transaction_session_timeout``.
    ``time_limit_grace_ms``: added to a time limit for the driver's own
    timeout, the backstop behind ``statement_timeout``.
    ``statement_savepoints``: see the module docstring.
    """

    def __init__(
        self,
        dsn: str | None = None,
        *,
        schema: str = "public",
        create_schema: bool = True,
        schema_ttl: float = 300.0,
        application_name: str = "dblayer",
        statement_savepoints: bool = True,
        max_write_connections: int | None = None,
        server_deadline_grace_ms: int = 1000,
        time_limit_grace_ms: int = 500,
        connect_timeout: float = 10.0,
        server_settings: Mapping[str, str] | None = None,
        connect_kwargs: Mapping[str, Any] | None = None,
    ) -> None:
        if schema_ttl <= 0:
            raise ValueError("schema_ttl must be positive")
        self.dsn = dsn
        self.schema = schema
        self.create_schema = create_schema
        self.schema_ttl = schema_ttl
        self.application_name = application_name
        self.statement_savepoints = statement_savepoints
        self.max_write_connections = max_write_connections
        self.server_deadline_grace_ms = server_deadline_grace_ms
        self.time_limit_grace_ms = time_limit_grace_ms
        self.connect_timeout = connect_timeout
        self.server_settings = dict(server_settings or {})
        self.connect_kwargs = dict(connect_kwargs or {})
        self.dialect: Dialect = _DIALECT
        self.capabilities = Capabilities(
            concurrent_writers=True,
            transactional_ddl=True,
            implicit_rowid=False,
            cheap_change_hint=False,
            hard_kill=False,
            memory_limit=False,
            table_definition_sql=False,
            full_text_search=False,
            sleep_releases_everything=True,
            returning=True,
            snapshot_reads=True,
        )
        self._schema_ready = False
        self._hint_generation = 0

    def __repr__(self) -> str:
        return f"Postgres(schema={self.schema!r})"

    # -- Backend protocol --------------------------------------------------

    async def open(self, *, write: bool) -> "PostgresConnection":
        import asyncpg

        settings = {
            "search_path": _DIALECT.quote_identifier(self.schema),
            "application_name": self.application_name,
            "statement_timeout": "0",
            "idle_in_transaction_session_timeout": "0",
            "default_transaction_read_only": "off" if write else "on",
            **self.server_settings,
        }
        raw = await asyncpg.connect(
            self.dsn,
            server_settings=settings,
            timeout=self.connect_timeout,
            **self.connect_kwargs,
        )
        try:
            if self.create_schema and not self._schema_ready:
                await self._ensure_schema(raw)
        except BaseException:
            raw.terminate()
            raise
        return PostgresConnection(self, raw, write=write)

    def change_hint(self) -> object:
        """No cheap change detector exists: a token that moves every
        ``schema_ttl`` seconds and on ``invalidate_change_hint()``. Never
        opens a connection."""
        return (self._hint_generation, int(time.monotonic() // self.schema_ttl))

    def invalidate_change_hint(self) -> None:
        """Called by ``Database.refresh_schema()``."""
        self._hint_generation += 1

    async def close(self) -> None:
        # Connections belong to the core pool, which closes them; the
        # backend holds nothing else.
        self._schema_ready = False

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
        self, sql: str | None, time_limit_ms: int | None, body: Awaitable[T]
    ) -> T:
        """Run ``body`` after the prelude. Inside a transaction a failure is
        rolled back to the statement savepoint, so the transaction stays
        usable, as it does on SQLite. ``sql=None`` means a whole named
        operation."""
        try:
            savepoint = self._use_savepoint(sql)
            if sql is not None and not savepoint and self._in_tx() and self._sp_pending_release:
                # A transaction-control statement: settle our savepoint
                # first so it cannot swallow the caller's.
                await self._raw.execute(f"RELEASE SAVEPOINT {_SAVEPOINT}")
                self._sp_pending_release = False
            prelude = self._prelude(time_limit_ms, savepoint)
            if prelude:
                await self._raw.execute(prelude)
        except BaseException:
            _close_awaitable(body)
            raise
        if not savepoint:
            return await body
        try:
            result = await body
        except asyncio.CancelledError:
            raise
        except BaseException:
            if not self._raw.is_closed() and self._in_tx():
                try:
                    await self._raw.execute(
                        f"ROLLBACK TO SAVEPOINT {_SAVEPOINT}; RELEASE SAVEPOINT {_SAVEPOINT}"
                    )
                except Exception:
                    pass
            raise
        self._sp_pending_release = True
        return result

    async def _statement(self, sql: str, rows: list[list[Any]]) -> Any:
        """A cached prepared statement for ``sql`` (an asyncpg
        ``PreparedStatement``), adapted to the parameter values in ``rows``
        (one list per execution).

        PostgreSQL types a parameter it cannot infer as ``text``, and asyncpg
        then refuses anything but ``str`` for it. For such a parameter:
        if every non-null value is of one non-string kind, the statement is
        re-prepared with a cast (``$1::int8``), so ``select :a`` returns the
        value as SQLite would; if strings and numbers are mixed, the numbers
        are sent as their text (what SQLite's TEXT affinity stores). ``rows``
        is changed in place in that case."""
        stmt = await _prepare(self._raw, sql)
        if not rows or not rows[0]:
            return stmt
        casts: dict[int, str] = {}
        for n, ptype in enumerate(stmt.get_parameters(), start=1):
            if ptype.name not in _STRING_TYPES:
                continue
            kinds = {_cast_for(r[n - 1]) for r in rows if r[n - 1] is not None}
            if not kinds or kinds == {None}:
                continue
            if len(kinds) == 1:
                casts[n] = kinds.pop()  # type: ignore[assignment]
                continue
            for r in rows:
                v = r[n - 1]
                if isinstance(v, (int, float, Decimal)) and not isinstance(v, bool):
                    r[n - 1] = str(v)
                elif isinstance(v, bool):
                    r[n - 1] = "1" if v else "0"
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
                self._execute(sql, args, time_limit_ms, max_rows, max_result_bytes),
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
        return await self._call(self._guarded(sql, None, self._execute_many(sql, seq)))

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
        if mode == "write":
            sql = "BEGIN READ WRITE"
        elif mode == "read":
            sql = "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"
        else:
            raise ValueError(f"unknown transaction mode {mode!r}")
        if deadline_ms is not None:
            idle = int(deadline_ms) + self._backend.server_deadline_grace_ms
            sql += f"; SET LOCAL idle_in_transaction_session_timeout = {idle}"
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
        if self._closed or self._raw.is_closed() or not self._in_tx():
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
            return await _read_schema(raw, self._backend.schema)
        finally:
            if own_tx and not raw.is_closed() and self._in_tx():
                await raw.execute("ROLLBACK")

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
        try:
            await asyncio.wait_for(self._raw.close(), 5)
        except BaseException:
            self._raw.terminate()
            raise


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


# -- introspection --------------------------------------------------------------

_VERSION_SQL = """
with ns as (select oid from pg_namespace where nspname = $1)
select md5(coalesce(string_agg(x, '|' order by x), '')) from (
  select 'c' || c.oid || ':' || c.relname || ':' || c.relkind::text
    from pg_class c where c.relnamespace = (select oid from ns)
  union all
  select 'a' || a.attrelid || ':' || a.attnum || ':' || a.attname || ':' || a.atttypid
         || ':' || a.atttypmod || ':' || a.attnotnull || ':' || a.attgenerated::text
    from pg_attribute a join pg_class c on c.oid = a.attrelid
   where c.relnamespace = (select oid from ns) and a.attnum > 0 and not a.attisdropped
  union all
  select 'd' || d.oid from pg_attrdef d join pg_class c on c.oid = d.adrelid
   where c.relnamespace = (select oid from ns)
  union all
  select 'k' || k.oid || ':' || k.conname from pg_constraint k
   where k.connamespace = (select oid from ns)
  union all
  select 't' || t.oid || ':' || t.tgname || ':' || t.tgenabled::text from pg_trigger t
    join pg_class c on c.oid = t.tgrelid
   where c.relnamespace = (select oid from ns) and not t.tgisinternal
  union all
  select 'r' || r.oid || ':' || md5(r.ev_action::text) from pg_rewrite r
    join pg_class c on c.oid = r.ev_class
   where c.relnamespace = (select oid from ns)
) parts(x)
"""

_RELATIONS_SQL = """
select c.oid, c.relname, c.relkind::text
  from pg_class c join pg_namespace n on n.oid = c.relnamespace
 where n.nspname = $1 and c.relkind in ('r', 'p', 'v', 'm', 'f')
 order by c.oid
"""

_COLUMNS_SQL = """
select a.attrelid, a.attname, upper(format_type(a.atttypid, a.atttypmod)), a.attnotnull,
       case when a.attgenerated = '' then pg_get_expr(d.adbin, d.adrelid) end,
       a.attgenerated <> '' as generated
  from pg_attribute a
  join pg_class c on c.oid = a.attrelid
  join pg_namespace n on n.oid = c.relnamespace
  left join pg_attrdef d on d.adrelid = a.attrelid and d.adnum = a.attnum
 where n.nspname = $1 and c.relkind in ('r', 'p', 'v', 'm', 'f')
   and a.attnum > 0 and not a.attisdropped
 order by a.attrelid, a.attnum
"""

_CONSTRAINTS_SQL = """
select k.conrelid, k.conname, k.contype::text,
       array(select a.attname from unnest(k.conkey) with ordinality u(n, i)
             join pg_attribute a on a.attrelid = k.conrelid and a.attnum = u.n order by u.i),
       f.relname,
       array(select a.attname from unnest(k.confkey) with ordinality u(n, i)
             join pg_attribute a on a.attrelid = k.confrelid and a.attnum = u.n order by u.i),
       k.confupdtype::text, k.confdeltype::text, pg_get_constraintdef(k.oid)
  from pg_constraint k
  join pg_class c on c.oid = k.conrelid
  join pg_namespace n on n.oid = c.relnamespace
  left join pg_class f on f.oid = k.confrelid
 where n.nspname = $1 and k.contype in ('p', 'u', 'f', 'c')
 order by k.conrelid, k.contype, k.conname
"""

_INDEXES_SQL = """
select i.indrelid, ic.relname, i.indisunique, i.indisprimary,
       exists(select 1 from pg_constraint k where k.conindid = i.indexrelid and k.contype = 'u'),
       i.indpred is not null,
       pg_get_indexdef(i.indexrelid),
       array(select a.attname from unnest(i.indkey::int2[]) with ordinality u(n, o)
             left join pg_attribute a on a.attrelid = i.indrelid and a.attnum = u.n
             where u.o <= i.indnkeyatts order by u.o)
  from pg_index i
  join pg_class ic on ic.oid = i.indexrelid
  join pg_class t on t.oid = i.indrelid
  join pg_namespace n on n.oid = t.relnamespace
 where n.nspname = $1
 order by ic.relname
"""

_TRIGGERS_SQL = """
select t.tgname, c.relname, pg_get_triggerdef(t.oid)
  from pg_trigger t
  join pg_class c on c.oid = t.tgrelid
  join pg_namespace n on n.oid = c.relnamespace
 where n.nspname = $1 and not t.tgisinternal
 order by t.oid
"""


async def _read_schema(raw: "asyncpg.Connection", schema: str) -> Schema:
    version = await raw.fetchval(_VERSION_SQL, schema)
    relations = await raw.fetch(_RELATIONS_SQL, schema)
    columns: dict[int, list[Any]] = {}
    for oid, name, type_, not_null, default, generated in await raw.fetch(_COLUMNS_SQL, schema):
        columns.setdefault(oid, []).append((name, type_, not_null, default, generated))
    pks: dict[int, tuple[str, ...]] = {}
    fks: dict[int, list[ForeignKey]] = {}
    checks: dict[int, list[str]] = {}
    for oid, _name, kind, cols, other, other_cols, on_upd, on_del, cdef in await raw.fetch(
        _CONSTRAINTS_SQL, schema
    ):
        if kind == "c":
            checks.setdefault(oid, []).append(cdef)
        elif kind == "p":
            pks[oid] = tuple(cols)
        elif kind == "f":
            fks.setdefault(oid, []).append(
                ForeignKey(
                    columns=tuple(cols),
                    other_table=other,
                    other_columns=tuple(other_cols),
                    on_update=_FK_ACTIONS.get(on_upd, on_upd),
                    on_delete=_FK_ACTIONS.get(on_del, on_del),
                )
            )
    indexes: dict[int, list[Index]] = {}
    for oid, iname, unique, primary, constraint_u, partial, idef, icols in await raw.fetch(
        _INDEXES_SQL, schema
    ):
        origin = "pk" if primary else ("u" if constraint_u else "c")
        indexes.setdefault(oid, []).append(
            Index(
                name=iname,
                columns=tuple(icols),
                unique=unique,
                origin=origin,
                partial=partial,
                definition=idef,
            )
        )
    triggers = {
        name: Trigger(name=name, table=table, definition=defn)
        for name, table, defn in await raw.fetch(_TRIGGERS_SQL, schema)
    }
    tables: dict[str, TableInfo] = {}
    views: dict[str, TableInfo] = {}
    for oid, name, kind in relations:
        pk = pks.get(oid, ())
        cols = tuple(
            ColumnInfo(
                name=c,
                type=t,
                not_null=nn,
                default=dflt,
                primary_key=pk.index(c) + 1 if c in pk else 0,
                generated=gen,
            )
            for c, t, nn, dflt, gen in columns.get(oid, ())
        )
        if kind in ("v", "m"):
            views[name] = TableInfo(name=name, kind="view", columns=cols)
        else:
            tables[name] = TableInfo(
                name=name,
                kind="table",
                columns=cols,
                foreign_keys=tuple(fks.get(oid, ())),
                indexes=tuple(indexes.get(oid, ())),
                virtual=kind == "f",
                checks=tuple(checks.get(oid, ())),
            )
    return Schema(version=version, tables=tables, views=views, triggers=triggers)

