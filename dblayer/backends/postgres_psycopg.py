"""PostgreSQL backend on psycopg 3's native asyncio API (no threads).

A second PostgreSQL backend next to the asyncpg one
(``dblayer.backends.postgres``), with the same behaviour, so the two drivers
can be compared on one conformance suite. Install with the ``psycopg``
extra. Everything driver-independent (dialect, error table, catalog
queries, settings, native upsert) is shared through ``_postgres_common``.

* **One ``Database`` = one schema**, through ``search_path`` set at connect
  time (libpq ``options``), as on asyncpg. No driver pool: the core pool is
  the pool.
* **Placeholders.** The core rewrites ``:name`` / ``?`` to ``$1..$n`` (the
  dialect's style, shared with asyncpg) and statements run on psycopg's
  *raw* cursors (``AsyncRawCursor``), which pass ``$n`` to the server
  unchanged. psycopg's own ``%s`` / ``%(name)s`` style would need every
  literal ``%`` in the SQL doubled (``like 'a%'``), a second rewriter and a
  second set of edge cases, and is client-side only in name: psycopg still
  sends parameters separately.
* **Parameter types** travel with the values: psycopg sends each
  parameter's type OID, so ``select :a`` with an int comes back as an int
  without any cast, and a ``str`` is sent as type ``unknown`` so the server
  converts it to whatever the context needs (``cast(:x as integer)`` with
  ``"42"``). Two dumpers are changed from psycopg's defaults: ``int`` is
  sent as ``int4``/``int8``/``numeric`` by size, never ``int2`` (psycopg's
  smallest-fit choice makes ``select :a * :b`` with 300 and 300 overflow
  smallint); ``bool`` is sent as ``int4`` 1/0, since portable columns have
  no boolean type and SQLite stores booleans as 1/0 (``select :b`` returns
  1, as on asyncpg and SQLite). The cost: a Python ``bool`` compared with
  or stored into a native ``boolean`` column needs ``:b::boolean``, where
  the asyncpg backend, which asks the server for parameter types first,
  needs nothing.
* **No server-side prepared statements** by default (``prepare_threshold=
  None``): every statement is parsed by the server, but in the same round
  trip (unnamed statement), so nothing goes stale after DDL ("cached plan
  must not change result type" cannot happen). Set ``prepare_threshold``
  to psycopg's default (5) to trade that for server-side plan reuse.
* **Read connections** are ``default_transaction_read_only=on``; read
  transactions ``REPEATABLE READ READ ONLY``; writers are not queued.
* **Statement savepoints in pipeline mode.** Inside a transaction a call
  runs as ``[SET LOCAL statement_timeout] SAVEPOINT; <statement>; RELEASE``
  sent as one pipeline: one round trip, the same as the statement alone
  (the asyncpg backend needs two). Only a failure costs a second round trip
  (``ROLLBACK TO SAVEPOINT``).
* **Time limits**: ``statement_timeout`` (session ``SET`` outside
  transactions, re-sent only when it changes; ``SET LOCAL`` inside), plus a
  client-side timeout (limit + ``time_limit_grace_ms``) as a backstop.
  Cancelling the awaiting task makes psycopg send a cancel request
  (libpq 17's non-blocking cancel) and wait for the server, so the
  connection stays usable. ``interrupt()`` is the public ``cancel_safe()``.
* **Row and byte limits protect memory.** A ``SELECT`` outside a
  transaction on a read connection, with ``max_rows`` or
  ``max_result_bytes`` set, is read with libpq's chunked-rows mode
  (``cursor.stream(size=...)``): rows are counted and measured as they
  arrive and the statement is cancelled on the server once the limit is
  reached. Inside a transaction (where a cancel would abort it) the
  statement runs as ``DECLARE`` / ``FETCH max_rows + 1`` in one pipeline,
  so at most ``max_rows + 1`` rows are fetched.
* ``stream()`` outside a transaction uses the same chunked mode (no
  transaction or server cursor is needed); inside one, a server-side
  cursor.
* **Catalog reads are pipelined**: ``schema()`` sends ``BEGIN``, the six
  catalog queries and ``ROLLBACK`` in one round trip.
* **Native operations**: ``insert_rows`` by ``COPY ... FROM STDIN`` (text
  format, so the server converts values as it would literals: much less
  strict than asyncpg's binary COPY) and ``upsert_rows`` by multi-row
  ``INSERT ... ON CONFLICT`` (shared with asyncpg).
* **Closing from another event loop** needs nothing special:
  ``AsyncConnection.close()`` is libpq's ``PQfinish``, which does not use
  the event loop.
* **Statements without parameters** use the simple query protocol (psycopg's
  choice), so ``"select 1; select 2"`` runs both and returns the last
  result, where asyncpg refuses more than one statement.

Capability rounding is as for the asyncpg backend.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import AsyncIterator, Awaitable, Mapping, Sequence
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from ..core.params import POSTGRES_FLAVOR, leading_keyword
from ..core.results import Column, Results, WriteResult
from ..core.schema import Schema
from ..errors import DblayerError, QueryError, ResourceLimitExceeded
from ._postgres_common import (
    DML,
    SAVEPOINT,
    TX_CONTROL,
    VERSION_SQL,
    ConnTx,
    PostgresBackendBase,
    PostgresDialectBase,
    begin_sql,
    interrupted,
    map_sqlstate,
    read_schema,
    row_bytes,
    upsert_rows_native,
)
from .base import Dialect, NativeParams

if TYPE_CHECKING:
    import psycopg

__all__ = ["PostgresPsycopg", "PsycopgDialect", "PsycopgConnection"]

_CURSOR = "_dblayer_cur"
_STREAM_CHUNK = 1000
"""Rows per libpq chunk when ``execute`` reads with a row or byte limit."""
_SELECTS = frozenset({"SELECT", "VALUES", "TABLE"})
"""Statements ``DECLARE ... CURSOR FOR`` accepts (``WITH ... SELECT`` is
reported as ``SELECT``)."""


class PsycopgDialect(PostgresDialectBase):
    """The PostgreSQL dialect with psycopg's exceptions mapped."""

    def map_driver_exception(self, exc: BaseException) -> DblayerError | None:
        try:
            import psycopg
        except ImportError:  # pragma: no cover - psycopg is required to get here
            return None
        if not isinstance(exc, psycopg.Error):
            return None
        state = exc.sqlstate
        if state:
            return map_sqlstate(state, _message(exc))
        if isinstance(exc, (psycopg.DataError, psycopg.ProgrammingError)):
            # Raised by psycopg before sending: a value it cannot adapt.
            return QueryError(str(exc), "DATA_ERROR")
        return None


def _message(exc: BaseException) -> str:
    # psycopg's str() is the primary message, then DETAIL/HINT lines, as
    # asyncpg's is; keep just the first line for parity.
    return str(exc).split("\n", 1)[0]


_DIALECT = PsycopgDialect()


# -- adaptation -------------------------------------------------------------------

_adapters_ready = False
_IntDumperCls: Any = None
_BoolDumperCls: Any = None


def _dumpers() -> tuple[Any, Any]:
    """The two dumpers dblayer changes (see the module docstring), built on
    first use so importing this module does not need psycopg."""
    global _adapters_ready, _IntDumperCls, _BoolDumperCls
    if not _adapters_ready:
        from psycopg import adapt
        from psycopg.types.numeric import IntDumper

        class _Int4UpDumper(IntDumper):
            """``int`` as ``int4``, ``int8`` or ``numeric``: never ``int2``."""

            def upgrade(self, obj: int, format: Any) -> Any:
                if -(2**31) <= obj < 2**31:
                    return self._int4_dumper
                return super().upgrade(obj, format)

        class _BoolIntDumper(adapt.Dumper):
            """``bool`` as ``int4`` 1 / 0."""

            oid = 23  # int4

            def dump(self, obj: bool) -> bytes:
                return b"1" if obj else b"0"

        _IntDumperCls = _Int4UpDumper
        _BoolDumperCls = _BoolIntDumper
        _adapters_ready = True
    return _IntDumperCls, _BoolDumperCls


def _configure(raw: "psycopg.AsyncConnection[Any]") -> None:
    int_dumper, bool_dumper = _dumpers()
    raw.adapters.register_dumper(int, int_dumper)
    raw.adapters.register_dumper(bool, bool_dumper)


def _options(settings: Mapping[str, str]) -> str:
    """libpq ``options``: ``-c name=value`` pairs, with spaces and
    backslashes in values escaped as libpq requires."""
    out = []
    for name, value in settings.items():
        v = str(value).replace("\\", "\\\\").replace(" ", "\\ ")
        out.append(f"-c {name}={v}")
    return " ".join(out)


_TYPE_NAMES: dict[int, str | None] = {}


def _type_name(oid: int) -> str | None:
    """Short type name for a result column's type OID (``int8``, ``text``),
    as asyncpg reports it; ``None`` for types psycopg does not know."""
    try:
        return _TYPE_NAMES[oid]
    except KeyError:
        from psycopg.postgres import types

        info = types.get(oid)
        name = info.name if info is not None else None
        _TYPE_NAMES[oid] = name
        return name


def _columns(description: Sequence[Any] | None) -> tuple[Column, ...]:
    if not description:
        return ()
    return tuple(Column(d.name, _type_name(d.type_code)) for d in description)


# -- backend ------------------------------------------------------------------


class PostgresPsycopg(PostgresBackendBase):
    """PostgreSQL backend on psycopg 3: one ``Database`` is one schema of the
    database named by ``dsn``. Settings as ``PostgresBackendBase``, plus
    ``prepare_threshold`` (psycopg's: executions of one statement before it
    is prepared on the server; ``None``, the default here, never)."""

    dialect: Dialect = _DIALECT

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
        prepare_threshold: int | None = None,
    ) -> None:
        super().__init__(
            dsn,
            schema=schema,
            create_schema=create_schema,
            schema_ttl=schema_ttl,
            application_name=application_name,
            statement_savepoints=statement_savepoints,
            max_write_connections=max_write_connections,
            server_deadline_grace_ms=server_deadline_grace_ms,
            time_limit_grace_ms=time_limit_grace_ms,
            connect_timeout=connect_timeout,
            server_settings=server_settings,
            connect_kwargs=connect_kwargs,
        )
        self.prepare_threshold = prepare_threshold

    async def open(self, *, write: bool) -> "PsycopgConnection":
        import psycopg

        settings = self.session_settings(write=write)
        app = settings.pop("application_name")
        kwargs: dict[str, Any] = {
            "autocommit": True,  # dblayer sends BEGIN / COMMIT itself
            "prepare_threshold": self.prepare_threshold,
            "cursor_factory": psycopg.AsyncRawCursor,
            "application_name": app,
            "options": _options(settings),
            "connect_timeout": max(1, round(self.connect_timeout)),
            **self.connect_kwargs,
        }
        raw = await asyncio.wait_for(
            psycopg.AsyncConnection.connect(self.dsn or "", **kwargs), self.connect_timeout
        )
        try:
            raw.server_cursor_factory = psycopg.AsyncRawServerCursor
            _configure(raw)
            if self.create_schema and not self._schema_ready:
                await self._ensure_schema(raw)
        except BaseException:
            raw.pgconn.finish()
            raise
        return PsycopgConnection(self, raw, write=write)

    async def _ensure_schema(self, raw: "psycopg.AsyncConnection[Any]") -> None:
        import psycopg

        cur = await raw.execute(
            "select exists(select 1 from pg_namespace where nspname = $1)", [self.schema]
        )
        row = await cur.fetchone()
        if not (row and row[0]):
            try:
                await raw.execute(
                    "BEGIN READ WRITE; CREATE SCHEMA IF NOT EXISTS "
                    f"{_DIALECT.quote_identifier(self.schema)}; COMMIT"
                )
            except psycopg.errors.UniqueViolation:
                # Another connection created it at the same moment.
                await raw.execute("ROLLBACK")
            except BaseException:
                if not raw.closed:
                    with contextlib.suppress(Exception):
                        await raw.execute("ROLLBACK")
                raise
        self._schema_ready = True


# -- connection ---------------------------------------------------------------


class PsycopgConnection:
    """One psycopg ``AsyncConnection`` (autocommit; dblayer sends BEGIN and
    COMMIT), checked out by one task at a time."""

    def __init__(
        self, backend: PostgresPsycopg, raw: "psycopg.AsyncConnection[Any]", *, write: bool
    ) -> None:
        from psycopg import pq

        self._pq = pq
        self.backend = backend
        self._raw = raw
        self.write = write
        self._closed = False
        self._session_timeout = 0
        self._local_timeout: int | None = None
        self._rearm_idle_timer = False
        self._server_deadline: float | None = None

    def __repr__(self) -> str:
        kind = "write" if self.write else "read"
        return f"<PsycopgConnection {kind} pid={self.server_pid}>"

    @property
    def server_pid(self) -> int:
        return self._raw.info.backend_pid

    # -- plumbing ------------------------------------------------------------

    def _in_tx(self) -> bool:
        status = self._raw.info.transaction_status
        TS = self._pq.TransactionStatus
        return status in (TS.INTRANS, TS.INERROR)

    def _timeout_sql(self, time_limit_ms: int | None) -> str | None:
        """``SET [LOCAL] statement_timeout`` if it must change for this
        call, else ``None``."""
        want = 0 if time_limit_ms is None else max(int(time_limit_ms), 1)
        if self._in_tx():
            current = (
                self._local_timeout if self._local_timeout is not None else self._session_timeout
            )
            if current != want:
                self._local_timeout = want
                return f"SET LOCAL statement_timeout = {want}"
        elif self._session_timeout != want:
            self._session_timeout = want
            return f"SET statement_timeout = {want}"
        return None

    def _use_savepoint(self, sql: str | None) -> bool:
        if not (self.backend.statement_savepoints and self._in_tx()):
            return False
        return sql is None or leading_keyword(sql, POSTGRES_FLAVOR) not in TX_CONTROL

    def _client_timeout(self, time_limit_ms: int | None) -> float | None:
        if time_limit_ms is None:
            return None
        return (max(time_limit_ms, 1) + self.backend.time_limit_grace_ms) / 1000

    async def _timed[T](self, aw: Awaitable[T], time_limit_ms: int | None) -> T:
        """The client-side backstop behind ``statement_timeout``: on expiry
        psycopg cancels the statement on the server and waits for it."""
        timeout = self._client_timeout(time_limit_ms)
        try:
            if timeout is None:
                return await aw
            async with asyncio.timeout(timeout):
                return await aw
        except TimeoutError:
            raise interrupted() from None
        except Exception as e:
            self._raise_if_expired(e)
            raise

    def _raise_if_expired(self, exc: BaseException) -> None:
        """The server ends a session whose transaction ran past its server
        deadline. asyncpg reads the server's FATAL message (SQLSTATE 25P03);
        psycopg in pipeline mode often reports only "server closed the
        connection unexpectedly". A lost connection after the server
        deadline passed is taken to be that."""
        deadline = self._server_deadline
        if deadline is None or time.monotonic() < deadline:
            return
        if self._raw.closed or self._raw.broken:
            from ..errors import TransactionExpired

            raise TransactionExpired(
                "the server ended the session: the transaction ran past its "
                "deadline and was rolled back"
            ) from exc

    async def _rollback_to_savepoint(self) -> None:
        if self._raw.closed or not self._in_tx():
            return
        with contextlib.suppress(Exception):
            await self._raw.execute(
                f"ROLLBACK TO SAVEPOINT {SAVEPOINT}; RELEASE SAVEPOINT {SAVEPOINT}"
            )

    async def _settle_timeout(self, time_limit_ms: int | None) -> None:
        """Send the ``statement_timeout`` change, if any, on its own (for
        paths that cannot be pipelined)."""
        sql = self._timeout_sql(time_limit_ms)
        if sql is not None:
            await self._raw.execute(sql)

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
        return await self._timed(
            self._execute(sql, args, time_limit_ms, max_rows, max_result_bytes), time_limit_ms
        )

    async def _execute(
        self,
        sql: str,
        args: list[Any],
        time_limit_ms: int | None,
        max_rows: int | None,
        max_result_bytes: int | None,
    ) -> Results:
        verb = leading_keyword(sql, POSTGRES_FLAVOR)
        limited = max_rows is not None or max_result_bytes is not None
        if not self._in_tx():
            await self._settle_timeout(time_limit_ms)
            if limited and verb in _SELECTS and not self.write:
                return await self._read_streamed(sql, args, max_rows, max_result_bytes)
            cur = self._raw.cursor()
            await cur.execute(sql, args)
            return self._results(cur, await self._fetch(cur), verb, max_rows, max_result_bytes)
        savepoint = self._use_savepoint(sql)
        pre = [s for s in (self._timeout_sql(time_limit_ms),) if s]
        if savepoint:
            pre.append(f"SAVEPOINT {SAVEPOINT}")
        post = [f"RELEASE SAVEPOINT {SAVEPOINT}"] if savepoint else []
        cursor_read = max_rows is not None and verb in _SELECTS
        try:
            if cursor_read:
                assert max_rows is not None
                cur = await self._pipeline(
                    pre + [f"DECLARE {_CURSOR} NO SCROLL CURSOR FOR {sql}"],
                    f"FETCH FORWARD {max_rows + 1} FROM {_CURSOR}",
                    [],
                    [f"CLOSE {_CURSOR}"] + post,
                    first_args=args,
                )
            else:
                cur = await self._pipeline(pre, sql, args, post)
        except BaseException:
            if savepoint:
                await self._rollback_to_savepoint()
            raise
        return self._results(cur, await self._fetch(cur), verb, max_rows, max_result_bytes)

    async def _pipeline(
        self,
        pre: list[str],
        sql: str,
        args: list[Any],
        post: list[str],
        *,
        first_args: list[Any] | None = None,
    ) -> Any:
        """Run ``pre`` statements, ``sql`` and ``post`` statements in one
        pipeline (one round trip) and return the cursor that ran ``sql``.
        ``first_args`` are the parameters of the last ``pre`` statement (a
        ``DECLARE``). Without ``pre``/``post``, a plain execute."""
        raw = self._raw
        cur = raw.cursor()
        if not pre and not post:
            await cur.execute(sql, args)
            return cur
        aux = raw.cursor()
        async with self._pipe():
            for i, s in enumerate(pre):
                last = i == len(pre) - 1
                await aux.execute(s, first_args if (last and first_args) else None)
            await cur.execute(sql, args)
            for s in post:
                await aux.execute(s)
        return cur

    async def _fetch(self, cur: Any) -> list[tuple[Any, ...]]:
        if cur.description is None:
            return []
        return await cur.fetchall()

    def _results(
        self,
        cur: Any,
        rows: list[tuple[Any, ...]],
        verb: str | None,
        max_rows: int | None,
        max_result_bytes: int | None,
    ) -> Results:
        truncated = False
        if max_rows is not None and len(rows) > max_rows:
            truncated = True
            rows = rows[:max_rows]
        if max_result_bytes is not None:
            size = 0
            for r in rows:
                size += row_bytes(r)
                if size > max_result_bytes:
                    raise ResourceLimitExceeded(
                        f"result exceeded max_result_bytes={max_result_bytes}"
                    )
        return Results.from_tuples(
            _columns(cur.description),
            rows,
            truncated=truncated,
            rowcount=cur.rowcount if verb in DML else -1,
        )

    async def _read_streamed(
        self, sql: str, args: list[Any], max_rows: int | None, max_result_bytes: int | None
    ) -> Results:
        """A read outside a transaction, in libpq chunked-rows mode: stop
        (psycopg cancels the statement on the server) as soon as there are
        ``max_rows + 1`` rows or more than ``max_result_bytes``."""
        cur = self._raw.cursor()
        rows: list[tuple[Any, ...]] = []
        size = 0
        want = None if max_rows is None else max_rows + 1
        chunk = _STREAM_CHUNK if want is None else max(1, min(_STREAM_CHUNK, want))
        gen = cur.stream(sql, args, size=chunk)
        try:
            async for row in gen:
                if max_result_bytes is not None:
                    size += row_bytes(row)
                    if size > max_result_bytes:
                        raise ResourceLimitExceeded(
                            f"result exceeded max_result_bytes={max_result_bytes}"
                        )
                rows.append(row)
                if want is not None and len(rows) >= want:
                    break
        finally:
            await gen.aclose()
        truncated = False
        if max_rows is not None and len(rows) > max_rows:
            truncated = True
            rows = rows[:max_rows]
        return Results.from_tuples(_columns(cur.description), rows, truncated=truncated)

    async def execute_batch(
        self,
        statements: Sequence[tuple[str, NativeParams]],
        *,
        time_limit_ms: int | None,
        max_rows: int | None,
    ) -> list[Results]:
        """``SupportsBatch``: every statement in one pipeline (one round
        trip), with no statement savepoints: a failing batch stops there and
        raises, and the transaction is then aborted."""

        async def run() -> list[Results]:
            raw = self._raw
            pre = self._timeout_sql(time_limit_ms)
            curs = []
            async with self._pipe():
                if pre:
                    await raw.cursor().execute(pre)
                for sql, params in statements:
                    cur = raw.cursor()
                    await cur.execute(sql, list(params) if params else [])
                    curs.append((cur, leading_keyword(sql, POSTGRES_FLAVOR)))
            return [
                self._results(cur, await self._fetch(cur), verb, max_rows, None)
                for cur, verb in curs
            ]

        return await self._timed(run(), time_limit_ms)

    async def execute_many(self, sql: str, params_seq: Sequence[NativeParams]) -> WriteResult:
        """psycopg's ``executemany``: pipelined (one round trip) and it
        reports the total row count."""
        seq = [list(p) if p else [] for p in params_seq]
        if not seq:
            return WriteResult(rowcount=0)
        savepoint = self._use_savepoint(sql)
        if not self._in_tx():
            await self._settle_timeout(None)
        pre = [s for s in (self._timeout_sql(None),) if s] if self._in_tx() else []
        if savepoint:
            pre.append(f"SAVEPOINT {SAVEPOINT}")
        try:
            result = await self._timed(self._execute_many(sql, seq, pre), None)
        except BaseException:
            if savepoint:
                await self._rollback_to_savepoint()
            raise
        if savepoint:
            await self._raw.execute(f"RELEASE SAVEPOINT {SAVEPOINT}")
        return result

    async def _execute_many(self, sql: str, seq: list[list[Any]], pre: Sequence[str] = ()) -> WriteResult:
        raw = self._raw
        cur = raw.cursor()
        verb = leading_keyword(sql, POSTGRES_FLAVOR)
        # executemany() always runs in a pipeline, so it goes through _pipe()
        # for the idle-timer re-arm too.
        async with self._pipe():
            for s in pre:
                await raw.cursor().execute(s)
            await cur.executemany(sql, seq)
        return WriteResult(rowcount=cur.rowcount if verb in DML else -1)

    def stream(
        self,
        sql: str,
        params: NativeParams,
        *,
        chunk_rows: int,
        time_limit_ms: int | None = None,
        max_result_bytes: int | None = None,
    ) -> "_PsycopgStream":
        return _PsycopgStream(
            self, sql, list(params) if params else [], chunk_rows, time_limit_ms, max_result_bytes
        )

    # -- transactions ----------------------------------------------------------

    async def begin(self, mode: str) -> None:
        await self.begin_with_deadline(mode, None)

    async def begin_with_deadline(self, mode: str, deadline_ms: int | None) -> None:
        """``BEGIN`` plus, with a deadline, ``SET LOCAL
        idle_in_transaction_session_timeout`` (deadline + grace), in one
        round trip: if the client stops running, the server ends the
        session, which rolls the transaction back.

        On PostgreSQL 17+ ``transaction_timeout`` is set to the same value.
        It is needed here: psycopg ends every pipeline with Sync then Flush,
        and the server disarms its idle-in-transaction timer when the Flush
        arrives without arming it again (``scripts/psycopg_flush_disarms_
        idle_timeout.py``), so after any pipelined call the idle timeout
        alone would never fire. On older servers each pipeline in such a
        transaction is followed by an empty query, which re-arms the timer
        at the cost of a round trip."""
        modern = self._raw.info.server_version >= 170000
        sql = begin_sql(
            mode,
            deadline_ms,
            self.backend.server_deadline_grace_ms,
            transaction_timeout=modern,
        )
        self._end_tx()
        await self._raw.execute(sql)
        self._rearm_idle_timer = deadline_ms is not None and not modern
        if deadline_ms is not None:
            grace = self.backend.server_deadline_grace_ms
            self._server_deadline = time.monotonic() + (deadline_ms + grace) / 1000

    def _end_tx(self) -> None:
        self._local_timeout = None
        self._rearm_idle_timer = False
        self._server_deadline = None

    @contextlib.asynccontextmanager
    async def _pipe(self) -> AsyncIterator[None]:
        """psycopg's pipeline mode (one round trip for everything queued in
        the block), re-arming the server's idle-in-transaction timer
        afterwards where only that timer enforces the deadline."""
        async with self._raw.pipeline():
            yield
        if self._rearm_idle_timer and self._in_tx():
            await self._raw.execute("")

    async def commit(self) -> None:
        if self._closed or not self._in_tx():
            return
        try:
            cur = await self._timed(self._raw.execute("COMMIT"), None)
        finally:
            self._end_tx()
        if cur.statusmessage == "ROLLBACK":
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
        if self._raw.closed or self._raw.broken:
            # The server ended the session (idle_in_transaction timeout, a
            # restart...): the pool must not reuse it.
            self._end_tx()
            raise ConnectionError("the server closed this connection")
        if not self._in_tx():
            self._end_tx()
            return
        try:
            await self._raw.execute("ROLLBACK")
        finally:
            self._end_tx()

    # -- schema ------------------------------------------------------------------

    async def schema_version(self) -> object:
        pre = self._timeout_sql(None)
        cur = await self._pipeline([pre] if pre else [], VERSION_SQL, [self.backend.schema], [])
        row = await cur.fetchone()
        return row[0] if row else None

    async def schema(self) -> Schema:
        return await read_schema(self._fetch_snapshot, self.backend.schema)

    async def _fetch_snapshot(self, queries: Sequence[str], schema: str) -> list[list[Any]]:
        """Every catalog query in one pipeline, inside a ``REPEATABLE READ``
        transaction of its own unless one is open: one round trip."""
        own_tx = not self._in_tx()
        pre = self._timeout_sql(None)
        raw = self._raw
        curs = [raw.cursor() for _ in queries]
        try:
            async with self._pipe():
                aux = raw.cursor()
                if pre:
                    await aux.execute(pre)
                if own_tx:
                    await aux.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                for cur, q in zip(curs, queries):
                    await cur.execute(q, [schema])
                if own_tx:
                    await aux.execute("ROLLBACK")
        except BaseException:
            if own_tx and not raw.closed and self._in_tx():
                with contextlib.suppress(Exception):
                    await raw.execute("ROLLBACK")
            raise
        return [await c.fetchall() for c in curs]

    async def _fetch_many(self, queries: Sequence[str], schema: str) -> list[list[Any]]:
        raw = self._raw
        curs = [raw.cursor() for _ in queries]
        async with self._pipe():
            for cur, q in zip(curs, queries):
                await cur.execute(q, [schema])
        return [await c.fetchall() for c in curs]

    async def cached_schema(self) -> Schema:
        """``ConnTx``: the schema as this connection's transaction sees it;
        the full catalog is read only when the version moved."""
        cur = await self._raw.execute(VERSION_SQL, [self.backend.schema])
        row = await cur.fetchone()
        version = row[0] if row else None
        memo = self.backend._schema_memo
        if memo is not None and memo.version == version:
            return memo
        schema = await read_schema(self._fetch_many, self.backend.schema)
        self.backend._schema_memo = schema
        return schema

    async def run_native(self, sql: str, args: list[Any]) -> Results:
        """``ConnTx``: one statement inside the open transaction, without a
        savepoint of its own."""
        cur = self._raw.cursor()
        await cur.execute(sql, args)
        return self._results(
            cur, await self._fetch(cur), leading_keyword(sql, POSTGRES_FLAVOR), None, None
        )

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
        if not ConnTx.precheck(name, args):
            return NotImplemented
        savepoint = self._use_savepoint(None)
        pre = self._timeout_sql(None)
        if savepoint or pre:
            await self._raw.execute(
                "; ".join(s for s in (pre, f"SAVEPOINT {SAVEPOINT}" if savepoint else None) if s)
            )
        try:
            result = await self._timed(op(**args), None)
        except BaseException:
            if savepoint:
                await self._rollback_to_savepoint()
            raise
        if savepoint:
            if result is NotImplemented:
                await self._rollback_to_savepoint()
            else:
                await self._raw.execute(f"RELEASE SAVEPOINT {SAVEPOINT}")
        return result

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

        tx = ConnTx(self)
        created, added = await _ensure_table(tx, table, rows, tuple(pk or ()), alter)
        info = (await tx.schema()).tables.get(table)
        types = {c.name: c.type for c in info.columns} if info else {}
        total = 0
        d = self.backend.dialect
        for cols, group in _groups(rows):
            records = [tuple(r[c] for c in cols) for r in group]
            if all(_copyable(types.get(c), [rec[i] for rec in records]) for i, c in enumerate(cols)):
                total += await self._copy(table, cols, records)
            else:
                # Values text COPY would read differently from INSERT (a
                # float for an integer column...): the INSERT path.
                r = await tx.execute_many(d.insert_sql(table, cols), records)
                total += max(r.rowcount, 0)
        return OperationResult(rowcount=total, table_created=created, columns_added=added)

    async def _copy(self, table: str, cols: Sequence[str], records: list[tuple[Any, ...]]) -> int:
        d = self.backend.dialect
        cur = self._raw.cursor()
        statement = (
            f"COPY {d.quote_identifier(table)} ({d.quote_identifiers(cols)}) FROM STDIN"
        )
        async with cur.copy(statement) as copy:
            for record in records:
                await copy.write_row(record)
        return max(cur.rowcount, 0)

    async def _op_upsert_rows(
        self,
        table: str,
        rows: list[dict[str, Any]],
        pk: list[str] | None = None,
        alter: bool = False,
    ) -> object:
        return await upsert_rows_native(ConnTx(self), table, rows, pk, alter)

    # -- misc --------------------------------------------------------------------

    async def interrupt(self) -> None:
        """Cancel the statement running on this connection, if any
        (psycopg's ``cancel_safe``: libpq 17's non-blocking cancel). The
        interrupted call raises ``QueryInterrupted`` (SQLSTATE 57014). The
        core repeats this while a call is still running, which covers a
        cancel that reaches the server before the statement does."""
        if self._closed or self._raw.closed:
            return
        if self._raw.info.transaction_status == self._pq.TransactionStatus.ACTIVE:
            await self._raw.cancel_safe(timeout=5.0)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        # PQfinish: synchronous, needs no event loop, so it works for a
        # connection left behind on a closed loop too. The server sees the
        # session end and rolls back any open transaction.
        await self._raw.close()


# Python types text COPY reads exactly as an INSERT would store them, by
# the column type introspection reports (without a modifier). Others go
# through INSERT, which converts values as execute() does.
_INTS = (int,)  # bool included: dumped as 1/0
_NUMBERS = (int, float, Decimal)
_COPY_TYPES: dict[str, tuple[type, ...]] = {
    "BIGINT": _INTS,
    "INTEGER": _INTS,
    "SMALLINT": _INTS,
    "DOUBLE PRECISION": _NUMBERS,
    "REAL": _NUMBERS,
    "NUMERIC": _NUMBERS,
    "TEXT": (str, int, float, Decimal),
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


class _PsycopgStream:
    """``stream()``: outside a transaction, libpq chunked-rows mode (no
    transaction or cursor needed; closing early cancels the statement on
    the server); inside one, a server-side cursor fetched ``chunk_rows`` at
    a time. ``time_limit_ms`` is a budget of time spent waiting on the
    server across chunks."""

    def __init__(
        self,
        conn: PsycopgConnection,
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
        self._rows: AsyncIterator[tuple[Any, ...]] | None = None
        self._cursor: Any = None
        self._server_cursor: Any = None
        self._started = False
        self._done = False
        self.columns: tuple[Column, ...] | None = None

    def __aiter__(self) -> "_PsycopgStream":
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
            return await aw
        if self._remaining <= 0:
            close = getattr(aw, "close", None)
            if close is not None:
                close()
            raise interrupted()
        start = time.perf_counter()
        try:
            async with asyncio.timeout(self._remaining):
                return await aw
        except TimeoutError:
            raise interrupted() from None
        finally:
            self._remaining -= time.perf_counter() - start

    async def _next_chunk(self) -> list[tuple[Any, ...]]:
        conn = self._conn
        raw = conn._raw
        if not self._started:
            self._started = True
            await conn._settle_timeout(None)
            if conn._in_tx():
                self._server_cursor = raw.cursor(_CURSOR)
                await self._server_cursor.execute(self._sql, self._args)
                self.columns = _columns(self._server_cursor.description)
            else:
                self._cursor = raw.cursor()
                self._rows = self._cursor.stream(self._sql, self._args, size=self._chunk_rows)
        if self._server_cursor is not None:
            if not self.columns:
                return []
            chunk = await self._server_cursor.fetchmany(self._chunk_rows)
        else:
            assert self._rows is not None
            chunk = []
            async for row in self._rows:
                chunk.append(row)
                if len(chunk) >= self._chunk_rows:
                    break
            if self.columns is None:
                self.columns = _columns(self._cursor.description)
        if self._max_bytes is not None:
            for r in chunk:
                self._bytes += row_bytes(r)
            if self._bytes > self._max_bytes:
                raise ResourceLimitExceeded(f"result exceeded max_result_bytes={self._max_bytes}")
        return chunk

    async def aclose(self) -> None:
        if self._done:
            return
        self._done = True
        rows, self._rows = self._rows, None
        server, self._server_cursor = self._server_cursor, None
        if rows is not None:
            aclose = getattr(rows, "aclose", None)
            if aclose is not None:
                with contextlib.suppress(Exception):
                    await aclose()
        if server is not None and not self._conn._raw.closed:
            with contextlib.suppress(Exception):
                await server.close()

