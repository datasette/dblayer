"""Interactive transactions: the ``Transaction`` object, nesting detection
and deadlines.

``db.transaction(mode, deadline_ms)`` returns a ``Transaction``; entering it
checks a connection out of the pool (the write connection *is* the
database's write slot, so write transactions queue FIFO behind each other)
and begins. Leaving the block commits on a clean exit and rolls back on any
exception, ``CancelledError`` included.

Deadline: a timer started at ``BEGIN``. When it fires the transaction is
marked expiring; if a statement is running on a pool thread the connection
is interrupted (repeatedly, until the thread returns), and once no call is
in flight the transaction is rolled back and the connection returned to the
pool by a background task, so the write slot is released even if the
caller is asleep. Every later call on the transaction raises
``TransactionExpired``, as does a clean exit from the block. A connection
is never used by two threads: the rollback waits on the same lock that
serialises calls, and that lock is held until the thread has returned.
"""

from __future__ import annotations

import asyncio
import contextvars
import enum
from collections.abc import Awaitable, Callable, Iterable, Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Any

from ..backends.base import (
    BackendConnection,
    Capabilities,
    Dialect,
    SupportsBatch,
    SupportsOperations,
    SupportsServerDeadline,
)
from ..errors import (
    DatabaseClosed,
    NestedWriteError,
    QueryInterrupted,
    TransactionClosed,
    TransactionExpired,
)
from .api import IntrospectionMixin, OperationsMixin
from .common import DEFAULT, Opt, map_errors, pick, spawn
from .operations import OPERATIONS, OperationResult, normalise_args
from .params import Params, leading_keyword
from .pool import Pool, _should_discard
from .results import Results, WriteResult
from .schema import Schema

if TYPE_CHECKING:
    from .database import Database

__all__ = ["Transaction", "active_transaction"]

_active: contextvars.ContextVar[tuple["Transaction", ...]] = contextvars.ContextVar(
    "dblayer_active_transactions", default=()
)

# Statements that cannot change the schema; anything else in a write
# transaction invalidates the schema caches.
_DML = frozenset({"SELECT", "INSERT", "UPDATE", "DELETE", "REPLACE", "VALUES", "MERGE"})

_INTERRUPT_EVERY = 0.02  # seconds between interrupts while a statement runs


def active_transaction(db: "Database") -> "Transaction | None":
    """The transaction on ``db`` open in the current context, if any.

    Context variables are copied into tasks created inside the block, so a
    child task sees its parent's transaction: a write from there would also
    wait forever on the write slot the parent holds."""
    for tx in _active.get():
        if tx._db is db and tx._state in (_State.OPEN, _State.EXPIRING):
            return tx
    return None


class _State(enum.Enum):
    NEW = "new"
    OPEN = "open"
    EXPIRING = "expiring"
    COMMITTED = "committed"
    ROLLED_BACK = "rolled back"
    EXPIRED = "expired"


class Transaction(IntrospectionMixin, OperationsMixin):
    """An interactive transaction. Create it with ``db.transaction()`` and
    use it as ``async with``; it can be entered once.

    Offers ``execute``, ``execute_many``, every named operation and every
    introspection method; introspection sees the transaction's own
    uncommitted DDL. Calls are serialised, so several tasks may share one
    ``tx``.
    """

    def __init__(self, db: "Database", mode: str, deadline_ms: int | None) -> None:
        if mode not in ("write", "read"):
            raise ValueError(f"mode must be 'write' or 'read', not {mode!r}")
        if deadline_ms is not None and deadline_ms <= 0:
            raise ValueError("deadline_ms must be positive or None")
        self._db = db
        self.mode = mode
        self.deadline_ms = deadline_ms
        self._state = _State.NEW
        self._conn: BackendConnection | None = None
        self._pool: Pool | None = None
        self._lock = asyncio.Lock()
        self._busy = False
        self._task: asyncio.Task[Any] | None = None
        """The task that opened the transaction (``Database._use_pool``
        refuses it a second connection on a single-session backend)."""
        self._timer: asyncio.TimerHandle | None = None
        self._token: contextvars.Token[tuple[Transaction, ...]] | None = None
        self._schema: Schema | None = None

    def __repr__(self) -> str:
        return f"<Transaction {self.mode} {self._state.value} on {self._db!r}>"

    # -- properties ------------------------------------------------------------

    @property
    def capabilities(self) -> Capabilities:
        return self._db.capabilities

    @property
    def dialect(self) -> Dialect:
        return self._db._backend.dialect

    @property
    def active(self) -> bool:
        """True between a successful enter and the end (or expiry)."""
        return self._state is _State.OPEN

    @property
    def expired(self) -> bool:
        return self._state in (_State.EXPIRING, _State.EXPIRED)

    @property
    def state(self) -> str:
        return self._state.value

    # -- enter / exit ------------------------------------------------------------

    async def __aenter__(self) -> "Transaction":
        if self._state is not _State.NEW:
            raise RuntimeError("a Transaction can only be entered once")
        db = self._db
        db._check_open()
        if active_transaction(db) is not None:
            raise NestedWriteError(
                f"a transaction is already open on database {db.name!r} in this task; "
                "transactions cannot be nested"
            )
        write = self.mode == "write"
        with map_errors(db._backend):
            pool = await db._use_pool()
            conn = await pool.acquire(write=write)
        try:
            with map_errors(db._backend):
                if self.deadline_ms is not None and isinstance(conn, SupportsServerDeadline):
                    await conn.begin_with_deadline(self.mode, self.deadline_ms)
                else:
                    await conn.begin(self.mode)
        except BaseException as e:
            await pool.release(conn, discard=_should_discard(e))
            raise
        self._conn = conn
        self._pool = pool
        self._state = _State.OPEN
        if self.deadline_ms is not None:
            loop = asyncio.get_running_loop()
            self._timer = loop.call_later(self.deadline_ms / 1000, self._on_deadline)
        self._task = asyncio.current_task()
        self._token = _active.set(_active.get() + (self,))
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._forget_context()
        if self._state in (_State.OPEN, _State.EXPIRING):
            commit = exc_type is None
            task = spawn(self._end(commit))
            try:
                # Shielded: a second cancellation must not abandon the
                # rollback half-way; the task finishes in the background.
                await asyncio.shield(task)
            except Exception:
                if exc_type is None:
                    raise
                # The block's own exception wins; the connection was
                # discarded if the rollback failed.
        if exc_type is None and self._state is _State.EXPIRED:
            raise TransactionExpired(self._expired_message())

    def _forget_context(self) -> None:
        token, self._token = self._token, None
        if token is None:
            return
        try:
            _active.reset(token)
        except ValueError:
            # Exited in a different context than entered: drop just this one.
            _active.set(tuple(t for t in _active.get() if t is not self))

    async def _end(self, commit: bool) -> None:
        async with self._lock:
            if self._state is _State.OPEN:
                await self._finish(commit)
            elif self._state is _State.EXPIRING:
                await self._finish(False)

    async def _finish(self, commit: bool) -> None:
        """Commit or roll back and return the connection. Lock held."""
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        conn = self._conn
        assert conn is not None
        db = self._db
        expiring = self._state is _State.EXPIRING
        final = _State.COMMITTED if commit else (_State.EXPIRED if expiring else _State.ROLLED_BACK)
        discard = False
        error: BaseException | None = None
        try:
            if commit:
                try:
                    with map_errors(db._backend):
                        await conn.commit()
                except BaseException as e:
                    error = e
                    final = _State.ROLLED_BACK
                    try:
                        await conn.rollback()
                    except BaseException:
                        discard = True
            else:
                try:
                    await conn.rollback()
                except BaseException:
                    # A connection we could not roll back is closed instead,
                    # which rolls back on every engine.
                    discard = True
        finally:
            self._state = final
            self._conn = None
            self._schema = None
            try:
                assert self._pool is not None
                await self._pool.release(conn, discard=discard)
            finally:
                if self.mode == "write":
                    db._invalidate_schema()
        if error is not None:
            raise error

    # -- deadline ----------------------------------------------------------------

    def _on_deadline(self) -> None:
        self._timer = None
        if self._state is not _State.OPEN:
            return
        self._state = _State.EXPIRING
        spawn(self._expire())

    async def _expire(self) -> None:
        conn = self._conn
        while self._busy and self._state is _State.EXPIRING and conn is not None:
            # A statement is running on a pool thread (or queued for one):
            # interrupt until it returns. Interrupting an idle SQLite
            # connection is a no-op, so repeat to catch a statement that had
            # not started yet.
            try:
                await conn.interrupt()
            except Exception:
                pass
            await asyncio.sleep(_INTERRUPT_EVERY)
        try:
            await self._end(False)
        except Exception:
            pass

    def _expired_message(self) -> str:
        return (
            f"transaction on {self._db.name!r} passed its deadline of "
            f"{self.deadline_ms} ms and was rolled back"
        )

    # -- calls -------------------------------------------------------------------

    def _check_usable(self) -> None:
        state = self._state
        if state is _State.OPEN:
            if self._db.closed:
                raise DatabaseClosed(f"database {self._db.name!r} is closed")
            return
        if state in (_State.EXPIRING, _State.EXPIRED):
            raise TransactionExpired(self._expired_message())
        if state is _State.NEW:
            raise TransactionClosed("transaction has not been started; use 'async with'")
        raise TransactionClosed(f"transaction is already {state.value}")

    async def _run[T](self, fn: Callable[[BackendConnection], Awaitable[T]]) -> T:
        async with self._lock:
            self._check_usable()
            conn = self._conn
            assert conn is not None
            self._busy = True
            try:
                with map_errors(self._db._backend):
                    return await fn(conn)
            except QueryInterrupted as e:
                if self._state is _State.EXPIRING:
                    raise TransactionExpired(self._expired_message()) from e
                raise
            finally:
                self._busy = False

    def _schema_may_have_changed(self) -> None:
        if self.mode == "write":
            self._schema = None
            self._db._invalidate_schema()

    def _after(self, sql: str) -> None:
        if self.mode == "write" and leading_keyword(sql, self.dialect.flavor) not in _DML:
            self._schema_may_have_changed()

    async def execute(
        self,
        sql: str,
        params: Params = None,
        *,
        time_limit_ms: Opt[int] = DEFAULT,
        max_rows: Opt[int] = DEFAULT,
        max_result_bytes: Opt[int] = DEFAULT,
    ) -> Results:
        """Run one statement (read or, in a write transaction, write) inside
        the transaction. Limits default to the database's defaults; pass
        ``None`` for no limit. ``rowcount``/``last_pk`` are set for writes."""
        db = self._db
        native_sql, native_params = db._native(sql, params)
        tl = pick(time_limit_ms, db.time_limit_ms)
        mr = pick(max_rows, db.max_rows)
        mb = pick(max_result_bytes, db.max_result_bytes)
        try:
            return await self._run(
                lambda c: c.execute(
                    native_sql, native_params, time_limit_ms=tl, max_rows=mr, max_result_bytes=mb
                )
            )
        finally:
            self._after(sql)

    async def execute_many(self, sql: str, params_seq: Iterable[Params]) -> WriteResult:
        """Run one statement once per parameter set. ``rowcount`` is the
        total."""
        db = self._db
        rewritten = [db._native(sql, p) for p in params_seq]
        if not rewritten:
            return WriteResult(rowcount=0)
        native_sql = rewritten[0][0]
        seq = [p for _, p in rewritten]
        try:
            return await self._run(lambda c: c.execute_many(native_sql, seq))
        finally:
            self._after(sql)

    async def _execute_batch(
        self, statements: Sequence[tuple[str, Params]]
    ) -> list[WriteResult]:
        """All statements in one backend call where the backend supports it
        (``SupportsBatch``), else one call each."""
        db = self._db
        native = [db._native(sql, params) for sql, params in statements]
        tl, mr = db.time_limit_ms, db.returning_limit
        try:
            conn = self._conn
            if isinstance(conn, SupportsBatch):
                results = await self._run(
                    lambda c: c.execute_batch(native, time_limit_ms=tl, max_rows=mr)  # type: ignore[attr-defined]
                )
            else:
                results = []
                for s, p in native:
                    results.append(
                        await self._run(lambda c: c.execute(s, p, time_limit_ms=tl, max_rows=mr))
                    )
        finally:
            for sql, _ in statements:
                self._after(sql)
        return [
            WriteResult(
                rowcount=r.rowcount,
                last_pk=r.last_pk,
                columns=r.columns,
                rows=r.rows,
                truncated=r.truncated,
            )
            for r in results
        ]

    # -- introspection -------------------------------------------------------------

    async def schema(self) -> Schema:
        """The schema as this transaction sees it, own DDL included. Cached
        for the transaction; reuses the database's cached schema when the
        version matches (one cheap query instead of a full read)."""
        if self._schema is None:
            cached = self._db._cached_schema()
            if cached is not None:
                version = await self._run(lambda c: c.schema_version())
                if version == cached.version:
                    self._schema = cached
                    return cached
            self._schema = await self._run(lambda c: c.schema())
        return self._schema

    async def schema_version(self) -> object:
        return await self._run(lambda c: c.schema_version())

    # -- named operations ------------------------------------------------------------

    async def _operation(self, name: str, args: dict[str, Any]) -> OperationResult:
        args = normalise_args(name, args)
        self._check_usable()
        try:
            if self._db.native_operations and isinstance(self._conn, SupportsOperations):
                result = await self._run(lambda c: c.run_operation(name, args))  # type: ignore[attr-defined]
                if result is not NotImplemented:
                    if isinstance(result, OperationResult):
                        return result
                    return OperationResult.from_dict(result)  # type: ignore[arg-type]
            return await OPERATIONS[name](self, **args)
        finally:
            self._schema_may_have_changed()
