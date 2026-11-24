"""The ``Database`` object: the only thing callers hold."""

from __future__ import annotations

import itertools
from collections.abc import AsyncIterator, Iterable, Mapping, Sequence
from typing import Any

from ..backends.base import Backend, Capabilities
from ..errors import DatabaseClosed, NestedWriteError
from .api import IntrospectionMixin, OperationsMixin
from .common import DEFAULT, Opt, map_errors, pick
from .operations import OperationResult
from .params import Params, rewrite
from .pool import DEFAULT_MAX_READ_CONNECTIONS, Pool, PoolStats
from .results import Results, Row, WriteResult, make_index
from .schema import Schema, SchemaCache
from .transactions import Transaction, active_transaction

__all__ = ["Database", "DEFAULT", "map_errors"]

DEFAULT_DEADLINE_MS = 5000

# Old private names, kept for modules that imported them.
_Opt = Opt
_pick = pick


class Database(IntrospectionMixin, OperationsMixin):
    """One database. Creating it opens nothing; connections are opened on
    first use and released by ``sleep()`` (reversibly) or ``close()`` (for
    good).

    Limits given here are defaults for each call; per-call keyword arguments
    override them (``None`` meaning "no limit").
    """

    def __init__(
        self,
        backend: Backend,
        *,
        name: str | None = None,
        max_read_connections: int = DEFAULT_MAX_READ_CONNECTIONS,
        time_limit_ms: int | None = None,
        max_rows: int | None = None,
        max_result_bytes: int | None = None,
        returning_limit: int | None = 1000,
        chunk_rows: int = 1000,
        deadline_ms: int | None = DEFAULT_DEADLINE_MS,
        native_operations: bool = True,
    ) -> None:
        self._backend = backend
        self.name = name
        self.time_limit_ms = time_limit_ms
        self.max_rows = max_rows
        self.max_result_bytes = max_result_bytes
        self.returning_limit = returning_limit
        self.chunk_rows = chunk_rows
        self.deadline_ms = deadline_ms
        self.native_operations = native_operations
        """When false, named operations always use the default
        implementations, never a backend's ``run_operation`` override."""
        self._pool = Pool(backend, max_read_connections=max_read_connections)
        self._schema_cache = SchemaCache()
        self._closed = False

    def __repr__(self) -> str:
        return f"<Database {self.name!r} {self._backend!r}>"

    # -- properties ------------------------------------------------------------

    @property
    def capabilities(self) -> Capabilities:
        return self._backend.capabilities

    @property
    def closed(self) -> bool:
        return self._closed

    def stats(self) -> PoolStats:
        """Connection counts for this database. Never opens anything."""
        return self._pool.stats()

    # -- helpers -------------------------------------------------------------

    def _check_open(self) -> None:
        if self._closed:
            raise DatabaseClosed(f"database {self.name!r} is closed")

    def _native(self, sql: str, params: Params) -> tuple[str, Any]:
        d = self._backend.dialect
        return rewrite(sql, params, d.paramstyle, d.flavor)

    # -- SQL execution -----------------------------------------------------------

    async def execute(
        self,
        sql: str,
        params: Params = None,
        *,
        time_limit_ms: Opt[int] = DEFAULT,
        max_rows: Opt[int] = DEFAULT,
        max_result_bytes: Opt[int] = DEFAULT,
    ) -> Results:
        """One read, on a read-only connection."""
        self._check_open()
        native_sql, native_params = self._native(sql, params)
        with map_errors(self._backend):
            async with self._pool.connection(write=False) as conn:
                with map_errors(self._backend):
                    return await conn.execute(
                    native_sql,
                    native_params,
                    time_limit_ms=pick(time_limit_ms, self.time_limit_ms),
                    max_rows=pick(max_rows, self.max_rows),
                    max_result_bytes=pick(max_result_bytes, self.max_result_bytes),
                )

    async def stream(
        self,
        sql: str,
        params: Params = None,
        *,
        time_limit_ms: Opt[int] = DEFAULT,
        chunk_rows: int | None = None,
        max_result_bytes: Opt[int] = DEFAULT,
    ) -> AsyncIterator[list[Row]]:
        """Async iterator of row chunks. Each chunk is fetched only when the
        consumer asks for it, and one read connection is held until the
        iterator is exhausted or closed. Use ``contextlib.aclosing`` (or
        exhaust it) to release the connection promptly when breaking out
        early.

        ``time_limit_ms`` is a budget of time spent inside the database across
        all chunks; time the consumer spends between chunks does not count.
        """
        self._check_open()
        native_sql, native_params = self._native(sql, params)
        with map_errors(self._backend):
            conn = await self._pool.acquire(write=False)
        discard = False
        try:
            rs = conn.stream(
                native_sql,
                native_params,
                chunk_rows=chunk_rows or self.chunk_rows,
                time_limit_ms=pick(time_limit_ms, self.time_limit_ms),
                max_result_bytes=pick(max_result_bytes, self.max_result_bytes),
            )
            try:
                index: dict[str, int] | None = None
                while True:
                    with map_errors(self._backend):
                        try:
                            chunk = await rs.__anext__()
                        except StopAsyncIteration:
                            break
                    if index is None:
                        index = make_index(rs.columns or ())
                    yield [Row(t, index) for t in chunk]
            finally:
                await rs.aclose()
        except BaseException as e:
            from .pool import _should_discard

            discard = _should_discard(e)
            raise
        finally:
            await self._pool.release(conn, discard=discard)

    async def execute_write(
        self,
        sql: str,
        params: Params = None,
        *,
        returning_limit: Opt[int] = DEFAULT,
        time_limit_ms: Opt[int] = DEFAULT,
    ) -> WriteResult:
        """One write in its own transaction (an autocommit statement), on the
        database's single write connection. Writes are serialised per
        database by the pool's write slot. Raises ``NestedWriteError`` from
        inside an open write transaction on this database (it would wait
        forever for the write slot that transaction holds)."""
        self._check_open()
        self._check_no_write_transaction()
        native_sql, native_params = self._native(sql, params)
        try:
            with map_errors(self._backend):
                async with self._pool.connection(write=True) as conn:
                    with map_errors(self._backend):
                        res = await conn.execute(
                        native_sql,
                        native_params,
                        time_limit_ms=pick(time_limit_ms, self.time_limit_ms),
                        max_rows=pick(returning_limit, self.returning_limit),
                    )
        finally:
            # Invalidate before the caller hears back, success or not.
            self._schema_cache.clear()
        return WriteResult(
            rowcount=res.rowcount,
            last_pk=res.last_pk,
            columns=res.columns,
            rows=res.rows,
            truncated=res.truncated,
        )

    # -- transactions -------------------------------------------------------------

    def _check_no_write_transaction(self) -> None:
        tx = active_transaction(self)
        if tx is not None and tx.mode == "write":
            raise NestedWriteError(
                f"write on database {self.name!r} from inside an open write transaction "
                "on the same database; use the transaction object instead"
            )

    def transaction(
        self, mode: str = "write", *, deadline_ms: Opt[int] = DEFAULT
    ) -> Transaction:
        """An interactive transaction, used as ``async with db.transaction()
        as tx``. ``mode="write"`` (default) holds the write slot until the
        block ends; ``mode="read"`` uses a read connection, never takes the
        write slot and sees one snapshot (from its first statement) where
        ``capabilities.snapshot_reads`` is true. ``deadline_ms`` defaults to
        the database's ``deadline_ms`` (5000); ``None`` disables it.

        Opening a transaction inside another on the same database (in the
        same task, or a task created inside the block) raises
        ``NestedWriteError``. Reads on ``db`` from inside a transaction work
        and use other connections."""
        self._check_open()
        return Transaction(self, mode, pick(deadline_ms, self.deadline_ms))

    # -- named operations ----------------------------------------------------------

    async def _operation(self, name: str, args: dict[str, Any]) -> OperationResult:
        """A named operation in its own write transaction."""
        async with self.transaction() as tx:
            return await tx._operation(name, args)

    async def _batched(
        self, name: str, rows: Iterable[Mapping[str, Any]], batch_size: int, args: dict[str, Any]
    ) -> OperationResult:
        if batch_size < 1:
            raise ValueError("batch_size must be at least 1")
        total = OperationResult()
        it = iter(rows)
        while batch := list(itertools.islice(it, batch_size)):
            async with self.transaction() as tx:
                total = total.merge(await tx._operation(name, {**args, "rows": batch}))
        return total

    async def insert_rows(
        self,
        table: str,
        rows: Iterable[Mapping[str, Any]],
        *,
        pk: str | Sequence[str] | None = None,
        alter: bool = False,
        ignore: bool = False,
        replace: bool = False,
        batch_size: int = 100,
    ) -> OperationResult:
        """Insert rows as many short transactions of ``batch_size`` rows each
        (other writers get the write slot in between), so a failure part-way
        leaves earlier batches committed. ``rows`` may be any iterable,
        consumed lazily. For all-or-nothing use ``tx.insert_rows`` or
        ``staged_import``."""
        return await self._batched(
            "insert_rows",
            rows,
            batch_size,
            dict(table=table, pk=pk, alter=alter, ignore=ignore, replace=replace),
        )

    async def upsert_rows(
        self,
        table: str,
        rows: Iterable[Mapping[str, Any]],
        *,
        pk: str | Sequence[str] | None = None,
        alter: bool = False,
        batch_size: int = 100,
    ) -> OperationResult:
        """Batched like ``insert_rows``."""
        return await self._batched(
            "upsert_rows", rows, batch_size, dict(table=table, pk=pk, alter=alter)
        )

    # -- introspection -----------------------------------------------------------

    async def schema(self) -> Schema:
        """Everything introspection knows, from the cache when it is current."""
        self._check_open()
        cache = self._schema_cache
        hint = self._backend.change_hint() if self.capabilities.cheap_change_hint else None
        cached = cache.fresh_by_hint(hint)
        if cached is not None:
            return cached
        with map_errors(self._backend):
            async with self._pool.connection(write=False) as conn:
                with map_errors(self._backend):
                    if cache.schema is not None:
                        version = await conn.schema_version()
                        if version == cache.version:
                            cache.store(cache.schema, hint)
                            return cache.schema
                    schema = await conn.schema()
        cache.store(schema, hint)
        return schema

    async def schema_version(self) -> object:
        """Opaque token that changes when the schema changes. Always asks the
        database (one cheap query)."""
        self._check_open()
        with map_errors(self._backend):
            async with self._pool.connection(write=False) as conn:
                with map_errors(self._backend):
                    return await conn.schema_version()

    def change_hint(self) -> object:
        """Cheap token that changes if the schema *might* have changed. Never
        opens a connection or wakes the database. Not a coroutine, matching
        ``Backend.change_hint``."""
        self._check_open()
        return self._backend.change_hint()

    # -- lifecycle ---------------------------------------------------------------

    async def sleep(self) -> None:
        """Release connections (and, where the backend allows, everything
        else). The next call wakes the database transparently. Connections in
        use right now finish their call and are closed when returned."""
        self._check_open()
        self._schema_cache.clear()
        await self._pool.close_idle()
        if self.capabilities.sleep_releases_everything and self._pool.in_use == 0:
            await self._backend.close()

    async def close(self) -> None:
        """Shut down for good. Every later call raises ``DatabaseClosed``.
        Idempotent."""
        if self._closed:
            return
        self._closed = True
        self._schema_cache.clear()
        await self._pool.close()
        await self._backend.close()

    async def __aenter__(self) -> "Database":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    # -- for later milestones ----------------------------------------------------

    def _connection(self, *, write: bool) -> Any:
        """Pooled connection context manager, for core modules only
        (transactions in milestone 2). Yields a ``BackendConnection``."""
        return self._pool.connection(write=write)

