"""The ``Database`` object: the only thing callers hold."""

from __future__ import annotations

import enum
from collections.abc import AsyncIterator, Iterator
from contextlib import contextmanager
from typing import Any, Final

from ..backends.base import Backend, Capabilities
from ..errors import DatabaseClosed, DblayerError, NotSupported, TableNotFound
from .params import Params, rewrite
from .pool import DEFAULT_MAX_READ_CONNECTIONS, Pool, PoolStats
from .results import Results, Row, WriteResult, make_index
from .schema import ColumnInfo, ForeignKey, Index, Schema, SchemaCache, TableInfo

__all__ = ["Database", "DEFAULT"]


class _Default(enum.Enum):
    DEFAULT = "DEFAULT"

    def __repr__(self) -> str:
        return "DEFAULT"


DEFAULT: Final = _Default.DEFAULT
"""Sentinel for "use the Database's default"; pass ``None`` for no limit."""

type _Opt[T] = T | None | _Default


def _pick[T](value: _Opt[T], default: T | None) -> T | None:
    return default if value is DEFAULT else value  # type: ignore[return-value]


@contextmanager
def map_errors(backend: Backend) -> Iterator[None]:
    """Translate driver exceptions into dblayer exceptions, keeping the
    driver exception as ``__cause__``."""
    try:
        yield
    except DblayerError:
        raise
    except Exception as exc:
        mapped = backend.dialect.map_exception(exc)
        if mapped is None:
            raise
        raise mapped from exc


class Database:
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
    ) -> None:
        self._backend = backend
        self.name = name
        self.time_limit_ms = time_limit_ms
        self.max_rows = max_rows
        self.max_result_bytes = max_result_bytes
        self.returning_limit = returning_limit
        self.chunk_rows = chunk_rows
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
        time_limit_ms: _Opt[int] = DEFAULT,
        max_rows: _Opt[int] = DEFAULT,
        max_result_bytes: _Opt[int] = DEFAULT,
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
                    time_limit_ms=_pick(time_limit_ms, self.time_limit_ms),
                    max_rows=_pick(max_rows, self.max_rows),
                    max_result_bytes=_pick(max_result_bytes, self.max_result_bytes),
                )

    async def stream(
        self,
        sql: str,
        params: Params = None,
        *,
        time_limit_ms: _Opt[int] = DEFAULT,
        chunk_rows: int | None = None,
        max_result_bytes: _Opt[int] = DEFAULT,
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
                time_limit_ms=_pick(time_limit_ms, self.time_limit_ms),
                max_result_bytes=_pick(max_result_bytes, self.max_result_bytes),
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
        returning_limit: _Opt[int] = DEFAULT,
        time_limit_ms: _Opt[int] = DEFAULT,
    ) -> WriteResult:
        """One write in its own transaction (an autocommit statement), on the
        database's single write connection. Writes are serialised per
        database by the pool's write slot."""
        self._check_open()
        native_sql, native_params = self._native(sql, params)
        try:
            with map_errors(self._backend):
                async with self._pool.connection(write=True) as conn:
                    with map_errors(self._backend):
                        res = await conn.execute(
                        native_sql,
                        native_params,
                        time_limit_ms=_pick(time_limit_ms, self.time_limit_ms),
                        max_rows=_pick(returning_limit, self.returning_limit),
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

    async def table_names(self) -> list[str]:
        return (await self.schema()).table_names()

    async def view_names(self) -> list[str]:
        return (await self.schema()).view_names()

    async def _info(self, table: str) -> TableInfo:
        info = (await self.schema()).get(table)
        if info is None:
            raise TableNotFound(f"no such table: {table}", code="NOT_FOUND")
        return info

    async def table_columns(self, table: str) -> list[ColumnInfo]:
        return list((await self._info(table)).columns)

    async def primary_keys(self, table: str) -> list[str]:
        return list((await self._info(table)).primary_keys)

    async def foreign_keys(self, table: str) -> list[ForeignKey]:
        return list((await self._info(table)).foreign_keys)

    async def indexes(self, table: str) -> list[Index]:
        return list((await self._info(table)).indexes)

    async def definition(self, name: str) -> str | None:
        """The ``CREATE`` statement for a table, view, index or trigger."""
        self._check_open()
        if not self.capabilities.table_definition_sql:
            raise NotSupported("this backend cannot supply CREATE statements")
        schema = await self.schema()
        try:
            return schema.definition(name)
        except KeyError:
            raise TableNotFound(f"no such table, view, index or trigger: {name}", code="NOT_FOUND") from None

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

