"""The ``Database`` object: the only thing callers hold."""

from __future__ import annotations

import asyncio
import itertools
import logging
from contextlib import asynccontextmanager
from datetime import timedelta
from collections.abc import AsyncGenerator, AsyncIterator, Iterable, Mapping, Sequence
from typing import Any

from ..backends.base import Backend, Capabilities
from ..errors import DatabaseClosed, NestedWriteError
from .api import IntrospectionMixin, OperationsMixin
from .common import DEFAULT, Opt, map_errors, pick, spawn
from .operations import OperationResult
from .params import Params, rewrite
from .hub import Hub, LifecycleHook, default_hub
from .pool import DEFAULT_MAX_READ_CONNECTIONS, Pool, PoolStats
from .results import Results, Row, WriteResult, make_index
from .schema import Schema, SchemaCache
from .staged import StagedImport, sweep_staged
from .transactions import Transaction, active_transaction

__all__ = ["Database", "RowStream", "DEFAULT", "map_errors"]

DEFAULT_DEADLINE_MS = 5000

logger = logging.getLogger("dblayer")

# Old private names, kept for modules that imported them.
_Opt = Opt
_pick = pick


class RowStream:
    """What ``db.stream()`` returns: an async iterator of row chunks that is
    also an async context manager (``__aexit__`` closes it, returning the
    connection) and has ``aclose()``."""

    __slots__ = ("_gen",)

    def __init__(self, gen: AsyncGenerator[list[Row], None]) -> None:
        self._gen = gen

    def __aiter__(self) -> "RowStream":
        return self

    async def __anext__(self) -> list[Row]:
        return await self._gen.__anext__()

    async def aclose(self) -> None:
        await self._gen.aclose()

    async def __aenter__(self) -> "RowStream":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()


class Database(IntrospectionMixin, OperationsMixin):
    """One database. Creating it opens nothing; connections are opened on
    first use and released by ``sleep()`` (reversibly) or ``close()`` (for
    good).

    Limits given here are defaults for each call; per-call keyword arguments
    override them (``None`` meaning "no limit").
    """

    # Slots, and the pool / schema cache / staging set created only when
    # first needed, keep a never-used or asleep database to one small object
    # (the 10,000-database scale test measures it).
    __slots__ = (
        "_backend",
        "name",
        "max_read_connections",
        "time_limit_ms",
        "max_rows",
        "max_result_bytes",
        "returning_limit",
        "chunk_rows",
        "deadline_ms",
        "native_operations",
        "_staging",
        "_pool",
        "_schema_cache",
        "_closed",
        "_opened_total",
        "_closed_total",
        "_awake",
        "_transition",
        "idle_timeout",
        "_hub",
        "_cold",
    )

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
        idle_timeout: Opt[float] = DEFAULT,
    ) -> None:
        if max_read_connections < 1:
            raise ValueError("max_read_connections must be at least 1")
        self._backend = backend
        self.name = name
        self.max_read_connections = max_read_connections
        self.time_limit_ms = time_limit_ms
        self.max_rows = max_rows
        self.max_result_bytes = max_result_bytes
        self.returning_limit = returning_limit
        self.chunk_rows = chunk_rows
        self.deadline_ms = deadline_ms
        self.native_operations = native_operations
        """When false, named operations always use the default
        implementations, never a backend's ``run_operation`` override."""
        self._staging: set[str] | None = None
        self._pool: Pool | None = None
        self._schema_cache: SchemaCache | None = None
        self._closed = False
        self._opened_total = 0
        self._closed_total = 0
        self._awake = False
        self.idle_timeout = idle_timeout
        """Seconds a connection may sit unused before it is closed; when the
        last one closes the database is asleep. ``DEFAULT`` takes the hub's
        (registry's) setting, 30 s unless configured; ``None`` never closes
        idle connections."""
        self._hub: Hub | None = None
        self._cold = False
        self._transition: tuple[str, asyncio.Future[Any]] | None = None
        """A wake or sleep in progress: ``(kind, future)``."""

    def __repr__(self) -> str:
        return f"<Database {self.name!r} {self._backend!r}>"

    # -- properties ------------------------------------------------------------

    @property
    def capabilities(self) -> Capabilities:
        return self._backend.capabilities

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def state(self) -> str:
        """``"awake"`` (connections open or opening), ``"asleep"`` (none;
        the next call wakes it), ``"cold"`` (asleep, and evicted from local
        disk by the registry's ``on_evict`` hook; the next call wakes it
        through ``on_wake``, which must fetch it back) or ``"closed"`` (for
        good)."""
        if self._closed:
            return "closed"
        if self._awake:
            return "awake"
        return "cold" if self._cold else "asleep"

    def stats(self) -> PoolStats:
        """Connection counts for this database. Never opens anything."""
        pool = self._pool
        if pool is None:
            return PoolStats(0, 0, 0, 0, 0, 0, self._opened_total, self._closed_total)
        return pool.stats()

    # -- helpers -------------------------------------------------------------

    def _get_hub(self) -> Hub:
        return self._hub if self._hub is not None else default_hub()

    def _idle_timeout(self) -> float | None:
        if self.idle_timeout is DEFAULT:
            return self._get_hub().idle_timeout
        return self.idle_timeout  # type: ignore[return-value]

    def _get_pool(self) -> Pool:
        """The current pool, created if there is none. Does not handle a
        change of event loop; the core uses ``_use_pool``."""
        pool = self._pool
        if pool is None:
            self._check_open()
            try:
                loop: asyncio.AbstractEventLoop | None = asyncio.get_running_loop()
            except RuntimeError:
                loop = None
            pool = self._pool = Pool(self, loop)
        return pool

    async def _use_pool(self) -> Pool:
        """The pool for the running event loop.

        A pool (its asyncio locks, and connections some drivers bind to a
        loop) belongs to one event loop. When the database is used from a
        different loop and nothing is checked out on the old one, the old
        pool is retired: its idle connections are closed, the database goes
        to sleep as usual, and a fresh pool is made for this loop. If the
        old loop is closed, connections it left checked out are closed too.
        Moving while a connection is checked out on a loop that is still
        alive raises ``RuntimeError``."""
        loop = asyncio.get_running_loop()
        pool = self._pool
        if pool is not None and pool.loop is loop:
            return pool
        self._check_open()
        if pool is None:
            pool = self._pool = Pool(self, loop)
            return pool
        if pool.loop is None:
            pool.loop = loop  # created outside any loop and never used
            return pool
        return await self._move_to_loop(pool, loop)

    async def _move_to_loop(self, old: Pool, loop: asyncio.AbstractEventLoop) -> Pool:
        assert old.loop is not None
        old_alive = not old.loop.is_closed()
        if old_alive and old.users:
            raise RuntimeError(
                f"database {self.name!r} has connections checked out on another event "
                "loop; a Database can move to a new loop only when nothing is in use"
            )
        entries = old._retire(include_in_use=not old_alive)
        was_awake = self._awake
        self._awake = False
        new = self._pool = Pool(self, loop)
        # Callers arriving now wait for this, as for any sleep.
        fut = loop.create_future()
        transition = ("sleep", fut)
        self._transition = transition
        try:
            for e in entries:
                try:
                    await e.conn.close()
                except Exception:
                    pass
                self._closed_total += 1
                old.hub.release()
            if was_awake:
                await self._release_everything(old.hub)
        finally:
            if self._transition is transition:
                self._transition = None
            fut.set_result(None)
        return new

    # -- lifecycle internals -----------------------------------------------------

    async def _ensure_awake(self) -> None:
        """Called by the pool before it opens a connection while the
        database is not awake. The wake lock: the first caller runs the
        hub's ``on_wake`` hook while every concurrent caller waits for it
        (and shares its error, if it fails). A sleep or eviction in progress
        is waited for first."""
        while not self._awake:
            t = self._transition
            if t is not None:
                kind, fut = t
                if kind == "wake":
                    await asyncio.shield(fut)  # the waker's error is ours too
                else:
                    try:
                        await asyncio.shield(fut)
                    except Exception:
                        pass
                continue
            self._check_open()
            hub = self._get_hub()
            if hub.on_wake is None:
                if self._cold:
                    raise RuntimeError(
                        f"database {self.name!r} is cold (evicted from local disk) "
                        "and there is no on_wake hook to fetch it back"
                    )
                self._mark_awake(hub)
                return
            fut = asyncio.get_running_loop().create_future()
            transition = ("wake", fut)
            self._transition = transition
            try:
                await hub.on_wake(self)
            except asyncio.CancelledError:
                fut.set_result(None)  # not an error: a waiter takes over
                raise
            except BaseException as e:
                fut.set_exception(e)
                fut.exception()  # retrieved, even if nobody was waiting
                raise
            else:
                self._mark_awake(hub)
                fut.set_result(None)
            finally:
                if self._transition is transition:
                    self._transition = None

    def _mark_awake(self, hub: Hub) -> None:
        self._awake = True
        hub.wakes += 1
        hub.awake += 1
        if self._cold:
            self._cold = False
            hub.cold -= 1

    async def _evict(self, hub: Hub, on_evict: LifecycleHook) -> bool:
        """Asleep -> cold through ``on_evict``, holding the transition so
        that a request arriving meanwhile waits and then wakes (through
        ``on_wake``). Only an asleep database is evicted: returns False if
        it is awake or changing state, or if the hook returns False."""
        self._check_open()
        if self._cold:
            return True
        pool = self._pool
        if self._awake or self._transition is not None or (pool is not None and not pool.empty):
            return False
        fut = asyncio.get_running_loop().create_future()
        transition = ("evict", fut)
        self._transition = transition
        try:
            if await on_evict(self) is False:
                return False
            self._cold = True
            hub.evictions += 1
            hub.cold += 1
            return True
        finally:
            if self._transition is transition:
                self._transition = None
            fut.set_result(None)

    def _pool_empty(self, pool: Pool) -> None:
        """The pool has no connections and no users: go to sleep."""
        if pool is not self._pool or not self._awake:
            return
        self._awake = False
        fut = asyncio.get_running_loop().create_future()
        transition = ("sleep", fut)
        self._transition = transition
        spawn(self._sleep_job(transition, pool.hub))

    async def _sleep_job(self, transition: tuple[str, asyncio.Future[Any]], hub: Hub) -> None:
        """Runs once the last connection has closed: release the backend,
        run ``on_sleep``, drop the pool object. A wake waits for this."""
        try:
            await self._release_everything(hub)
        finally:
            if self._transition is transition:
                self._transition = None
            pool = self._pool
            if pool is not None and pool.empty and not self._awake:
                self._pool = None
            transition[1].set_result(None)

    async def _release_everything(self, hub: Hub) -> None:
        hub.sleeps += 1
        hub.awake -= 1
        if self._closed or self.capabilities.sleep_releases_everything:
            try:
                await self._backend.close()
            except Exception:
                logger.exception("closing the backend of %r failed", self)
        if hub.on_sleep is not None:
            try:
                await hub.on_sleep(self)
            except Exception:
                logger.exception("on_sleep hook failed for %r", self)

    async def _wait_transition(self) -> None:
        t = self._transition
        if t is not None:
            try:
                await asyncio.shield(t[1])
            except Exception:
                pass

    def _staging_names(self) -> set[str]:
        """Names of staging tables owned by imports running in this
        process (created on first use)."""
        if self._staging is None:
            self._staging = set()
        return self._staging

    def _cache(self) -> SchemaCache:
        if self._schema_cache is None:
            self._schema_cache = SchemaCache()
        return self._schema_cache

    def _invalidate_schema(self) -> None:
        if self._schema_cache is not None:
            self._schema_cache.clear()

    def _cached_schema(self) -> Schema | None:
        return None if self._schema_cache is None else self._schema_cache.schema

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
            async with (await self._use_pool()).connection(write=False) as conn:
                with map_errors(self._backend):
                    return await conn.execute(
                    native_sql,
                    native_params,
                    time_limit_ms=pick(time_limit_ms, self.time_limit_ms),
                    max_rows=pick(max_rows, self.max_rows),
                    max_result_bytes=pick(max_result_bytes, self.max_result_bytes),
                )

    def stream(
        self,
        sql: str,
        params: Params = None,
        *,
        time_limit_ms: Opt[int] = DEFAULT,
        chunk_rows: int | None = None,
        max_result_bytes: Opt[int] = DEFAULT,
    ) -> "RowStream":
        """Async iterator of row chunks. Each chunk is fetched only when the
        consumer asks for it, and one read connection is held until the
        iterator is exhausted or closed.

        Use it as ``async with db.stream(sql) as chunks: async for chunk in
        chunks: ...`` to return the connection deterministically when the
        block ends, even after ``break`` or an exception. Plain ``async for``
        also works but, if abandoned early, releases the connection only
        when the iterator is garbage collected.

        ``time_limit_ms`` is a budget of time spent inside the database across
        all chunks; time the consumer spends between chunks does not count.
        """
        return RowStream(
            self._stream(
                sql,
                params,
                time_limit_ms=time_limit_ms,
                chunk_rows=chunk_rows,
                max_result_bytes=max_result_bytes,
            )
        )

    async def _stream(
        self,
        sql: str,
        params: Params,
        *,
        time_limit_ms: Opt[int],
        chunk_rows: int | None,
        max_result_bytes: Opt[int],
    ) -> AsyncIterator[list[Row]]:
        self._check_open()
        native_sql, native_params = self._native(sql, params)
        with map_errors(self._backend):
            pool = await self._use_pool()
            conn = await pool.acquire(write=False)
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
            await pool.release(conn, discard=discard)

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
                async with (await self._use_pool()).connection(write=True) as conn:
                    with map_errors(self._backend):
                        res = await conn.execute(
                        native_sql,
                        native_params,
                        time_limit_ms=pick(time_limit_ms, self.time_limit_ms),
                        max_rows=pick(returning_limit, self.returning_limit),
                    )
        finally:
            # Invalidate before the caller hears back, success or not.
            self._invalidate_schema()
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

    async def batch(
        self,
        statements: Iterable[tuple[str, Params] | str],
        *,
        expect_schema_version: object | None = None,
        deadline_ms: Opt[int] = DEFAULT,
    ) -> list[WriteResult]:
        """Run ``statements`` (``(sql, params)`` pairs, or bare SQL strings)
        in one write transaction: all or nothing. Where the backend supports
        it (``SupportsBatch``) they go in one call with no awaits between
        them. With ``expect_schema_version``, the version is checked inside
        the transaction, after the write slot is held, and ``SchemaChanged``
        is raised with nothing run if it moved. Returns one ``WriteResult``
        per statement (``RETURNING`` rows capped by ``returning_limit``)."""
        from ..errors import SchemaChanged

        items = [(s, None) if isinstance(s, str) else (s[0], s[1]) for s in statements]
        async with self.transaction(deadline_ms=deadline_ms) as tx:
            if expect_schema_version is not None:
                version = await tx.schema_version()
                if version != expect_schema_version:
                    raise SchemaChanged(
                        f"schema version is {version!r}, expected {expect_schema_version!r}"
                    )
            if not items:
                return []
            return await tx._execute_batch(items)

    # -- staged import ---------------------------------------------------------------

    def staged_import(
        self,
        table: str,
        *,
        mode: str = "create",
        pk: str | Sequence[str] | None = None,
        alter: bool = False,
        columns: Mapping[str, str | type] | None = None,
    ) -> StagedImport:
        """An all-or-nothing import made of short transactions; see
        ``dblayer.core.staged``. ``mode`` is ``create``, ``replace`` or
        ``append``. ``pk`` and ``columns`` define the table for ``create``
        (otherwise the first chunk does); ``alter=True`` lets chunks add
        columns."""
        self._check_open()
        return StagedImport(self, table, mode=mode, pk=pk, alter=alter, columns=columns)

    async def sweep_staged(self, max_age: float | timedelta) -> list[str]:
        """Drop staging tables (orphans of crashed imports) older than
        ``max_age`` seconds; returns their names."""
        self._check_open()
        return await sweep_staged(self, max_age)

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
        cache = self._cache()
        # Every backend's hint is trusted while it stays the same: a file
        # stat for SQLite, a TTL token for PostgreSQL (so changes made by
        # other clients are seen after the TTL or refresh_schema()); None
        # (no hint at all) always checks the version.
        hint = self._backend.change_hint()
        cached = cache.fresh_by_hint(hint)
        if cached is not None:
            return cached
        with map_errors(self._backend):
            async with (await self._use_pool()).connection(write=False) as conn:
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
            async with (await self._use_pool()).connection(write=False) as conn:
                with map_errors(self._backend):
                    return await conn.schema_version()

    def change_hint(self) -> object:
        """Cheap token that changes if the schema *might* have changed. Never
        opens a connection or wakes the database. Not a coroutine, matching
        ``Backend.change_hint``."""
        self._check_open()
        return self._backend.change_hint()

    async def refresh_schema(self) -> Schema:
        """Drop the cached schema and read it again, and move
        ``change_hint()`` on so callers tracking it re-check. On backends
        whose hint is only a TTL (PostgreSQL), this is how a schema change
        made by another client is seen before the TTL runs out."""
        self._check_open()
        invalidate = getattr(self._backend, "invalidate_change_hint", None)
        if invalidate is not None:
            invalidate()
        if self._schema_cache is not None:
            self._schema_cache.clear()
        return await self.schema()

    # -- lifecycle ---------------------------------------------------------------

    async def sleep(self) -> None:
        """Release connections (and, where the backend allows, everything
        else). The next call wakes the database transparently. Connections in
        use right now finish their call and are closed when returned; once
        the last one is back the backend itself is closed, as it is at once
        when nothing is in use."""
        self._check_open()
        self._invalidate_schema()
        pool = self._pool
        if pool is not None:
            # Closing the last idle connection starts the sleep; connections
            # in use start it when they come back.
            await pool.close_idle()
            await self._wait_transition()
        elif self.capabilities.sleep_releases_everything:
            await self._backend.close()

    async def close(self) -> None:
        """Shut down for good. Every later call raises ``DatabaseClosed``.
        Idempotent."""
        if self._closed:
            return
        self._closed = True
        self._invalidate_schema()
        pool = self._pool
        if pool is not None:
            await pool.close()
        await self._wait_transition()
        if pool is None or pool.empty:
            await self._backend.close()
        # Otherwise connections are still in use: the backend is closed by
        # the sleep that follows the last one coming back.

    async def __aenter__(self) -> "Database":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    # -- for later milestones ----------------------------------------------------

    @asynccontextmanager
    async def _connection(self, *, write: bool) -> AsyncIterator[Any]:
        """Pooled connection context manager, for core modules only.
        Yields a ``BackendConnection``."""
        async with (await self._use_pool()).connection(write=write) as conn:
            yield conn

