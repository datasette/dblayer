"""``Registry``: many databases, one set of shared resources.

A registry holds databases by name and owns what they share: the hub that
tracks idle connections (idle timeout), the thread pool blocking drivers
run on (``max_threads``, fixed: it does not grow with the number of
databases), the global connection cap and the lifecycle hooks.

Adding, removing and looking up a database are dict operations: nothing
scans or copies the full set, and a registered database that is asleep
costs only its own small ``Database`` object.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterator
from concurrent.futures import ThreadPoolExecutor

from ..backends.base import UsesExecutor
from ..errors import UnknownDatabase
from .executor import DEFAULT_MAX_THREADS
from .hub import DEFAULT_IDLE_TIMEOUT, Hub
from .database import Database

__all__ = ["Registry", "Resolver"]

type Resolver = Callable[[str], Awaitable[Database | None]]


class Registry:
    """Databases by name, with lazy lookup through ``resolver``.

    ``resolver`` (async, ``name -> Database | None``) is called by ``get()``
    for a name the registry does not know; a returned database is added
    under that name. Concurrent ``get()`` calls for one unknown name share
    one resolver call.
    """

    def __init__(
        self,
        *,
        max_threads: int = DEFAULT_MAX_THREADS,
        idle_timeout: float | None = DEFAULT_IDLE_TIMEOUT,
        resolver: Resolver | None = None,
    ) -> None:
        if max_threads < 1:
            raise ValueError("max_threads must be at least 1")
        self.max_threads = max_threads
        self.executor = ThreadPoolExecutor(
            max_workers=max_threads, thread_name_prefix="dblayer-registry"
        )
        """The shared thread pool. Threads start on demand, up to
        ``max_threads``, and are reused; databases added here run their
        blocking calls on it (see ``UsesExecutor``)."""
        self._hub = Hub(idle_timeout=idle_timeout)
        self.resolver = resolver
        self._dbs: dict[str, Database] = {}
        self._resolving: dict[str, asyncio.Future[Database | None]] = {}
        self._closed = False

    def __repr__(self) -> str:
        return f"<Registry {len(self._dbs)} databases>"

    @property
    def idle_timeout(self) -> float | None:
        return self._hub.idle_timeout

    # -- membership ----------------------------------------------------------

    def add(self, name: str, db: Database) -> Database:
        """Register ``db`` under ``name`` and return it. The database must
        not be in another registry, and must have nothing open (it joins
        this registry's accounting asleep). Sets ``db.name`` if unset."""
        self._check_open()
        if name in self._dbs:
            raise ValueError(f"a database named {name!r} is already registered")
        if db._hub is not None:
            raise ValueError(f"{db!r} already belongs to a registry")
        pool = db._pool
        if pool is not None:
            if not pool.empty:
                raise ValueError(f"{db!r} has open connections; sleep it before adding")
            db._pool = None
        db._hub = self._hub
        backend = db._backend
        if isinstance(backend, UsesExecutor) and backend.executor is None:
            backend.executor = self.executor
        if db.name is None:
            db.name = name
        self._dbs[name] = db
        return db

    async def remove(self, name: str, *, close: bool = True) -> Database:
        """Unregister a database and return it, closed unless
        ``close=False``. With ``close=False`` it is put to sleep and leaves
        the registry's shared resources; connections still checked out are
        counted by the registry until they come back."""
        db = self._dbs.pop(name, None)
        if db is None:
            raise UnknownDatabase(f"no database named {name!r}")
        if close:
            await db.close()
        else:
            if not db.closed:
                await db.sleep()
            db._hub = None
        backend = db._backend
        if isinstance(backend, UsesExecutor) and backend.executor is self.executor:
            backend.executor = None
        return db

    def __contains__(self, name: object) -> bool:
        return name in self._dbs

    def __len__(self) -> int:
        return len(self._dbs)

    def __iter__(self) -> Iterator[str]:
        return iter(self._dbs)

    def peek(self, name: str) -> Database | None:
        """The registered database called ``name``, without calling the
        resolver."""
        return self._dbs.get(name)

    async def get(self, name: str) -> Database:
        """The database called ``name``. Unknown names go to ``resolver``;
        raises ``UnknownDatabase`` if there is none or it returns ``None``.
        Does not wake the database: that happens on its first call."""
        while True:
            db = self._dbs.get(name)
            if db is not None:
                return db
            self._check_open()
            if self.resolver is None:
                raise UnknownDatabase(f"no database named {name!r}")
            pending = self._resolving.get(name)
            if pending is None:
                return await self._resolve(name, self.resolver)
            resolved = await asyncio.shield(pending)
            if resolved is not None:
                return resolved
            # The caller resolving it was cancelled: try again.

    async def _resolve(self, name: str, resolver: Resolver) -> Database:
        fut: asyncio.Future[Database | None] = asyncio.get_running_loop().create_future()
        self._resolving[name] = fut
        try:
            resolved = await resolver(name)
            db = self._dbs.get(name)  # added by someone else meanwhile?
            if db is None:
                if resolved is None:
                    raise UnknownDatabase(f"no database named {name!r}")
                db = self.add(name, resolved)
        except asyncio.CancelledError:
            fut.set_result(None)  # waiters retry
            raise
        except BaseException as e:
            fut.set_exception(e)
            fut.exception()  # mark retrieved: there may be no waiters
            raise
        else:
            fut.set_result(db)
            return db
        finally:
            del self._resolving[name]

    # -- shutdown ------------------------------------------------------------

    def _check_open(self) -> None:
        if self._closed:
            raise RuntimeError("registry is closed")

    @property
    def closed(self) -> bool:
        return self._closed

    async def close(self) -> None:
        """Close every database and release the shared resources."""
        if self._closed:
            return
        self._closed = True
        dbs = list(self._dbs.values())
        self._dbs.clear()
        for db in dbs:
            await db.close()
        # Every call has finished, so the threads are idle: this only joins.
        self.executor.shutdown(wait=True)

    async def __aenter__(self) -> "Registry":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()
