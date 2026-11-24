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
from dataclasses import dataclass
from collections.abc import Awaitable, Callable, Iterator
from concurrent.futures import ThreadPoolExecutor

from ..backends.base import UsesExecutor
from ..errors import UnknownDatabase
from .executor import DEFAULT_MAX_THREADS
from .hub import DEFAULT_IDLE_TIMEOUT, Hub, LifecycleHook
from .database import Database

__all__ = ["Registry", "RegistryStats", "Resolver", "DEFAULT_MAX_OPEN_CONNECTIONS"]

DEFAULT_MAX_OPEN_CONNECTIONS = 200

type Resolver = Callable[[str], Awaitable[Database | None]]


@dataclass(frozen=True, slots=True)
class RegistryStats:
    """A snapshot from ``Registry.stats()``. Counting is O(1) apart from
    ``idle_connections`` (one step per distinct idle timeout)."""

    databases: int
    """Registered."""
    awake: int
    """Databases with connections open (or opening)."""
    cold: int
    """Databases evicted from local disk."""
    open_connections: int
    """Open, opening or closing, across every database (bounded by
    ``max_open_connections``)."""
    idle_connections: int
    waiting: int
    """Callers waiting for a place under the cap."""
    wakes: int
    sleeps: int
    evictions: int
    """Databases made cold by ``evict()``."""
    cap_evictions: int
    """Idle connections closed to make room under the cap."""
    idle_closes: int
    """Connections closed by the idle timeout."""
    max_open_connections: int | None
    max_threads: int


class Registry:
    """Databases by name, with lazy lookup through ``resolver``.

    Lifecycle hooks, each ``async (db) -> None``:

    * ``on_wake`` runs before an asleep database opens its first
      connection. Concurrent first requests trigger it once (the wake
      lock); the others wait, and share its exception if it raises (the
      next request tries again).
    * ``on_sleep`` runs after the last connection, and where the backend
      allows the backend itself, has closed (idle timeout, cap eviction,
      ``db.sleep()``, ``db.close()``). A wake waits for it to finish.
      Exceptions are logged to the ``dblayer`` logger, not raised.
    * ``on_evict`` is called by ``evict(name)`` for an asleep database, and
      makes it cold (nothing local). While it runs, a request for that
      database waits; it then wakes through ``on_wake``, which must fetch
      the file back. The registry never decides on its own to evict:
      callers (a disk-quota policy) call ``evict()``.

    ``resolver`` (async, ``name -> Database | None``) is called by ``get()``
    for a name the registry does not know; a returned database is added
    under that name. Concurrent ``get()`` calls for one unknown name share
    one resolver call.
    """

    def __init__(
        self,
        *,
        max_open_connections: int | None = DEFAULT_MAX_OPEN_CONNECTIONS,
        max_threads: int = DEFAULT_MAX_THREADS,
        idle_timeout: float | None = DEFAULT_IDLE_TIMEOUT,
        resolver: Resolver | None = None,
        on_wake: LifecycleHook | None = None,
        on_sleep: LifecycleHook | None = None,
        on_evict: LifecycleHook | None = None,
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
        self._hub = Hub(idle_timeout=idle_timeout, max_open_connections=max_open_connections)
        self._hub.on_wake = on_wake
        self._hub.on_sleep = on_sleep
        self.on_evict = on_evict
        """Async ``(db) -> bool | None``: remove an asleep database's local
        file (after copying it out, if it was written). Return ``False`` to
        refuse; raise to report a failure. Called by ``evict()`` only."""
        self.resolver = resolver
        self._dbs: dict[str, Database] = {}
        self._resolving: dict[str, asyncio.Future[Database | None]] = {}
        self._closed = False

    def __repr__(self) -> str:
        return f"<Registry {len(self._dbs)} databases>"

    @property
    def idle_timeout(self) -> float | None:
        return self._hub.idle_timeout

    @property
    def on_wake(self) -> LifecycleHook | None:
        return self._hub.on_wake

    @on_wake.setter
    def on_wake(self, hook: LifecycleHook | None) -> None:
        self._hub.on_wake = hook

    @property
    def on_sleep(self) -> LifecycleHook | None:
        return self._hub.on_sleep

    @on_sleep.setter
    def on_sleep(self, hook: LifecycleHook | None) -> None:
        self._hub.on_sleep = hook

    @property
    def max_open_connections(self) -> int | None:
        """Cap on connections open across every database here. At the cap
        the least recently used idle connection anywhere is closed; if none
        is idle, the caller waits. A task that holds one connection and
        waits for another can deadlock when every place is held that way, so
        the cap should exceed the number of concurrent requests that hold a
        connection while asking for a second."""
        return self._hub.max_open_connections

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

    # -- introspection ---------------------------------------------------------

    def stats(self) -> RegistryStats:
        hub = self._hub
        return RegistryStats(
            databases=len(self._dbs),
            awake=hub.awake,
            cold=hub.cold,
            open_connections=hub.open,
            idle_connections=hub.idle_count,
            waiting=hub.waiting,
            wakes=hub.wakes,
            sleeps=hub.sleeps,
            evictions=hub.evictions,
            cap_evictions=hub.cap_evictions,
            idle_closes=hub.idle_closes,
            max_open_connections=hub.max_open_connections,
            max_threads=self.max_threads,
        )

    # -- eviction --------------------------------------------------------------

    async def evict(self, name: str) -> bool:
        """Make the asleep database ``name`` cold through ``on_evict``.
        Returns True if it is now cold, False if it is awake, changing
        state, or ``on_evict`` refused. Never waits for a database to fall
        asleep (so it is safe to call from inside a hook); call
        ``db.sleep()`` first to release idle connections."""
        db = self._dbs.get(name)
        if db is None:
            raise UnknownDatabase(f"no database named {name!r}")
        if self.on_evict is None:
            raise RuntimeError("evict() needs an on_evict hook")
        if self._hub.on_wake is None:
            raise RuntimeError("evict() needs an on_wake hook to bring databases back")
        return await db._evict(self._hub, self.on_evict)

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
