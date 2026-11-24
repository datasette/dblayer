"""Experimental: SQLite in a small pool of multi-tenant C worker processes.

``SqliteCPool(path, pool=...)`` runs every connection of every database that
uses a ``CPool`` inside a few ``dblayer-cworker --multi`` processes (default
2). Inside a process each connection has a thread of its own, so requests
for different databases run at the same time; the parent multiplexes many
in-flight requests over each process's pipes, matched by ``id``, each with
its own kill timer. A new database on a running process costs one ``open``
round trip.

The price is a shared blast radius. Some statements cannot be interrupted
(one long C function call: ``instr``/``LIKE``/``GLOB`` on huge strings,
``randomblob(1e9)``; see ``tests/faults/test_worker_time.py``), and the only
way to stop one is to SIGKILL its process, which takes every connection on
it. What happens to the other tenants when the parent kills a process
because of one request (time limit, cancel or transaction deadline overrun):

* the culprit gets ``QueryInterrupted`` / ``TransactionExpired`` as on the
  single-tenant workers, and its database is *quarantined*: for
  ``quarantine_s`` seconds its new connections go to a separate quarantine
  process, so a repeat offender only takes down other offenders;
* in-flight requests that are known to be idempotent are retried once,
  transparently, on a replacement process: ``execute``, ``batch``,
  ``schema`` and ``schema_version`` on a *read* connection (opened
  ``mode=ro``, so it cannot write) outside a transaction. Streams are not
  retried (rows may already have been handed out);
* every other in-flight request (a write, anything inside a transaction, a
  stream) fails with ``WorkerCrashed``. A write that was in flight may or
  may not have committed; it is never retried, so it is never applied
  twice, and the caller is always told;
* idle connections reconnect on their next use (one ``open`` round trip on
  a live process); one with an open transaction raises ``WorkerCrashed``.

A process that dies with no known culprit (killed from outside, a crash, an
oversized frame) fails every in-flight request with ``WorkerCrashed``: there
is no way to tell an innocent read from the one that crashed it.

Memory: ``heap_limit_mb`` (``sqlite3_hard_heap_limit64``) and
``soft_heap_limit_mb`` are per *process*, shared by every tenant on it;
``cache_kb`` bounds each connection's page cache. One tenant can use the
whole heap limit for a moment (``randomblob``), during which another
tenant's allocations fail with ``ResourceLimitExceeded``; it cannot read or
corrupt another tenant's memory short of a bug in SQLite or the worker.
"""

from __future__ import annotations

import asyncio
import itertools
import os
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from ..errors import NotSupported, QueryInterrupted, ResourceLimitExceeded, WorkerCrashed
from .sqlite_cworker import SqliteCWorker, _default_codec, find_binary
from .sqlite_worker import (
    DEFAULT_KILL_GRACE_MS,
    DEFAULT_MAX_FRAME_BYTES,
    WorkerConfig,
    WorkerConnection,
    WorkerError,
    WorkerProcess,
    _intern,
    _Waiter,
)

__all__ = ["CPool", "SqliteCPool", "MultiWorkerConfig", "MuxWorkerProcess", "default_pool"]

RETRYABLE_TYPES = frozenset({"execute", "batch", "schema", "schema_version"})


class InnocentCrash(WorkerCrashed):
    """Raised to a request whose process was killed for another request's
    sake. ``SqliteCPool`` connections retry idempotent reads on it."""


@dataclass(frozen=True, slots=True)
class MultiWorkerConfig(WorkerConfig):
    """``WorkerConfig`` for ``dblayer-cworker --multi``."""

    max_connections: int = 256
    heap_limit_mb: int | None = None
    soft_heap_limit_mb: int | None = None
    cache_kb: int | None = None
    lookaside: tuple[int, int] | None = None

    def argv(self) -> list[str]:
        args = WorkerConfig.argv(self) + ["--multi", "--max-connections", str(self.max_connections)]
        if self.heap_limit_mb:
            args += ["--heap-limit-mb", str(int(self.heap_limit_mb))]
        if self.soft_heap_limit_mb:
            args += ["--soft-heap-limit-mb", str(int(self.soft_heap_limit_mb))]
        if self.cache_kb:
            args += ["--cache-kb", str(int(self.cache_kb))]
        if self.lookaside is not None:
            args += ["--lookaside", f"{int(self.lookaside[0])},{int(self.lookaside[1])}"]
        return args


class MuxWorkerProcess(WorkerProcess):
    """A multi-tenant worker: any number of requests in flight at once (one
    per connection: the worker runs each connection's requests in order),
    matched by ``id``. ``WorkerProcess`` sends one at a time."""

    def __init__(self, config: WorkerConfig, pool: CPool | None = None, *, quarantine: bool = False) -> None:
        super().__init__(config)
        self.pool = pool
        self.quarantine = quarantine
        self._retryable: dict[int, bool] = {}
        self._by_owner: dict[int, int] = {}  # id(owner) -> its in-flight request
        self.conn_ids: set[int] = set()

    def __repr__(self) -> str:
        state = "dead" if self.dead else "alive"
        q = " quarantine" if self.quarantine else ""
        return f"<MuxWorkerProcess pid={self.pid} {state}{q} handles={self.handles}>"

    async def call(
        self,
        msg: dict[str, Any],
        *,
        owner: object,
        limit_ms: float | None = None,
        expect_frames: int | None = None,
        on_sent: Callable[[int], None] | None = None,
        retryable: bool = False,
    ) -> Any:
        self._check()
        mid = msg["id"] = msg.get("id") or next(self._ids)
        if expect_frames is not None and msg.get("type") == "more":
            del msg["id"]
            mid = msg["stream"]
        fut: asyncio.Future[Any] = self.loop.create_future()
        self._waiters[mid] = _Waiter(fut, owner, expect_frames)
        self._retryable[mid] = retryable
        try:
            self.send(msg)
        except BaseException:
            self._waiters.pop(mid, None)
            self._retryable.pop(mid, None)
            raise
        if on_sent is not None:
            on_sent(mid)
        timer = None
        if limit_ms is not None:
            timer = self.loop.call_later(
                max(limit_ms, 0) / 1000 + self.config.kill_grace_ms / 1000,
                self._kill_for_time, mid, limit_ms,
            )
        okey = id(owner)
        self._by_owner[okey] = mid
        self.inflight = (mid, owner)
        try:
            reply = await self._wait(fut, mid)
        finally:
            if self._by_owner.get(okey) == mid:
                del self._by_owner[okey]
            if self.inflight is not None and self.inflight[0] == mid:
                self.inflight = None
            self._retryable.pop(mid, None)
            if timer is not None:
                timer.cancel()
        if expect_frames is None:
            from .sqlite_worker import _unwrap

            return _unwrap(reply, self)
        return reply

    def inflight_of(self, owner: object) -> int | None:
        return self._by_owner.get(id(owner))

    def cancel(self, owner: object) -> None:
        mid = self._by_owner.get(id(owner))
        if mid is not None and self.dead is None:
            try:
                self.send({"type": "cancel", "target": mid})
            except Exception:
                pass

    # -- kills with a known culprit ------------------------------------------------

    def kill_culprit(self, mid: int | None, culprit_exc: BaseException, reason: str) -> None:
        """SIGKILL this process because of request ``mid`` (or, with
        ``mid=None`` and ``reason``, a connection's transaction). The culprit
        gets ``culprit_exc``; idempotent reads in flight get
        ``InnocentCrash`` (their connections retry them); everything else
        gets ``WorkerCrashed``."""
        culprit_owner = None
        w = self._waiters.get(mid) if mid is not None else None
        if w is not None:
            culprit_owner = w.owner
        for_ids: dict[int, BaseException] = {}
        for wid, waiter in self._waiters.items():
            if wid == mid:
                for_ids[wid] = culprit_exc
            elif self._retryable.get(wid) and waiter.expect is None:
                for_ids[wid] = InnocentCrash(
                    f"worker {self.pid} was killed because {reason} (another database's request); "
                    "this read is retried on a new process"
                )
            else:
                for_ids[wid] = WorkerCrashed(
                    f"worker {self.pid} was killed because {reason} (another database's request); "
                    "this request was a write, inside a transaction or a stream, so it is not retried"
                )
        if self.pool is not None:
            self.pool._on_kill(self, culprit_owner, len([e for e in for_ids.values() if type(e) is InnocentCrash]))
        self.kill(WorkerCrashed(f"worker {self.pid} was killed because {reason}"), for_ids=for_ids)

    def _kill_for_time(self, mid: int, limit_ms: float) -> None:
        interrupted = QueryInterrupted(
            f"query did not stop within its time limit ({limit_ms:.0f} ms) plus "
            f"{self.config.kill_grace_ms} ms; worker {self.pid} was killed"
        )
        interrupted.__cause__ = WorkerError(f"SIGKILL sent to worker {self.pid}")
        self.kill_culprit(mid, interrupted, "a query on it overran its time limit")

    async def _wait(self, fut: asyncio.Future[Any], mid: int) -> Any:
        try:
            return await asyncio.shield(fut)
        except asyncio.CancelledError:
            if fut.done() or self.dead is not None:
                raise
            try:
                self.send({"type": "cancel", "target": mid})
            except Exception:
                raise asyncio.CancelledError() from None
            end = time.monotonic() + self.config.kill_grace_ms / 1000
            while not fut.done() and self.dead is None:
                remaining = end - time.monotonic()
                if remaining <= 0:
                    self.kill_culprit(
                        mid,
                        WorkerCrashed(f"worker {self.pid} did not stop a cancelled request; killed"),
                        "a cancelled request on it did not stop",
                    )
                    break
                try:
                    await asyncio.wait_for(asyncio.shield(fut), remaining)
                except asyncio.CancelledError:
                    continue
                except BaseException:
                    break
            if fut.done() and not fut.cancelled():
                fut.exception()
            raise


@dataclass
class PoolStats:
    processes_started: int = 0
    kills: int = 0
    """Processes the parent killed because of one request."""
    reads_retried: int = 0
    retried_ok: int = 0
    reconnects: int = 0
    quarantined: int = 0
    last_kill_at: float | None = None
    stalls: list[float] = field(default_factory=list)
    """Seconds from a kill to each retried read's reply."""


class CPool:
    """A process-wide pool of multi-tenant C worker processes, shared by
    every ``SqliteCPool`` database that names it.

    * ``size``: worker processes for ordinary tenants (default 2), started on
      demand. A connection goes to an idle process, else a new one while
      there are fewer than ``size``, else the one with fewest connections.
    * ``max_connections``: per process (the worker's thread cap).
    * ``quarantine_s``: after a kill, the culprit's database places new
      connections on a separate quarantine process for this long (0: off).
    * ``heap_limit_mb``, ``soft_heap_limit_mb``: process-wide SQLite heap
      limits; ``cache_kb``: per-connection page cache; ``memory_limit_mb``:
      ``RLIMIT_AS`` per process (Linux).
    * ``lookaside``: ``(slot_bytes, slots)`` for each connection's lookaside
      slab (SQLite's default is 1,200 x 100, about 120 KB per connection,
      most of an idle connection's cost); ``(0, 0)`` turns it off.
    * ``linger``: seconds a process with no connections stays up.

    Belongs to one event loop at a time (a new loop closes the old
    processes); ``close()`` stops everything.
    """

    def __init__(
        self,
        size: int = 2,
        *,
        binary: str | os.PathLike[str] | None = None,
        codec: str | None = None,
        max_connections: int = 256,
        quarantine_s: float = 60.0,
        heap_limit_mb: int | None = None,
        soft_heap_limit_mb: int | None = None,
        cache_kb: int | None = None,
        lookaside: tuple[int, int] | None = None,
        memory_limit_mb: int | None = None,
        kill_grace_ms: int = DEFAULT_KILL_GRACE_MS,
        max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
        linger: float = 5.0,
        debug: bool = False,
    ) -> None:
        if size < 1:
            raise ValueError("size must be at least 1")
        self.size = size
        self.linger = linger
        self.quarantine_s = quarantine_s
        self.config: MultiWorkerConfig = _intern(  # type: ignore[assignment]
            MultiWorkerConfig(
                codec=codec or _default_codec(),
                memory_limit_mb=memory_limit_mb,
                kill_grace_ms=kill_grace_ms,
                max_frame_bytes=max_frame_bytes,
                debug=debug,
                executable=find_binary(binary),
                max_connections=max_connections,
                heap_limit_mb=heap_limit_mb,
                soft_heap_limit_mb=soft_heap_limit_mb,
                cache_kb=cache_kb,
                lookaside=lookaside,
            )
        )
        self.procs: list[MuxWorkerProcess] = []
        self.quarantine_proc: MuxWorkerProcess | None = None
        self.quarantined: dict[str, float] = {}
        """Database path -> monotonic time its quarantine ends."""
        self._starting: dict[bool, asyncio.Future[MuxWorkerProcess]] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        self.stats = PoolStats()

    def __repr__(self) -> str:
        return f"<CPool size={self.size} running={len(self.procs)}>"

    def slot(self, path: str) -> "_Slot":
        return _Slot(self, path)

    def processes(self) -> list[MuxWorkerProcess]:
        out = [p for p in self.procs if p.alive]
        if self.quarantine_proc is not None and self.quarantine_proc.alive:
            out.append(self.quarantine_proc)
        return out

    def is_quarantined(self, path: str) -> bool:
        until = self.quarantined.get(path)
        if until is None:
            return False
        if time.monotonic() >= until:
            del self.quarantined[path]
            return False
        return True

    def _check_loop(self) -> None:
        loop = asyncio.get_running_loop()
        if self._loop is not loop:
            self.close()
            self._loop = loop

    async def _start(self, quarantine: bool) -> MuxWorkerProcess:
        """Start one process; concurrent callers for the same kind share
        the start."""
        pending = self._starting.get(quarantine)
        if pending is not None:
            return await asyncio.shield(pending)
        fut: asyncio.Future[MuxWorkerProcess] = asyncio.get_running_loop().create_future()
        self._starting[quarantine] = fut
        try:
            proc = MuxWorkerProcess(self.config, self, quarantine=quarantine)
            await proc.start()
            if not proc.hello.get("multi"):
                proc.kill()
                raise NotSupported("the C worker binary does not support --multi (rebuild: make -C cworker)")
            self.stats.processes_started += 1
            if quarantine:
                self.quarantine_proc = proc
            else:
                self.procs.append(proc)
            fut.set_result(proc)
            return proc
        except BaseException as e:
            fut.set_exception(e)
            fut.exception()
            raise
        finally:
            del self._starting[quarantine]

    async def acquire(self, path: str) -> MuxWorkerProcess:
        self._check_loop()
        cap = self.config.max_connections
        if self.quarantine_s > 0 and self.is_quarantined(path):
            proc = self.quarantine_proc
            if proc is None or not proc.alive or proc.handles >= cap:
                proc = await self._start(True)
        else:
            self.procs = [p for p in self.procs if p.alive]
            live = [p for p in self.procs if p.handles < cap]
            idle = [p for p in live if p.handles == 0]
            if idle:
                proc = idle[0]
            elif len(self.procs) < self.size or not live:
                if len(self.procs) >= self.size and not live and self.procs:
                    raise ResourceLimitExceeded(
                        f"every C pool process has {cap} connections (max_connections x size)"
                    )
                proc = await self._start(False)
            else:
                proc = min(live, key=lambda p: p.handles)
        proc.handles += 1
        if proc.idle_timer is not None:
            proc.idle_timer.cancel()
            proc.idle_timer = None
        return proc

    def release(self, proc: WorkerProcess) -> None:
        proc.handles -= 1
        if proc.handles > 0 or not proc.alive:
            return
        if self.linger <= 0:
            self._retire(proc)
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            self._retire(proc)
            return
        if loop is proc.loop:
            proc.idle_timer = loop.call_later(self.linger, self._retire, proc)
        else:
            self._retire(proc)

    def _retire(self, proc: WorkerProcess) -> None:
        if proc.handles != 0:
            return
        proc.idle_timer = None
        proc.close()
        if proc in self.procs:
            self.procs.remove(proc)  # type: ignore[arg-type]
        if proc is self.quarantine_proc:
            self.quarantine_proc = None

    def _on_kill(self, proc: MuxWorkerProcess, culprit: object, innocent_reads: int) -> None:
        self.stats.kills += 1
        self.stats.last_kill_at = time.perf_counter()
        backend = getattr(culprit, "_backend", None)
        path = getattr(backend, "_path", None)
        if path is not None and self.quarantine_s > 0:
            if not self.is_quarantined(path):
                self.stats.quarantined += 1
            self.quarantined[path] = time.monotonic() + self.quarantine_s
        if proc in self.procs:
            self.procs.remove(proc)
        if proc is self.quarantine_proc:
            self.quarantine_proc = None

    def close(self) -> None:
        """Stop every process (graceful: each rolls back and exits)."""
        procs, self.procs = self.procs, []
        if self.quarantine_proc is not None:
            procs.append(self.quarantine_proc)
            self.quarantine_proc = None
        for p in procs:
            p.close()

    async def aclose(self) -> None:
        self.close()


class _Slot:
    """``CPool`` seen from one database: what ``WorkerBackend`` calls
    ``workers`` (``acquire()``, ``release()``, ``config``)."""

    __slots__ = ("pool", "path")

    def __init__(self, pool: CPool, path: str) -> None:
        self.pool = pool
        self.path = path

    @property
    def config(self) -> MultiWorkerConfig:
        return self.pool.config

    async def acquire(self) -> MuxWorkerProcess:
        return await self.pool.acquire(self.path)

    def release(self, proc: WorkerProcess) -> None:
        self.pool.release(proc)

    def close(self) -> None:
        pass


_default: CPool | None = None


def default_pool() -> CPool:
    """The process-wide pool used when ``SqliteCPool`` is given none."""
    global _default
    if _default is None:
        _default = CPool()
    return _default


class SqliteCPool(SqliteCWorker):
    """SQLite on a shared pool of multi-tenant C worker processes. Options
    as ``SqliteCWorker`` (``sql_limits``, ``extensions``, ``immutable``,
    ``create``, ``busy_timeout``), plus ``pool`` (default: the
    process-wide ``default_pool()``). Process settings (codec, limits,
    grace) come from the pool."""

    __slots__ = ()

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        pool: CPool | None = None,
        extensions: Sequence[str | os.PathLike[str]] = (),
        sql_limits: Mapping[str, int] | None = None,
        **options: Any,
    ) -> None:
        if "workers" in options or "placement" in options:
            raise ValueError("SqliteCPool places connections through its pool (pool=CPool(...))")
        if "memory_limit_mb" in options:
            raise ValueError("memory limits are per process: CPool(heap_limit_mb=..., memory_limit_mb=...)")
        pool = pool if pool is not None else default_pool()
        abspath = os.path.abspath(os.fspath(path))
        super().__init__(
            path,
            binary=pool.config.executable,
            extensions=extensions,
            sql_limits=sql_limits,
            codec=pool.config.codec,
            workers=pool.slot(abspath),
            kill_grace_ms=pool.config.kill_grace_ms,
            max_frame_bytes=pool.config.max_frame_bytes,
            **options,
        )
        self.engine_config.pop("memory_limit_mb", None)  # per process, from the pool

    @property
    def pool(self) -> CPool:
        return self.workers.pool  # type: ignore[union-attr]

    async def open(self, *, write: bool) -> "CPoolConnection":
        if write and self.immutable:
            raise NotSupported("database is immutable; writes are not allowed")
        assert self.workers is not None
        proc = await self.workers.acquire()
        conn = CPoolConnection(self, proc, write=write, shared=True)
        try:
            await conn._open()
        except BaseException:
            await conn.close()
            raise
        return conn


class CPoolConnection(WorkerConnection):
    """A connection inside a multi-tenant process. Retries an idempotent
    read once when its process was killed for another request's sake, and
    reconnects (one ``open``) on its next use after any death while idle."""

    def _retryable(self, msg: Mapping[str, Any]) -> bool:
        return (
            not self.write
            and self._tx is None
            and not self._in_tx
            and msg.get("type") in RETRYABLE_TYPES
        )

    async def _ensure_alive(self) -> None:
        if not self._proc.alive and self._tx is None and not self._in_tx:
            self._backend.workers.pool.stats.reconnects += 1  # type: ignore[union-attr]
        await super()._ensure_alive()

    async def _call(self, msg: dict[str, Any], *, limit_ms: float | None = None) -> dict[str, Any]:
        if self._closed:
            raise WorkerCrashed("connection is closed")
        await self._ensure_alive()
        retryable = self._retryable(msg)
        proc = self._proc
        assert isinstance(proc, MuxWorkerProcess)
        msg["conn"] = self._conn_id
        try:
            reply = await proc.call(msg, owner=self, limit_ms=limit_ms, retryable=retryable)
        except InnocentCrash:
            if not retryable:
                raise
            stats = self._backend.workers.pool.stats  # type: ignore[union-attr]
            stats.reads_retried += 1
            await self._ensure_alive()
            msg.pop("id", None)
            msg["conn"] = self._conn_id
            proc = self._proc
            assert isinstance(proc, MuxWorkerProcess)
            reply = await proc.call(msg, owner=self, limit_ms=limit_ms, retryable=False)
            stats.retried_ok += 1
            if stats.last_kill_at is not None:
                stats.stalls.append(time.perf_counter() - stats.last_kill_at)
        if "in_tx" in reply or "result" in reply:
            self._in_tx = bool(reply.get("in_tx"))
        return reply

    async def _open(self) -> None:
        await super()._open()
        proc = self._proc
        if isinstance(proc, MuxWorkerProcess) and self._conn_id is not None:
            proc.conn_ids.add(self._conn_id)

    def _on_deadline_overrun(self, tx: str, deadline_ms: int) -> None:
        self._deadline_timer = None
        if self._tx != tx or self._tx_done:
            return
        proc = self._proc
        assert isinstance(proc, MuxWorkerProcess)
        mid = proc.inflight_of(self)
        exc = QueryInterrupted(
            f"transaction passed its deadline ({deadline_ms} ms) and the worker did not "
            f"confirm rollback; worker {proc.pid} was killed"
        )
        if mid is None:
            # Nothing of ours in flight: make the connection the culprit
            # through a stand-in waiter so quarantine still applies.
            mid = -next(_overrun_ids)
            proc._waiters[mid] = _Waiter(proc.loop.create_future(), self, None)
        proc.kill_culprit(mid, exc, "a transaction on it overran its deadline")


_overrun_ids = itertools.count(1)
