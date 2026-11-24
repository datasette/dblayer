"""The worker process: ``python -m dblayer.worker [--memory-limit-mb N]
[--debug]``.

Runs an in-process backend (in-process SQLite by default; any engine named
in ``open``, see ``dblayer.worker.engines``) on behalf of a parent process,
speaking the protocol in ``dblayer.worker.protocol`` over stdin (requests)
and stdout (replies). Stray output (an init hook's ``print``) is redirected
to stderr, so it cannot corrupt the protocol stream.

The worker uses the engine only through the ``Backend`` /
``BackendConnection`` protocols (plus the optional ``SupportsOperations``,
``SupportsBatch`` and an ``in_transaction`` attribute), exactly as the core
does in-process.

Threads
-------
* The **main thread** runs an event loop. It reads and decodes frames as
  they arrive (``add_reader`` on stdin), writes every reply, and executes
  requests one at a time, in arrival order.
* The engine's blocking calls run on **one executor thread**, so the loop
  stays free while a statement runs: ``cancel`` cancels the request's task,
  which interrupts the statement (the backend's own cancel path: for
  SQLite, ``interrupt()`` repeated until the statement returns), and a
  statement time limit is backed by a timer that does the same at the limit
  (the second line of defence behind the engine's own limit, the progress
  handler on SQLite).

This differs from the spec (statements on the main thread, a reader
thread), because the worker must drive the engine through its async
protocol to stay engine-neutral; the roles are the same.

Deadlines
---------
Each open transaction's deadline is tracked here, independently of the
parent: at the deadline the worker cancels a request running in that
transaction (it fails with ``TransactionExpired``), rolls back and sends an
``expired`` notice. Later requests naming that transaction get
``TransactionExpired``; a ``rollback`` of it succeeds.

End of input
------------
On EOF the worker cancels whatever runs, rolls back and closes every
connection and backend, and exits. If that takes longer than 5 s (a
statement that ignores interrupts), it exits anyway.

Memory
------
``--memory-limit-mb`` sets ``RLIMIT_AS`` on Linux (the whole process,
counted from the address space mapped once start-up is done) before any
database is opened. Engines add their own limits (SQLite's
``hard_heap_limit``). A ``MemoryError`` while serving a request is reported
as ``ResourceLimitExceeded`` with ``fatal: true``, and the worker exits.
"""

from __future__ import annotations

import argparse
import asyncio
import itertools
import json
import os
import signal
import sys
import time
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from ..backends.base import Backend, BackendConnection, SupportsBatch, SupportsOperations
from ..core.operations import OPERATIONS
from ..core.params import leading_keyword, rewrite
from ..core.results import Results, WriteResult
from ..core.schema import Schema
from ..errors import (
    DblayerError,
    NotSupported,
    QueryError,
    QueryInterrupted,
    TransactionClosed,
    TransactionExpired,
)
from .engines import SQLITE_ENGINE, load_engine
from .protocol import (
    DEFAULT_MAX_FRAME_BYTES,
    PROTOCOL_VERSION,
    FrameDecoder,
    FrameTooLarge,
    available_codecs,
    frame,
    get_codec,
    nested_columns,
    results_to_wire,
    schema_to_wire,
    wire_rows,
)

_DML = frozenset({"SELECT", "INSERT", "UPDATE", "DELETE", "REPLACE", "VALUES", "MERGE"})
_BUILTIN_ERRORS = (OverflowError, ValueError, TypeError, KeyError, IndexError)
_SHUTDOWN_GRACE = 5.0


class _ParentGone(Exception):
    pass


class _Interrupted(Exception):
    """Cause of a request the worker itself stopped (cancel, time-limit
    backstop, deadline) before the engine raised anything."""


class Job:
    __slots__ = ("id", "msg", "type", "cancelled", "cancel_reason", "task", "wc")

    def __init__(self, msg: dict[str, Any], type_: str | None = None) -> None:
        self.msg = msg
        self.id = msg.get("id")
        self.type = type_ or msg.get("type")
        self.cancelled = False
        self.cancel_reason: str = "cancelled"
        self.task: asyncio.Task[Any] | None = None
        self.wc: Conn | None = None


class Engine:
    """One backend instance inside the worker, shared by its connections."""

    __slots__ = ("key", "backend", "conns")

    def __init__(self, key: str, backend: Backend) -> None:
        self.key = key
        self.backend = backend
        self.conns = 0


class Conn:
    __slots__ = ("id", "engine", "conn", "write", "tx", "tx_timer", "expired", "_tx_ids", "lock")

    def __init__(self, cid: int, engine: Engine, conn: BackendConnection, write: bool) -> None:
        self.id = cid
        self.engine = engine
        self.conn = conn
        self.write = write
        self.tx: str | None = None
        self.tx_timer: asyncio.TimerHandle | None = None
        self.expired: list[str] = []
        self._tx_ids = itertools.count(1)
        self.lock = asyncio.Lock()
        """Held by the request using this connection, and by a deadline
        rollback, so they never overlap."""

    @property
    def in_tx(self) -> bool:
        flag = getattr(self.conn, "in_transaction", None)
        if isinstance(flag, bool):
            return flag
        return self.tx is not None

    def new_tx_id(self) -> str:
        return f"t{next(self._tx_ids)}"


class Stream:
    __slots__ = ("id", "wc", "rs", "credit", "first", "nested")

    def __init__(self, sid: Any, wc: Conn, rs: Any, credit: int) -> None:
        self.id = sid
        self.wc = wc
        self.rs = rs
        self.credit = credit
        self.first = True
        self.nested: list[int] = []


class Server:
    def __init__(self, in_fd: int, out_fd: int, *, memory_limit_mb: int | None, debug: bool) -> None:
        self.in_fd = in_fd
        self.out_fd = out_fd
        self.memory_limit_mb = memory_limit_mb
        self.rlimit_as = False
        self.debug = debug
        self.codec = get_codec("json")
        self.max_frame_bytes = DEFAULT_MAX_FRAME_BYTES
        self.dec = FrameDecoder(self.max_frame_bytes)
        self.loop = asyncio.get_running_loop()
        self.executor = ThreadPoolExecutor(1, thread_name_prefix="dblayer-worker")
        self.q: asyncio.Queue[Job | None] = asyncio.Queue()
        self.current: Job | None = None
        self.pending: dict[Any, Job] = {}
        self.streams: dict[Any, Stream] = {}
        self.conns: dict[int, Conn] = {}
        self.engines: dict[str, Engine] = {}
        self._conn_ids = itertools.count(1)
        self.eof = False
        self.exit_requested = False

    # -- output ------------------------------------------------------------------

    def send(self, msg: dict[str, Any]) -> None:
        """Blocking write: if the parent stops reading, the worker blocks
        here (and the parent's timer kills it)."""
        try:
            data = frame(self.codec.encode(msg), self.max_frame_bytes)
        except FrameTooLarge as e:
            if msg.get("id") is None:
                raise
            data = frame(
                self.codec.encode(
                    {"id": msg["id"], "error": {"type": "ResourceLimitExceeded", "message": f"reply too large: {e}"}}
                )
            )
        view = memoryview(data)
        while view:
            try:
                n = os.write(self.out_fd, view)
            except BrokenPipeError:
                raise _ParentGone() from None
            view = view[n:]

    # -- input -------------------------------------------------------------------------

    def on_readable(self) -> None:
        try:
            data = os.read(self.in_fd, 1 << 18)
        except BlockingIOError:
            return
        except OSError:
            data = b""
        if not data:
            self.on_eof()
            return
        try:
            for payload in self.dec.feed(data):
                self.dispatch(self.codec.decode(payload))
        except FrameTooLarge as e:
            print(f"dblayer.worker: {e}; dropping the connection", file=sys.stderr)
            self.on_eof()
        except Exception as e:  # undecodable input: same as a dropped connection
            print(f"dblayer.worker: bad input ({e!r}); dropping the connection", file=sys.stderr)
            self.on_eof()

    def dispatch(self, msg: dict[str, Any]) -> None:
        t = msg.get("type")
        if t == "hello":
            self.hello(msg)
        elif t == "cancel":
            target = msg.get("target")
            job = self.current
            if job is not None and job.id == target:
                self.cancel_job(job, "cancelled")
            elif target in self.pending:
                self.pending[target].cancelled = True
            elif target in self.streams:
                self.enqueue(Job({"id": target, "stream": target}, "_close_stream"))
        elif t == "more":
            sid = msg.get("stream")
            self.enqueue(Job({"id": sid, "stream": sid, "credit": msg.get("credit", 1)}, "_more"))
        else:
            self.enqueue(Job(msg))

    def enqueue(self, job: Job) -> None:
        self.pending[job.id] = job
        self.q.put_nowait(job)

    def cancel_job(self, job: Job, reason: str) -> None:
        job.cancelled = True
        job.cancel_reason = reason
        if job.task is not None and not job.task.done():
            job.task.cancel()

    def on_eof(self) -> None:
        if self.eof:
            return
        self.eof = True
        try:
            self.loop.remove_reader(self.in_fd)
        except Exception:
            pass
        for job in self.pending.values():
            job.cancelled = True
        if self.current is not None:
            self.cancel_job(self.current, "cancelled")
        self.q.put_nowait(None)
        # Exit even if a statement ignores every interrupt.
        self.loop.call_later(_SHUTDOWN_GRACE, os._exit, 0)

    def hello(self, msg: dict[str, Any]) -> None:
        wanted = msg.get("codecs") or ["json"]
        have = available_codecs()
        codec = next((c for c in wanted if c in have), "json")
        if "max_frame_bytes" in msg:
            self.max_frame_bytes = int(msg["max_frame_bytes"])
            self.dec.max_frame_bytes = self.max_frame_bytes
        self.send(
            {
                "id": msg.get("id"),
                "type": "hello",
                "protocol": PROTOCOL_VERSION,
                "codec": codec,
                "sqlite_version": __import__("sqlite3").sqlite_version,
                "python": sys.version.split()[0],
                "pid": os.getpid(),
                "rlimit_as": self.rlimit_as,
            }
        )  # in JSON; everything after it in the agreed codec
        self.codec = get_codec(codec)

    # -- the request loop ------------------------------------------------------------

    async def serve(self) -> None:
        os.set_blocking(self.in_fd, False)
        self.loop.add_reader(self.in_fd, self.on_readable)
        try:
            while not self.exit_requested:
                job = await self.q.get()
                if job is None:
                    break
                await self.run(job)
        except _ParentGone:
            pass
        finally:
            await self.shutdown()

    async def run(self, job: Job) -> None:
        if self.pending.get(job.id) is job:
            del self.pending[job.id]
        self.current = job
        try:
            job.task = asyncio.ensure_future(self._handle(job))
            try:
                reply = await asyncio.shield(job.task)
            except asyncio.CancelledError:
                if not job.task.cancelled():
                    raise  # this task itself is being cancelled
                raise QueryInterrupted("query cancelled") from None
            if reply is not None:
                reply["id"] = job.id
                self.send(reply)
        except _ParentGone:
            raise
        except MemoryError:
            self.exit_requested = True
            self.send(
                {
                    "id": job.id,
                    "error": {"type": "ResourceLimitExceeded", "message": "worker ran out of memory (memory limit)"},
                    "fatal": True,
                }
            )
        except BaseException as e:
            if isinstance(e, (SystemExit, KeyboardInterrupt)):
                raise
            e = self.translate(job, e)
            reply = {"id": job.id, "error": self.error_payload(e, job)}
            if job.wc is not None and job.wc.in_tx:
                reply["in_tx"] = True
            self.send(reply)
            if job.type in ("stream", "_more"):
                await self._drop_stream(job.id)
        finally:
            self.current = None

    def translate(self, job: Job, e: BaseException) -> BaseException:
        """A request cut short by its transaction's deadline reports
        ``TransactionExpired``; one cut short by ``cancel``,
        ``QueryInterrupted``."""
        interrupted = isinstance(e, (QueryInterrupted, asyncio.CancelledError)) or (
            job.wc is not None and isinstance(job.wc.engine.backend.dialect.map_exception(e), QueryInterrupted)
        )
        if job.cancelled and interrupted:
            new: DblayerError
            if job.cancel_reason == "deadline":
                new = TransactionExpired(self._expired_msg())
            elif job.cancel_reason == "time_limit":
                new = QueryInterrupted("query interrupted (time limit exceeded)")
            else:
                new = QueryInterrupted("query cancelled")
            # Keep the driver's exception as the cause when there is one;
            # a cancel that stopped the call before the driver raised has
            # none, so say what stopped it.
            cause = e.__cause__ if isinstance(e, DblayerError) else e
            if cause is None or isinstance(cause, (asyncio.CancelledError, DblayerError)):
                cause = _Interrupted(f"request stopped by the worker ({job.cancel_reason})")
            new.__cause__ = cause
            return new
        return e

    async def _handle(self, job: Job) -> dict[str, Any] | None:
        if job.cancelled:
            raise QueryInterrupted("query cancelled")
        name = str(job.type).lstrip("_")
        handler = getattr(self, "do_" + name, None)
        if handler is None or (job.type == "_debug" and not self.debug):
            raise QueryError(f"unknown message type {job.type!r}", "PROTOCOL")
        if name in ("open", "sleep", "debug"):
            return await handler(job)
        if name in ("more", "close_stream"):
            s = self.streams.get(job.msg["stream"])
            if s is None:
                return None  # finished or closed meanwhile
            wc = job.wc = s.wc
        else:
            wc = job.wc = self.conn_for(job.msg)
        async with wc.lock:
            return await handler(job, wc)

    def error_payload(self, e: BaseException, job: Job) -> dict[str, Any]:
        cause: BaseException | None = None
        mapped: BaseException | None = None
        if isinstance(e, DblayerError):
            mapped, cause = e, e.__cause__
        else:
            if job.wc is not None:
                mapped = job.wc.engine.backend.dialect.map_exception(e)
            if mapped is not None:
                cause = e
            elif isinstance(e, _BUILTIN_ERRORS):
                return {"type": type(e).__name__, "message": str(e), "builtin": True}
            else:
                mapped = QueryError(f"worker error: {type(e).__name__}: {e}", "WORKER")
                cause = e
        err: dict[str, Any] = {"type": type(mapped).__name__, "message": str(mapped)}
        if isinstance(mapped, QueryError) and mapped.code is not None:
            err["code"] = mapped.code
        if cause is not None:
            err["cause"] = {"type": f"{type(cause).__module__}.{type(cause).__qualname__}", "message": str(cause)}
        return err

    async def shutdown(self) -> None:
        for s in list(self.streams.values()):
            await self._close_stream(s)
        for wc in list(self.conns.values()):
            await self._close_conn(wc)
        sys.stderr.flush()
        os._exit(0)

    # -- helpers -----------------------------------------------------------------------

    def conn_for(self, msg: dict[str, Any]) -> Conn:
        cid = msg.get("conn")
        if cid is None:
            if len(self.conns) == 1:
                return next(iter(self.conns.values()))
            raise QueryError(f"request has no 'conn' and the worker has {len(self.conns)} connections", "PROTOCOL")
        try:
            return self.conns[cid]
        except KeyError:
            raise QueryError(f"no connection {cid!r} in this worker", "PROTOCOL") from None

    def check_tx(self, wc: Conn, tx: str | None) -> None:
        if tx is None or tx == wc.tx:
            return
        if tx in wc.expired:
            raise TransactionExpired(f"transaction {tx} passed its deadline and was rolled back")
        raise TransactionClosed(f"transaction {tx} is not open")

    @staticmethod
    def _expired_msg() -> str:
        return "transaction passed its deadline and was rolled back by the worker"

    def _time_limit_backstop(self, job: Job, time_limit_ms: int | None) -> asyncio.TimerHandle | None:
        """Behind the engine's own time limit: cancel the request (which
        interrupts the statement) once the limit has passed."""
        if time_limit_ms is None:
            return None
        # 50 ms behind the limit: engines whose own limit is a timer
        # (DuckDB's interrupt lands within a few ms) should get there first,
        # so the caller sees the engine's error.
        return self.loop.call_later(
            time_limit_ms / 1000 + 0.05,
            lambda: self.current is job and self.cancel_job(job, "time_limit"),
        )

    def reply_for(self, wc: Conn, body: dict[str, Any]) -> dict[str, Any]:
        if wc.in_tx:
            body["in_tx"] = True
        return body

    # -- deadlines -------------------------------------------------------------------------

    def _on_deadline(self, wc: Conn, tx: str) -> None:
        wc.tx_timer = None
        if wc.tx != tx:
            return
        wc.expired.append(tx)
        del wc.expired[:-64]
        job = self.current
        if job is not None and job.wc is wc:
            self.cancel_job(job, "deadline")
        self.loop.create_task(self._expire(wc, tx))

    async def _expire(self, wc: Conn, tx: str) -> None:
        async with wc.lock:
            if wc.tx != tx:
                return
            wc.tx = None
            try:
                await wc.conn.rollback()
            except Exception as e:
                print(f"dblayer.worker: rollback at deadline failed: {e!r}", file=sys.stderr)
            try:
                self.send({"type": "expired", "conn": wc.id, "tx": tx})
            except _ParentGone:
                pass

    def _clear_tx(self, wc: Conn) -> None:
        wc.tx = None
        if wc.tx_timer is not None:
            wc.tx_timer.cancel()
            wc.tx_timer = None

    # -- requests ---------------------------------------------------------------------------

    async def do_open(self, job: Job) -> dict[str, Any]:
        m = job.msg
        write = m.get("mode", "read") == "write"
        engine_name = m.get("engine") or SQLITE_ENGINE
        config = m.get("config") or {}
        path = m["path"]
        key = json.dumps([engine_name, path, config], sort_keys=True, default=str)
        engine = self.engines.get(key)
        if engine is None:
            factory = load_engine(engine_name)
            backend = factory(path, **config)
            if hasattr(backend, "executor"):
                backend.executor = self.executor  # type: ignore[attr-defined]
            engine = self.engines[key] = Engine(key, backend)
        conn = await engine.backend.open(write=write)
        engine.conns += 1
        cid = next(self._conn_ids)
        self.conns[cid] = Conn(cid, engine, conn, write)
        return {"ok": True, "conn": cid}

    async def do_close(self, job: Job, wc: Conn) -> dict[str, Any]:
        await self._close_conn(wc, locked=True)
        return {"ok": True}

    async def do_sleep(self, job: Job) -> dict[str, Any] | None:
        for wc in list(self.conns.values()):
            await self._close_conn(wc)
        self.send({"id": job.id, "ok": True})
        self.exit_requested = True
        return None

    async def _close_conn(self, wc: Conn, *, locked: bool = False) -> None:
        self.conns.pop(wc.id, None)
        for s in [s for s in self.streams.values() if s.wc is wc]:
            await self._close_stream(s)
        self._clear_tx(wc)
        for step in (wc.conn.rollback, wc.conn.close):
            try:
                await step()
            except Exception:
                pass
        engine = wc.engine
        engine.conns -= 1
        if engine.conns == 0:
            self.engines.pop(engine.key, None)
            try:
                await engine.backend.close()
            except Exception:
                pass

    async def do_execute(self, job: Job, wc: Conn) -> dict[str, Any]:
        m = job.msg
        self.check_tx(wc, m.get("tx"))
        params = m.get("params")
        tl = m.get("time_limit_ms")
        timer = self._time_limit_backstop(job, tl)
        try:
            r = await wc.conn.execute(
                m["sql"],
                params if params is not None else (),
                time_limit_ms=tl,
                max_rows=m.get("max_rows"),
                max_result_bytes=m.get("max_result_bytes"),
            )
        finally:
            if timer is not None:
                timer.cancel()
        return self.reply_for(wc, {"result": results_to_wire(r)})

    async def do_execute_many(self, job: Job, wc: Conn) -> dict[str, Any]:
        m = job.msg
        self.check_tx(wc, m.get("tx"))
        r = await wc.conn.execute_many(m["sql"], m["params_seq"])
        return self.reply_for(wc, {"result": {"rowcount": r.rowcount}})

    async def do_batch(self, job: Job, wc: Conn) -> dict[str, Any]:
        m = job.msg
        self.check_tx(wc, m.get("tx"))
        statements = [(s, p if p is not None else ()) for s, p in m["statements"]]
        tl, mr = m.get("time_limit_ms"), m.get("max_rows")
        if isinstance(wc.conn, SupportsBatch):
            results = await wc.conn.execute_batch(statements, time_limit_ms=tl, max_rows=mr)
        else:
            results = [await wc.conn.execute(s, p, time_limit_ms=tl, max_rows=mr) for s, p in statements]
        return self.reply_for(wc, {"result": [results_to_wire(r) for r in results]})

    async def do_begin(self, job: Job, wc: Conn) -> dict[str, Any]:
        m = job.msg
        mode = m.get("mode", "write")
        if wc.tx is not None:
            raise QueryError("cannot start a transaction within a transaction", "SQLITE_ERROR")
        await wc.conn.begin(mode)
        tx = wc.tx = wc.new_tx_id()
        deadline_ms = m.get("deadline_ms")
        if deadline_ms is not None:
            wc.tx_timer = self.loop.call_later(deadline_ms / 1000, self._on_deadline, wc, tx)
        return {"ok": True, "tx": tx}

    async def do_commit(self, job: Job, wc: Conn) -> dict[str, Any]:
        self.check_tx(wc, job.msg.get("tx"))
        # A commit that has started is not cut short by the deadline.
        if wc.tx_timer is not None:
            wc.tx_timer.cancel()
            wc.tx_timer = None
        await wc.conn.commit()
        self._clear_tx(wc)
        return {"ok": True}

    async def do_rollback(self, job: Job, wc: Conn) -> dict[str, Any]:
        tx = job.msg.get("tx")
        if tx is not None and tx in wc.expired and wc.tx != tx:
            wc.expired.remove(tx)
            return {"ok": True, "expired": True}
        self.check_tx(wc, tx)
        await wc.conn.rollback()
        self._clear_tx(wc)
        return {"ok": True}

    async def do_schema(self, job: Job, wc: Conn) -> dict[str, Any]:
        self.check_tx(wc, job.msg.get("tx"))
        return self.reply_for(wc, {"result": schema_to_wire(await wc.conn.schema())})

    async def do_schema_version(self, job: Job, wc: Conn) -> dict[str, Any]:
        self.check_tx(wc, job.msg.get("tx"))
        return self.reply_for(wc, {"result": await wc.conn.schema_version()})

    async def do_op(self, job: Job, wc: Conn) -> dict[str, Any]:
        m = job.msg
        self.check_tx(wc, m.get("tx"))
        name = m["name"]
        if name not in OPERATIONS:
            raise QueryError(f"unknown operation {name!r}", "PROTOCOL")
        args = m.get("args", {})
        result: Any = NotImplemented
        if isinstance(wc.conn, SupportsOperations):
            result = await wc.conn.run_operation(name, args)
        if result is NotImplemented:
            result = await OPERATIONS[name](LocalTx(wc), **args)
        if not isinstance(result, dict):
            result = result.to_dict()
        return self.reply_for(wc, {"result": result})

    # -- streams --------------------------------------------------------------------------

    async def do_stream(self, job: Job, wc: Conn) -> None:
        m = job.msg
        self.check_tx(wc, m.get("tx"))
        params = m.get("params")
        rs = wc.conn.stream(
            m["sql"],
            params if params is not None else (),
            chunk_rows=int(m.get("chunk_rows", 1000)),
            time_limit_ms=m.get("time_limit_ms"),
            max_result_bytes=m.get("max_result_bytes"),
        )
        s = self.streams[job.id] = Stream(job.id, wc, rs, int(m.get("credit", 1)))
        await self._pump(s, job)

    async def do_more(self, job: Job, wc_unused: Conn | None = None) -> None:
        s = self.streams.get(job.msg["stream"])
        if s is None:
            return None  # finished or closed meanwhile
        s.credit += int(job.msg.get("credit", 1))
        await self._pump(s, job)
        return None

    async def do_close_stream(self, job: Job, wc_unused: Conn | None = None) -> dict[str, Any] | None:
        s = self.streams.get(job.msg["stream"])
        if s is None:
            return None  # its last frame was already sent
        await self._close_stream(s)
        return {"end": True, "cancelled": True}

    async def _pump(self, s: Stream, job: Job) -> None:
        while s.credit > 0:
            if job.cancelled:
                raise QueryInterrupted("query cancelled")
            msg: dict[str, Any] = {"id": s.id}
            try:
                chunk = await s.rs.__anext__()
            except StopAsyncIteration:
                chunk = None
            if s.first:
                cols = s.rs.columns or ()
                msg["columns"] = [str.__str__(c) for c in cols]
                types = [getattr(c, "type", None) for c in cols]
                if any(t is not None for t in types):
                    msg["types"] = types
                    s.nested = nested_columns(types)
                s.first = False
            if chunk is None:
                await self._drop_stream(s.id)
                msg["end"] = True
                self.send(msg)
                return
            msg["rows"] = wire_rows(chunk, s.nested)
            s.credit -= 1
            self.send(msg)

    async def _drop_stream(self, sid: Any) -> None:
        s = self.streams.get(sid)
        if s is not None:
            await self._close_stream(s)

    async def _close_stream(self, s: Stream) -> None:
        self.streams.pop(s.id, None)
        try:
            await s.rs.aclose()
        except Exception:
            pass

    # -- test hooks (only with --debug) -------------------------------------------

    async def do_debug(self, job: Job) -> dict[str, Any] | None:
        action = job.msg.get("action")
        if action == "sleep":  # blocks the whole worker; nothing interrupts it
            time.sleep(float(job.msg.get("seconds", 1)))
            return {"ok": True}
        if action == "oversized_frame":
            os.write(self.out_fd, (2**31).to_bytes(4, "big") + b"x" * 16)
            return None
        if action == "truncated_frame":
            os.write(self.out_fd, (100).to_bytes(4, "big") + b'{"id":')
            os._exit(3)
        if action == "garbage":
            os.write(self.out_fd, frame(b"\xff\xfe not a valid payload"))
            return None
        if action == "alloc":  # Python allocation, outside SQLite's heap limit
            blocks = [bytearray(1024 * 1024) for _ in range(int(job.msg["mb"]))]
            return {"ok": True, "allocated_mb": len(blocks)}
        if action == "exit":
            os._exit(int(job.msg.get("code", 1)))
        if action == "print":
            print(job.msg.get("text", ""))  # goes to stderr, not the protocol
            return {"ok": True}
        raise QueryError(f"unknown debug action {action!r}", "PROTOCOL")


class LocalTx:
    """A ``TxLike`` over one worker connection, so a named operation runs
    here with no round trips. Mirrors ``Transaction``: placeholders are
    rewritten the same way, and the schema is cached until a statement that
    may change it."""

    def __init__(self, wc: Conn) -> None:
        self._conn = wc.conn
        self.dialect = wc.engine.backend.dialect
        self.capabilities = wc.engine.backend.capabilities
        self._schema: Schema | None = None

    def _after(self, sql: str) -> None:
        if leading_keyword(sql, self.dialect.flavor) not in _DML:
            self._schema = None

    async def execute(self, sql: str, params: Any = None, **_limits: Any) -> Results:
        d = self.dialect
        native_sql, native_params = rewrite(sql, params, d.paramstyle, d.flavor)
        try:
            return await self._conn.execute(native_sql, native_params, time_limit_ms=None, max_rows=None)
        finally:
            self._after(sql)

    async def execute_many(self, sql: str, params_seq: Iterable[Any]) -> WriteResult:
        d = self.dialect
        rewritten = [rewrite(sql, p, d.paramstyle, d.flavor) for p in params_seq]
        if not rewritten:
            return WriteResult(rowcount=0)
        try:
            return await self._conn.execute_many(rewritten[0][0], [p for _, p in rewritten])
        finally:
            self._after(sql)

    async def schema(self) -> Schema:
        if self._schema is None:
            self._schema = await self._conn.schema()
        return self._schema


def _set_rlimit_as(memory_limit_mb: int) -> bool:
    """Linux only: cap the address space at what is mapped now plus the
    limit. Returns True if applied."""
    if not sys.platform.startswith("linux"):
        return False
    try:
        import resource

        with open("/proc/self/status") as f:
            vm_kb = next(int(line.split()[1]) for line in f if line.startswith("VmSize:"))
        limit = vm_kb * 1024 + memory_limit_mb * 1024 * 1024
        _soft, hard = resource.getrlimit(resource.RLIMIT_AS)
        if hard != resource.RLIM_INFINITY:
            limit = min(limit, hard)
        resource.setrlimit(resource.RLIMIT_AS, (limit, hard))
        return True
    except (OSError, ValueError, StopIteration):
        return False


async def _main(args: argparse.Namespace, in_fd: int, out_fd: int) -> None:
    server = Server(in_fd, out_fd, memory_limit_mb=args.memory_limit_mb, debug=args.debug)
    # Start the executor thread before RLIMIT_AS, so its stack is mapped.
    server.executor.submit(lambda: None).result()
    if args.memory_limit_mb:
        server.rlimit_as = _set_rlimit_as(args.memory_limit_mb)
    await server.serve()


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="python -m dblayer.worker")
    p.add_argument("--memory-limit-mb", type=int, default=None)
    p.add_argument("--debug", action="store_true", help="enable test-only _debug messages")
    args = p.parse_args(argv)
    # The parent's Ctrl-C is not ours: we exit when our stdin closes.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    in_fd = 0
    out_fd = os.dup(1)
    os.dup2(2, 1)  # anything printed goes to stderr, never into the protocol
    sys.stdout = sys.stderr
    asyncio.run(_main(args, in_fd, out_fd))


if __name__ == "__main__":
    main()
