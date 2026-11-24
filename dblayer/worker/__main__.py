"""The worker process: ``python -m dblayer.worker [--memory-limit-mb N]
[--debug]``.

Runs the in-process SQLite backend (``dblayer.backends.sqlite``) against
database files on behalf of a parent process, speaking the protocol in
``dblayer.worker.protocol`` over stdin (requests) and stdout (replies).
Stray output from init hooks is redirected to stderr, so it cannot corrupt
the protocol stream.

Threads
-------
* The **main thread** executes one request at a time, in arrival order.
  It owns every sqlite3 connection and writes every reply.
* The **reader thread** reads and decodes frames. It queues requests for the
  main thread and acts at once on control messages: ``cancel`` interrupts
  the running statement (or marks a queued request cancelled), ``more``
  queues a stream continuation. It also wakes every 10 ms while a statement
  with a deadline runs and calls ``interrupt()`` once the deadline has
  passed: the second line of defence after the progress handler. On end of
  input it interrupts whatever runs and the main thread rolls back, closes
  every connection and exits.

Deadlines
---------
Each open transaction's deadline is tracked here, independently of the
parent: the main thread wakes for the earliest one, rolls the transaction
back and sends an ``expired`` notice; a statement running at the deadline
is interrupted (progress handler and reader thread) and fails with
``TransactionExpired``. Later requests naming that transaction get
``TransactionExpired``; a ``rollback`` of it succeeds.

Memory
------
``--memory-limit-mb`` sets ``PRAGMA hard_heap_limit`` (SQLite's own
allocations, every platform) and, on Linux, ``RLIMIT_AS`` (the whole
process, counted from the address space in use once startup is done)
before any database is opened. A Python ``MemoryError`` while serving a
request is reported as ``ResourceLimitExceeded`` with ``fatal: true``, and
the worker then exits, since its state can no longer be trusted.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import itertools
import math
import os
import queue
import select
import signal
import sqlite3
import sys
import threading
import time
from collections.abc import Callable, Iterable, Sequence
from typing import Any

from ..backends.sqlite import SqliteConnection, SqliteInProcess, _TimeLimit
from ..core.operations import OPERATIONS
from ..core.params import leading_keyword, rewrite
from ..core.results import Results, WriteResult
from ..core.schema import Schema
from ..errors import (
    DblayerError,
    QueryError,
    QueryInterrupted,
    ResourceLimitExceeded,
    TransactionClosed,
    TransactionExpired,
)
from .protocol import (
    DEFAULT_MAX_FRAME_BYTES,
    PROTOCOL_VERSION,
    FrameDecoder,
    FrameTooLarge,
    available_codecs,
    frame,
    get_codec,
    results_to_wire,
    schema_to_wire,
)

_DIALECT = SqliteInProcess.dialect
_DML = frozenset({"SELECT", "INSERT", "UPDATE", "DELETE", "REPLACE", "VALUES"})
_TICK = 0.01
_BUILTIN_ERRORS = (OverflowError, ValueError, TypeError, KeyError, IndexError)

SQL_LIMITS = {
    name[len("SQLITE_LIMIT_") :].lower(): getattr(sqlite3, name)
    for name in dir(sqlite3)
    if name.startswith("SQLITE_LIMIT_")
}
"""``open`` ``limits.sql`` keys (``length``, ``sql_length``, ``expr_depth``,
``like_pattern_length``...) and their ``sqlite3.SQLITE_LIMIT_*`` constant."""


class _ParentGone(Exception):
    pass


class _Eof:
    pass


_EOF = _Eof()


class Job:
    __slots__ = ("id", "msg", "type", "cancelled", "deadline", "raw")

    def __init__(self, msg: dict[str, Any], type_: str | None = None) -> None:
        self.msg = msg
        self.id = msg.get("id")
        self.type = type_ or msg.get("type")
        self.cancelled = False
        self.deadline: float | None = None
        """Monotonic time after which the reader thread interrupts."""
        self.raw: sqlite3.Connection | None = None
        """Connection the running statement uses (set by the main thread)."""


class Conn:
    """One database connection inside the worker."""

    __slots__ = ("id", "backend", "conn", "write", "tx", "tx_deadline", "expired", "_tx_ids")

    def __init__(self, cid: int, backend: SqliteInProcess, conn: SqliteConnection, write: bool) -> None:
        self.id = cid
        self.backend = backend
        self.conn = conn
        self.write = write
        self.tx: str | None = None
        self.tx_deadline: float | None = None
        self.expired: set[str] = set()
        self._tx_ids = itertools.count(1)

    @property
    def raw(self) -> sqlite3.Connection:
        return self.conn._raw

    def new_tx_id(self) -> str:
        return f"t{next(self._tx_ids)}"


class Stream:
    __slots__ = ("id", "wc", "rs", "credit", "first")

    def __init__(self, sid: Any, wc: Conn, rs: Any, credit: int) -> None:
        self.id = sid
        self.wc = wc
        self.rs = rs
        self.credit = credit
        self.first = True


class Server:
    def __init__(self, in_fd: int, out_fd: int, *, memory_limit_mb: int | None, debug: bool) -> None:
        self.in_fd = in_fd
        self.out_fd = out_fd
        self.memory_limit_mb = memory_limit_mb
        self.rlimit_as = False
        self.debug = debug
        self.codec = get_codec("json")
        self.max_frame_bytes = DEFAULT_MAX_FRAME_BYTES
        self.q: queue.SimpleQueue[Job | _Eof] = queue.SimpleQueue()
        self.lock = threading.Lock()
        self.write_lock = threading.Lock()
        self.current: Job | None = None
        self.pending: dict[Any, Job] = {}
        self.stream_ids: set[Any] = set()  # mirrors self.streams, for the reader
        self.streams: dict[Any, Stream] = {}
        self.conns: dict[int, Conn] = {}
        self._conn_ids = itertools.count(1)
        self.eof = False
        self.exit_requested = False
        self.loop = asyncio.new_event_loop()
        self.heap_limit_set = False
        self.ready = threading.Event()
        """Set by the main thread once start-up (``RLIMIT_AS``) is done."""

    # -- output ------------------------------------------------------------------

    def send(self, msg: dict[str, Any]) -> None:
        try:
            data = frame(self.codec.encode(msg), self.max_frame_bytes)
        except FrameTooLarge as e:
            if "id" not in msg:
                raise
            data = frame(
                self.codec.encode(
                    {
                        "id": msg["id"],
                        "error": {
                            "type": "ResourceLimitExceeded",
                            "message": f"reply too large: {e}",
                        },
                    }
                )
            )
        view = memoryview(data)
        with self.write_lock:
            while view:
                try:
                    n = os.write(self.out_fd, view)
                except BrokenPipeError:
                    raise _ParentGone() from None
                view = view[n:]

    # -- reader thread -------------------------------------------------------------

    def reader(self) -> None:
        self.ready.wait()
        dec = FrameDecoder(self.max_frame_bytes)
        try:
            while True:
                wait = None
                with self.lock:
                    job = self.current
                    if job is not None and (job.cancelled or job.deadline is not None):
                        wait = _TICK
                r, _, _ = select.select([self.in_fd], [], [], wait)
                if r:
                    try:
                        data = os.read(self.in_fd, 1 << 16)
                    except OSError:
                        data = b""
                    if not data:
                        break
                    try:
                        frames = dec.feed(data)
                    except FrameTooLarge as e:
                        print(f"dblayer.worker: {e}; dropping the connection", file=sys.stderr)
                        break
                    for f in frames:
                        self.dispatch(self.codec.decode(f))
                self.check_current()
        except Exception as e:  # undecodable input: same as a dropped connection
            print(f"dblayer.worker: reader failed: {e!r}", file=sys.stderr)
        self.on_eof()

    def dispatch(self, msg: dict[str, Any]) -> None:
        t = msg.get("type")
        if t == "hello":
            self.hello(msg)
            return
        if t == "cancel":
            target = msg.get("target")
            with self.lock:
                job = self.current
                if job is not None and job.id == target:
                    job.cancelled = True
                    self._interrupt(job)
                    return
                job = self.pending.get(target)
                if job is not None:
                    job.cancelled = True
                    return
                if target in self.stream_ids:
                    j = Job({"id": target, "stream": target}, "_close_stream")
                    self.pending[target] = j
                    self.q.put(j)
            return
        if t == "more":
            sid = msg.get("stream")
            j = Job({"id": sid, "stream": sid, "credit": msg.get("credit", 1)}, "_more")
            with self.lock:
                self.pending[sid] = j
            self.q.put(j)
            return
        job = Job(msg)
        with self.lock:
            self.pending[job.id] = job
        self.q.put(job)

    def check_current(self) -> None:
        with self.lock:
            job = self.current
            if job is None:
                return
            if job.cancelled or (job.deadline is not None and time.monotonic() >= job.deadline):
                self._interrupt(job)

    def _interrupt(self, job: Job) -> None:
        """Lock held. ``job.raw`` is cleared (under the lock) as soon as a
        statement returns, so this never hits a later statement."""
        raw = job.raw
        if raw is not None:
            try:
                raw.interrupt()
            except Exception:
                pass

    def on_eof(self) -> None:
        with self.lock:
            self.eof = True
            for job in self.pending.values():
                job.cancelled = True
        self.q.put(_EOF)
        # Keep interrupting whatever runs until the main thread exits.
        while True:
            with self.lock:
                job = self.current
                if job is not None:
                    job.cancelled = True
                    self._interrupt(job)
            time.sleep(0.02)

    def hello(self, msg: dict[str, Any]) -> None:
        wanted = msg.get("codecs") or ["json"]
        have = available_codecs()
        codec = next((c for c in wanted if c in have), "json")
        reply = {
            "id": msg.get("id"),
            "type": "hello",
            "protocol": PROTOCOL_VERSION,
            "codec": codec,
            "sqlite_version": sqlite3.sqlite_version,
            "python": sys.version.split()[0],
            "pid": os.getpid(),
            "rlimit_as": self.rlimit_as,
        }
        if "max_frame_bytes" in msg:
            self.max_frame_bytes = int(msg["max_frame_bytes"])
        self.send(reply)  # in JSON; everything after it in the agreed codec
        self.codec = get_codec(codec)

    # -- main thread -----------------------------------------------------------------

    def serve(self) -> None:
        try:
            while not self.exit_requested:
                timeout = self._next_deadline()
                try:
                    job = self.q.get(timeout=timeout) if timeout is not None else self.q.get()
                except queue.Empty:
                    self.expire_due()
                    continue
                if isinstance(job, _Eof):
                    break
                self.expire_due()
                self.run(job)
        except _ParentGone:
            pass
        finally:
            self.shutdown()

    def _next_deadline(self) -> float | None:
        ds = [c.tx_deadline for c in self.conns.values() if c.tx_deadline is not None]
        if not ds:
            return None
        return max(min(ds) - time.monotonic(), 0.0)

    def run(self, job: Job) -> None:
        with self.lock:
            if self.pending.get(job.id) is job:
                del self.pending[job.id]
            self.current = job
        try:
            if job.cancelled:
                raise QueryInterrupted("query cancelled")
            handler = getattr(self, "do_" + str(job.type).lstrip("_"), None)
            if handler is None or (job.type == "_debug" and not self.debug):
                raise QueryError(f"unknown message type {job.type!r}", "PROTOCOL")
            reply = handler(job)
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
                    "error": {
                        "type": "ResourceLimitExceeded",
                        "message": "worker ran out of memory (memory limit)",
                    },
                    "fatal": True,
                }
            )
        except BaseException as e:
            err = self.error_payload(e, job)
            if isinstance(e, SystemExit):
                raise
            reply: dict[str, Any] = {"id": job.id, "error": err}
            wc = self.conns.get(job.msg.get("conn")) if "conn" in job.msg else (
                next(iter(self.conns.values())) if len(self.conns) == 1 else None
            )
            if wc is not None and wc.raw.in_transaction:
                reply["in_tx"] = True
            self.send(reply)
            if job.type in ("stream", "_more"):
                self._drop_stream(job.id)
        finally:
            with self.lock:
                self.current = None
                job.raw = None
                job.deadline = None

    def error_payload(self, e: BaseException, job: Job) -> dict[str, Any]:
        cause: BaseException | None = None
        if isinstance(e, sqlite3.Error):
            mapped = _DIALECT.map_exception(e)
            cause = e
            if isinstance(mapped, QueryInterrupted) and job.cancelled:
                mapped = QueryInterrupted("query cancelled")
        elif isinstance(e, DblayerError):
            mapped = e
            cause = e.__cause__
        elif isinstance(e, _BUILTIN_ERRORS):
            return {"type": type(e).__name__, "message": str(e), "builtin": True}
        else:
            mapped = QueryError(f"worker error: {type(e).__name__}: {e}", "WORKER")
            cause = e
        assert mapped is not None
        err: dict[str, Any] = {"type": type(mapped).__name__, "message": str(mapped)}
        if isinstance(mapped, QueryError) and mapped.code is not None:
            err["code"] = mapped.code
        if cause is not None:
            err["cause"] = {
                "type": f"{type(cause).__module__}.{type(cause).__qualname__}",
                "message": str(cause),
            }
        return err

    def shutdown(self) -> None:
        for s in list(self.streams.values()):
            self._close_stream(s)
        for wc in list(self.conns.values()):
            self._close_conn(wc)
        os._exit(0)

    # -- helpers ---------------------------------------------------------------------

    def conn_for(self, msg: dict[str, Any]) -> Conn:
        cid = msg.get("conn")
        if cid is None:
            if len(self.conns) == 1:
                return next(iter(self.conns.values()))
            raise QueryError("request has no 'conn' and the worker has "
                             f"{len(self.conns)} connections", "PROTOCOL")
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

    def call[T](self, wc: Conn, job: Job, time_limit_ms: int | None, fn: Callable[[], T]) -> T:
        """Run one statement's worth of work with the statement time limit
        and the transaction deadline both enforced (progress handler, plus
        the reader thread's interrupt)."""
        if job.cancelled:
            raise QueryInterrupted("query cancelled")
        now = time.monotonic()
        limit = None if time_limit_ms is None else time_limit_ms / 1000
        txd = wc.tx_deadline if wc.tx is not None else None
        by_deadline = False
        if txd is not None:
            rem = txd - now
            if rem <= 0:
                self.expire(wc, notify=False)
                raise TransactionExpired(self._expired_msg())
            if limit is None or rem < limit:
                limit, by_deadline = rem, True
        ms = None if limit is None else max(int(math.ceil(limit * 1000)), 0)
        with self.lock:
            job.raw = wc.raw
            job.deadline = None if limit is None else now + limit
        try:
            with _TimeLimit(wc.raw, ms):
                return fn()
        except sqlite3.OperationalError as e:
            if _DIALECT.map_exception(e).__class__ is QueryInterrupted:
                if job.cancelled:
                    raise QueryInterrupted("query cancelled") from e
                if by_deadline and time.monotonic() >= txd - 0.002:  # type: ignore[operator]
                    self.expire(wc, notify=False)
                    raise TransactionExpired(self._expired_msg()) from e
            raise
        finally:
            with self.lock:
                job.raw = None
                job.deadline = None

    @staticmethod
    def _expired_msg() -> str:
        return "transaction passed its deadline and was rolled back by the worker"

    def expire(self, wc: Conn, *, notify: bool) -> None:
        tx = wc.tx
        try:
            if wc.raw.in_transaction:
                wc.raw.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        if tx is not None:
            wc.expired.add(tx)
            if len(wc.expired) > 64:
                wc.expired.pop()
        wc.tx = None
        wc.tx_deadline = None
        if notify and tx is not None:
            self.send({"type": "expired", "conn": wc.id, "tx": tx})

    def expire_due(self) -> None:
        now = time.monotonic()
        for wc in list(self.conns.values()):
            if wc.tx is not None and wc.tx_deadline is not None and wc.tx_deadline <= now:
                self.expire(wc, notify=True)

    def reply_for(self, wc: Conn, body: dict[str, Any]) -> dict[str, Any]:
        if wc.raw.in_transaction:
            body["in_tx"] = True
        return body

    # -- requests ---------------------------------------------------------------------

    def do_open(self, job: Job) -> dict[str, Any]:
        m = job.msg
        write = m.get("mode", "read") == "write"
        limits = m.get("limits") or {}
        opts = m.get("options") or {}
        hooks = [_import_hook(name) for name in m.get("init") or ()]
        sql_limits = limits.get("sql") or {}
        for name in sql_limits:
            if name not in SQL_LIMITS:
                raise QueryError(f"unknown SQL limit {name!r}", "PROTOCOL")

        def init(raw: sqlite3.Connection) -> None:
            for name, value in sql_limits.items():
                raw.setlimit(SQL_LIMITS[name], int(value))
            for hook in hooks:
                hook(raw)

        backend = SqliteInProcess(
            m["path"],
            immutable=bool(opts.get("immutable", False)),
            create=bool(opts.get("create", True)),
            busy_timeout=float(opts.get("busy_timeout", 5.0)),
            init=init,
        )
        if write and backend.immutable:
            from ..errors import NotSupported

            raise NotSupported("database is immutable; writes are not allowed")
        raw = backend._connect(write)
        if self.memory_limit_mb and not self.heap_limit_set:
            raw.execute(f"pragma hard_heap_limit = {int(self.memory_limit_mb) * 1024 * 1024}")
            self.heap_limit_set = True
        cid = next(self._conn_ids)
        self.conns[cid] = Conn(cid, backend, SqliteConnection(backend, raw, write=write), write)
        return {"ok": True, "conn": cid}

    def do_close(self, job: Job) -> dict[str, Any]:
        wc = self.conn_for(job.msg)
        self._close_conn(wc)
        return {"ok": True}

    def do_sleep(self, job: Job) -> dict[str, Any] | None:
        for wc in list(self.conns.values()):
            self._close_conn(wc)
        self.send({"id": job.id, "ok": True})
        self.exit_requested = True
        return None

    def _close_conn(self, wc: Conn) -> None:
        for s in [s for s in self.streams.values() if s.wc is wc]:
            self._close_stream(s)
        try:
            if wc.raw.in_transaction:
                wc.raw.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        try:
            wc.raw.close()
        except sqlite3.Error:
            pass
        self.conns.pop(wc.id, None)

    def do_execute(self, job: Job) -> dict[str, Any]:
        m = job.msg
        wc = self.conn_for(m)
        self.check_tx(wc, m.get("tx"))
        params = m.get("params")
        if params is None:
            params = ()
        r = self.call(
            wc, job, m.get("time_limit_ms"),
            lambda: wc.conn._execute(m["sql"], params, None, m.get("max_rows"), m.get("max_result_bytes")),
        )
        return self.reply_for(wc, {"result": results_to_wire(r)})

    def do_execute_many(self, job: Job) -> dict[str, Any]:
        m = job.msg
        wc = self.conn_for(m)
        self.check_tx(wc, m.get("tx"))
        r = self.call(wc, job, m.get("time_limit_ms"), lambda: _execute_many(wc.raw, m["sql"], m["params_seq"]))
        return self.reply_for(wc, {"result": {"rowcount": r.rowcount}})

    def do_batch(self, job: Job) -> dict[str, Any]:
        m = job.msg
        wc = self.conn_for(m)
        self.check_tx(wc, m.get("tx"))
        out = []
        for sql, params in m["statements"]:
            r = self.call(
                wc, job, m.get("time_limit_ms"),
                lambda: wc.conn._execute(sql, params if params is not None else (), None, m.get("max_rows"), None),
            )
            out.append(results_to_wire(r))
        return self.reply_for(wc, {"result": out})

    def do_begin(self, job: Job) -> dict[str, Any]:
        m = job.msg
        wc = self.conn_for(m)
        mode = m.get("mode", "write")
        if wc.tx is not None or wc.raw.in_transaction:
            raise QueryError("cannot start a transaction within a transaction", "SQLITE_ERROR")
        sql = {"write": "BEGIN IMMEDIATE", "read": "BEGIN"}.get(mode)
        if sql is None:
            raise ValueError(f"unknown transaction mode {mode!r}")
        self.call(wc, job, None, lambda: wc.raw.execute(sql))
        wc.tx = wc.new_tx_id()
        deadline_ms = m.get("deadline_ms")
        wc.tx_deadline = None if deadline_ms is None else time.monotonic() + deadline_ms / 1000
        return {"ok": True, "tx": wc.tx}

    def do_commit(self, job: Job) -> dict[str, Any]:
        m = job.msg
        wc = self.conn_for(m)
        tx = m.get("tx")
        self.check_tx(wc, tx)
        if wc.raw.in_transaction:
            # Never interrupted by the deadline: a commit that started runs.
            wc.raw.execute("COMMIT")
        wc.tx = None
        wc.tx_deadline = None
        return {"ok": True}

    def do_rollback(self, job: Job) -> dict[str, Any]:
        m = job.msg
        wc = self.conn_for(m)
        tx = m.get("tx")
        if tx is not None and tx in wc.expired:
            wc.expired.discard(tx)
            return {"ok": True, "expired": True}
        self.check_tx(wc, tx)
        if wc.raw.in_transaction:
            wc.raw.execute("ROLLBACK")
        wc.tx = None
        wc.tx_deadline = None
        return {"ok": True}

    def do_schema(self, job: Job) -> dict[str, Any]:
        m = job.msg
        wc = self.conn_for(m)
        self.check_tx(wc, m.get("tx"))
        s = self.call(wc, job, m.get("time_limit_ms"), wc.conn._schema)
        return self.reply_for(wc, {"result": schema_to_wire(s)})

    def do_schema_version(self, job: Job) -> dict[str, Any]:
        m = job.msg
        wc = self.conn_for(m)
        self.check_tx(wc, m.get("tx"))
        v = self.call(wc, job, None, lambda: wc.raw.execute("pragma schema_version").fetchone()[0])
        return self.reply_for(wc, {"result": v})

    def do_op(self, job: Job) -> dict[str, Any]:
        m = job.msg
        wc = self.conn_for(m)
        self.check_tx(wc, m.get("tx"))
        name = m["name"]
        try:
            fn = OPERATIONS[name]
        except KeyError:
            raise QueryError(f"unknown operation {name!r}", "PROTOCOL") from None
        tx = LocalTx(self, wc, job)
        result = self.loop.run_until_complete(fn(tx, **m.get("args", {})))
        return self.reply_for(wc, {"result": result.to_dict()})

    # -- streams --------------------------------------------------------------------

    def do_stream(self, job: Job) -> None:
        m = job.msg
        wc = self.conn_for(m)
        self.check_tx(wc, m.get("tx"))
        params = m.get("params")
        rs = wc.conn.stream(
            m["sql"],
            params if params is not None else (),
            chunk_rows=int(m.get("chunk_rows", 1000)),
            time_limit_ms=m.get("time_limit_ms"),
            max_result_bytes=m.get("max_result_bytes"),
        )
        s = Stream(job.id, wc, rs, int(m.get("credit", 1)))
        self.streams[job.id] = s
        with self.lock:
            self.stream_ids.add(job.id)
        self._pump(s, job)

    def do_more(self, job: Job) -> None:
        s = self.streams.get(job.msg["stream"])
        if s is None:
            return  # finished or closed meanwhile
        s.credit += int(job.msg.get("credit", 1))
        self._pump(s, job)

    def do_close_stream(self, job: Job) -> dict[str, Any] | None:
        s = self.streams.get(job.msg["stream"])
        if s is None:
            return None  # its last frame was already sent
        self._close_stream(s)
        return {"end": True, "cancelled": True}

    def _pump(self, s: Stream, job: Job) -> None:
        rs = s.rs
        while s.credit > 0:
            if job.cancelled:
                raise QueryInterrupted("query cancelled")
            rem = rs._remaining_ms
            with self.lock:
                job.raw = s.wc.raw
                job.deadline = None if rem is None else time.monotonic() + max(rem, 0) / 1000
            try:
                chunk = rs._next_chunk()
            except sqlite3.OperationalError as e:
                if job.cancelled and isinstance(_DIALECT.map_exception(e), QueryInterrupted):
                    raise QueryInterrupted("query cancelled") from e
                raise
            finally:
                with self.lock:
                    job.raw = None
                    job.deadline = None
            msg: dict[str, Any] = {"id": s.id}
            if s.first:
                msg["columns"] = [str.__str__(c) for c in rs.columns or ()]
                s.first = False
            if chunk is None:
                self._drop_stream(s.id)
                msg["end"] = True
                self.send(msg)
                return
            msg["rows"] = chunk
            s.credit -= 1
            self.send(msg)

    def _drop_stream(self, sid: Any) -> None:
        s = self.streams.get(sid)
        if s is not None:
            self._close_stream(s)

    def _close_stream(self, s: Stream) -> None:
        self.streams.pop(s.id, None)
        with self.lock:
            self.stream_ids.discard(s.id)
        cur, s.rs._cur = s.rs._cur, None
        s.rs._done = True
        if cur is not None:
            try:
                cur.close()
            except sqlite3.Error:
                pass

    # -- test hooks (only with --debug) -------------------------------------------

    def do_debug(self, job: Job) -> dict[str, Any] | None:
        action = job.msg.get("action")
        if action == "sleep":  # holds the main thread; nothing interrupts it
            time.sleep(float(job.msg.get("seconds", 1)))
            return {"ok": True}
        if action == "oversized_frame":
            with self.write_lock:
                os.write(self.out_fd, (2**31).to_bytes(4, "big") + b"x" * 16)
            return None
        if action == "truncated_frame":
            with self.write_lock:
                os.write(self.out_fd, (100).to_bytes(4, "big") + b"{\"id\":")
            os._exit(3)
        if action == "garbage":
            with self.write_lock:
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
    rewritten the same way and the schema is cached until a statement that
    may change it."""

    def __init__(self, server: Server, wc: Conn, job: Job) -> None:
        self._server = server
        self._wc = wc
        self._job = job
        self._schema: Schema | None = None
        self.dialect = _DIALECT
        self.capabilities = wc.backend.capabilities

    def _after(self, sql: str) -> None:
        if leading_keyword(sql, _DIALECT.flavor) not in _DML:
            self._schema = None

    async def execute(self, sql: str, params: Any = None, **_limits: Any) -> Results:
        native_sql, native_params = rewrite(sql, params, _DIALECT.paramstyle, _DIALECT.flavor)
        wc = self._wc
        try:
            return self._server.call(
                wc, self._job, None, lambda: wc.conn._execute(native_sql, native_params, None, None, None)
            )
        finally:
            self._after(sql)

    async def execute_many(self, sql: str, params_seq: Iterable[Any]) -> WriteResult:
        rewritten = [rewrite(sql, p, _DIALECT.paramstyle, _DIALECT.flavor) for p in params_seq]
        if not rewritten:
            return WriteResult(rowcount=0)
        wc = self._wc
        try:
            return self._server.call(
                wc, self._job, None,
                lambda: _execute_many(wc.raw, rewritten[0][0], [p for _, p in rewritten]),
            )
        finally:
            self._after(sql)

    async def schema(self) -> Schema:
        if self._schema is None:
            self._schema = self._server.call(self._wc, self._job, None, self._wc.conn._schema)
        return self._schema


def _execute_many(raw: sqlite3.Connection, sql: str, params_seq: Sequence[Any]) -> WriteResult:
    cur = raw.executemany(sql, params_seq)
    try:
        return WriteResult(rowcount=cur.rowcount)
    finally:
        cur.close()


def _import_hook(name: str) -> Callable[[sqlite3.Connection], None]:
    module, sep, attr = name.partition(":")
    if not sep or not module or not attr:
        raise QueryError(f"init hook {name!r} must be 'module:function'", "INIT")
    try:
        obj: Any = importlib.import_module(module)
        for part in attr.split("."):
            obj = getattr(obj, part)
    except (ImportError, AttributeError) as e:
        raise QueryError(f"cannot import init hook {name!r}: {e}", "INIT") from e
    if not callable(obj):
        raise QueryError(f"init hook {name!r} is not callable", "INIT")
    return obj


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


def main(argv: Sequence[str] | None = None) -> None:
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
    server = Server(in_fd, out_fd, memory_limit_mb=args.memory_limit_mb, debug=args.debug)
    reader = threading.Thread(target=server.reader, name="dblayer-worker-reader", daemon=True)
    reader.start()
    if args.memory_limit_mb:
        server.rlimit_as = _set_rlimit_as(args.memory_limit_mb)
    server.ready.set()
    server.serve()


if __name__ == "__main__":
    main()
