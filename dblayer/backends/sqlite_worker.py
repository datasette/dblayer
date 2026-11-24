"""SQLite in a separate worker process: the parent side.

``SqliteWorker(path)`` is a ``Backend`` whose connections live in child
processes running ``python -m dblayer.worker``, which in turn runs the
in-process SQLite backend against the file. Its purpose is to make untrusted
SQL safe to run: a worker can be given a memory limit and killed when a query
will not stop.

Process model
-------------
* ``placement="dedicated"`` (default): every pooled connection *is* a worker
  process, so a database has at most one write worker and up to
  ``max_read_connections`` read workers, started on demand by the core pool,
  closed by its idle timeout and the global cap like any connection (each one
  is one place under the cap). An asleep database has no processes. An
  interactive transaction pins its connection, hence its worker.
* ``placement="shared"`` with a ``SharedWorkers`` group: a fixed set of
  processes serves many databases, one request at a time per process (each
  pooled connection is a connection *inside* one of them). Start-up is paid
  once per process instead of once per connection; the price is that a slow
  query delays every database on its process, and a kill (time limit, memory)
  takes every connection on it down with it.

Transport
---------
``subprocess.Popen`` with three pipes, read with ``loop.add_reader`` on
non-blocking descriptors. Not ``asyncio.create_subprocess_exec``, as the spec
suggests: on macOS (and Linux without pidfd) asyncio waits for each child on
a thread of its own, which would make the thread count grow with the number
of workers, and asyncio subprocess transports cannot be closed from another
event loop. Here the parent uses no thread at all, and children are reaped
with ``waitpid(WNOHANG)`` polls.

Limits and kill
---------------
* Time: the worker enforces ``time_limit_ms`` with a progress handler and
  ``interrupt()`` from its reader thread. The parent arms a timer per request
  at ``time_limit_ms + kill_grace_ms`` (250 ms by default) and SIGKILLs the
  worker if no reply came; the call raises ``QueryInterrupted``, any other
  call on that process ``WorkerCrashed``. The pool replaces the worker on the
  next request.
* Memory: ``memory_limit_mb`` sets ``PRAGMA hard_heap_limit`` in the worker
  and, on Linux, ``RLIMIT_AS`` (capability ``memory_limit`` is true only
  there).
* Result size: ``max_result_bytes`` stops fetching in the worker; the
  parent's reader refuses any frame over ``max_frame_bytes`` (kills the
  worker, ``ResourceLimitExceeded``).
* SQL complexity: ``sql_limits={"length": ..., "expr_depth": ...}`` is
  applied with ``Connection.setlimit()`` in the worker.

Deadlines
---------
``begin_with_deadline`` sends the deadline to the worker, which rolls back
on its own when it passes. The parent also arms a timer at the deadline plus
``kill_grace_ms`` plus ``deadline_slack_ms``; if the worker has not confirmed
the end of the transaction by then (a commit/rollback reply, or its
``expired`` notice), the parent kills it.
"""

from __future__ import annotations

import asyncio
import atexit
import collections
import itertools
import logging
import os
import signal
import subprocess
import sys
import time
import weakref
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Literal

from ..core.results import Column, Results, WriteResult
from ..core.schema import Schema
from ..errors import (
    DatabaseClosed,
    DblayerError,
    IntegrityError,
    NestedWriteError,
    NotSupported,
    ParameterError,
    QueryError,
    QueryInterrupted,
    ResourceLimitExceeded,
    SchemaChanged,
    TableNotFound,
    TransactionClosed,
    TransactionExpired,
    WorkerCrashed,
    WriteConflict,
)
from ..worker.protocol import (
    DEFAULT_MAX_FRAME_BYTES,
    PROTOCOL_VERSION,
    FrameDecoder,
    FrameTooLarge,
    available_codecs,
    frame,
    get_codec,
    results_from_wire,
    schema_from_wire,
)
from .base import Capabilities, Dialect, NativeParams
from .sqlite import SqliteInProcess, _stat

__all__ = [
    "SqliteWorker",
    "SharedWorkers",
    "WorkerConnection",
    "WorkerError",
    "WorkerConfig",
    "live_worker_pids",
    "reap_workers",
]

logger = logging.getLogger("dblayer.worker")

DEFAULT_KILL_GRACE_MS = 250
DEFAULT_DEADLINE_SLACK_MS = 250
_STDERR_TAIL = 8192
_PACKAGE_PARENT = str(Path(__file__).resolve().parent.parent.parent)

_EXCEPTIONS: dict[str, type[DblayerError]] = {
    c.__name__: c
    for c in (
        QueryError,
        IntegrityError,
        ParameterError,
        TableNotFound,
        QueryInterrupted,
        ResourceLimitExceeded,
        TransactionClosed,
        TransactionExpired,
        WriteConflict,
        NestedWriteError,
        WorkerCrashed,
        NotSupported,
        DatabaseClosed,
        SchemaChanged,
    )
}
_BUILTINS: dict[str, type[Exception]] = {
    c.__name__: c for c in (OverflowError, ValueError, TypeError, KeyError, IndexError)
}


class WorkerError(Exception):
    """Stands in, as ``__cause__``, for the driver exception raised inside
    the worker (which cannot cross the process boundary)."""


def error_from_wire(err: Mapping[str, Any]) -> BaseException:
    """The exception an error reply describes."""
    name = err.get("type", "QueryError")
    message = err.get("message", "")
    if err.get("builtin") and name in _BUILTINS:
        return _BUILTINS[name](message)
    cls = _EXCEPTIONS.get(name, QueryError)
    exc: DblayerError
    if issubclass(cls, QueryError):
        exc = cls(message, err.get("code"))
    else:
        exc = cls(message)
    cause = err.get("cause")
    if cause is not None:
        exc.__cause__ = WorkerError(f"{cause.get('type')}: {cause.get('message')}")
    return exc


@dataclass(frozen=True, slots=True)
class WorkerConfig:
    """How worker processes are started. Shared by every backend (or
    group) that uses the same settings."""

    codec: str = "json"
    memory_limit_mb: int | None = None
    kill_grace_ms: int = DEFAULT_KILL_GRACE_MS
    deadline_slack_ms: int = DEFAULT_DEADLINE_SLACK_MS
    max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES
    python: str = sys.executable
    start_timeout: float = 30.0
    debug: bool = False
    """Enables the worker's test-only ``_debug`` messages."""

    def argv(self) -> list[str]:
        args = [self.python, "-m", "dblayer.worker"]
        if self.memory_limit_mb:
            args += ["--memory-limit-mb", str(int(self.memory_limit_mb))]
        if self.debug:
            args.append("--debug")
        return args


# -- process bookkeeping ---------------------------------------------------------

_unreaped: set[subprocess.Popen[bytes]] = set()


def live_worker_pids() -> list[int]:
    """Pids of worker processes started by this process that have not
    exited (or not been reaped) yet."""
    return [p.pid for p in list(_unreaped) if p.poll() is None]


def reap_workers(timeout: float = 5.0) -> list[int]:
    """Wait (blocking) up to ``timeout`` seconds for every worker to exit,
    then SIGKILL the rest. Returns the pids that had to be killed. For tests
    and interpreter exit."""
    end = time.monotonic() + timeout
    killed = []
    for p in list(_unreaped):
        try:
            p.wait(max(end - time.monotonic(), 0))
        except subprocess.TimeoutExpired:
            killed.append(p.pid)
            p.kill()
            p.wait()
        _unreaped.discard(p)
    return killed


def _poll(p: subprocess.Popen[bytes]) -> bool:
    if p.poll() is not None:
        _unreaped.discard(p)
        return True
    return False


def _reap_later(p: subprocess.Popen[bytes], grace: float) -> None:
    """Reap ``p`` without blocking: poll it from the running loop, SIGKILL
    it after ``grace`` seconds. With no running loop, wait for it here."""
    if _poll(p):
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        try:
            p.wait(grace)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait()
        _unreaped.discard(p)
        return
    deadline = time.monotonic() + grace
    delay = [0.002]

    def check() -> None:
        if _poll(p):
            return
        if time.monotonic() >= deadline:
            try:
                p.kill()
            except OSError:
                pass
        if not loop.is_closed():
            delay[0] = min(delay[0] * 2, 0.05)
            loop.call_later(delay[0], check)

    loop.call_later(delay[0], check)


@atexit.register
def _kill_all_at_exit() -> None:
    for p in list(_unreaped):
        if p.poll() is None:
            try:
                p.kill()
                p.wait(1)
            except Exception:
                pass


# -- one worker process --------------------------------------------------------------


class _Waiter:
    """A pending request. ``frames`` collects stream frames until the
    exchange is complete."""

    __slots__ = ("fut", "owner", "expect", "frames")

    def __init__(self, fut: asyncio.Future[Any], owner: object, expect: int | None) -> None:
        self.fut = fut
        self.owner = owner
        self.expect = expect  # stream exchanges: frames to wait for
        self.frames: list[dict[str, Any]] = []


class WorkerProcess:
    """One child process and the parent's end of its pipes. Belongs to the
    event loop that started it. Requests are serialised (FIFO) so that a time
    limit is measured from when the request is actually sent: the worker
    runs one at a time anyway."""

    def __init__(self, config: WorkerConfig) -> None:
        self.config = config
        self.loop = asyncio.get_running_loop()
        self.popen: subprocess.Popen[bytes] | None = None
        self.pid: int | None = None
        self.hello: dict[str, Any] = {}
        self.codec = get_codec("json")
        self.lock = asyncio.Lock()
        self._ids = itertools.count(1)
        self._waiters: dict[int, _Waiter] = {}
        self._dec = FrameDecoder(config.max_frame_bytes)
        self._wbuf = bytearray()
        self._writing = False
        self._fds: tuple[int, int, int] | None = None
        self.dead: BaseException | None = None
        """Set once the process is gone (or being killed): the exception
        calls get from now on."""
        self.stderr_tail = collections.deque[bytes](maxlen=64)
        self._stderr_size = 0
        self.notice_handlers: dict[Any, Callable[[dict[str, Any]], None]] = {}
        self.inflight: tuple[int, object] | None = None
        self.handles = 0
        """Connections open in this process (shared placement)."""
        self.idle_timer: asyncio.TimerHandle | None = None
        self.started_at = 0.0
        self.recorder: list[tuple[str, dict[str, Any]]] | None = WorkerProcess.record_new
        """Test hook: when a list, every frame sent and received is
        appended as ``("->" | "<-", message)``."""

    record_new: list[tuple[str, dict[str, Any]]] | None = None
    """Test hook: processes started while this is a list record into it
    from their first frame (``hello``). Used to capture the example
    session in ``docs/worker-protocol.md``."""

    def __repr__(self) -> str:
        state = "dead" if self.dead else "alive"
        return f"<WorkerProcess pid={self.pid} {state}>"

    # -- start / stop ----------------------------------------------------------

    async def start(self) -> None:
        cfg = self.config
        env = dict(os.environ)
        pp = env.get("PYTHONPATH")
        env["PYTHONPATH"] = _PACKAGE_PARENT + (os.pathsep + pp if pp else "")
        env.setdefault("MALLOC_ARENA_MAX", "2")  # keeps RLIMIT_AS meaningful
        self.started_at = time.perf_counter()
        p = subprocess.Popen(
            cfg.argv(),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            close_fds=True,
        )
        _unreaped.add(p)
        self.popen = p
        self.pid = p.pid
        assert p.stdin and p.stdout and p.stderr
        fds = (p.stdin.fileno(), p.stdout.fileno(), p.stderr.fileno())
        for fd in fds:
            os.set_blocking(fd, False)
        self._fds = fds
        self.loop.add_reader(fds[1], self._on_stdout)
        self.loop.add_reader(fds[2], self._on_stderr)
        codecs = [cfg.codec] if cfg.codec == "json" else [cfg.codec, "json"]
        try:
            async with asyncio.timeout(cfg.start_timeout):
                reply = await self.call(
                    {
                        "type": "hello",
                        "protocol": PROTOCOL_VERSION,
                        "codecs": codecs,
                        "max_frame_bytes": cfg.max_frame_bytes,
                    },
                    owner=None,
                )
        except BaseException:
            self.kill(WorkerCrashed("worker did not start"))
            raise
        self.hello = reply
        if cfg.codec not in ("json", reply.get("codec")):
            logger.warning("worker does not support codec %r; using %r", cfg.codec, reply.get("codec"))

    def kill(self, exc: BaseException | None = None, *, for_ids: Mapping[int, BaseException] | None = None) -> None:
        """SIGKILL the process now and fail every pending call: with
        ``for_ids[id]`` if given, else ``exc`` (default ``WorkerCrashed``)."""
        if exc is None:
            exc = WorkerCrashed(f"worker {self.pid} was killed")
        if self.popen is not None and self.popen.poll() is None:
            try:
                self.popen.kill()
            except OSError:
                pass
        self._teardown(exc, for_ids or {})

    def close(self) -> None:
        """Graceful: close the worker's stdin; it rolls back, closes its
        connections and exits. Never blocks; safe from any event loop or
        none."""
        if self.dead is None:
            self._teardown(WorkerCrashed(f"worker {self.pid} was closed"), {})
        elif self._fds is not None:
            self._close_fds()

    def _teardown(self, exc: BaseException, for_ids: Mapping[int, BaseException]) -> None:
        if self.dead is None:
            self.dead = exc
        waiters, self._waiters = self._waiters, {}
        for mid, w in waiters.items():
            if not w.fut.done():
                e = for_ids.get(mid, exc)
                w.fut.set_exception(e)
                w.fut.exception()  # retrieved even if the caller is gone
        self._close_fds()
        if self.idle_timer is not None:
            self.idle_timer.cancel()
            self.idle_timer = None
        if self.popen is not None:
            _reap_later(self.popen, self.config.kill_grace_ms / 1000 + 1.0)

    def _close_fds(self) -> None:
        fds, self._fds = self._fds, None
        if fds is None:
            return
        same_loop = False
        try:
            same_loop = asyncio.get_running_loop() is self.loop
        except RuntimeError:
            pass
        if same_loop or not self.loop.is_closed():
            for fd in fds[1:]:
                try:
                    self.loop.remove_reader(fd)
                except Exception:
                    pass
            if self._writing:
                try:
                    self.loop.remove_writer(fds[0])
                except Exception:
                    pass
        self._writing = False
        p = self.popen
        if p is not None:
            for f in (p.stdin, p.stdout, p.stderr):
                try:
                    if f is not None:
                        f.close()
                except OSError:
                    pass

    @property
    def alive(self) -> bool:
        return self.dead is None

    def stderr_text(self) -> str:
        return b"".join(self.stderr_tail).decode("utf-8", "replace")[-_STDERR_TAIL:]

    # -- I/O ---------------------------------------------------------------------

    def _on_stderr(self) -> None:
        fds = self._fds
        if fds is None:
            return
        try:
            data = os.read(fds[2], 65536)
        except BlockingIOError:
            return
        except OSError:
            data = b""
        if not data:
            try:
                self.loop.remove_reader(fds[2])
            except Exception:
                pass
            return
        self.stderr_tail.append(data)
        for line in data.decode("utf-8", "replace").splitlines():
            logger.info("worker %s: %s", self.pid, line)

    def _on_stdout(self) -> None:
        fds = self._fds
        if fds is None:
            return
        try:
            data = os.read(fds[1], 1 << 18)
        except BlockingIOError:
            return
        except OSError:
            data = b""
        if not data:
            self._on_eof()
            return
        try:
            payloads = self._dec.feed(data)
        except FrameTooLarge as e:
            self.kill(ResourceLimitExceeded(f"worker {self.pid}: {e}; worker killed"))
            return
        for payload in payloads:
            try:
                msg = self.codec.decode(payload)
                if not isinstance(msg, dict):
                    raise ValueError("frame is not a message")
            except Exception as e:
                self.kill(WorkerCrashed(f"worker {self.pid} sent an undecodable frame ({e!r}); worker killed"))
                return
            if self.recorder is not None:
                self.recorder.append(("<-", msg))
            self._dispatch(msg)
            if self.dead is not None:
                return

    def _on_eof(self) -> None:
        detail = ""
        if self._dec.pending:
            detail = f" mid-frame ({self._dec.pending} bytes of a truncated frame)"
        code = self.popen.poll() if self.popen is not None else None
        tail = self.stderr_text().strip()
        msg = f"worker {self.pid} exited{detail}" + (f" with code {code}" if code is not None else "")
        if tail:
            msg += f"; stderr: {tail[-500:]}"
        self._teardown(WorkerCrashed(msg), {})

    def _dispatch(self, msg: dict[str, Any]) -> None:
        mid = msg.get("id")
        if mid is None:
            handler = self.notice_handlers.get(msg.get("conn"))
            if handler is None:
                handler = self.notice_handlers.get(None)
            if handler is not None:
                handler(msg)
            return
        w = self._waiters.get(mid)
        if w is None:
            return  # a reply for a call that gave up (killed, closed)
        if mid == 0 or msg.get("type") == "hello":
            codec = msg.get("codec")
            if codec and codec != self.codec.name:
                self.codec = get_codec(codec)
        if w.expect is None:
            del self._waiters[mid]
            if not w.fut.done():
                w.fut.set_result(msg)
            return
        w.frames.append(msg)
        terminal = "error" in msg or msg.get("end")
        if terminal or sum(1 for f in w.frames if "rows" in f) >= w.expect:
            del self._waiters[mid]
            if not w.fut.done():
                w.fut.set_result(w.frames)

    def _write(self, data: bytes) -> None:
        fds = self._fds
        if fds is None:
            raise self.dead or WorkerCrashed("worker is closed")
        if self._wbuf:
            self._wbuf += data
            return
        try:
            n = os.write(fds[0], data)
        except BlockingIOError:
            n = 0
        except (BrokenPipeError, OSError):
            self._on_eof()
            raise self.dead or WorkerCrashed("worker is gone") from None
        if n < len(data):
            self._wbuf += data[n:]
            self.loop.add_writer(fds[0], self._on_writable)
            self._writing = True

    def _on_writable(self) -> None:
        fds = self._fds
        if fds is None:
            return
        try:
            n = os.write(fds[0], self._wbuf)
        except BlockingIOError:
            return
        except OSError:
            self._on_eof()
            return
        del self._wbuf[:n]
        if not self._wbuf:
            self.loop.remove_writer(fds[0])
            self._writing = False

    def send(self, msg: dict[str, Any]) -> None:
        """Encode and send one message (no reply expected or awaited)."""
        if self.dead is not None:
            raise self.dead
        try:
            data = frame(self.codec.encode(msg), self.config.max_frame_bytes)
        except FrameTooLarge as e:
            raise ResourceLimitExceeded(f"request too large to send: {e}") from None
        if self.recorder is not None:
            self.recorder.append(("->", msg))
        self._write(data)

    # -- calls -----------------------------------------------------------------------

    def _check(self) -> None:
        if self.dead is not None:
            raise WorkerCrashed(str(self.dead))

    async def call(
        self,
        msg: dict[str, Any],
        *,
        owner: object,
        limit_ms: float | None = None,
        expect_frames: int | None = None,
        on_sent: Callable[[int], None] | None = None,
    ) -> Any:
        """Send a request and wait for its reply (or, for a stream exchange,
        ``expect_frames`` rows frames or the end). Raises the error the
        reply carries. ``limit_ms``: SIGKILL the worker if nothing came back
        within ``limit_ms + kill_grace_ms``; the call then raises
        ``QueryInterrupted``. Cancelling the awaiting task sends ``cancel``
        and waits (up to the grace period, then kills) for the worker to
        stop, so the connection is never left busy."""
        async with self.lock:
            self._check()
            mid = msg["id"] = msg.get("id") or next(self._ids)
            if expect_frames is not None and msg.get("type") == "more":
                del msg["id"]  # a ``more`` has no id of its own: frames carry the stream's
                mid = msg["stream"]
            fut: asyncio.Future[Any] = self.loop.create_future()
            self._waiters[mid] = _Waiter(fut, owner, expect_frames)
            try:
                self.send(msg)
            except BaseException:
                self._waiters.pop(mid, None)
                raise
            if on_sent is not None:
                on_sent(mid)
            timer = None
            if limit_ms is not None:
                timer = self.loop.call_later(
                    max(limit_ms, 0) / 1000 + self.config.kill_grace_ms / 1000,
                    self._kill_for_time, mid, limit_ms,
                )
            self.inflight = (mid, owner)
            try:
                reply = await self._wait(fut, mid)
            finally:
                self.inflight = None
                if timer is not None:
                    timer.cancel()
        if expect_frames is None:
            return _unwrap(reply, self)
        return reply

    def _kill_for_time(self, mid: int, limit_ms: float) -> None:
        interrupted = QueryInterrupted(
            f"query did not stop within its time limit ({limit_ms:.0f} ms) plus "
            f"{self.config.kill_grace_ms} ms; worker {self.pid} was killed"
        )
        # The "driver exception", as for every other backend's errors.
        interrupted.__cause__ = WorkerError(f"SIGKILL sent to worker {self.pid}")
        self.kill(
            WorkerCrashed(f"worker {self.pid} was killed: a query on it overran its time limit"),
            for_ids={mid: interrupted},
        )

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
                    self.kill(WorkerCrashed(f"worker {self.pid} did not stop a cancelled request; killed"))
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

    def cancel(self, owner: object) -> None:
        """Ask the worker to interrupt the in-flight request, if it is
        ``owner``'s. Does nothing otherwise."""
        cur = self.inflight
        if cur is not None and cur[1] is owner and self.dead is None:
            try:
                self.send({"type": "cancel", "target": cur[0]})
            except Exception:
                pass


def _unwrap(reply: dict[str, Any], proc: WorkerProcess) -> dict[str, Any]:
    if reply.get("fatal"):
        # The worker exits after this reply; stop using it now.
        proc.kill(WorkerCrashed(f"worker {proc.pid} exited after a fatal error"))
    err = reply.get("error")
    if err is not None:
        raise error_from_wire(err)
    return reply


# -- shared placement -------------------------------------------------------------------


class SharedWorkers:
    """A group of worker processes shared by many ``SqliteWorker``
    backends (``placement="shared"``). At most ``size`` processes; each
    connection goes to an idle process, a new one while there are fewer than
    ``size``, else the one with the fewest connections. A process with no
    connections exits after ``linger`` seconds. Belongs to one event loop at
    a time; call ``close()`` when done (also safe with no loop running)."""

    def __init__(
        self,
        size: int = 4,
        *,
        codec: str = "json",
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
        self.config = WorkerConfig(
            codec=codec,
            memory_limit_mb=memory_limit_mb,
            kill_grace_ms=kill_grace_ms,
            max_frame_bytes=max_frame_bytes,
            debug=debug,
        )
        self.procs: list[WorkerProcess] = []
        self._starting = 0
        self._loop: asyncio.AbstractEventLoop | None = None
        self.started = 0

    def __repr__(self) -> str:
        return f"<SharedWorkers size={self.size} running={len(self.procs)}>"

    async def acquire(self) -> WorkerProcess:
        loop = asyncio.get_running_loop()
        if self._loop is not loop:
            self.close()
            self._loop = loop
        self.procs = [p for p in self.procs if p.alive]
        idle = [p for p in self.procs if p.handles == 0]
        if idle:
            proc = idle[0]
        elif len(self.procs) + self._starting < self.size or not self.procs:
            self._starting += 1
            try:
                proc = WorkerProcess(self.config)
                await proc.start()
            finally:
                self._starting -= 1
            self.started += 1
            self.procs.append(proc)
        else:
            proc = min(self.procs, key=lambda p: p.handles)
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
        if proc.handles == 0:
            proc.idle_timer = None
            proc.close()
            if proc in self.procs:
                self.procs.remove(proc)

    def close(self) -> None:
        """Stop every process (graceful: each rolls back and exits)."""
        procs, self.procs = self.procs, []
        for p in procs:
            p.close()

    async def aclose(self) -> None:
        self.close()


# -- the backend ------------------------------------------------------------------------


_configs: dict[WorkerConfig, WorkerConfig] = {}


def _intern(cfg: WorkerConfig) -> WorkerConfig:
    return _configs.setdefault(cfg, cfg)


_caps_cache: dict[Capabilities, Capabilities] = {}


def _worker_capabilities(engine_caps: Capabilities) -> Capabilities:
    """The engine's flags, plus what the process boundary adds:
    ``hard_kill`` wherever SIGKILL exists, ``memory_limit`` where
    ``RLIMIT_AS`` is enforced (Linux) or the engine has its own."""
    caps = _caps_cache.get(engine_caps)
    if caps is None:
        caps = _caps_cache[engine_caps] = replace(
            engine_caps,
            hard_kill=hasattr(signal, "SIGKILL"),
            memory_limit=sys.platform.startswith("linux") or engine_caps.memory_limit,
        )
    return caps


class WorkerBackend:
    """Any in-process backend, run in worker processes.

    ``engine`` is an importable ``"module:callable"`` that returns a
    ``Backend`` when called as ``factory(path, **engine_config)``; it is
    called in each worker (for real) and once here, to learn the engine's
    ``dialect``, ``capabilities`` and ``change_hint()`` (so it must open
    nothing when called). ``engine_config`` must be plain data (it is sent
    in the ``open`` message). ``SqliteWorker`` is the SQLite-specific
    constructor; a DuckDB worker would be ``WorkerBackend(path,
    engine="dblayer.backends.duckdb:DuckDB", engine_config={...})``.

    Process options:

    * ``placement``: ``"dedicated"`` (a process per pooled connection) or
      ``"shared"`` (pass ``workers=SharedWorkers(...)``, whose codec, memory
      limit and grace settings then apply instead of these).
    * ``codec``: ``"json"`` or ``"msgpack"`` (optional extra).
    * ``memory_limit_mb``: ``RLIMIT_AS`` in the worker on Linux (engines may
      add their own limit: SQLite's ``hard_heap_limit``).
    * ``kill_grace_ms``: how long past a time limit, a cancel or a
      transaction deadline the parent waits before SIGKILL.
    * ``max_frame_bytes``: the frame size cap, both directions.
    """

    __slots__ = ("_path", "engine", "engine_config", "placement", "workers", "config",
                 "_local", "capabilities", "dialect", "processes_started",
                 "__weakref__")

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        engine: str,
        engine_config: Mapping[str, Any] | None = None,
        placement: Literal["dedicated", "shared"] = "dedicated",
        workers: SharedWorkers | None = None,
        codec: str = "json",
        memory_limit_mb: int | None = None,
        kill_grace_ms: int = DEFAULT_KILL_GRACE_MS,
        max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
        debug: bool = False,
    ) -> None:
        from ..worker.engines import load_engine

        if placement not in ("dedicated", "shared"):
            raise ValueError(f"placement must be 'dedicated' or 'shared', not {placement!r}")
        if placement == "shared" and workers is None:
            raise ValueError("placement='shared' needs workers=SharedWorkers(...)")
        if workers is not None:
            placement = "shared"
        if codec not in ("json", "msgpack"):
            raise ValueError(f"unknown codec {codec!r}")
        if codec == "msgpack" and "msgpack" not in available_codecs():
            raise NotSupported("codec 'msgpack' needs the msgpack package (the 'msgpack' extra)")
        self._path = os.path.abspath(os.fspath(path))
        self.engine = engine
        self.engine_config: dict[str, Any] = dict(engine_config or {})
        self.placement = placement
        self.workers = workers
        self.config = _intern(
            WorkerConfig(
                codec=codec,
                memory_limit_mb=memory_limit_mb,
                kill_grace_ms=kill_grace_ms,
                max_frame_bytes=max_frame_bytes,
                debug=debug,
            )
        )
        self._local: Any = load_engine(engine)(self._path, **self.engine_config)
        """The engine's backend, in this process: never opened; only its
        dialect, capabilities and change_hint() are used."""
        self.capabilities = _worker_capabilities(self._local.capabilities)
        self.dialect: Dialect = self._local.dialect
        self.processes_started = 0
        # No ``executor``: the parent side runs no blocking code, so this
        # is deliberately not a ``UsesExecutor`` backend.

    @property
    def path(self) -> Path:
        return Path(self._path)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._path!r}, placement={self.placement!r})"

    def _open_message(self, write: bool) -> dict[str, Any]:
        msg: dict[str, Any] = {"type": "open", "path": self._path, "mode": "write" if write else "read"}
        if self.engine != _SQLITE_ENGINE:
            msg["engine"] = self.engine
        if self.engine_config:
            msg["config"] = self.engine_config
        return msg

    # -- Backend protocol ----------------------------------------------------------

    async def open(self, *, write: bool) -> "WorkerConnection":
        if self.workers is not None:
            proc = await self.workers.acquire()
            shared = True
        else:
            proc = WorkerProcess(self.config)
            await proc.start()
            self.processes_started += 1
            shared = False
        conn = WorkerConnection(self, proc, write=write, shared=shared)
        try:
            await conn._open()
        except BaseException:
            await conn.close()
            raise
        return conn

    def change_hint(self) -> object:
        return self._local.change_hint()

    async def close(self) -> None:
        pass  # every process belongs to a connection (or a SharedWorkers group)


_SQLITE_ENGINE = "dblayer.worker.engines:sqlite"


class SqliteWorker(WorkerBackend):
    """SQLite in worker processes: ``WorkerBackend`` running the in-process
    SQLite backend (engine ``dblayer.worker.engines:sqlite``).

    ``SqliteWorker(path)`` gives dedicated placement, the JSON codec and no
    memory limit. SQLite-specific options, applied inside the worker:

    * ``memory_limit_mb``: also ``PRAGMA hard_heap_limit`` (every platform).
    * ``init``: importable ``"module:function"`` names, each called with
      every new raw ``sqlite3.Connection`` (register SQL functions, load
      extensions). They run inside the worker, under its limits.
    * ``sql_limits``: ``{"length": 1_000_000, "expr_depth": 100, ...}``,
      applied with ``Connection.setlimit()``.
    * ``immutable``, ``create``, ``busy_timeout``: as ``SqliteInProcess``.

    Capability flags: as the in-process file backend, plus ``hard_kill``
    and ``memory_limit`` (Linux only, where ``RLIMIT_AS`` is enforced).
    """

    __slots__ = ()

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        init: Sequence[str] = (),
        sql_limits: Mapping[str, int] | None = None,
        immutable: bool = False,
        create: bool = True,
        busy_timeout: float = 5.0,
        memory_limit_mb: int | None = None,
        **options: Any,
    ) -> None:
        for name in init:
            if ":" not in name:
                raise ValueError(f"init hook {name!r} must be 'module:function'")
        config: dict[str, Any] = {}
        if init:
            config["init"] = list(init)
        if sql_limits:
            config["sql_limits"] = dict(sql_limits)
        if immutable:
            config["immutable"] = True
        if not create:
            config["create"] = False
        if busy_timeout != 5.0:
            config["busy_timeout"] = busy_timeout
        workers = options.get("workers")
        heap_mb = memory_limit_mb if workers is None else workers.config.memory_limit_mb
        if heap_mb:
            config["memory_limit_mb"] = heap_mb
        super().__init__(
            path, engine=_SQLITE_ENGINE, engine_config=config, memory_limit_mb=memory_limit_mb, **options
        )

    @property
    def immutable(self) -> bool:
        return bool(self.engine_config.get("immutable"))

    async def open(self, *, write: bool) -> "WorkerConnection":
        if write and self.immutable:
            raise NotSupported("database is immutable; writes are not allowed")
        return await super().open(write=write)


class WorkerConnection:
    """One pooled connection: a dedicated worker process, or a connection
    inside a shared one. Used by one task at a time (the pool's rule)."""

    def __init__(self, backend: WorkerBackend, proc: WorkerProcess, *, write: bool, shared: bool) -> None:
        self._backend = backend
        self._proc = proc
        self.write = write
        self._shared = shared
        self._conn_id: int | None = None
        self._tx: str | None = None
        self._tx_done = True
        self._in_tx = False
        self._deadline_timer: asyncio.TimerHandle | None = None
        self._closed = False

    def __repr__(self) -> str:
        return f"<WorkerConnection {'write' if self.write else 'read'} pid={self._proc.pid}>"

    @property
    def pid(self) -> int | None:
        return self._proc.pid

    @property
    def process(self) -> WorkerProcess:
        return self._proc

    async def _open(self) -> None:
        msg = self._backend._open_message(self.write)
        reply = await self._proc.call(msg, owner=self)
        self._conn_id = reply["conn"]
        self._proc.notice_handlers[self._conn_id if self._shared else None] = self._on_notice

    # -- plumbing ----------------------------------------------------------------

    async def _ensure_alive(self) -> None:
        """A connection whose worker died while it sat idle in the pool
        (killed from outside, or by a fault on a shared process) gets a new
        worker on its next request, unless it had a transaction open: that
        is lost, and the call raises ``WorkerCrashed``."""
        if self._proc.alive:
            return
        if self._tx is not None or self._in_tx:
            raise WorkerCrashed(str(self._proc.dead))
        old = self._proc
        b = self._backend
        if self._shared:
            assert b.workers is not None
            b.workers.release(old)
            self._proc = await b.workers.acquire()
        else:
            self._proc = WorkerProcess(b.config)
            await self._proc.start()
            b.processes_started += 1
        try:
            await self._open()
        except BaseException:
            self._closed = True
            raise

    async def _call(self, msg: dict[str, Any], *, limit_ms: float | None = None) -> dict[str, Any]:
        if self._closed:
            raise WorkerCrashed("connection is closed")
        await self._ensure_alive()
        if self._shared:
            msg["conn"] = self._conn_id
        reply = await self._proc.call(msg, owner=self, limit_ms=limit_ms)
        if "in_tx" in reply or "result" in reply:
            self._in_tx = bool(reply.get("in_tx"))
        return reply

    def _tx_field(self, msg: dict[str, Any]) -> dict[str, Any]:
        if self._tx is not None:
            msg["tx"] = self._tx
        return msg

    def _on_notice(self, msg: dict[str, Any]) -> None:
        if msg.get("type") == "expired" and msg.get("tx") == self._tx:
            self._tx_done = True  # the worker rolled back on its own
            self._in_tx = False
            self._cancel_deadline_timer()

    def _cancel_deadline_timer(self) -> None:
        t, self._deadline_timer = self._deadline_timer, None
        if t is not None:
            t.cancel()

    def _on_deadline_overrun(self, tx: str, deadline_ms: int) -> None:
        self._deadline_timer = None
        if self._tx != tx or self._tx_done:
            return
        proc = self._proc
        cur = proc.inflight
        for_ids = {}
        if cur is not None and cur[1] is self:
            for_ids[cur[0]] = QueryInterrupted(
                f"transaction passed its deadline ({deadline_ms} ms) and the worker did not "
                f"confirm rollback; worker {proc.pid} was killed"
            )
        proc.kill(
            WorkerCrashed(f"worker {proc.pid} was killed: a transaction on it overran its deadline"),
            for_ids=for_ids,
        )

    # -- BackendConnection ----------------------------------------------------------

    async def execute(
        self,
        sql: str,
        params: NativeParams,
        *,
        time_limit_ms: int | None,
        max_rows: int | None,
        max_result_bytes: int | None = None,
    ) -> Results:
        msg: dict[str, Any] = {"type": "execute", "sql": sql}
        if params:
            msg["params"] = params if isinstance(params, (dict, list)) else (
                dict(params) if isinstance(params, Mapping) else list(params)
            )
        if time_limit_ms is not None:
            msg["time_limit_ms"] = time_limit_ms
        if max_rows is not None:
            msg["max_rows"] = max_rows
        if max_result_bytes is not None:
            msg["max_result_bytes"] = max_result_bytes
        reply = await self._call(self._tx_field(msg), limit_ms=time_limit_ms)
        return results_from_wire(reply["result"])

    async def execute_batch(
        self,
        statements: Sequence[tuple[str, NativeParams]],
        *,
        time_limit_ms: int | None,
        max_rows: int | None,
    ) -> list[Results]:
        """``SupportsBatch``: every statement in one message."""
        msg: dict[str, Any] = {
            "type": "batch",
            "statements": [[s, _plain(p)] for s, p in statements],
        }
        if time_limit_ms is not None:
            msg["time_limit_ms"] = time_limit_ms
        if max_rows is not None:
            msg["max_rows"] = max_rows
        limit = None if time_limit_ms is None else time_limit_ms * max(len(statements), 1)
        reply = await self._call(self._tx_field(msg), limit_ms=limit)
        return [results_from_wire(r) for r in reply["result"]]

    async def execute_many(self, sql: str, params_seq: Sequence[NativeParams]) -> WriteResult:
        msg = {"type": "execute_many", "sql": sql, "params_seq": [_plain(p) for p in params_seq]}
        reply = await self._call(self._tx_field(msg))
        return WriteResult(rowcount=reply["result"]["rowcount"])

    def stream(
        self,
        sql: str,
        params: NativeParams,
        *,
        chunk_rows: int,
        time_limit_ms: int | None = None,
        max_result_bytes: int | None = None,
    ) -> "WorkerStream":
        if chunk_rows < 1:
            raise ValueError("chunk_rows must be at least 1")
        return WorkerStream(self, sql, params, chunk_rows, time_limit_ms, max_result_bytes)

    async def begin(self, mode: str) -> None:
        await self._begin(mode, None)

    async def begin_with_deadline(self, mode: str, deadline_ms: int) -> None:
        """``SupportsServerDeadline``: the worker enforces the deadline
        itself, and the parent kills it if it does not confirm the
        rollback in time."""
        await self._begin(mode, deadline_ms)

    async def _begin(self, mode: str, deadline_ms: int | None) -> None:
        if mode not in ("write", "read"):
            raise ValueError(f"unknown transaction mode {mode!r}")
        msg: dict[str, Any] = {"type": "begin", "mode": mode}
        if deadline_ms is not None:
            msg["deadline_ms"] = deadline_ms
        reply = await self._call(msg)
        self._tx = reply["tx"]
        self._tx_done = False
        self._in_tx = True
        if deadline_ms is not None:
            cfg = self._proc.config
            self._deadline_timer = self._proc.loop.call_later(
                (deadline_ms + cfg.kill_grace_ms + cfg.deadline_slack_ms) / 1000,
                self._on_deadline_overrun, self._tx, deadline_ms,
            )

    def _end_tx(self) -> None:
        self._tx = None
        self._tx_done = True
        self._in_tx = False
        self._cancel_deadline_timer()

    async def commit(self) -> None:
        if self._tx is None and not self._in_tx:
            return
        await self._call(self._tx_field({"type": "commit"}))
        self._end_tx()

    async def rollback(self) -> None:
        if self._closed:
            return
        if not self._proc.alive:
            raise WorkerCrashed(str(self._proc.dead))
        if self._tx is None and not self._in_tx:
            return
        try:
            await self._call(self._tx_field({"type": "rollback"}))
        finally:
            if self._proc.alive:
                self._end_tx()

    async def schema(self) -> Schema:
        reply = await self._call(self._tx_field({"type": "schema"}))
        return schema_from_wire(reply["result"])

    async def schema_version(self) -> object:
        reply = await self._call(self._tx_field({"type": "schema_version"}))
        return reply["result"]

    async def run_operation(self, name: str, args: dict[str, Any]) -> object:
        """``SupportsOperations``: the worker runs the default
        implementation of the operation locally, in one round trip."""
        reply = await self._call(self._tx_field({"type": "op", "name": name, "args": args}))
        return reply["result"]

    async def interrupt(self) -> None:
        self._proc.cancel(self)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._cancel_deadline_timer()
        proc = self._proc
        if self._shared:
            if self._conn_id is not None:
                proc.notice_handlers.pop(self._conn_id, None)
            if proc.alive and self._conn_id is not None:
                try:
                    if asyncio.get_running_loop() is proc.loop:
                        await proc.call({"type": "close", "conn": self._conn_id}, owner=self)
                except Exception:
                    pass
            assert self._backend.workers is not None
            self._backend.workers.release(proc)
        else:
            proc.close()


def _plain(params: NativeParams | None) -> Any:
    if params is None:
        return None
    if isinstance(params, (dict, list)):
        return params
    if isinstance(params, Mapping):
        return dict(params)
    return list(params)


class WorkerStream:
    """Rows from a ``stream`` request, with credit-based backpressure: the
    worker sends at most ``credit`` frames of ``chunk_rows`` rows, then
    pauses with its cursor open until a ``more`` message grants more. The
    parent asks for ``STREAM_CREDIT`` frames at a time, only when the
    consumer has used up what it has, so memory on both sides is bounded by
    ``STREAM_CREDIT * chunk_rows`` rows. ``time_limit_ms`` is a budget of
    time spent inside the worker across chunks (measured there), as for the
    in-process backend."""

    STREAM_CREDIT = 2

    def __init__(
        self,
        conn: WorkerConnection,
        sql: str,
        params: NativeParams,
        chunk_rows: int,
        time_limit_ms: int | None,
        max_result_bytes: int | None,
    ) -> None:
        self._conn = conn
        self._sql = sql
        self._params = params
        self._chunk_rows = chunk_rows
        self._time_limit_ms = time_limit_ms
        self._spent_ms = 0.0
        self._max_bytes = max_result_bytes
        self._sid: int | None = None
        self._buffer: collections.deque[list[tuple[Any, ...]]] = collections.deque()
        self._paused = False  # the worker holds an open cursor for us
        self._done = False
        self.columns: tuple[Column, ...] | None = None

    def __aiter__(self) -> "WorkerStream":
        return self

    async def __anext__(self) -> list[tuple[Any, ...]]:
        while not self._buffer:
            if self._done:
                raise StopAsyncIteration
            try:
                await self._exchange()
            except BaseException:
                self._done = True
                self._paused = False
                raise
        return self._buffer.popleft()

    def _set_sid(self, mid: int) -> None:
        self._sid = mid

    async def _exchange(self) -> None:
        conn = self._conn
        proc = conn._proc
        credit = self.STREAM_CREDIT
        if self._sid is None:
            msg: dict[str, Any] = {"type": "stream", "sql": self._sql, "chunk_rows": self._chunk_rows, "credit": credit}
            if self._params:
                msg["params"] = _plain(self._params)
            if self._time_limit_ms is not None:
                msg["time_limit_ms"] = self._time_limit_ms
            if self._max_bytes is not None:
                msg["max_result_bytes"] = self._max_bytes
            if conn._shared:
                msg["conn"] = conn._conn_id
            conn._tx_field(msg)
        else:
            msg = {"type": "more", "stream": self._sid, "credit": credit}
        limit = None
        if self._time_limit_ms is not None:
            limit = max(self._time_limit_ms - self._spent_ms, 0)
        t0 = time.perf_counter()
        if conn._closed:
            raise WorkerCrashed("connection is closed")
        if self._sid is None:
            await conn._ensure_alive()
            proc = conn._proc
            if conn._shared:
                msg["conn"] = conn._conn_id
        self._paused = False
        frames = await proc.call(msg, owner=conn, limit_ms=limit, expect_frames=credit, on_sent=self._set_sid if self._sid is None else None)
        self._spent_ms += (time.perf_counter() - t0) * 1000
        for f in frames:
            if "columns" in f and self.columns is None:
                types = f.get("types") or ()
                self.columns = tuple(
                    Column(c, types[i] if i < len(types) else None) for i, c in enumerate(f["columns"])
                )
            if "error" in f:
                self._done = True
                raise error_from_wire(f["error"])
            if "rows" in f:
                self._buffer.append([tuple(r) for r in f["rows"]])
            if f.get("end"):
                self._done = True
        if not self._done:
            self._paused = True

    async def aclose(self) -> None:
        if self._done and not self._paused:
            return
        self._done = True
        self._buffer.clear()
        if not self._paused or self._sid is None:
            return
        self._paused = False
        proc = self._conn._proc
        if not proc.alive or self._conn._closed:
            return
        # Close the worker's cursor: ``cancel`` on a paused stream makes
        # the worker close it and answer with a final ``end`` frame.
        try:
            await proc.call(
                {"type": "cancel", "target": self._sid, "id": self._sid},
                owner=self._conn,
                limit_ms=5000,  # nothing runs: the worker answers at once unless it is wedged
                expect_frames=1,
            )
        except DblayerError:
            pass
