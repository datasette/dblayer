"""The parent side of the C worker's ``export`` request (``SupportsExport``).

Transports (``docs/worker-protocol.md``, "export"):

* ``"frames"``: data in codec frames (MessagePack ``bin``), credit-based.
* ``"raw"``: data in frames the codec never sees (payload ``0x00``, the
  request id, the bytes), credit-based. The frame reader in
  ``sqlite_worker.WorkerProcess`` hands them over as memoryviews.
* ``"fifo"``: data on a named pipe made for this export; the pipe's own
  buffer is the backpressure; the control pipe carries only the final
  reply (row count, truncated) or the error.
* ``"socket"`` is not here: see ``dblayer.backends.cworker_handoff``.
"""

from __future__ import annotations

import asyncio
import collections
import contextlib
import itertools
import json
import os
import shutil
import tempfile
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from ..core.results import Column
from ..errors import DblayerError, WorkerCrashed
from ..worker.protocol import HEADER

if TYPE_CHECKING:
    from .sqlite_worker import WorkerConnection

__all__ = ["CWorkerExport", "FifoExport", "TRANSPORTS"]

TRANSPORTS = ("frames", "raw", "fifo")
EXPORT_CREDIT = 4
FIFO_READ = 1 << 18
FIFO_HIGH_WATER = 1 << 20


def _plain(params: Any) -> Any:
    if params is None or isinstance(params, (dict, list)):
        return params
    if isinstance(params, dict):
        return dict(params)
    return list(params)


def _message(conn: "WorkerConnection", sql: str, params: Any, encoding: str, transport: str,
             max_rows: int | None, time_limit_ms: int | None, chunk_bytes: int) -> dict[str, Any]:
    msg: dict[str, Any] = {"type": "export", "sql": sql, "encoding": encoding, "transport": transport,
                           "chunk_bytes": chunk_bytes}
    if params:
        msg["params"] = _plain(params)
    if max_rows is not None:
        msg["max_rows"] = max_rows
    if time_limit_ms is not None:
        msg["time_limit_ms"] = time_limit_ms
    if conn._shared:
        msg["conn"] = conn._conn_id
    return conn._tx_field(msg)


class CWorkerExport:
    """``"frames"`` and ``"raw"``: like ``WorkerStream``, the worker sends
    ``EXPORT_CREDIT`` data frames and pauses until ``more``."""

    def __init__(self, conn: "WorkerConnection", sql: str, params: Any, *, encoding: str, transport: str,
                 max_rows: int | None, time_limit_ms: int | None, chunk_bytes: int) -> None:
        self._conn = conn
        self._msg = _message(conn, sql, params, encoding, transport, max_rows, time_limit_ms, chunk_bytes)
        self._sid: int | None = None
        self._buffer: collections.deque[Any] = collections.deque()
        self._done = False
        self._paused = False
        self.columns: tuple[Column, ...] | None = None
        self.row_count = 0
        self.truncated = False

    def _set_sid(self, mid: int) -> None:
        self._sid = mid

    async def _exchange(self) -> None:
        conn = self._conn
        if self._sid is None:
            if conn._closed:
                raise WorkerCrashed("connection is closed")
            await conn._ensure_alive()
            msg = self._msg
            if conn._shared:
                msg["conn"] = conn._conn_id
            msg["credit"] = EXPORT_CREDIT
        else:
            msg = {"type": "more", "stream": self._sid, "credit": EXPORT_CREDIT}
        self._paused = False
        try:
            frames = await conn._proc.call(msg, owner=conn, expect_frames=EXPORT_CREDIT,
                                           on_sent=self._set_sid if self._sid is None else None)
        except BaseException:
            self._done = True
            raise
        from .sqlite_worker import error_from_wire

        for f in frames:
            if "error" in f:
                self._done = True
                raise error_from_wire(f["error"])
            if "columns" in f:
                self.columns = tuple(Column(c) for c in f["columns"])
            data = f.get("data")
            if data is not None:
                self._buffer.append(data)
            if f.get("end"):
                self._done = True
                self.row_count = f.get("row_count", self.row_count)
                self.truncated = bool(f.get("truncated"))
        if not self._done:
            self._paused = True

    async def start(self) -> Sequence[Column]:
        while self.columns is None:
            if self._done:
                raise WorkerCrashed("export ended without columns")
            await self._exchange()
        return self.columns

    def __aiter__(self) -> "CWorkerExport":
        return self

    async def __anext__(self) -> Any:
        while not self._buffer:
            if self._done:
                raise StopAsyncIteration
            await self._exchange()
        return self._buffer.popleft()

    async def aclose(self) -> None:
        self._buffer.clear()
        if self._done or not self._paused or self._sid is None:
            self._done = True
            return
        self._done = True
        self._paused = False
        proc = self._conn._proc
        if not proc.alive or self._conn._closed:
            return
        with contextlib.suppress(DblayerError):
            await proc.call({"type": "cancel", "target": self._sid, "id": self._sid},
                            owner=self._conn, limit_ms=5000, expect_frames=1)


_fifo_dir: str | None = None
_fifo_names = itertools.count(1)


def _fifo_path() -> str:
    global _fifo_dir
    if _fifo_dir is None or not os.path.isdir(_fifo_dir):
        _fifo_dir = tempfile.mkdtemp(prefix="dblayer-export-")  # mode 0700
        import atexit

        atexit.register(shutil.rmtree, _fifo_dir, True)
    return os.path.join(_fifo_dir, f"x{os.getpid()}-{next(_fifo_names)}")


class FifoExport:
    """``"fifo"``: the worker writes to a named pipe; this reads it with
    ``loop.add_reader`` into fresh ``bytes`` (one ``os.read`` per chunk,
    no other copy), pausing the reader above ``FIFO_HIGH_WATER`` buffered
    bytes so the worker blocks on a full pipe."""

    def __init__(self, conn: "WorkerConnection", sql: str, params: Any, *, encoding: str,
                 max_rows: int | None, time_limit_ms: int | None, chunk_bytes: int) -> None:
        self._conn = conn
        self._msg = _message(conn, sql, params, encoding, "fifo", max_rows, time_limit_ms, chunk_bytes)
        self._loop = asyncio.get_running_loop()
        self._path: str | None = None
        self._fd: int | None = None
        self._wfd: int | None = None
        self._reading = False
        self._buffer: collections.deque[bytes] = collections.deque()
        self._buffered = 0
        self._pre = bytearray()
        self._eof = False
        self._wake: asyncio.Future[None] | None = None
        self._call: asyncio.Future[Any] | None = None
        self.columns: tuple[Column, ...] | None = None
        self.row_count = 0
        self.truncated = False
        self._finished = False

    def _notify(self) -> None:
        w = self._wake
        if w is not None and not w.done():
            w.set_result(None)

    def _on_readable(self) -> bool:
        """Read once; True if bytes came. This process holds a write end
        of its own (``_wfd``), so the pipe never reports end of file: an
        empty pipe is EAGAIN, and the end of the data is known from the
        worker's reply, sent after it closed its end."""
        fd = self._fd
        if fd is None:
            return False
        try:
            data = os.read(fd, FIFO_READ)
        except (BlockingIOError, InterruptedError):
            return False
        if not data:
            return False
        if self.columns is None:
            self._pre += data
            if len(self._pre) >= 4:
                (n,) = HEADER.unpack_from(self._pre)
                if len(self._pre) >= 4 + n:
                    self.columns = tuple(Column(c) for c in json.loads(bytes(self._pre[4:4 + n])))
                    rest = bytes(self._pre[4 + n:])
                    self._pre.clear()
                    if rest:
                        self._buffer.append(rest)
                        self._buffered += len(rest)
        else:
            self._buffer.append(data)
            self._buffered += len(data)
            if self._buffered >= FIFO_HIGH_WATER:
                self._stop_reading()
        self._notify()
        return True

    def _drain(self) -> None:
        """After the worker's reply: read what is left; EAGAIN is the end."""
        while self._buffered < FIFO_HIGH_WATER:
            if not self._on_readable():
                self._eof = True
                return

    def _stop_reading(self) -> None:
        if self._reading and self._fd is not None:
            self._loop.remove_reader(self._fd)
            self._reading = False

    def _start_reading(self) -> None:
        if not self._reading and self._fd is not None and not self._eof:
            self._loop.add_reader(self._fd, self._on_readable)
            self._reading = True

    async def _wait(self) -> None:
        self._wake = self._loop.create_future()
        waits: list[asyncio.Future[Any]] = [self._wake]
        if self._call is not None and not self._call.done():
            waits.append(self._call)
        try:
            await asyncio.wait(waits, return_when=asyncio.FIRST_COMPLETED)
        finally:
            self._wake = None

    async def start(self) -> Sequence[Column]:
        conn = self._conn
        await conn._ensure_alive()
        if conn._shared:
            self._msg["conn"] = conn._conn_id
        self._path = path = _fifo_path()
        os.mkfifo(path, 0o600)
        self._fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
        self._wfd = os.open(path, os.O_WRONLY | os.O_NONBLOCK)
        self._msg["data_path"] = path
        self._start_reading()
        self._call = asyncio.ensure_future(conn._proc.call(self._msg, owner=conn))
        while self.columns is None:
            if self._call.done():
                self._drain()
                if self.columns is not None:
                    break
                await self._finish()  # raises the worker's error
                if self.columns is None:
                    raise WorkerCrashed("export ended without columns")
                break
            await self._wait()
        return self.columns

    async def _finish(self) -> None:
        if self._finished:
            return
        assert self._call is not None
        reply = await self._call
        self._finished = True
        result = reply.get("result") or {}
        self.row_count = result.get("row_count", 0)
        self.truncated = bool(result.get("truncated"))
        self._close_fd()

    def __aiter__(self) -> "FifoExport":
        return self

    async def __anext__(self) -> bytes:
        while not self._buffer:
            if self._eof:
                await self._finish()
                raise StopAsyncIteration
            call = self._call
            if call is not None and call.done():
                if call.exception() is not None:
                    await self._finish()  # raises
                # The worker closed its end before it replied, so what is
                # left can be read now.
                self._drain()
                continue
            await self._wait()
        data = self._buffer.popleft()
        self._buffered -= len(data)
        if self._buffered < FIFO_HIGH_WATER // 2:
            self._start_reading()
        return data

    def _close_fd(self) -> None:
        self._stop_reading()
        fd, self._fd = self._fd, None
        if fd is not None:
            os.close(fd)
        wfd, self._wfd = self._wfd, None
        if wfd is not None:
            os.close(wfd)
        path, self._path = self._path, None
        if path is not None:
            with contextlib.suppress(OSError):
                os.unlink(path)

    async def aclose(self) -> None:
        # Closing the read end first makes a worker blocked on a full pipe
        # fail its write (EPIPE) and reply; then wait for that reply.
        self._close_fd()
        self._buffer.clear()
        call = self._call
        if call is None or self._finished:
            return
        if not call.done():
            call.cancel()
        with contextlib.suppress(BaseException):
            await call
        self._finished = True
