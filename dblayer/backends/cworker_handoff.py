"""Experimental: socket handoff for C worker exports.

The HTTP server sends the status line, the headers and the first body
chunk (the CSV header row or JSON envelope) itself, then passes the
*client's own connection* to the C worker (``SCM_RIGHTS`` over a Unix
socket), which writes every row to it as HTTP/1.1 chunks. Python never
touches a row byte. When the worker reports the end on its control pipe
(after closing its copy of the descriptor), the server writes the last
chunk (the JSON tail) and the terminating ``0\\r\\n\\r\\n``; the connection
stays usable for keep-alive.

Preconditions: plain HTTP/1.1 on a socket this process owns (TLS
terminated elsewhere, no HTTP/2 or 3, no response compression or other
body-rewriting middleware, nothing buffered in the server's own write
buffer at handoff time). ASGI does not give an application the socket,
so this needs a server that does: see ``benchmarks/handoff_server.py``.

Failure: any error after the headers were sent (SQL error mid-result,
time limit, worker killed, client gone) leaves the chunked body without
its terminating chunk; the caller must then close the connection, so
that the client sees a broken transfer, never a complete-looking one.
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import os
import socket
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ..core.export import ExportSpec
from ..core.results import Column
from ..errors import NotSupported
from ._cworker_export import _fifo_path, _message

if TYPE_CHECKING:
    from ..core.database import Database
    from .sqlite_cworker import CWorkerConnection

__all__ = ["HandoffResult", "handoff_export"]

_names = itertools.count(1)


@dataclass(slots=True)
class HandoffResult:
    columns: tuple[Column, ...]
    row_count: int
    truncated: bool
    body_bytes: int
    """Row bytes the worker wrote (without chunk framing)."""


async def _run(conn: "CWorkerConnection", sql: str, params: Any, spec: ExportSpec, client: socket.socket,
               send_head: Callable[[tuple[Column, ...]], Awaitable[None]], *, max_rows: int | None,
               time_limit_ms: int | None, chunk_bytes: int) -> HandoffResult:
    from .sqlite_worker import error_from_wire

    proc = conn._proc
    if "socket" not in ((proc.hello.get("export") or {}).get("transports") or ()):
        raise NotSupported("this C worker has no socket handoff")
    loop = asyncio.get_running_loop()
    path = _fifo_path() + ".sock"
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(path)
    os.chmod(path, 0o600)
    listener.listen(1)
    listener.setblocking(False)
    end_call: asyncio.Future[Any] | None = None
    try:
        msg = _message(conn, sql, params, spec.encoding, "socket", max_rows, time_limit_ms, chunk_bytes)
        msg["handoff_path"] = path
        await conn._ensure_alive()
        sid: list[int] = []
        frames = await proc.call(msg, owner=conn, expect_frames=1, on_sent=sid.append)
        f = frames[-1]
        if "error" in f:
            raise error_from_wire(f["error"])
        columns = tuple(Column(c) for c in f["columns"])
        await send_head(columns)
        # Wait for the end frame: a `more` the worker ignores (the export
        # is not a paused stream) registers the waiter before anything can
        # arrive, since the worker writes nothing until it has the socket.
        end_call = asyncio.ensure_future(
            proc.call({"type": "more", "stream": sid[0], "credit": 1}, owner=conn, expect_frames=1)
        )
        peer, _ = await loop.sock_accept(listener)
        with peer:
            socket.send_fds(peer, [b"\x00"], [client.fileno()])
        listener.close()
        frames = await end_call
        end_call = None
        f = frames[-1]
        if "error" in f:
            raise error_from_wire(f["error"])
        return HandoffResult(columns, f.get("row_count", 0), bool(f.get("truncated")), f.get("bytes", 0))
    finally:
        listener.close()
        with contextlib.suppress(OSError):
            os.unlink(path)
        if end_call is not None and not end_call.done():
            end_call.cancel()  # sends `cancel`; waits for the worker to stop
            with contextlib.suppress(BaseException):
                await end_call


async def handoff_export(db: "Database", sql: str, params: Any = None, *, client: socket.socket,
                         send_head: Callable[[tuple[Column, ...]], Awaitable[None]], format: str = "csv",
                         json_shape: str = "objects", max_rows: int | None = None,
                         time_limit_ms: int | None = None, chunk_bytes: int = 1 << 16) -> HandoffResult:
    """Export ``sql`` on ``db`` (a ``SqliteCWorker`` database) straight into
    ``client``. ``send_head(columns)`` must write the status line, headers
    and the first chunk and return only when they are all on the wire. On
    success the caller writes ``spec.tail(truncated)`` as a last chunk and
    the ``0`` chunk; on any exception it must close the connection."""
    from ..core.pool import _should_discard

    spec = ExportSpec(format, json_shape)
    native_sql, native_params = db._native(sql, params)
    pool = await db._use_pool()
    conn = await pool.acquire(write=False)
    discard = False
    try:
        if not hasattr(conn, "export_formats"):
            raise NotSupported("socket handoff needs a SqliteCWorker database")
        return await _run(conn, native_sql, native_params, spec, client, send_head,  # type: ignore[arg-type]
                          max_rows=max_rows, time_limit_ms=time_limit_ms, chunk_bytes=chunk_bytes)
    except BaseException as e:
        discard = _should_discard(e)
        raise
    finally:
        await pool.release(conn, discard=discard)


def chunk(data: bytes) -> bytes:
    """One HTTP/1.1 chunk."""
    return b"%x\r\n%s\r\n" % (len(data), data) if data else b""

