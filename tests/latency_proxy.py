"""A proxy in front of the PostgreSQL test server that delays everything the
server sends by a fixed time, so each client/server round trip costs about
that much. Counting round trips then needs no driver internals: time a call
and divide by the delay.

The proxy listens on a Unix socket in a temporary directory (libpq's
``host=<dir>``) and forwards to the server named by ``tests.pg.DSN`` (Unix
socket or TCP). Only the database the DSN names is ever reached.
"""

from __future__ import annotations

import asyncio
import contextlib
import shutil
import tempfile
import urllib.parse
from collections.abc import AsyncIterator
from pathlib import Path

from . import pg


def _upstream(dsn: str) -> tuple[str | None, str | None, int]:
    """``(unix_dir, tcp_host, port)`` of the server ``dsn`` names."""
    parts = urllib.parse.urlsplit(dsn)
    query = urllib.parse.parse_qs(parts.query)
    host = query.get("host", [parts.hostname or "/tmp"])[0]
    port = int(query.get("port", [parts.port or 5432])[0])
    if host.startswith("/"):
        return host, None, port
    return None, host, port


def _proxied_dsn(dsn: str, socket_dir: str, port: int) -> str:
    parts = urllib.parse.urlsplit(dsn)
    query = urllib.parse.parse_qs(parts.query)
    query["host"] = [socket_dir]
    query["port"] = [str(port)]
    user = ""
    if parts.username:
        user = parts.username + (f":{parts.password}" if parts.password else "") + "@"
    return f"postgresql://{user}{parts.path}?{urllib.parse.urlencode(query, doseq=True)}"


@contextlib.asynccontextmanager
async def latency_proxy(delay: float) -> AsyncIterator[str]:
    """Run the proxy on the current event loop; yields a DSN that goes
    through it."""
    unix_dir, tcp_host, port = _upstream(pg.DSN)
    tmp = tempfile.mkdtemp(prefix="dblpx")
    loop = asyncio.get_running_loop()
    tasks: set[asyncio.Task[None]] = set()

    async def pump(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, lag: float) -> None:
        queue: asyncio.Queue[tuple[float, bytes | None]] = asyncio.Queue()

        async def send() -> None:
            while True:
                at, data = await queue.get()
                if data is None:
                    break
                wait = at + lag - loop.time()
                if wait > 0:
                    await asyncio.sleep(wait)
                writer.write(data)
                await writer.drain()
            writer.close()

        sender = asyncio.ensure_future(send())
        try:
            while data := await reader.read(65536):
                queue.put_nowait((loop.time(), data))
        except (ConnectionError, OSError):
            pass
        finally:
            queue.put_nowait((0.0, None))
            with contextlib.suppress(Exception):
                await sender

    async def handle(c_reader: asyncio.StreamReader, c_writer: asyncio.StreamWriter) -> None:
        if unix_dir is not None:
            s_reader, s_writer = await asyncio.open_unix_connection(
                str(Path(unix_dir) / f".s.PGSQL.{port}")
            )
        else:
            s_reader, s_writer = await asyncio.open_connection(tcp_host, port)
        both = asyncio.gather(pump(c_reader, s_writer, 0.0), pump(s_reader, c_writer, delay))
        task = asyncio.ensure_future(both)
        tasks.add(task)  # type: ignore[arg-type]
        with contextlib.suppress(Exception):
            await task

    server = await asyncio.start_unix_server(handle, str(Path(tmp) / f".s.PGSQL.{port}"))
    try:
        yield _proxied_dsn(pg.DSN, tmp, port)
    finally:
        server.close()
        for t in tasks:
            t.cancel()
        with contextlib.suppress(Exception):
            await server.wait_closed()
        shutil.rmtree(tmp, ignore_errors=True)
