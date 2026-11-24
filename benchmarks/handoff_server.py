"""A minimal asyncio HTTP/1.1 server for the socket-handoff experiment.

ASGI hides the client socket, so the handoff cannot be shown through
Starlette and uvicorn without private APIs. This server is only big enough
to compare, over a real socket and with keep-alive, the ways of sending a
``db.export()`` body:

    GET /export?sql=...&format=csv|tsv|json|ndjson&mode=MODE[&max_rows=N]

``mode``: ``fallback`` (Python serialises), ``native`` (the backend's
bytes forwarded by this process: on a ``SqliteCWorker`` database, its
``export_transport``), or ``handoff`` (the C worker writes the body to the
client's socket). Every body is ``Transfer-Encoding: chunked``. An error
after the headers aborts the connection without the terminating chunk.

    uv run python -m benchmarks.handoff_server DB_PATH [--port 8790] [--transport raw]
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import urllib.parse
from typing import Any

from dblayer import Database
from dblayer.backends.cworker_handoff import chunk, handoff_export
from dblayer.core.export import ExportSpec

logger = logging.getLogger("handoff_server")


def _head(status: str, content_type: str, *, keep_alive: bool) -> bytes:
    return (
        f"HTTP/1.1 {status}\r\ncontent-type: {content_type}\r\ntransfer-encoding: chunked\r\n"
        f"connection: {'keep-alive' if keep_alive else 'close'}\r\n\r\n"
    ).encode()


async def _drained(writer: asyncio.StreamWriter) -> None:
    await writer.drain()
    transport = writer.transport
    # drain() returns below the high-water mark; the handoff needs the
    # buffer empty, or the worker's bytes would overtake ours.
    while transport.get_write_buffer_size():
        await asyncio.sleep(0.0005)


class Server:
    def __init__(self, db: Database) -> None:
        self.db = db
        self.requests = 0

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                headers: dict[str, str] = {}
                while True:
                    h = await reader.readline()
                    if h in (b"\r\n", b"\n", b""):
                        break
                    k, _, v = h.decode("latin-1").partition(":")
                    headers[k.strip().lower()] = v.strip()
                method, target, _version = line.decode("latin-1").split(" ", 2)
                keep_alive = headers.get("connection", "").lower() != "close"
                ok = await self.respond(method, target, writer, keep_alive)
                self.requests += 1
                if not ok or not keep_alive:
                    break
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()

    async def respond(self, method: str, target: str, writer: asyncio.StreamWriter, keep_alive: bool) -> bool:
        url = urllib.parse.urlsplit(target)
        q = dict(urllib.parse.parse_qsl(url.query))
        if url.path != "/export" or "sql" not in q:
            body = b"not found\n"
            writer.write(b"HTTP/1.1 404 Not Found\r\ncontent-length: %d\r\n\r\n%s" % (len(body), body))
            await writer.drain()
            return True
        fmt = q.get("format", "csv")
        shape = q.get("shape", "objects")
        spec = ExportSpec(fmt, shape)
        mode = q.get("mode", "native")
        max_rows = int(q["max_rows"]) if "max_rows" in q else None
        if mode == "handoff":
            return await self._handoff(q["sql"], spec, writer, keep_alive, max_rows)
        started = False
        try:
            async with self.db.export(q["sql"], format=fmt, json_shape=shape, max_rows=max_rows,
                                      native=mode != "fallback") as body:
                async for data in body:
                    if not started:
                        writer.write(_head("200 OK", spec.content_type, keep_alive=keep_alive))
                        started = True
                    writer.write(b"%x\r\n" % len(data))
                    writer.write(data)
                    writer.write(b"\r\n")
                    await writer.drain()
            if not started:
                writer.write(_head("200 OK", spec.content_type, keep_alive=keep_alive))
            writer.write(b"0\r\n\r\n")
            await writer.drain()
            return True
        except Exception as e:
            return self._failed(writer, started, e)

    async def _handoff(self, sql: str, spec: ExportSpec, writer: asyncio.StreamWriter, keep_alive: bool,
                       max_rows: int | None) -> bool:
        sock: Any = writer.get_extra_info("socket")
        started = False

        async def send_head(columns: Any) -> None:
            nonlocal started
            writer.write(_head("200 OK", spec.content_type, keep_alive=keep_alive))
            started = True
            writer.write(chunk(spec.head(columns)))
            await _drained(writer)

        try:
            result = await handoff_export(self.db, sql, client=sock, send_head=send_head, format=spec.format,
                                          json_shape=spec.json_shape, max_rows=max_rows)
            writer.write(chunk(spec.tail(result.truncated)) + b"0\r\n\r\n")
            await writer.drain()
            return True
        except Exception as e:
            return self._failed(writer, started, e)

    def _failed(self, writer: asyncio.StreamWriter, started: bool, e: BaseException) -> bool:
        if not started:
            body = f"{type(e).__name__}: {e}\n".encode()
            writer.write(b"HTTP/1.1 400 Bad Request\r\ncontent-length: %d\r\n\r\n%s" % (len(body), body))
            return True
        # Headers are out: abort, so the client sees a broken transfer.
        logger.warning("export failed mid-body: %r", e)
        writer.transport.abort()
        return False


async def serve(db: Database, host: str, port: int, ready: asyncio.Event | None = None) -> None:
    server = Server(db)
    srv = await asyncio.start_server(server.handle, host, port)
    if ready is not None:
        ready.set()
    async with srv:
        await srv.serve_forever()


def main() -> None:
    from dblayer.backends.sqlite_cworker import SqliteCWorker

    ap = argparse.ArgumentParser()
    ap.add_argument("path")
    ap.add_argument("--port", type=int, default=8790)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--transport", default="raw")
    args = ap.parse_args()
    db = Database(SqliteCWorker(args.path, export_transport=args.transport))
    asyncio.run(serve(db, args.host, args.port))


if __name__ == "__main__":
    main()
