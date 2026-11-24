"""Socket handoff: the C worker writes an export straight to the client's
connection (``dblayer.backends.cworker_handoff``, ``benchmarks/handoff_server.py``)."""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import signal
import socket
from pathlib import Path
from typing import Any

import pytest

httpx = pytest.importorskip("httpx")  # the demo dependency group; CI jobs without it skip this file

from dblayer import Database, QueryError, QueryInterrupted  # noqa: E402
from dblayer.backends.cworker_handoff import handoff_export  # noqa: E402
from dblayer.core.export import ExportSpec  # noqa: E402

from ..conftest import _cworker_available  # type: ignore[attr-defined]  # noqa: E402

pytestmark = pytest.mark.skipif(bool(_cworker_available()), reason=str(_cworker_available()))

SERIES = "with recursive c(x) as (select 1 union all select x + 1 from c where x < {n}) select x, 'row ' || x as t from c"
INFINITE = "with recursive c(x) as (select 1 union all select x + 1 from c) select x, 'row ' || x as t from c"


def _server_module() -> Any:
    path = Path(__file__).resolve().parents[2] / "benchmarks" / "handoff_server.py"
    spec = importlib.util.spec_from_file_location("handoff_server", path)
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


@pytest.fixture
async def db(tmp_path: Path) -> Any:
    from dblayer.backends.sqlite_cworker import SqliteCWorker

    d = Database(SqliteCWorker(tmp_path / "h.db"))
    yield d
    await d.close()


@pytest.fixture
async def server(db: Database) -> Any:
    mod = _server_module()
    srv = await asyncio.start_server(mod.Server(db).handle, "127.0.0.1", 0)
    port = srv.sockets[0].getsockname()[1]
    yield f"http://127.0.0.1:{port}"
    srv.close()
    await srv.wait_closed()


def q(sql: str, **kw: Any) -> dict[str, Any]:
    return {"sql": sql, **kw}


@pytest.mark.parametrize("fmt", ["csv", "tsv", "json", "ndjson"])
async def test_handoff_body_equals_native_and_fallback(server, db, fmt):
    sql = SERIES.format(n=30000)
    async with httpx.AsyncClient(base_url=server, timeout=30) as client:
        bodies = {}
        for mode in ("handoff", "native", "fallback"):
            r = await client.get("/export", params=q(sql, format=fmt, mode=mode))
            assert r.status_code == 200
            assert r.headers["transfer-encoding"] == "chunked"
            bodies[mode] = r.content
    assert bodies["handoff"] == bodies["native"] == bodies["fallback"]
    assert bodies["handoff"] == await db.export(sql, format=fmt).read_all()


async def test_keep_alive_after_handoff(server):
    # One connection, several requests: the socket comes back usable.
    limits = httpx.Limits(max_connections=1, max_keepalive_connections=1)
    async with httpx.AsyncClient(base_url=server, timeout=30, limits=limits) as client:
        for i in range(5):
            r = await client.get("/export", params=q(SERIES.format(n=1000 + i), format="csv", mode="handoff"))
            assert r.content.count(b"\r\n") == 1001 + i + (0 if i else 0)
        r = await client.get("/export", params=q(SERIES.format(n=3), format="json", mode="native"))
        assert json.loads(r.content)["rows"][2] == {"x": 3, "t": "row 3"}


async def test_truncation_in_json_tail(server):
    async with httpx.AsyncClient(base_url=server, timeout=30) as client:
        r = await client.get("/export", params=q(INFINITE, format="json", mode="handoff", max_rows=10))
    doc = json.loads(r.content)
    assert doc["truncated"] is True and len(doc["rows"]) == 10


async def test_error_mid_body_aborts_the_response(server, db):
    # A row error after many rows: the client must see a broken transfer.
    sql = "select x, case when x > 50000 then json_extract('not json', '$') else x end as y from (" + \
        SERIES.format(n=100000) + ")"
    async with httpx.AsyncClient(base_url=server, timeout=30) as client:
        with pytest.raises(httpx.RemoteProtocolError):
            await client.get("/export", params=q(sql, format="csv", mode="handoff"))
    assert db.stats().in_use_read == 0
    assert (await db.execute("select 1 as a")).rows == [(1,)]


async def test_error_before_body_is_a_400(server):
    async with httpx.AsyncClient(base_url=server, timeout=30) as client:
        r = await client.get("/export", params=q("select * from nope", format="csv", mode="handoff"))
    assert r.status_code == 400


async def test_client_disconnect_mid_download(server, db):
    host, port = server.removeprefix("http://").split(":")
    reader, writer = await asyncio.open_connection(host, int(port))
    import urllib.parse

    path = "/export?" + urllib.parse.urlencode(q(INFINITE, format="csv", mode="handoff"))
    writer.write(f"GET {path} HTTP/1.1\r\nhost: x\r\n\r\n".encode())
    got = 0
    while got < 200_000:
        got += len(await reader.read(65536))
    writer.transport.abort()  # the client goes away mid-body
    for _ in range(200):
        if db.stats().in_use_read == 0:
            break
        await asyncio.sleep(0.05)
    assert db.stats().in_use_read == 0
    # the worker survived (EPIPE, not SIGPIPE) and serves the next request
    assert (await db.execute("select 2 as a")).rows == [(2,)]


async def _socketpair_export(db: Database, sql: str, **kw: Any) -> tuple[socket.socket, asyncio.Task[Any]]:
    a, b = socket.socketpair()
    a.setblocking(False)
    b.setblocking(False)

    async def send_head(columns: Any) -> None:
        b.sendall(b"HEAD\r\n")

    task = asyncio.create_task(handoff_export(db, sql, client=b, send_head=send_head, **kw))
    task.add_done_callback(lambda _: b.close())
    return a, task


async def test_cancel_from_the_parent_while_the_worker_is_blocked(db):
    a, task = await _socketpair_export(db, INFINITE)
    loop = asyncio.get_running_loop()
    await loop.sock_recv(a, 1000)  # started; nobody reads now, so the worker blocks on a full socket
    await asyncio.sleep(0.3)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 5)
    a.close()
    assert db.stats().in_use_read == 0
    assert (await db.execute("select 3 as a")).rows == [(3,)]


async def test_time_limit_mid_body(db):
    a, task = await _socketpair_export(db, INFINITE, time_limit_ms=300)
    loop = asyncio.get_running_loop()

    async def drain() -> None:
        while await loop.sock_recv(a, 1 << 16):
            pass

    reader = asyncio.create_task(drain())
    with pytest.raises(QueryInterrupted):
        await asyncio.wait_for(task, 10)
    reader.cancel()
    a.close()
    assert (await db.execute("select 4 as a")).rows == [(4,)]


async def test_worker_killed_mid_body(db):
    from dblayer import WorkerCrashed

    a, task = await _socketpair_export(db, INFINITE)
    loop = asyncio.get_running_loop()
    await loop.sock_recv(a, 1000)
    victim = _busy_worker_pid(db)
    os.kill(victim, signal.SIGKILL)
    with pytest.raises((WorkerCrashed, QueryInterrupted, QueryError)):
        await asyncio.wait_for(task, 10)
    a.close()
    assert (await db.execute("select 5 as a")).rows == [(5,)]


async def test_worker_killed_mid_body_over_http(server, db):
    # The client must see a broken transfer (no terminating chunk), never a
    # complete-looking body that is merely short.
    async def kill_soon() -> None:
        for _ in range(200):
            pool = db._pool
            if pool is not None and pool._in_use:
                await asyncio.sleep(0.3)
                os.kill(_busy_worker_pid(db), signal.SIGKILL)
                return
            await asyncio.sleep(0.02)

    killer = asyncio.create_task(kill_soon())
    async with httpx.AsyncClient(base_url=server, timeout=30) as client:
        with pytest.raises(httpx.RemoteProtocolError):
            await client.get("/export", params=q(INFINITE, format="csv", mode="handoff"))
    await killer
    assert (await db.execute("select 6 as a")).rows == [(6,)]


def _busy_worker_pid(db: Database) -> int:
    pool = db._pool
    assert pool is not None and pool._in_use
    return next(iter(pool._in_use.values())).conn.pid


async def test_worker_keeps_no_copy_of_the_socket(db):
    a, task = await _socketpair_export(db, SERIES.format(n=10))
    loop = asyncio.get_running_loop()
    data = b""
    res = await asyncio.wait_for(task, 10)  # done: b is closed by the callback
    while True:
        part = await asyncio.wait_for(loop.sock_recv(a, 65536), 5)
        if not part:
            break  # EOF: no other process holds the other end
        data += part
    a.close()
    assert data.startswith(b"HEAD\r\n")
    assert res.row_count == 10 and data.endswith(b"\r\n")
