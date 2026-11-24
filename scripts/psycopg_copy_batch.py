"""psycopg 3: how much of ``COPY ... TO STDOUT`` costs one await per row.

(a) psycopg's public API: ``await copy.read()`` once per row (what
    ``dblayer/backends/_pg_export.py`` does).
(b) libpq through ``psycopg.pq`` (psycopg's public low-level module):
    drain every row already in libpq's buffer with
    ``PQgetCopyData(async=1)`` in a plain loop, waiting on the socket only
    when it reports "would block".

Measured 2026-11-24, psycopg 3.3.9 (binary), PostgreSQL 18.3, load ~22:
1,000,000 narrow rows (26 MB) 0.65 s -> 0.17 s of this process's CPU;
1,000,000 wide rows (173 MB) 0.88 s -> 0.42 s. Run on the tables
``benchmarks/export.py`` creates in the ``bench_export`` schema:

    uv run --all-extras python scripts/psycopg_copy_batch.py [bench_export.narrow_1000000]
Uses only the dblayer_test database (``DBLAYER_TEST_POSTGRES``).
"""
import asyncio
import os
import resource
import sys
import time

import psycopg
from psycopg import pq

DSN = os.environ.get("DBLAYER_TEST_POSTGRES", "postgresql:///dblayer_test?host=/tmp")
assert "dblayer_test" in DSN
TABLE = sys.argv[1] if len(sys.argv) > 1 else "bench_export.narrow_1000000"
SQL = f"COPY (select * from {TABLE} order by 1) TO STDOUT (FORMAT csv)"


def cpu() -> float:
    r = resource.getrusage(resource.RUSAGE_SELF)
    return r.ru_utime + r.ru_stime


async def per_row() -> tuple[int, int]:
    async with await psycopg.AsyncConnection.connect(DSN, autocommit=True) as c:
        n = b = 0
        async with c.cursor().copy(SQL) as copy:
            while row := await copy.read():
                n += 1
                b += len(row)
        return n, b


async def drained() -> tuple[int, int]:
    loop = asyncio.get_running_loop()
    async with await psycopg.AsyncConnection.connect(DSN, autocommit=True) as c:
        pg = c.pgconn
        pg.send_query(SQL.encode())
        while pg.flush():
            pass
        n = b = 0
        chunks: list[bytes] = []
        fd = pg.socket
        # wait for the COPY OUT result
        while True:
            pg.consume_input()
            if not pg.is_busy():
                r = pg.get_result()
                assert r is not None and r.status == pq.ExecStatus.COPY_OUT, r and r.status
                break
            fut = loop.create_future()
            loop.add_reader(fd, fut.set_result, None)
            try:
                await fut
            finally:
                loop.remove_reader(fd)
        buf = bytearray()
        while True:
            nbytes, data = pg.get_copy_data(1)
            if nbytes > 0:
                n += 1
                b += nbytes
                buf += data
                if len(buf) >= 1 << 16:
                    chunks.append(bytes(buf))
                    buf.clear()
                continue
            if nbytes == -1:
                break
            if nbytes == -2:
                raise RuntimeError(pg.error_message)
            fut = loop.create_future()
            loop.add_reader(fd, fut.set_result, None)
            try:
                await fut
            finally:
                loop.remove_reader(fd)
            pg.consume_input()
        while (r := pg.get_result()) is not None:
            assert r.status == pq.ExecStatus.COMMAND_OK, r.status
        return n, b


async def main() -> None:
    for name, fn in (("per-row await (copy.read)", per_row), ("drain PQgetCopyData", drained)) * 2:
        c0, t0 = cpu(), time.perf_counter()
        n, b = await fn()
        print(f"{name:28} rows={n} bytes={b} cpu={cpu() - c0:.3f}s wall={time.perf_counter() - t0:.3f}s", flush=True)


asyncio.run(main())
