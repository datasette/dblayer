"""psycopg 3: leaving an async ``cursor.copy()`` block of a COPY TO early
leaves the connection unusable.

After reading one row of ``COPY (...) TO STDOUT`` and exiting the ``async
with`` block, the next statement fails with "sending query failed: another
command is already in progress". Cancelling on the server
(``conn.cancel_safe()``) and reading until the copy ends, before exiting,
leaves the connection usable. Seen with psycopg 3.3.9 and PostgreSQL 18.3.

    uv run --all-extras python scripts/psycopg_copy_early_exit.py
Uses only the dblayer_test database (``DBLAYER_TEST_POSTGRES``).
"""

import asyncio
import os

import psycopg

DSN = os.environ.get("DBLAYER_TEST_POSTGRES", "postgresql:///dblayer_test?host=/tmp")
COPY = "COPY (select g from generate_series(1, 100000000) g) TO STDOUT"


async def main() -> None:
    assert "dblayer_test" in DSN
    conn = await psycopg.AsyncConnection.connect(DSN, autocommit=True)
    async with conn.cursor().copy(COPY) as cp:
        await cp.read()
    try:
        await conn.execute("select 1")
        print("early exit: connection fine")
    except psycopg.OperationalError as e:
        print("early exit: next statement fails:", e)
    await conn.close()
    async with await psycopg.AsyncConnection.connect(DSN, autocommit=True) as conn:
        async with conn.cursor().copy(COPY) as cp:
            await cp.read()
            await conn.cancel_safe()
            try:
                while await cp.read():
                    pass
            except psycopg.errors.QueryCanceled:
                pass
        await conn.execute("select 1")
        print("cancel_safe + drain: connection fine")


asyncio.run(main())
