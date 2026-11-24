"""PostgreSQL test server helpers.

Tests use one dedicated database, ``dblayer_test``, from
``DBLAYER_TEST_POSTGRES`` (default: the local server's unix socket in
``/tmp``). Every backend made here gets its own schema, named
``dbltest_<pid>_<n>_<hex>``, and an ``application_name`` naming that schema,
so cleanup can find this test's own sessions and nothing else. Schemas are
dropped at teardown; schemas left by crashed runs (their pid no longer
alive) are dropped when the server is first probed.

Safety: the DSN must name the ``dblayer_test`` database; anything else is
refused before connecting.
"""

from __future__ import annotations

import asyncio
import functools
import itertools
import os
import threading
import urllib.parse
import uuid
from collections.abc import Coroutine
from typing import Any

DSN = os.environ.get("DBLAYER_TEST_POSTGRES", "postgresql:///dblayer_test?host=/tmp")
TEST_DATABASE = "dblayer_test"
SCHEMA_PREFIX = "dbltest_"
_counter = itertools.count(1)


def _dsn_database(dsn: str) -> str | None:
    parts = urllib.parse.urlsplit(dsn)
    query = urllib.parse.parse_qs(parts.query)
    if "dbname" in query:
        return query["dbname"][0]
    return urllib.parse.unquote(parts.path.lstrip("/")) or None


def run_sync[T](coro: Coroutine[Any, Any, T]) -> T:
    """Run a coroutine to completion on a fresh loop in another thread (safe
    from sync code, whether or not a loop is running here)."""
    out: list[Any] = []

    def target() -> None:
        try:
            out.append((True, asyncio.run(coro)))
        except BaseException as e:  # noqa: BLE001
            out.append((False, e))

    t = threading.Thread(target=target)
    t.start()
    t.join()
    ok, value = out[0]
    if not ok:
        raise value
    return value


async def connect(application_name: str = "dblayer-test-admin") -> Any:
    import asyncpg

    return await asyncpg.connect(
        DSN, timeout=5, server_settings={"application_name": application_name}
    )


@functools.cache
def unavailable() -> str | None:
    """``None`` when the test server can be used, else a skip reason."""
    try:
        import asyncpg  # noqa: F401
    except ImportError:
        return "asyncpg is not installed (uv run --extra postgres pytest)"
    if _dsn_database(DSN) != TEST_DATABASE:
        return f"DBLAYER_TEST_POSTGRES must name the {TEST_DATABASE!r} database; refusing {DSN!r}"

    async def probe() -> None:
        conn = await connect()
        try:
            name = await conn.fetchval("select current_database()")
            if name != TEST_DATABASE:
                raise RuntimeError(f"connected to {name!r}, not {TEST_DATABASE!r}")
            await _sweep_orphans(conn)
        finally:
            await conn.close()

    try:
        run_sync(probe())
    except Exception as e:  # noqa: BLE001
        return f"PostgreSQL test server not reachable at {DSN!r}: {type(e).__name__}: {e}"
    return None


async def _sweep_orphans(conn: Any) -> None:
    rows = await conn.fetch(
        "select nspname from pg_namespace where starts_with(nspname, $1)", SCHEMA_PREFIX
    )
    for (name,) in rows:
        try:
            pid = int(name.split("_")[1])
        except (IndexError, ValueError):
            continue
        if pid == os.getpid() or _pid_alive(pid):
            continue
        await conn.execute(f'DROP SCHEMA IF EXISTS "{name}" CASCADE')


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def new_schema() -> str:
    return f"{SCHEMA_PREFIX}{os.getpid()}_{next(_counter)}_{uuid.uuid4().hex[:6]}"


def app_name(schema: str) -> str:
    return f"dblayer-test:{schema}"


def make(**kwargs: Any) -> Any:
    """A ``Postgres`` backend on a new, empty schema (created lazily on the
    first connection)."""
    from dblayer.backends.postgres import Postgres

    schema = kwargs.pop("schema", None) or new_schema()
    kwargs.setdefault("application_name", app_name(schema))
    return Postgres(DSN, schema=schema, **kwargs)


async def drop_schema(schema: str) -> None:
    """Drop a test schema, first ending any session this test left open on
    it (only sessions whose application_name names that schema, opened by
    this role on this database)."""
    conn = await connect()
    try:
        await conn.execute("set lock_timeout = '5s'")
        await conn.execute(
            """select pg_terminate_backend(pid) from pg_stat_activity
                where application_name = $1 and datname = current_database()
                  and usename = current_user and pid <> pg_backend_pid()""",
            app_name(schema),
        )
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await conn.close()


async def sessions(schema: str) -> int:
    """Server sessions open for a test schema's backend, from
    ``pg_stat_activity``."""
    conn = await connect()
    try:
        return await conn.fetchval(
            """select count(*) from pg_stat_activity
                where application_name = $1 and datname = current_database()""",
            app_name(schema),
        )
    finally:
        await conn.close()
