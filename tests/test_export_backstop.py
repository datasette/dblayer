"""The core's own time limit on ``db.export()`` is a backstop behind the
backend's: it fires ``BACKSTOP_GRACE_S`` later, and a connection whose
backend call it had to cancel is discarded, not reused.

Why: cancelling a backend call while the backend is itself stopping the
statement for the same budget left psycopg connections "busy" ("another
command is already in progress") and DuckDB worker connections with their
read transaction open, in about one run in five."""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

from dblayer import Database
from dblayer.backends.sqlite import SqliteInProcess
from dblayer.core import export as export_mod
from dblayer.errors import QueryInterrupted


class _DeafStream:
    """A stream whose backend ignores its time limit: every chunk takes
    ``delay`` seconds."""

    def __init__(self, inner: Any, delay: float) -> None:
        self.inner = inner
        self.delay = delay

    @property
    def columns(self) -> Any:
        return self.inner.columns

    def __aiter__(self) -> "_DeafStream":
        return self

    async def __anext__(self) -> Any:
        await asyncio.sleep(self.delay)
        return await self.inner.__anext__()

    async def aclose(self) -> None:
        await self.inner.aclose()


class _DeafConnection:
    def __init__(self, inner: Any, delay: float) -> None:
        self._inner = inner
        self._delay = delay

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def stream(self, sql: str, params: Any, **kw: Any) -> _DeafStream:
        return _DeafStream(self._inner.stream(sql, params, **kw), self._delay)


class _DeafBackend:
    def __init__(self, inner: Any, delay: float) -> None:
        self.inner = inner
        self.delay = delay
        self.capabilities = inner.capabilities
        self.dialect = inner.dialect

    async def open(self, *, write: bool) -> Any:
        return _DeafConnection(await self.inner.open(write=write), self.delay)

    def change_hint(self) -> object:
        return self.inner.change_hint()

    async def close(self) -> None:
        await self.inner.close()


@pytest.fixture
async def deaf_db(tmp_path):
    db = Database(_DeafBackend(SqliteInProcess(tmp_path / "x.db"), delay=2.0))
    yield db
    await db.close()


async def test_backstop_fires_after_the_grace_and_discards_the_connection(deaf_db):
    db = deaf_db
    await db.execute("select 1 as warm")
    closed_before = db.stats().closed_total
    t0 = time.perf_counter()
    with pytest.raises(QueryInterrupted):
        await db.export("select 1 as a", format="csv", time_limit_ms=100).read_all()
    elapsed = time.perf_counter() - t0
    # Not before the limit plus the grace; well before the 2 s the backend takes.
    assert elapsed >= 0.1 + export_mod.BACKSTOP_GRACE_S - 0.01
    assert elapsed < 1.5
    assert db.stats().in_use_read == 0
    assert db.stats().closed_total == closed_before + 1  # discarded, not pooled


async def test_backend_limit_firing_first_keeps_the_connection(tmp_path):
    db = Database(SqliteInProcess(tmp_path / "y.db"))
    try:
        await db.execute("select 1 as warm")
        closed_before = db.stats().closed_total
        sql = "with recursive c(x) as (select 1 union all select x + 1 from c) select x from c"
        with pytest.raises(QueryInterrupted):
            await db.export(sql, format="csv", time_limit_ms=100, native=False).read_all()
        assert db.stats().closed_total == closed_before
        assert (await db.execute("select 2 as a")).rows == [(2,)]
    finally:
        await db.close()
