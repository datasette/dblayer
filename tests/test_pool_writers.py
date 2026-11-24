"""The pool's number of write slots follows ``capabilities.concurrent_writers``
(and an optional ``max_write_connections`` on the backend)."""

import asyncio
import dataclasses

import pytest

from dblayer import Database
from dblayer.backends.sqlite import SqliteInProcess


class _Conn:
    async def rollback(self) -> None:
        pass

    async def close(self) -> None:
        pass


class _Backend:
    def __init__(self, concurrent: bool, max_write: int | None = None) -> None:
        base = SqliteInProcess.memory()
        self.dialect = base.dialect
        self.capabilities = dataclasses.replace(base.capabilities, concurrent_writers=concurrent)
        if max_write is not None:
            self.max_write_connections = max_write

    async def open(self, *, write: bool) -> _Conn:
        return _Conn()

    def change_hint(self) -> object:
        return None

    async def close(self) -> None:
        pass


async def _concurrent_writers(db: Database, n: int) -> int:
    pool = db._get_pool()
    held = []
    for _ in range(n):
        try:
            held.append(await asyncio.wait_for(pool.acquire(write=True), 0.05))
        except TimeoutError:
            break
    for c in held:
        await pool.release(c)
    return len(held)


@pytest.mark.parametrize(
    "concurrent, max_write, readers, expected",
    [(False, None, 3, 1), (True, None, 3, 3), (True, None, 5, 5), (True, 2, 5, 2), (False, 4, 3, 1)],
)
async def test_write_slots(concurrent, max_write, readers, expected):
    db = Database(_Backend(concurrent, max_write), max_read_connections=readers)
    assert await _concurrent_writers(db, 8) == expected
    assert db._get_pool().max_write_connections == expected
    stats = db.stats()
    assert stats.idle_write == expected and stats.in_use_write == 0
    await db.close()
