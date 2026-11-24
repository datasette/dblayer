"""PostgreSQL backend on psycopg 3's native asyncio API (work in progress).

A second PostgreSQL backend, next to the asyncpg one in
``dblayer.backends.postgres``, so the two drivers can be compared on the
same conformance suite. Install with the ``psycopg`` extra.
"""

from __future__ import annotations

__all__ = ["PostgresPsycopg"]


class PostgresPsycopg:
    """Not implemented yet."""

    def __init__(self, dsn: str | None = None, *, schema: str = "public") -> None:
        self.dsn = dsn
        self.schema = schema

    async def open(self, *, write: bool) -> object:
        raise NotImplementedError
