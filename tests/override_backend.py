"""A test-only backend wrapper whose connections provide native overrides
(``run_operation``) for some named operations, so the conformance suite can
prove that the core dispatches to overrides, falls back when they decline,
and that both paths give identical results.

The overrides are deliberately different code: ``insert_rows`` uses one
multi-row ``INSERT ... VALUES`` statement instead of ``executemany``, and
``delete_rows`` one ``DELETE ... WHERE pk IN (...)``. Like PostgreSQL's
planned ``COPY`` path, they only take the simple cases and decline
(``NotImplemented``) the rest.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from typing import Any

from dblayer.backends.base import Backend, BackendConnection, NativeParams
from dblayer.core.operations import OperationResult


class OverrideBackend:
    def __init__(self, inner: Backend) -> None:
        self.inner = inner
        self.capabilities = inner.capabilities
        self.dialect = inner.dialect
        self.calls: Counter[str] = Counter()  # overrides that ran
        self.declined: Counter[str] = Counter()

    def __repr__(self) -> str:
        return f"OverrideBackend({self.inner!r})"

    async def open(self, *, write: bool) -> "OverrideConnection":
        return OverrideConnection(await self.inner.open(write=write), self)

    def change_hint(self) -> object:
        return self.inner.change_hint()

    async def close(self) -> None:
        await self.inner.close()


class OverrideConnection:
    def __init__(self, inner: BackendConnection, backend: OverrideBackend) -> None:
        self.inner = inner
        self.backend = backend

    # -- delegation ------------------------------------------------------------

    async def execute(self, sql: str, params: NativeParams, *, time_limit_ms, max_rows, max_result_bytes=None):
        return await self.inner.execute(
            sql, params, time_limit_ms=time_limit_ms, max_rows=max_rows, max_result_bytes=max_result_bytes
        )

    async def execute_many(self, sql: str, params_seq: Sequence[NativeParams]):
        return await self.inner.execute_many(sql, params_seq)

    def stream(self, sql, params, *, chunk_rows, time_limit_ms=None, max_result_bytes=None):
        return self.inner.stream(
            sql, params, chunk_rows=chunk_rows, time_limit_ms=time_limit_ms, max_result_bytes=max_result_bytes
        )

    async def begin(self, mode: str) -> None:
        await self.inner.begin(mode)

    async def commit(self) -> None:
        await self.inner.commit()

    async def rollback(self) -> None:
        await self.inner.rollback()

    async def schema(self):
        return await self.inner.schema()

    async def schema_version(self):
        return await self.inner.schema_version()

    async def interrupt(self) -> None:
        await self.inner.interrupt()

    async def close(self) -> None:
        await self.inner.close()

    # -- overrides ---------------------------------------------------------------

    async def run_operation(self, name: str, args: dict[str, Any]) -> object:
        method = getattr(self, f"_op_{name}", None)
        result = NotImplemented if method is None else await method(**args)
        if result is NotImplemented:
            self.backend.declined[name] += 1
        else:
            self.backend.calls[name] += 1
        return result

    async def _run(self, sql: str, params: list[Any]):
        return await self.inner.execute(sql, params, time_limit_ms=None, max_rows=0)

    async def _op_insert_rows(self, table, rows, pk=None, alter=False, ignore=False, replace=False):
        if alter or ignore or replace or not rows:
            return NotImplemented
        cols = list(rows[0].keys())
        if any(list(r.keys()) != cols for r in rows):
            return NotImplemented
        schema = await self.inner.schema()
        if table not in schema.tables:
            return NotImplemented  # the default path creates it
        d = self.backend.dialect
        one = "(" + ", ".join("?" for _ in cols) + ")"
        sql = (
            f"INSERT INTO {d.quote_identifier(table)} ({d.quote_identifiers(cols)}) "
            f"VALUES {', '.join(one for _ in rows)}"
        )
        r = await self._run(sql, [r[c] for r in rows for c in cols])
        # Returned as a dict, as a worker would send it.
        return {"rowcount": r.rowcount, "table_created": False, "columns_added": []}

    async def _op_delete_rows(self, table, pk_values_list):
        schema = await self.inner.schema()
        info = schema.tables.get(table)
        if info is None or len(info.primary_keys) != 1 or not pk_values_list:
            return NotImplemented
        if any(isinstance(v, list) for v in pk_values_list):
            return NotImplemented
        d = self.backend.dialect
        marks = ", ".join("?" for _ in pk_values_list)
        r = await self._run(
            f"DELETE FROM {d.quote_identifier(table)} "
            f"WHERE {d.quote_identifier(info.primary_keys[0])} IN ({marks})",
            list(pk_values_list),
        )
        return OperationResult(rowcount=r.rowcount)
