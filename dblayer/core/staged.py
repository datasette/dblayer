"""Staged import: an all-or-nothing load made of short transactions.

``async with db.staged_import(table, mode=...) as imp`` creates a hidden
staging table, ``imp.write(rows)`` inserts each chunk in its own short write
transaction, and leaving the block cleanly swaps the staging table in with
one more short transaction. On any error (or cancellation) the staging
table is dropped.

Staging tables are ordinary tables named
``_dblayer_import_<unix-seconds>_<random>``. The creation time is in the
name, so ``db.sweep_staged(max_age)`` can find orphans left by a crash with
nothing but a table listing: no metadata table, no engine-specific creation
timestamps, works the same on every backend. The ``_dblayer_`` prefix
hides them from ``table_names()``; they stay readable by name.

Modes (the final step, in one transaction):

* ``create``: rename staging to the target. Fails if the target exists
  (checked at the start and again at the swap).
* ``replace``: drop the target and rename staging into place, then replay
  the target's index and trigger definitions. The staging table is created
  with the target's definition (columns, types, NOT NULL, defaults, primary
  key, UNIQUE constraints, foreign keys). If the target does not exist it
  behaves like ``create``.
* ``append``: ``INSERT INTO target SELECT ... FROM staging``, then drop
  staging. The staging table copies the target's columns and keys (not its
  foreign keys, which the final copy into the target checks).

Dependents of a replaced table, on SQLite: views keep working (they refer
to the name; the swap rename runs in legacy mode so the check does not fail
while the old table is gone); the target's own triggers and indexes are
recreated; other tables' foreign keys keep referring to the name. With
``PRAGMA foreign_keys=ON`` and other tables referencing the target, the
swap raises ``NotSupported`` (the drop would cascade or fail) and the
import is rolled back. Other backends record their behaviour in the
conformance suite.
"""

from __future__ import annotations

import asyncio
import re
import time
import uuid
from collections.abc import Iterable, Mapping, Sequence
from datetime import timedelta
from types import TracebackType
from typing import TYPE_CHECKING, Any

from ..errors import QueryError, TableNotFound
from .operations import (
    OperationResult,
    check_drop_safe,
    check_rebuildable,
    create_table,
    recreate_dependents,
    table_definition,
)
from .schema import HIDDEN_PREFIX, ColumnDef

if TYPE_CHECKING:
    from .database import Database

__all__ = ["StagedImport", "STAGING_PREFIX", "staging_name", "staging_created_at"]

STAGING_PREFIX = HIDDEN_PREFIX + "import_"
_NAME = re.compile(re.escape(STAGING_PREFIX) + r"(\d+)_[0-9a-f]+")
MODES = ("create", "replace", "append")


def staging_name(created: float | None = None) -> str:
    created = time.time() if created is None else created
    return f"{STAGING_PREFIX}{int(created)}_{uuid.uuid4().hex[:8]}"


def staging_created_at(name: str) -> float | None:
    """Unix time a staging table was created, from its name; ``None`` if
    ``name`` is not a staging table."""
    m = _NAME.fullmatch(name)
    return float(m.group(1)) if m else None


def _seconds(max_age: float | timedelta) -> float:
    return max_age.total_seconds() if isinstance(max_age, timedelta) else float(max_age)


async def sweep_staged(db: "Database", max_age: float | timedelta) -> list[str]:
    """Drop staging tables older than ``max_age`` (seconds or timedelta),
    skipping imports still running in this ``Database`` object. Returns the
    names dropped. An import running in another process for longer than
    ``max_age`` would lose its staging table; pick ``max_age`` well above
    the longest import."""
    limit = _seconds(max_age)
    now = time.time()
    dropped: list[str] = []
    async with db.transaction() as tx:
        schema = await tx.schema()
        for name in list(schema.tables):
            created = staging_created_at(name)
            if created is None or name in db._staging_names():
                continue
            if now - created >= limit:
                await tx.execute(tx.dialect.drop_table_sql(name, if_exists=True))
                dropped.append(name)
    return dropped


class StagedImport:
    """Returned by ``db.staged_import()``; use as ``async with``."""

    def __init__(
        self,
        db: "Database",
        table: str,
        *,
        mode: str = "create",
        pk: str | Sequence[str] | None = None,
        alter: bool = False,
        columns: Mapping[str, str | type] | None = None,
    ) -> None:
        if mode not in MODES:
            raise ValueError(f"mode must be one of {MODES}, not {mode!r}")
        self._db = db
        self.table = table
        self.mode = mode
        self.pk = pk
        self.alter = alter
        self.columns = dict(columns) if columns else None
        self.staging = staging_name()
        self.rowcount = 0
        self._state = "new"

    def __repr__(self) -> str:
        return f"<StagedImport {self.mode} {self.table!r} via {self.staging!r} {self._state}>"

    async def __aenter__(self) -> "StagedImport":
        if self._state != "new":
            raise RuntimeError("a StagedImport can only be entered once")
        db = self._db
        db._staging_names().add(self.staging)
        try:
            async with db.transaction() as tx:
                schema = await tx.schema()
                target = schema.tables.get(self.table)
                exists = target is not None or self.table in schema.views
                if self.mode == "create" and exists:
                    raise QueryError(f"table {self.table!r} already exists", code="TABLE_EXISTS")
                if self.mode == "append" and target is None:
                    raise TableNotFound(f"no such table: {self.table}", code="NOT_FOUND")
                if target is not None:
                    if self.mode == "replace":
                        check_rebuildable(target)
                    defn = table_definition(target, include_foreign_keys=self.mode == "replace")
                    await tx.execute(
                        tx.dialect.create_table_sql(
                            self.staging,
                            defn.columns,
                            pk=defn.pk,
                            uniques=defn.uniques,
                            foreign_keys=defn.foreign_keys,
                        )
                    )
                elif self.columns:
                    await create_table(tx, self.staging, self.columns, pk=self.pk)
        except BaseException:
            db._staging_names().discard(self.staging)
            self._state = "failed"
            raise
        self._state = "open"
        return self

    async def write(self, rows: Iterable[Mapping[str, Any]]) -> OperationResult:
        """Insert one chunk into the staging table, in its own short write
        transaction. The first chunk creates the staging table if no
        structure was known (``create`` mode without ``columns``)."""
        if self._state != "open":
            raise RuntimeError(f"staged import is {self._state}")
        async with self._db.transaction() as tx:
            r = await tx.insert_rows(self.staging, rows, pk=self.pk, alter=self.alter)
        self.rowcount += r.rowcount
        return r

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        swapped = False
        try:
            if exc_type is None:
                await self._swap()
                swapped = True
        finally:
            self._state = "done" if swapped else "failed"
            if not swapped:
                # Shielded so a cancellation cannot leave it half done;
                # anything left behind is an orphan for sweep_staged().
                task = asyncio.ensure_future(self._drop_staging())
                try:
                    await asyncio.shield(task)
                except Exception:
                    pass
            self._db._staging_names().discard(self.staging)

    async def _drop_staging(self) -> None:
        async with self._db.transaction() as tx:
            await tx.execute(tx.dialect.drop_table_sql(self.staging, if_exists=True))

    async def _swap(self) -> None:
        d = self._db._backend.dialect
        async with self._db.transaction() as tx:
            schema = await tx.schema()
            staging = schema.tables.get(self.staging)
            if staging is None:
                if self.mode == "append":
                    return
                raise ValueError(
                    "staged import wrote no rows and was given no columns: nothing to create"
                )
            target = schema.tables.get(self.table)
            if self.mode == "append":
                if target is None:
                    raise TableNotFound(f"no such table: {self.table}", code="NOT_FOUND")
                cols = [c.name for c in staging.columns if not c.hidden]
                have = {c.casefold() for c in target.column_names}
                for c in staging.columns:
                    if not c.hidden and c.name.casefold() not in have:
                        # Only reachable with alter=True (chunks added it).
                        await tx.execute(d.add_column_sql(self.table, ColumnDef(c.name, c.type)))
                q = d.quote_identifiers(cols)
                await tx.execute(
                    f"INSERT INTO {d.quote_identifier(self.table)} ({q}) "
                    f"SELECT {q} FROM {d.quote_identifier(self.staging)}"
                )
                await tx.execute(d.drop_table_sql(self.staging))
                return
            if target is not None or self.table in schema.views:
                if self.mode == "create" or target is None:
                    raise QueryError(f"table {self.table!r} already exists", code="TABLE_EXISTS")
                await check_drop_safe(tx, schema, self.table)
                await tx.execute(d.drop_table_sql(self.table))
            for sql in d.swap_rename_sql(self.staging, self.table):
                await tx.execute(sql)
            if target is not None:
                await recreate_dependents(tx, target, schema)
