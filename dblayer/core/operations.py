"""Default implementation of each named operation.

Every function here takes a *transaction-like* object (``TxLike``) as its
first argument and uses nothing else: ``execute``, ``execute_many``,
``schema()``, plus ``dialect`` and ``capabilities``. They contain no
backend-specific SQL; anything that differs between engines comes from the
``Dialect``. That keeps them runnable unchanged on PostgreSQL and DuckDB, and
inside the SQLite worker process against a local transaction object.

The functions never commit or roll back. Called through ``tx.<op>()`` they
run inside the caller's transaction; called through ``db.<op>()`` the core
opens one (or, for ``insert_rows``/``upsert_rows``, one per batch).

Arguments arrive in the plain form produced by ``normalise_args`` (lists,
dicts, strings, numbers), so an ``args`` dict can be sent to a backend's
``run_operation`` override or across a process boundary unchanged.
"""

from __future__ import annotations

import itertools
import re
import uuid
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from ..backends.base import Capabilities, Dialect
from ..errors import NotSupported, TableNotFound
from .params import Params
from .results import Results, WriteResult
from .schema import HIDDEN_PREFIX, ColumnDef, ForeignKey, Schema, TableInfo

__all__ = [
    "TxLike",
    "OperationResult",
    "OPERATIONS",
    "UNSET",
    "normalise_args",
    "create_table",
    "drop_table",
    "rename_table",
    "add_column",
    "alter_table",
    "create_index",
    "insert_rows",
    "upsert_rows",
    "update_row",
    "delete_rows",
    "portable_type",
]


class TxLike(Protocol):
    """What a default operation needs. ``dblayer.core.transactions.Transaction``
    implements it; so can anything else (the worker's local transaction)."""

    @property
    def dialect(self) -> Dialect: ...
    @property
    def capabilities(self) -> Capabilities: ...
    async def execute(self, sql: str, params: Params = None) -> Results: ...
    async def execute_many(self, sql: str, params_seq: Iterable[Params]) -> WriteResult: ...
    async def schema(self) -> Schema: ...


@dataclass(frozen=True, slots=True)
class OperationResult:
    """What every named operation returns.

    ``rowcount``: rows inserted, updated or deleted (as the engine counts
    them: an upsert that updated counts 1; a row skipped by ``ignore`` counts
    0). ``table_created``: the operation created its table.
    ``columns_added``: columns added by ``alter=True``.
    """

    rowcount: int = 0
    table_created: bool = False
    columns_added: tuple[str, ...] = ()

    def merge(self, other: "OperationResult") -> "OperationResult":
        added = self.columns_added + tuple(
            c for c in other.columns_added if c not in self.columns_added
        )
        return OperationResult(
            rowcount=self.rowcount + other.rowcount,
            table_created=self.table_created or other.table_created,
            columns_added=added,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "rowcount": self.rowcount,
            "table_created": self.table_created,
            "columns_added": list(self.columns_added),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "OperationResult":
        return cls(
            rowcount=int(data.get("rowcount", 0)),
            table_created=bool(data.get("table_created", False)),
            columns_added=tuple(data.get("columns_added", ())),
        )


class _Unset:
    def __repr__(self) -> str:
        return "UNSET"


UNSET: Any = _Unset()
"""``alter_table(pk=UNSET)`` keeps the current primary key."""


# -- small helpers -------------------------------------------------------------

_PY_TYPES: dict[type, str] = {
    bool: "integer",
    int: "integer",
    float: "float",
    str: "text",
    bytes: "blob",
    bytearray: "blob",
    memoryview: "blob",
}


def portable_type(value: Any) -> str:
    """The portable column type for a Python value (``text`` for ``None``
    and anything unrecognised)."""
    for t, name in _PY_TYPES.items():
        if isinstance(value, t):
            return name
    return "text"


def _type_name(t: str | type) -> str:
    if isinstance(t, type):
        try:
            return _PY_TYPES[t]
        except KeyError:
            raise TypeError(f"no portable column type for {t.__name__}") from None
    return t


def _pk_tuple(pk: str | Sequence[str] | None) -> tuple[str, ...]:
    if pk is None:
        return ()
    if isinstance(pk, str):
        return (pk,)
    return tuple(pk)


def _table(schema: Schema, name: str) -> TableInfo:
    info = schema.tables.get(name)
    if info is None:
        raise TableNotFound(f"no such table: {name}", code="NOT_FOUND")
    return info


def _fk_from_spec(
    spec: Any, table: str, pk_cols: Sequence[str], schema: Schema
) -> ForeignKey:
    """Accepts a ``ForeignKey``, a dict with ``columns``/``other_table``/
    ``other_columns``, or a tuple ``(column, other_table[, other_column])``."""
    if isinstance(spec, ForeignKey):
        return spec
    if isinstance(spec, Mapping):
        columns = tuple(spec["columns"])
        other = spec["other_table"]
        other_cols = tuple(spec.get("other_columns") or ())
    else:
        parts = list(spec)
        if len(parts) not in (2, 3):
            raise ValueError(f"foreign key must be (column, other_table[, other_column]): {spec!r}")
        columns = (parts[0],)
        other = parts[1]
        other_cols = (parts[2],) if len(parts) == 3 and parts[2] is not None else ()
    if not other_cols:
        if other == table:
            target_pk = tuple(pk_cols)
        else:
            target_pk = _table(schema, other).primary_keys
        if len(target_pk) != len(columns):
            raise ValueError(
                f"cannot infer the referenced column for a foreign key to {other!r}; "
                "pass it explicitly"
            )
        other_cols = target_pk
    return ForeignKey(columns=columns, other_table=other, other_columns=other_cols)


def _column_types(rows: Sequence[Mapping[str, Any]]) -> dict[str, str]:
    """Columns in order of first appearance, typed by their first non-None
    value."""
    types: dict[str, str | None] = {}
    for row in rows:
        for k, v in row.items():
            if types.get(k) is None:
                types[k] = None if v is None else portable_type(v)
    return {k: (t or "text") for k, t in types.items()}


def _groups(
    rows: Sequence[Mapping[str, Any]],
) -> Iterable[tuple[tuple[str, ...], list[Mapping[str, Any]]]]:
    """Consecutive runs of rows with the same keys. Rows are inserted with
    exactly the keys they have, so missing keys get the column default."""
    for cols, group in itertools.groupby(rows, key=lambda r: tuple(r.keys())):
        yield cols, list(group)


def _key_columns(dialect: Dialect, info: TableInfo) -> tuple[str, ...]:
    if info.primary_keys:
        return info.primary_keys
    if dialect.rowid_column and _has_rowid(info):
        return (dialect.rowid_column,)
    raise NotSupported(
        f"table {info.name!r} has no primary key, and this backend has no implicit row id"
    )


def _key_values(keys: Sequence[str], pk_values: Any) -> list[Any]:
    if len(keys) == 1 and not isinstance(pk_values, (list, tuple)):
        return [pk_values]
    if not isinstance(pk_values, (list, tuple)):
        raise ValueError(f"table key is {list(keys)}; pass a sequence of {len(keys)} values")
    values = list(pk_values)
    if len(values) != len(keys):
        raise ValueError(f"expected {len(keys)} primary key value(s) for {list(keys)}, got {values!r}")
    return values


def _where(dialect: Dialect, keys: Sequence[str]) -> str:
    return " AND ".join(f"{dialect.quote_identifier(k)} = ?" for k in keys)


def _table_options(info: TableInfo) -> str:
    """Text after the closing parenthesis of a CREATE TABLE (SQLite's
    ``WITHOUT ROWID`` / ``STRICT``)."""
    d = info.definition or ""
    return d[d.rfind(")") + 1 :].upper() if ")" in d else ""


def _has_rowid(info: TableInfo) -> bool:
    return not info.virtual and not re.search(r"WITHOUT\s+ROWID", _table_options(info))


# -- create / drop / rename / add / index --------------------------------------


async def create_table(
    tx: TxLike,
    name: str,
    columns: Mapping[str, str | type],
    *,
    pk: str | Sequence[str] | None = None,
    not_null: Iterable[str] = (),
    defaults: Mapping[str, Any] | None = None,
    foreign_keys: Iterable[Any] = (),
    if_not_exists: bool = False,
) -> OperationResult:
    """``columns`` maps names to portable types (``integer``, ``float``,
    ``text``, ``blob``), Python types, or native type strings passed
    through. ``defaults`` holds Python values, rendered by
    ``Dialect.literal``. ``foreign_keys`` items are ``(column, other_table)``,
    ``(column, other_table, other_column)`` or ``ForeignKey``. Without
    ``if_not_exists`` an existing table is the engine's error."""
    d = tx.dialect
    schema = await tx.schema()
    if if_not_exists and (name in schema.tables or name in schema.views):
        return OperationResult()
    pk_cols = _pk_tuple(pk)
    defaults = dict(defaults or {})
    not_null_set = set(not_null)
    unknown = (set(pk_cols) | not_null_set | set(defaults)) - set(columns)
    if unknown:
        raise ValueError(f"unknown column(s) {sorted(unknown)} in pk/not_null/defaults")
    cols = [
        ColumnDef(
            name=c,
            type=_type_name(t),
            not_null=c in not_null_set,
            default=d.literal(defaults[c]) if c in defaults else None,
        )
        for c, t in columns.items()
    ]
    fks = [_fk_from_spec(fk, name, pk_cols, schema) for fk in foreign_keys]
    await tx.execute(d.create_table_sql(name, cols, pk=pk_cols, foreign_keys=fks))
    return OperationResult(table_created=True)


async def drop_table(tx: TxLike, name: str, *, if_exists: bool = False) -> OperationResult:
    await tx.execute(tx.dialect.drop_table_sql(name, if_exists=if_exists))
    return OperationResult()


async def rename_table(tx: TxLike, old: str, new: str) -> OperationResult:
    await tx.execute(tx.dialect.rename_table_sql(old, new))
    return OperationResult()


async def add_column(
    tx: TxLike, table: str, name: str, type: str | type = "text"
) -> OperationResult:
    await tx.execute(tx.dialect.add_column_sql(table, ColumnDef(name, _type_name(type))))
    return OperationResult(columns_added=(name,))


async def create_index(
    tx: TxLike,
    table: str,
    columns: Sequence[str] | str,
    *,
    name: str | None = None,
    unique: bool = False,
    if_not_exists: bool = False,
) -> OperationResult:
    """Default name: ``idx_<table>_<col>_<col>`` (as sqlite-utils)."""
    cols = [columns] if isinstance(columns, str) else list(columns)
    if not cols:
        raise ValueError("create_index needs at least one column")
    name = name or "idx_{}_{}".format(table.replace(" ", "_"), "_".join(cols))
    await tx.execute(
        tx.dialect.create_index_sql(name, table, cols, unique=unique, if_not_exists=if_not_exists)
    )
    return OperationResult()


# -- rows ----------------------------------------------------------------------


async def _ensure_table(
    tx: TxLike,
    table: str,
    rows: Sequence[Mapping[str, Any]],
    pk: tuple[str, ...],
    alter: bool,
) -> tuple[bool, tuple[str, ...]]:
    """Create ``table`` from the rows if it is missing; with ``alter``, add
    columns the rows have and the table lacks. Returns (created, added)."""
    schema = await tx.schema()
    types = _column_types(rows)
    info = schema.tables.get(table)
    if info is None:
        if table in schema.views:
            return False, ()  # let the engine say a view is not insertable
        for p in pk:
            types.setdefault(p, "integer")
        await create_table(tx, table, types, pk=pk)
        return True, ()
    if not alter:
        return False, ()
    existing = {c.casefold() for c in info.column_names}
    missing = [c for c in types if c.casefold() not in existing]
    for c in missing:
        await tx.execute(tx.dialect.add_column_sql(table, ColumnDef(c, types[c])))
    return False, tuple(missing)


async def insert_rows(
    tx: TxLike,
    table: str,
    rows: Iterable[Mapping[str, Any]],
    *,
    pk: str | Sequence[str] | None = None,
    alter: bool = False,
    ignore: bool = False,
    replace: bool = False,
) -> OperationResult:
    """Insert rows (dicts), creating the table from them if it is missing.

    * ``alter=True`` first adds columns the rows have and the table lacks.
    * ``ignore=True`` skips rows that conflict with an existing row on a
      primary key or unique constraint (``ON CONFLICT DO NOTHING``). Other
      violations (NOT NULL, CHECK, foreign keys) still raise.
    * ``replace=True`` replaces the existing row with the same primary key
      (``pk`` or the table's) with the new row: supplied columns take the new
      values, unsupplied ones their defaults. Implemented as ``ON CONFLICT
      (pk) DO UPDATE`` of every non-key column, so the row keeps its identity
      (rowid) and no delete happens. Conflicts on other unique constraints
      raise ``IntegrityError``.
    * ``pk`` is used when creating the table, and as the conflict target.
    """
    if ignore and replace:
        raise ValueError("pass at most one of ignore and replace")
    rows = list(rows)
    if not rows:
        return OperationResult()
    pk_cols = _pk_tuple(pk)
    created, added = await _ensure_table(tx, table, rows, pk_cols, alter)
    d = tx.dialect
    update_cols: list[str] = []
    if replace:
        info = _table(await tx.schema(), table)
        pk_cols = pk_cols or info.primary_keys
        if not pk_cols:
            raise ValueError(f"replace=True needs a primary key; table {table!r} has none")
        update_cols = [
            c.name
            for c in info.columns
            if not c.hidden and not c.generated and c.name not in pk_cols
        ]
    total = 0
    for cols, group in _groups(rows):
        if replace:
            sql = d.upsert_sql(table, cols, pk_cols, update_columns=update_cols)
        elif ignore:
            sql = d.insert_sql(table, cols, conflict="ignore")
        else:
            sql = d.insert_sql(table, cols)
        r = await tx.execute_many(sql, [[row[c] for c in cols] for row in group])
        total += max(r.rowcount, 0)
    return OperationResult(rowcount=total, table_created=created, columns_added=added)


async def upsert_rows(
    tx: TxLike,
    table: str,
    rows: Iterable[Mapping[str, Any]],
    *,
    pk: str | Sequence[str] | None = None,
    alter: bool = False,
) -> OperationResult:
    """Insert each row, or update the supplied columns of the existing row
    with the same primary key (``pk`` or the table's). Columns a row does not
    mention are left alone. Creates the table if missing (then ``pk`` is
    required)."""
    rows = list(rows)
    if not rows:
        return OperationResult()
    pk_cols = _pk_tuple(pk)
    schema = await tx.schema()
    if table not in schema.tables and not pk_cols:
        raise ValueError("upsert_rows needs pk= to create a missing table")
    created, added = await _ensure_table(tx, table, rows, pk_cols, alter)
    if not pk_cols:
        pk_cols = _table(await tx.schema(), table).primary_keys
        if not pk_cols:
            raise ValueError(f"upsert_rows needs a primary key; table {table!r} has none")
    for row in rows:
        missing = [p for p in pk_cols if row.get(p) is None]
        if missing:
            raise ValueError(f"row has no value for primary key column(s) {missing}: {row!r}")
    d = tx.dialect
    total = 0
    for cols, group in _groups(rows):
        sql = d.upsert_sql(table, cols, pk_cols)
        r = await tx.execute_many(sql, [[row[c] for c in cols] for row in group])
        total += max(r.rowcount, 0)
    return OperationResult(rowcount=total, table_created=created, columns_added=added)


async def update_row(
    tx: TxLike, table: str, pk_values: Any, updates: Mapping[str, Any]
) -> OperationResult:
    """Update one row addressed by primary key (a value, or a sequence for
    a compound key; the rowid for tables without one, where the backend has
    implicit row ids). ``rowcount`` is 0 if no such row exists."""
    if not updates:
        return OperationResult()
    d = tx.dialect
    info = _table(await tx.schema(), table)
    keys = _key_columns(d, info)
    values = _key_values(keys, pk_values)
    sets = ", ".join(f"{d.quote_identifier(c)} = ?" for c in updates)
    r = await tx.execute(
        f"UPDATE {d.quote_identifier(table)} SET {sets} WHERE {_where(d, keys)}",
        [*updates.values(), *values],
    )
    return OperationResult(rowcount=max(r.rowcount, 0))


async def delete_rows(
    tx: TxLike, table: str, pk_values_list: Iterable[Any]
) -> OperationResult:
    """Delete rows by primary key (see ``update_row`` for key forms)."""
    d = tx.dialect
    info = _table(await tx.schema(), table)
    keys = _key_columns(d, info)
    params = [_key_values(keys, v) for v in pk_values_list]
    if not params:
        return OperationResult()
    r = await tx.execute_many(
        f"DELETE FROM {d.quote_identifier(table)} WHERE {_where(d, keys)}", params
    )
    return OperationResult(rowcount=max(r.rowcount, 0))


# -- alter ---------------------------------------------------------------------


async def alter_table(
    tx: TxLike,
    table: str,
    *,
    rename: Mapping[str, str] | None = None,
    drop: Iterable[str] = (),
    types: Mapping[str, str | type] | None = None,
    pk: Any = UNSET,
) -> OperationResult:
    """Rename, drop or retype columns, or change the primary key.

    ``rename``, ``drop`` and ``types`` use current column names; ``pk`` uses
    the final names (``None`` means no primary key). Changes listed in
    ``dialect.native_alter`` are applied with native ``ALTER`` statements
    first; whatever is left is done by rebuilding the table (see
    ``rebuild_table``).
    """
    d = tx.dialect
    rename = dict(rename or {})
    drop_list = list(drop)
    types_map = {k: _type_name(v) for k, v in (types or {}).items()}
    info = _table(await tx.schema(), table)
    names = set(info.column_names)
    unknown = (set(rename) | set(drop_list) | set(types_map)) - names
    if unknown:
        raise ValueError(f"no such column(s) in {table!r}: {sorted(unknown)}")
    if set(rename) & set(drop_list):
        raise ValueError("a column cannot be both renamed and dropped")
    if not (rename or drop_list or types_map or pk is not UNSET):
        return OperationResult()
    # Checked before anything runs: a native DROP COLUMN would silently
    # drop the primary key along with its column.
    if pk is UNSET and any(p in drop_list for p in info.primary_keys):
        raise ValueError("cannot drop a primary key column without passing a new pk")
    if pk is not UNSET:
        final = {rename.get(c, c) for c in names if c not in drop_list}
        bad = set(_pk_tuple(pk)) - final
        if bad:
            raise ValueError(f"pk column(s) {sorted(bad)} not in the table")

    native = d.native_alter_for(info, rename=rename, drop=drop_list, types=types_map)
    if rename and "rename" in native:
        for old, new in rename.items():
            await tx.execute(d.rename_column_sql(table, old, new))
        drop_list = [rename.get(c, c) for c in drop_list]
        types_map = {rename.get(k, k): v for k, v in types_map.items()}
        rename = {}
    if types_map and "types" in native:
        for c, t in types_map.items():
            await tx.execute(d.alter_column_type_sql(table, c, t))
        types_map = {}
    if drop_list and "drop" in native:
        for c in drop_list:
            await tx.execute(d.drop_column_sql(table, c))
        drop_list = []
    if pk is not UNSET and "pk" in d.native_alter and not (rename or drop_list or types_map):
        current = _table(await tx.schema(), table)
        new_pk = _pk_tuple(pk)
        if new_pk != current.primary_keys:
            constraint = next((i.name for i in current.indexes if i.origin == "pk"), None)
            for sql in d.set_primary_key_sql(table, constraint, new_pk):
                await tx.execute(sql)
        pk = UNSET
    if rename or drop_list or types_map or pk is not UNSET:
        await rebuild_table(tx, table, rename=rename, drop=drop_list, types=types_map, pk=pk)
    return OperationResult()


@dataclass(frozen=True, slots=True)
class TableDefinition:
    """The parts of a table that the rebuild and the staged import recreate."""

    columns: tuple[ColumnDef, ...]
    pk: tuple[str, ...]
    uniques: tuple[tuple[str, ...], ...]
    foreign_keys: tuple[ForeignKey, ...]


def check_rebuildable(info: TableInfo) -> None:
    """Refuse tables whose features the generated CREATE TABLE would lose,
    rather than silently dropping them (sqlite-utils ``transform`` drops
    CHECK constraints, STRICT and WITHOUT ROWID)."""
    if info.virtual:
        raise NotSupported(f"cannot rebuild virtual table {info.name!r}")
    if any(c.generated for c in info.columns):
        raise NotSupported(f"cannot rebuild {info.name!r}: it has generated columns")
    opts = _table_options(info)
    if "WITHOUT" in opts or "STRICT" in opts:
        raise NotSupported(f"cannot rebuild {info.name!r}: WITHOUT ROWID / STRICT tables are not supported")
    if info.checks or re.search(r"\bCHECK\s*\(", info.definition or "", re.IGNORECASE):
        raise NotSupported(f"cannot rebuild {info.name!r}: CHECK constraints would be lost")


def table_definition(
    info: TableInfo,
    *,
    rename: Mapping[str, str] | None = None,
    drop: Iterable[str] = (),
    types: Mapping[str, str] | None = None,
    pk: Any = UNSET,
    include_foreign_keys: bool = True,
) -> TableDefinition:
    rename = dict(rename or {})
    drop_set = set(drop)
    types = dict(types or {})

    def new(c: str) -> str:
        return rename.get(c, c)

    keep = [c for c in info.columns if not c.hidden and c.name not in drop_set]
    cols = tuple(
        ColumnDef(new(c.name), types.get(c.name, c.type), c.not_null, c.default) for c in keep
    )
    final = {c.name for c in cols}
    if pk is UNSET:
        if any(p in drop_set for p in info.primary_keys):
            raise ValueError("cannot drop a primary key column without passing a new pk")
        pk_cols = tuple(new(p) for p in info.primary_keys)
    else:
        pk_cols = _pk_tuple(pk)
        if set(pk_cols) - final:
            raise ValueError(f"pk column(s) {sorted(set(pk_cols) - final)} not in the table")

    def mapped(columns: Sequence[str | None]) -> tuple[str, ...] | None:
        if any(c is None or c in drop_set for c in columns):
            return None
        return tuple(new(c) for c in columns if c is not None)

    uniques = []
    for idx in info.indexes:
        if idx.origin == "u":
            m = mapped(idx.columns)
            if m:
                uniques.append(m)
    fks = []
    if include_foreign_keys:
        for fk in info.foreign_keys:
            m = mapped(fk.columns)
            if not m:
                continue
            other_cols = fk.other_columns
            if fk.other_table == info.name:
                om = mapped(other_cols)
                if not om:
                    continue
                other_cols = om
            fks.append(ForeignKey(m, fk.other_table, tuple(other_cols), fk.on_update, fk.on_delete))
    return TableDefinition(cols, pk_cols, tuple(uniques), tuple(fks))


async def check_drop_safe(tx: TxLike, schema: Schema, table: str) -> list[str]:
    """Raise ``NotSupported`` if dropping ``table`` would run ON DELETE
    actions or fail on other tables' foreign keys (SQLite with
    ``PRAGMA foreign_keys=ON``: even with ``defer_foreign_keys`` the
    implicit DELETE of DROP TABLE cascades, and the deferred-violation
    counter is not reset when the rows come back). Returns the names of
    tables that reference ``table``."""
    target = table.casefold()
    referrers = [
        t.name
        for t in schema.tables.values()
        if t.name.casefold() != target
        and any(fk.other_table.casefold() == target for fk in t.foreign_keys)
    ]
    sql = tx.dialect.foreign_keys_enforced_sql
    if referrers and sql is not None:
        r = await tx.execute(sql)
        if r.rows and r.rows[0][0]:
            raise NotSupported(
                f"cannot drop and recreate {table!r} inside a transaction while foreign "
                f"keys are enforced: {', '.join(referrers)} reference it, and the drop "
                "would delete or reject their rows"
            )
    return referrers


async def recreate_dependents(
    tx: TxLike, info: TableInfo, schema: Schema, *, drop: Iterable[str] = ()
) -> None:
    """Re-run the CREATE statements of ``info``'s own indexes and triggers
    (both are dropped with the table). Indexes on dropped columns are left
    out, as a native DROP COLUMN would drop them."""
    drop_set = set(drop)
    for idx in info.indexes:
        if idx.origin != "c":
            continue
        if any(c in drop_set for c in idx.columns if c is not None):
            continue
        if idx.definition is None:
            raise NotSupported(f"index {idx.name!r} has no CREATE statement to replay")
        await tx.execute(idx.definition)
    for trig in schema.triggers.values():
        if trig.table.casefold() == info.name.casefold() and trig.definition:
            await tx.execute(trig.definition)


async def rebuild_table(
    tx: TxLike,
    table: str,
    *,
    rename: Mapping[str, str] | None = None,
    drop: Iterable[str] = (),
    types: Mapping[str, str] | None = None,
    pk: Any = UNSET,
) -> None:
    """The sqlite-utils ``transform`` procedure: create a new table with the
    changed definition, copy the rows across (rowids too, for rowid tables
    that stay without an explicit key), drop the old table, rename the new
    one into place (``dialect.swap_rename_sql``), then replay the old
    table's index and trigger definitions. Views keep working because they
    refer to the table by name. Other tables' foreign keys keep pointing at
    the name; see ``check_drop_safe`` for when that is refused.

    Differences from sqlite-utils: indexes on dropped columns are dropped
    instead of raising; triggers are recreated; tables with CHECK
    constraints, generated columns, STRICT or WITHOUT ROWID are refused
    instead of silently losing those features; foreign-key enforcement makes
    it refuse (``NotSupported``) rather than defer, because deferring does not
    work inside a transaction.
    """
    d = tx.dialect
    rename = dict(rename or {})
    drop_list = list(drop)
    schema = await tx.schema()
    info = _table(schema, table)
    check_rebuildable(info)
    await check_drop_safe(tx, schema, table)
    if rename and (schema.triggers or any(i.origin == "c" for i in info.indexes)):
        # Only reachable on dialects without native RENAME COLUMN: replayed
        # definitions would still name the old column.
        raise NotSupported("renaming columns of an indexed or triggered table needs native RENAME COLUMN")
    defn = table_definition(info, rename=rename, drop=drop_list, types=types, pk=pk)
    tmp = f"{HIDDEN_PREFIX}alter_{uuid.uuid4().hex[:12]}"
    src = [c.name for c in info.columns if not c.hidden and c.name not in set(drop_list)]
    dst = [rename.get(c, c) for c in src]
    if d.rowid_column and not defn.pk and _has_rowid(info):
        src.insert(0, d.rowid_column)
        dst.insert(0, d.rowid_column)
    statements = [
        d.create_table_sql(
            tmp, defn.columns, pk=defn.pk, uniques=defn.uniques, foreign_keys=defn.foreign_keys
        ),
        f"INSERT INTO {d.quote_identifier(tmp)} ({d.quote_identifiers(dst)}) "
        f"SELECT {d.quote_identifiers(src)} FROM {d.quote_identifier(table)}",
        d.drop_table_sql(table),
        *d.swap_rename_sql(tmp, table),
    ]
    for sql in statements:
        await tx.execute(sql)
    await recreate_dependents(tx, info, schema, drop=drop_list)


# -- registry and argument normalisation ----------------------------------------

type Operation = Callable[..., Awaitable[OperationResult]]

OPERATIONS: dict[str, Operation] = {
    "create_table": create_table,
    "drop_table": drop_table,
    "rename_table": rename_table,
    "add_column": add_column,
    "alter_table": alter_table,
    "create_index": create_index,
    "insert_rows": insert_rows,
    "upsert_rows": upsert_rows,
    "update_row": update_row,
    "delete_rows": delete_rows,
}
"""Operation name -> default implementation. Every function takes
``(tx, **args)``."""


def _plain_key(v: Any) -> Any:
    return list(v) if isinstance(v, tuple) else v


def normalise_args(name: str, args: dict[str, Any]) -> dict[str, Any]:
    """Turn caller-friendly arguments into plain values (lists, dicts,
    strings) that can be sent to ``run_operation`` or over a wire. Python
    types become portable type names, iterables become lists, tuples
    become lists, ``ForeignKey`` objects become dicts and ``UNSET`` keys are
    left out."""
    out = {k: v for k, v in args.items() if v is not UNSET}
    if name == "create_table":
        out["columns"] = {k: _type_name(t) for k, t in out["columns"].items()}
        out["pk"] = list(_pk_tuple(out.get("pk"))) or None
        out["not_null"] = list(out.get("not_null") or ())
        out["defaults"] = dict(out.get("defaults") or {})
        fks = []
        for fk in out.get("foreign_keys") or ():
            if isinstance(fk, ForeignKey):
                fks.append(
                    {
                        "columns": list(fk.columns),
                        "other_table": fk.other_table,
                        "other_columns": list(fk.other_columns),
                    }
                )
            elif isinstance(fk, Mapping):
                fks.append(dict(fk))
            else:
                fks.append(list(fk))
        out["foreign_keys"] = fks
    elif name in ("insert_rows", "upsert_rows"):
        out["rows"] = [dict(r) for r in out["rows"]]
        if out.get("pk") is not None:
            out["pk"] = list(_pk_tuple(out["pk"]))
    elif name == "add_column":
        out["type"] = _type_name(out.get("type", "text"))
    elif name == "alter_table":
        out["rename"] = dict(out.get("rename") or {})
        out["drop"] = list(out.get("drop") or ())
        out["types"] = {k: _type_name(t) for k, t in (out.get("types") or {}).items()}
        if "pk" in out and out["pk"] is not None:
            out["pk"] = list(_pk_tuple(out["pk"]))
    elif name == "create_index":
        cols = out["columns"]
        out["columns"] = [cols] if isinstance(cols, str) else list(cols)
    elif name == "update_row":
        out["pk_values"] = _plain_key(out["pk_values"])
        out["updates"] = dict(out["updates"])
    elif name == "delete_rows":
        out["pk_values_list"] = [_plain_key(v) for v in out["pk_values_list"]]
    return out
