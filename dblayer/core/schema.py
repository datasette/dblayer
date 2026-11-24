"""Schema types returned by introspection, and the per-database schema cache."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

__all__ = [
    "ColumnInfo",
    "ForeignKey",
    "Index",
    "Trigger",
    "TableInfo",
    "Schema",
    "SchemaCache",
    "ColumnDef",
    "HIDDEN_PREFIX",
]

HIDDEN_PREFIX = "_dblayer_"
"""Tables whose names start with this are dblayer's own (staging tables,
alter rebuild scratch tables) and are left out of ``table_names()``."""


@dataclass(frozen=True, slots=True)
class ColumnDef:
    """A column to create: input to ``Dialect.create_table_sql``."""

    name: str
    type: str
    """A portable type (``integer``, ``float``, ``text``, ``blob``) or a native
    type string, passed through ``Dialect.native_type``."""
    not_null: bool = False
    default: str | None = None
    """Default as an SQL expression (already rendered), or ``None``."""


@dataclass(frozen=True, slots=True)
class ColumnInfo:
    name: str
    type: str
    """Declared type as the backend reports it (may be ``""`` on SQLite)."""
    not_null: bool = False
    default: str | None = None
    """Default as an SQL expression string, or ``None``."""
    primary_key: int = 0
    """1-based position in the primary key, ``0`` if not part of it."""
    hidden: bool = False
    """Hidden column (SQLite virtual-table hidden columns)."""
    generated: bool = False
    """Generated (computed) column."""


@dataclass(frozen=True, slots=True)
class ForeignKey:
    """One outbound foreign key. Composite keys have several columns."""

    columns: tuple[str, ...]
    other_table: str
    other_columns: tuple[str, ...]
    on_update: str | None = None
    on_delete: str | None = None


@dataclass(frozen=True, slots=True)
class Index:
    name: str
    columns: tuple[str | None, ...]
    """Indexed column names in order; ``None`` marks an expression."""
    unique: bool = False
    origin: str = "c"
    """``"c"`` created by CREATE INDEX, ``"u"`` UNIQUE constraint, ``"pk"``
    primary key (SQLite's ``index_list`` origin values)."""
    partial: bool = False
    definition: str | None = None


@dataclass(frozen=True, slots=True)
class Trigger:
    name: str
    table: str
    definition: str | None = None


@dataclass(frozen=True, slots=True)
class TableInfo:
    name: str
    kind: Literal["table", "view"]
    columns: tuple[ColumnInfo, ...] = ()
    foreign_keys: tuple[ForeignKey, ...] = ()
    indexes: tuple[Index, ...] = ()
    definition: str | None = None
    virtual: bool = False
    hidden: bool = False
    """A table callers normally don't list (SQLite FTS shadow tables)."""
    checks: tuple[str, ...] = ()
    """CHECK constraints (``CHECK (...)`` text), where the backend reports
    them separately from ``definition`` (PostgreSQL)."""

    @property
    def primary_keys(self) -> tuple[str, ...]:
        pks = sorted((c.primary_key, c.name) for c in self.columns if c.primary_key)
        return tuple(name for _, name in pks)

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)


@dataclass(frozen=True, slots=True)
class Schema:
    """Everything introspection knows about one database, from one call."""

    version: object
    tables: dict[str, TableInfo] = field(default_factory=dict)
    views: dict[str, TableInfo] = field(default_factory=dict)
    triggers: dict[str, Trigger] = field(default_factory=dict)

    def table_names(self) -> list[str]:
        """Table names, without dblayer's own hidden tables (names starting
        with ``HIDDEN_PREFIX``, such as staging tables). Those are still in
        ``tables`` and readable by name."""
        return [n for n in self.tables if not n.startswith(HIDDEN_PREFIX)]

    def view_names(self) -> list[str]:
        return list(self.views)

    def get(self, name: str) -> TableInfo | None:
        return self.tables.get(name) or self.views.get(name)

    def definition(self, name: str) -> str | None:
        """Look ``name`` up as a table, view, index or trigger. Raises
        ``KeyError`` when nothing has that name."""
        info = self.get(name)
        if info is not None:
            return info.definition
        if name in self.triggers:
            return self.triggers[name].definition
        for table in self.tables.values():
            for index in table.indexes:
                if index.name == name:
                    return index.definition
        raise KeyError(name)


class SchemaCache:
    """Caches one ``Schema`` keyed by its version, plus the change hint that
    was current when it was fetched.

    The core checks the hint first (no connection); if it is unchanged the
    cached schema is served. Otherwise it asks for ``schema_version()`` and
    only refetches ``schema()`` when the version moved.
    """

    __slots__ = ("schema", "hint")

    def __init__(self) -> None:
        self.schema: Schema | None = None
        self.hint: object = None

    def clear(self) -> None:
        self.schema = None
        self.hint = None

    def store(self, schema: Schema, hint: object) -> None:
        self.schema = schema
        self.hint = hint

    def fresh_by_hint(self, hint: object) -> Schema | None:
        """The cached schema if ``hint`` proves nothing changed, else None.
        A ``None`` hint never proves anything."""
        if self.schema is not None and hint is not None and hint == self.hint:
            return self.schema
        return None

    @property
    def version(self) -> object:
        return None if self.schema is None else self.schema.version
