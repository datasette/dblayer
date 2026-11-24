"""The backend interface: ``Backend`` / ``BackendConnection`` protocols,
``Capabilities`` and ``Dialect``.

A backend is the ten connection methods plus a dialect. The core owns
pooling, write ordering, deadlines, the schema cache, parameter rewriting and
error mapping (via ``Dialect.map_exception``). Backends receive SQL whose
placeholders were already rewritten to ``dialect.paramstyle``.

Contract notes for implementers (beyond the signatures):

* ``BackendConnection`` methods are only ever called by one task at a time
  (the pool guarantees it). Blocking work must run on the shared executor.
* ``execute`` serves reads and writes. Fetch at most ``max_rows + 1`` rows and
  set ``truncated`` if the extra row existed; raise ``ResourceLimitExceeded``
  as soon as the estimated size of fetched rows passes ``max_result_bytes``.
  For a write it must still complete the statement and report ``rowcount``
  and ``last_pk``.
* ``rollback()`` must be a cheap no-op when no transaction is open: the pool
  calls it on every connection it gets back (reset on return).
* ``time_limit_ms`` must not outlive the call it was given to.
* Raise driver exceptions as they are (or dblayer exceptions); the core maps
  driver exceptions with ``dialect.map_exception`` and keeps the original as
  ``__cause__``.
* ``Backend.close()`` releases backend-level resources. The backend must be
  usable again afterwards (``open()`` works), because the core calls it
  whenever the database goes to sleep (``db.sleep()``, idle timeout, global
  cap eviction) when ``capabilities.sleep_releases_everything`` is true. It
  is called only once every connection has been closed, and must be
  idempotent.
* ``BackendConnection.close()`` may be called from a different event loop
  than the one that opened the connection (a ``Database`` moving loops, or
  connections left behind by a closed loop). It should do its best and
  must not hang.
* Backends that run blocking code should implement ``UsesExecutor``: an
  ``executor`` attribute, ``None`` meaning "the module default". A
  ``Registry`` sets it to its shared thread pool when the database is
  added, so thread count is fixed by the registry, not the database count.
"""

from __future__ import annotations

import re
from concurrent.futures import Executor
from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import asdict, dataclass, fields
from typing import Any, Literal, Protocol, runtime_checkable

from ..core.params import SQLITE_FLAVOR, ParamStyle, SqlFlavor
from ..core.results import Column, Results, WriteResult
from ..core.schema import ColumnDef, ForeignKey, Schema, TableInfo
from ..errors import DblayerError

_SIMPLE_DEFAULT = re.compile(
    r"""[+-]?\d+(\.\d*)?([eE][+-]?\d+)?|'([^']|'')*'|[xX]'[0-9a-fA-F]*'"""
    r"""|NULL|TRUE|FALSE|CURRENT_TIME|CURRENT_DATE|CURRENT_TIMESTAMP""",
    re.IGNORECASE,
)

__all__ = [
    "Capabilities",
    "Dialect",
    "Backend",
    "BackendConnection",
    "RowStream",
    "SupportsOperations",
    "SupportsBatch",
    "SupportsServerDeadline",
    "NativeParams",
    "UsesExecutor",
]

type NativeParams = Mapping[str, Any] | Sequence[Any]


@dataclass(frozen=True, slots=True, kw_only=True)
class Capabilities:
    """What a backend supports. Every flag must be declared explicitly.

    The spec's matrix has some non-boolean cells ("Experimental", "To verify",
    "Server-side cancel", "Different syntax"); those are booleans here and the
    backend's docstring says which way it rounded.
    """

    concurrent_writers: bool
    transactional_ddl: bool
    implicit_rowid: bool
    cheap_change_hint: bool
    hard_kill: bool
    memory_limit: bool
    table_definition_sql: bool
    full_text_search: bool
    sleep_releases_everything: bool
    returning: bool
    """``INSERT/UPDATE/DELETE ... RETURNING``. Not in the spec's table."""
    snapshot_reads: bool
    """``transaction(mode="read")`` sees one consistent snapshot, unaffected
    by writes committed while it is open. Not in the spec's table: false for
    SQLite shared-cache memory databases, whose readers use
    ``read_uncommitted`` and share one page cache with the writer."""

    def has(self, flag: str) -> bool:
        if flag not in self.names():
            raise AttributeError(f"unknown capability flag {flag!r}")
        return bool(getattr(self, flag))

    @classmethod
    def names(cls) -> tuple[str, ...]:
        return tuple(f.name for f in fields(cls))

    def as_dict(self) -> dict[str, bool]:
        return asdict(self)


type ConflictAction = Literal["ignore", "replace"] | None


class Dialect:
    """Per-backend SQL details that the core and the default named operations
    (milestone 2) are written against.

    Generated SQL uses ``?`` placeholders; the core rewrites them to
    ``paramstyle`` like any other statement. Subclasses override what differs.
    The defaults follow PostgreSQL/standard syntax, which DuckDB and SQLite
    (3.24+) share for ``ON CONFLICT``.
    """

    name: str = "generic"
    paramstyle: ParamStyle = ParamStyle.QMARK
    flavor: SqlFlavor = SQLITE_FLAVOR
    identifier_quote: str = '"'
    type_map: Mapping[str, str] = {
        "integer": "INTEGER",
        "float": "DOUBLE PRECISION",
        "text": "TEXT",
        "blob": "BYTEA",
    }

    # -- identifiers and types -------------------------------------------

    def quote_identifier(self, name: str) -> str:
        q = self.identifier_quote
        return q + name.replace(q, q + q) + q

    def quote_identifiers(self, names: Sequence[str]) -> str:
        return ", ".join(self.quote_identifier(n) for n in names)

    def native_type(self, type_name: str) -> str:
        """Map a portable type (``integer``, ``float``, ``text``, ``blob``) to
        the native name; any other string is passed through unchanged."""
        return self.type_map.get(type_name.lower(), type_name)

    native_alter: frozenset[str] = frozenset({"rename", "drop", "types"})
    """Which ``alter_table`` changes this backend does with native ``ALTER``
    statements (``rename_column_sql``, ``drop_column_sql``,
    ``alter_column_type_sql`` and, for ``"pk"``, ``set_primary_key_sql``).
    Any other requested change is done by rebuilding the table, the way
    sqlite-utils ``transform`` does. Native changes are applied first, then
    the rebuild."""

    def native_alter_for(
        self,
        info: TableInfo,
        *,
        rename: Mapping[str, str],
        drop: Sequence[str],
        types: Mapping[str, str],
    ) -> frozenset[str]:
        """``native_alter`` for this particular table and change. A dialect
        whose engine refuses some ``ALTER`` forms depending on the table
        (DuckDB: any ``CREATE INDEX`` on it) returns a smaller set, so those
        changes go to the rebuild instead of failing (a failed statement may
        abort the whole transaction, so trying first is not an option)."""
        return self.native_alter

    rowid_column: str | None = None
    """Name of the implicit row id (``"rowid"`` on SQLite), used to address
    rows of tables without a primary key and preserved by rebuilds. ``None``
    when the backend has no implicit row id."""

    foreign_keys_enforced_sql: str | None = None
    """A query returning one row with a true value when foreign keys are
    enforced on this connection *and* dropping a referenced table would run
    ``ON DELETE`` actions (SQLite's ``PRAGMA foreign_keys``). Rebuilds and
    staged-import swaps refuse to drop a table other tables reference when it
    returns true. ``None``: dropping a referenced table never silently
    touches other tables on this backend (it fails instead)."""

    # -- DDL -------------------------------------------------------------

    def literal(self, value: Any) -> str:
        """Render a Python value as an SQL literal, for ``DEFAULT`` clauses."""
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, float)):
            return repr(value)
        if isinstance(value, str):
            return "'" + value.replace("'", "''") + "'"
        if isinstance(value, (bytes, bytearray, memoryview)):
            return self.blob_literal(bytes(value))
        raise TypeError(f"cannot render {type(value).__name__} as an SQL literal")

    def blob_literal(self, value: bytes) -> str:
        return "'\\x" + value.hex() + "'::bytea"

    def default_clause(self, expression: str) -> str:
        """``DEFAULT`` clause for an SQL expression. Simple literals are
        emitted as they are; anything else is parenthesised."""
        e = expression.strip()
        if _SIMPLE_DEFAULT.fullmatch(e):
            return f"DEFAULT {e}"
        return f"DEFAULT ({e})"

    def column_sql(self, col: ColumnDef) -> str:
        parts = [self.quote_identifier(col.name), self.native_type(col.type)]
        if col.not_null:
            parts.append("NOT NULL")
        if col.default is not None:
            parts.append(self.default_clause(col.default))
        return " ".join(p for p in parts if p)

    def create_table_sql(
        self,
        name: str,
        columns: Sequence[ColumnDef],
        *,
        pk: Sequence[str] = (),
        uniques: Sequence[Sequence[str]] = (),
        foreign_keys: Sequence[ForeignKey] = (),
        if_not_exists: bool = False,
    ) -> str:
        """``CREATE TABLE``. The primary key is always a table constraint
        (on SQLite, ``INTEGER`` + ``PRIMARY KEY ("id")`` still makes the
        column the rowid alias)."""
        items = [self.column_sql(c) for c in columns]
        if pk:
            items.append(f"PRIMARY KEY ({self.quote_identifiers(pk)})")
        for cols in uniques:
            items.append(f"UNIQUE ({self.quote_identifiers(cols)})")
        for fk in foreign_keys:
            clause = (
                f"FOREIGN KEY ({self.quote_identifiers(fk.columns)}) "
                f"REFERENCES {self.quote_identifier(fk.other_table)}"
            )
            if fk.other_columns:
                clause += f" ({self.quote_identifiers(fk.other_columns)})"
            for verb, action in (("UPDATE", fk.on_update), ("DELETE", fk.on_delete)):
                if action and action.upper() != "NO ACTION":
                    clause += f" ON {verb} {action}"
            items.append(clause)
        ine = "IF NOT EXISTS " if if_not_exists else ""
        body = ",\n    ".join(items)
        return f"CREATE TABLE {ine}{self.quote_identifier(name)} (\n    {body}\n)"

    def drop_table_sql(self, name: str, *, if_exists: bool = False) -> str:
        return f"DROP TABLE {'IF EXISTS ' if if_exists else ''}{self.quote_identifier(name)}"

    def rename_table_sql(self, old: str, new: str) -> str:
        return f"ALTER TABLE {self.quote_identifier(old)} RENAME TO {self.quote_identifier(new)}"

    def swap_rename_sql(self, staging: str, target: str) -> list[str]:
        """Statements that rename ``staging`` to ``target`` right after the
        old ``target`` was dropped in the same transaction (alter rebuilds and
        staged-import ``replace``). Views and triggers elsewhere still refer
        to ``target`` by name and must keep working."""
        return [self.rename_table_sql(staging, target)]

    def add_column_sql(self, table: str, col: ColumnDef) -> str:
        return f"ALTER TABLE {self.quote_identifier(table)} ADD COLUMN {self.column_sql(col)}"

    def rename_column_sql(self, table: str, old: str, new: str) -> str:
        return (
            f"ALTER TABLE {self.quote_identifier(table)} RENAME COLUMN "
            f"{self.quote_identifier(old)} TO {self.quote_identifier(new)}"
        )

    def drop_column_sql(self, table: str, column: str) -> str:
        return f"ALTER TABLE {self.quote_identifier(table)} DROP COLUMN {self.quote_identifier(column)}"

    def alter_column_type_sql(self, table: str, column: str, type_name: str) -> str:
        return (
            f"ALTER TABLE {self.quote_identifier(table)} ALTER COLUMN "
            f"{self.quote_identifier(column)} TYPE {self.native_type(type_name)}"
        )

    def set_primary_key_sql(
        self, table: str, current_constraint: str | None, pk: Sequence[str]
    ) -> list[str]:
        """Statements that replace ``table``'s primary key (named
        ``current_constraint``, or none) with ``pk`` (empty: no primary key).
        Only called when ``"pk"`` is in ``native_alter``."""
        raise NotImplementedError

    def create_index_sql(
        self,
        name: str,
        table: str,
        columns: Sequence[str],
        *,
        unique: bool = False,
        if_not_exists: bool = False,
    ) -> str:
        return (
            f"CREATE {'UNIQUE ' if unique else ''}INDEX "
            f"{'IF NOT EXISTS ' if if_not_exists else ''}{self.quote_identifier(name)} "
            f"ON {self.quote_identifier(table)} ({self.quote_identifiers(columns)})"
        )

    # -- statements used by named operations --------------------------------

    def insert_sql(
        self,
        table: str,
        columns: Sequence[str],
        *,
        conflict: ConflictAction = None,
        conflict_columns: Sequence[str] = (),
    ) -> str:
        """``INSERT`` of one row with ``?`` placeholders.

        ``conflict="ignore"`` skips rows that violate a uniqueness constraint;
        ``conflict="replace"`` overwrites the conflicting row (needs
        ``conflict_columns`` in this default implementation).
        """
        head = self._insert_head(table, columns)
        if conflict is None:
            return head
        if conflict == "ignore":
            target = (
                f" ({self.quote_identifiers(conflict_columns)})" if conflict_columns else ""
            )
            return f"{head} ON CONFLICT{target} DO NOTHING"
        if conflict == "replace":
            if not conflict_columns:
                raise ValueError("conflict='replace' needs conflict_columns")
            return self.upsert_sql(table, columns, conflict_columns)
        raise ValueError(f"unknown conflict action {conflict!r}")

    def upsert_sql(
        self,
        table: str,
        columns: Sequence[str],
        pk: Sequence[str],
        *,
        update_columns: Sequence[str] | None = None,
    ) -> str:
        """Insert, or update the row with the same key.

        By default the supplied non-key ``columns`` are updated. Passing
        ``update_columns`` (every non-key column of the table) makes this a
        whole-row replace: ``excluded`` holds the row as it would have been
        inserted, defaults included, so unsupplied columns get their defaults.
        """
        head = self._insert_head(table, columns)
        target = self.quote_identifiers(pk)
        if update_columns is None:
            update_columns = columns
        updates = [c for c in update_columns if c not in pk]
        if not updates:
            return f"{head} ON CONFLICT ({target}) DO NOTHING"
        sets = ", ".join(
            f"{self.quote_identifier(c)} = {self.excluded(c)}" for c in updates
        )
        return f"{head} ON CONFLICT ({target}) DO UPDATE SET {sets}"

    def excluded(self, column: str) -> str:
        return "excluded." + self.quote_identifier(column)

    def _insert_head(self, table: str, columns: Sequence[str], verb: str = "INSERT") -> str:
        cols = self.quote_identifiers(columns)
        marks = ", ".join("?" for _ in columns)
        return f"{verb} INTO {self.quote_identifier(table)} ({cols}) VALUES ({marks})"

    # -- errors --------------------------------------------------------------

    def map_exception(self, exc: BaseException) -> DblayerError | None:
        """Translate a driver exception into a dblayer exception, or return
        ``None`` to let it propagate unchanged. The core sets ``__cause__``."""
        return None


@runtime_checkable
class RowStream(Protocol):
    """What ``BackendConnection.stream`` returns: an async iterator of row
    chunks that also exposes the result columns (available once the first
    chunk has been produced) and can be closed early."""

    columns: tuple[Column, ...] | None

    def __aiter__(self) -> AsyncIterator[list[tuple[Any, ...]]]: ...
    async def __anext__(self) -> list[tuple[Any, ...]]: ...
    async def aclose(self) -> None: ...


@runtime_checkable
class BackendConnection(Protocol):
    async def execute(
        self,
        sql: str,
        params: NativeParams,
        *,
        time_limit_ms: int | None,
        max_rows: int | None,
        max_result_bytes: int | None = None,
    ) -> Results: ...

    async def execute_many(
        self, sql: str, params_seq: Sequence[NativeParams]
    ) -> WriteResult: ...

    def stream(
        self,
        sql: str,
        params: NativeParams,
        *,
        chunk_rows: int,
        time_limit_ms: int | None = None,
        max_result_bytes: int | None = None,
    ) -> RowStream: ...

    async def begin(self, mode: str) -> None: ...
    async def commit(self) -> None: ...
    async def rollback(self) -> None: ...
    async def schema(self) -> Schema: ...
    async def schema_version(self) -> object: ...
    async def interrupt(self) -> None: ...
    async def close(self) -> None: ...


@runtime_checkable
class SupportsOperations(Protocol):
    """Optional, on a ``BackendConnection``: native overrides for named
    operations.

    The core calls ``run_operation(name, args)`` on the connection that holds
    the caller's open transaction, before trying the default implementation
    in ``dblayer.core.operations``. ``name`` is the operation name
    (``"insert_rows"``...), ``args`` its keyword arguments, made of plain
    values (lists, dicts, str, numbers...) so they can cross a process
    boundary. The override must:

    * run inside the transaction that is already open on this connection
      (never commit or roll back);
    * return an ``OperationResult`` (or a dict ``OperationResult.from_dict``
      accepts) that is identical to what the default implementation returns;
    * or return ``NotImplemented`` to decline, in which case the core runs the
      default implementation. Declining must happen before anything ran.

    ``Database(..., native_operations=False)`` makes the core skip this hook,
    which is how the conformance suite tests both paths.
    """

    async def run_operation(self, name: str, args: dict[str, Any]) -> object: ...


@runtime_checkable
class SupportsBatch(Protocol):
    """Optional, on a ``BackendConnection``: run several statements inside
    the open transaction in one call (one thread hop / one message). Used by
    ``Database.batch``; without it the core calls ``execute`` per statement.
    Stops at the first failing statement and raises its error."""

    async def execute_batch(
        self,
        statements: Sequence[tuple[str, NativeParams]],
        *,
        time_limit_ms: int | None,
        max_rows: int | None,
    ) -> list[Results]: ...


@runtime_checkable
class SupportsServerDeadline(Protocol):
    """Optional, on a ``BackendConnection``: begin a transaction and have the
    *server* enforce its deadline too (PostgreSQL's
    ``idle_in_transaction_session_timeout``), so it ends even if the client
    stops running. The core's own deadline timer still applies. Used in
    place of ``begin`` when the transaction has a deadline."""

    async def begin_with_deadline(self, mode: str, deadline_ms: int) -> None: ...


@runtime_checkable
class Backend(Protocol):
    capabilities: Capabilities
    dialect: Dialect

    async def open(self, *, write: bool) -> BackendConnection: ...
    def change_hint(self) -> object: ...  # must not open a connection
    async def close(self) -> None: ...


@runtime_checkable
class UsesExecutor(Protocol):
    """Optional, on a ``Backend``: blocking work runs on ``executor``
    (``None``: ``dblayer.core.executor.get_default_executor()``), read at
    call time so it can be swapped while the backend is in use. A
    ``Registry`` sets it to its shared thread pool when the database is
    added, unless the backend was given an executor of its own."""

    executor: Executor | None
