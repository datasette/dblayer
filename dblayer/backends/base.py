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
  usable again afterwards (``open()`` works), because the core calls it on
  ``sleep()`` when ``capabilities.sleep_releases_everything`` is true.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import asdict, dataclass, fields
from typing import Any, Literal, Protocol, runtime_checkable

from ..core.params import SQLITE_FLAVOR, ParamStyle, SqlFlavor
from ..core.results import Column, Results, WriteResult
from ..core.schema import Schema
from ..errors import DblayerError

__all__ = [
    "Capabilities",
    "Dialect",
    "Backend",
    "BackendConnection",
    "RowStream",
    "SupportsOperations",
    "NativeParams",
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

    def upsert_sql(self, table: str, columns: Sequence[str], pk: Sequence[str]) -> str:
        """Insert, or update the non-key columns of the row with the same key."""
        head = self._insert_head(table, columns)
        target = self.quote_identifiers(pk)
        updates = [c for c in columns if c not in pk]
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
    """Optional: native overrides for named operations (milestone 2+)."""

    async def run_operation(self, name: str, args: dict[str, Any]) -> object: ...


@runtime_checkable
class Backend(Protocol):
    capabilities: Capabilities
    dialect: Dialect

    async def open(self, *, write: bool) -> BackendConnection: ...
    def change_hint(self) -> object: ...  # must not open a connection
    async def close(self) -> None: ...
