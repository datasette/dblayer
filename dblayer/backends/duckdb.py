"""DuckDB backend, using the ``duckdb`` package on the shared thread pool.

Facts this module relies on were checked against DuckDB 2.0.1 by
``scripts/duckdb_verify.py``; the comments cite them.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any

from ..core.params import DUCKDB_FLAVOR, ParamStyle
from ..core.schema import TableInfo
from ..errors import (
    DblayerError,
    IntegrityError,
    QueryError,
    QueryInterrupted,
    ResourceLimitExceeded,
    WriteConflict,
)
from .base import Dialect

__all__ = ["DuckDBDialect"]

# "Conflict on update!", "Transaction conflict: attempting to insert into
# table "t" but it has been altered by a different transaction".
_CONFLICT = re.compile(r"conflict", re.IGNORECASE)


class DuckDBDialect(Dialect):
    name = "duckdb"
    paramstyle = ParamStyle.NAMED_DOLLAR
    flavor = DUCKDB_FLAVOR
    type_map = {
        # SQLite integers are 64-bit; DuckDB INTEGER is 32-bit. REAL would
        # be a 4-byte FLOAT.
        "integer": "BIGINT",
        "float": "DOUBLE",
        "text": "TEXT",
        "blob": "BLOB",
    }
    native_alter = frozenset({"rename", "drop", "types"})
    rowid_column = None  # DuckDB's rowid pseudo-column is not stable
    # Dropping a table other tables reference fails ("main key table"), and
    # foreign keys have no ON DELETE actions, so nothing is touched silently.
    foreign_keys_enforced_sql = None

    def native_alter_for(
        self,
        info: TableInfo,
        *,
        rename: Mapping[str, str],
        drop: Sequence[str],
        types: Mapping[str, str],
    ) -> frozenset[str]:
        """DuckDB refuses every ALTER except ADD COLUMN / SET DEFAULT on a
        table that has a ``CREATE INDEX`` index (DependencyException), even
        for unindexed columns. It also refuses to drop or retype a PRIMARY
        KEY or UNIQUE column. Those cases rebuild the table."""
        if any(i.origin == "c" for i in info.indexes):
            return frozenset()
        keyed = set(info.primary_keys)
        for i in info.indexes:
            if i.origin == "u":
                keyed.update(c for c in i.columns if c is not None)
        native = set(self.native_alter)
        if keyed & set(drop):
            native.discard("drop")
        if keyed & set(types):
            native.discard("types")
        return frozenset(native)

    def literal(self, value: Any) -> str:
        # Portable columns have no boolean type (bool maps to integer), so
        # render booleans as numbers, as SQLite stores them.
        if isinstance(value, bool):
            return "1" if value else "0"
        return super().literal(value)

    def blob_literal(self, value: bytes) -> str:
        return "'" + "".join(f"\\x{b:02x}" for b in value) + "'::BLOB"

    def map_exception(self, exc: BaseException) -> DblayerError | None:
        try:
            import duckdb
        except ImportError:  # pragma: no cover
            return None
        if not isinstance(exc, duckdb.Error):
            return None
        message = str(exc)
        code = type(exc).__name__.removesuffix("Exception") or type(exc).__name__
        if isinstance(exc, duckdb.InterruptException):
            return QueryInterrupted("query interrupted (time limit exceeded or cancelled)")
        if isinstance(exc, duckdb.ConstraintException):
            return IntegrityError(message, code)
        if isinstance(exc, duckdb.OutOfMemoryException):
            return ResourceLimitExceeded(message)
        if isinstance(exc, duckdb.ConnectionException):
            # The instance was closed under the connection: not an SQL
            # error, so the pool discards the connection.
            return None
        if isinstance(exc, duckdb.TransactionException):
            if "Failed to commit" in message and "constraint" in message.lower():
                # Two transactions inserted the same key; the second learns
                # at COMMIT. Retrying would give an ordinary IntegrityError.
                return IntegrityError(message, "Transaction")
            if _CONFLICT.search(message):
                return WriteConflict(message)
            if "transaction is aborted" in message:
                return QueryError(message, "TRANSACTION_ABORTED")
        return QueryError(message, code)
