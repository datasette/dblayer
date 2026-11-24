"""PostgreSQL backend, using ``asyncpg`` (natively async: no threads).

* **One ``Database`` = one schema.** Every connection is opened with
  ``search_path`` set to the configured schema, so table names in the API
  stay unqualified. ``create_schema=True`` (default) creates the schema on
  the first connection.
* **No driver pool.** The core pool is the pool: ``open()`` makes one
  ``asyncpg`` connection and the core checks it in and out, closes idle ones
  and closes everything on ``sleep()``. An ``asyncpg.Pool`` underneath would
  be a second pool with its own idle policy.
* **Parameters** arrive as ``$1..$n`` (the core rewrites ``:name`` / ``?``).
  PostgreSQL infers the type of an untyped parameter from context and falls
  back to ``text``, and ``asyncpg`` then refuses a Python ``int`` for it
  (``select :a`` with ``{"a": 1}``). Where the inferred type is a string
  type and the value is an ``int``, ``float``, ``bool`` or ``bytes``, the
  statement is re-prepared with an explicit cast on that parameter
  (``$1::int8``), which is what SQLite's dynamic typing gives callers.
* **Read connections** have ``default_transaction_read_only=on``, the
  equivalent of SQLite's ``mode=ro``. ``mode="read"`` transactions are
  ``REPEATABLE READ, READ ONLY``; write transactions use the server default
  isolation. Writers are not queued: ``capabilities.concurrent_writers`` is
  true and the core pool lets up to ``max_write_connections`` writers in.
* **Time limits**: ``statement_timeout`` (``SET LOCAL`` in a transaction,
  session ``SET`` otherwise; only re-sent when the value changes) plus the
  driver's per-call timeout, a little longer, as a backstop that cancels on
  the server.
* **Transaction deadline** is enforced by the core's timer and backed by
  ``idle_in_transaction_session_timeout`` (deadline plus
  ``server_deadline_grace_ms``), so the server ends the session even if the
  client stops running.
* **Statement savepoints**: PostgreSQL aborts a whole transaction when one
  statement fails; SQLite keeps it usable. With ``statement_savepoints=True``
  (default) each call on a transaction runs inside a savepoint and a failed
  call is rolled back to it, so ``try: await tx.execute(...) except
  IntegrityError: ...`` works the same on both. Costs one extra round trip
  per call inside a transaction.
* **Introspection** reads ``pg_catalog`` restricted to the schema.
  ``schema_version()`` is an md5 of a catalog listing; ``change_hint()`` is
  a time bucket of ``schema_ttl`` seconds plus a counter bumped by
  ``Database.refresh_schema()``.
* **Values** are passed through as ``asyncpg`` returns them (``Decimal``,
  ``datetime``, ``UUID``, lists, ...).

Capability rounding: ``hard_kill`` is false (there is a server-side cancel,
but nothing that stops a statement that ignores it); ``memory_limit`` is
false (``work_mem`` is per operation, not a limit on a query);
``full_text_search`` is false (PostgreSQL has it, with different syntax
from the SQLite FTS the flag stands for).
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Sequence
from decimal import Decimal
from typing import Any

from ..core.params import POSTGRES_FLAVOR, ParamStyle
from ..errors import (
    DblayerError,
    IntegrityError,
    QueryError,
    QueryInterrupted,
    ResourceLimitExceeded,
    TransactionExpired,
    WriteConflict,
)
from .base import Dialect

__all__ = ["PostgresDialect"]

_FK_ACTIONS = {
    "a": "NO ACTION",
    "r": "RESTRICT",
    "c": "CASCADE",
    "n": "SET NULL",
    "d": "SET DEFAULT",
}

_MAX_IDENTIFIER = 63


class PostgresDialect(Dialect):
    name = "postgres"
    paramstyle = ParamStyle.NUMERIC_DOLLAR
    flavor = POSTGRES_FLAVOR
    type_map = {
        # SQLite integers are 64-bit; INTEGER here would be 32-bit.
        "integer": "BIGINT",
        "float": "DOUBLE PRECISION",
        "text": "TEXT",
        "blob": "BYTEA",
    }
    native_alter = frozenset({"rename", "drop", "types", "pk"})
    rowid_column = None
    # Dropping a table other tables reference fails (no CASCADE is ever
    # sent), so nothing is touched silently.
    foreign_keys_enforced_sql = None

    def literal(self, value: Any) -> str:
        # Portable columns have no boolean type (bool maps to integer), so
        # render booleans as numbers, as SQLite stores them.
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, Decimal):
            return str(value)
        return super().literal(value)

    def alter_column_type_sql(self, table: str, column: str, type_name: str) -> str:
        native = self.native_type(type_name)
        col = self.quote_identifier(column)
        return (
            f"ALTER TABLE {self.quote_identifier(table)} ALTER COLUMN {col} "
            f"TYPE {native} USING {col}::{native}"
        )

    def set_primary_key_sql(
        self, table: str, current_constraint: str | None, pk: Sequence[str]
    ) -> list[str]:
        """Replace the primary key in one ``ALTER TABLE``."""
        actions = []
        if current_constraint:
            actions.append(f"DROP CONSTRAINT {self.quote_identifier(current_constraint)}")
        if pk:
            actions.append(f"ADD PRIMARY KEY ({self.quote_identifiers(pk)})")
        if not actions:
            return []
        return [f"ALTER TABLE {self.quote_identifier(table)} " + ", ".join(actions)]

    def swap_rename_sql(self, staging: str, target: str) -> list[str]:
        """Rename staging into place, then rename the constraints it was
        created with (``<staging>_pkey``, ``<staging>_<col>_key``...) to the
        names PostgreSQL would have given them on ``target``. Constraint
        names are per schema, so a later import would otherwise collide or
        leave staging names behind. Views referring to the old target do
        not survive: PostgreSQL refuses to drop a table a view depends on."""
        reg = self.literal(self.quote_identifier(target))
        return [
            self.rename_table_sql(staging, target),
            "DO $dblayer$\nDECLARE r record;\nBEGIN\n"
            f"  FOR r IN SELECT conname FROM pg_constraint WHERE conrelid = {reg}::regclass\n"
            f"           AND starts_with(conname, {self.literal(staging)}) LOOP\n"
            "    EXECUTE format('ALTER TABLE %s RENAME CONSTRAINT %I TO %I',\n"
            f"      {reg}::regclass, r.conname,\n"
            f"      left({self.literal(target)} || substr(r.conname, {len(staging) + 1}), "
            f"{_MAX_IDENTIFIER}));\n"
            "  END LOOP;\nEND\n$dblayer$",
        ]

    def map_exception(self, exc: BaseException) -> DblayerError | None:
        try:
            import asyncpg
        except ImportError:  # pragma: no cover - asyncpg is required to get here
            return None
        if isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
            return QueryInterrupted("query interrupted (time limit exceeded or cancelled)")
        if isinstance(exc, asyncpg.PostgresError):
            state: str = exc.sqlstate or ""
            message = str(exc)
            if state == "57014":
                return QueryInterrupted(
                    "query interrupted (time limit exceeded or cancelled)"
                )
            if state in ("40001", "40P01"):
                return WriteConflict(message)
            if state == "25P03":
                return TransactionExpired(
                    "the server ended the session: the transaction was idle past "
                    "its deadline and was rolled back"
                )
            if state.startswith("23"):
                return IntegrityError(message, state)
            if state[:2] in ("53", "54"):
                return ResourceLimitExceeded(message)
            return QueryError(message, state or None)
        if isinstance(exc, asyncpg.DataError):
            # Raised by asyncpg before sending: a value its codec for the
            # parameter's type cannot encode.
            return QueryError(str(exc), "DATA_ERROR")
        return None


def _rowcount(status: str | None) -> int:
    """Rows affected from a command tag such as ``INSERT 0 3``."""
    if not status:
        return -1
    last = status.rsplit(" ", 1)[-1]
    return int(last) if last.isdigit() else -1

