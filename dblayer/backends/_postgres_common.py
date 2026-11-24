"""What the two PostgreSQL backends share, whatever the driver: the dialect
and its error-mapping table, the catalog queries behind ``schema()`` and
``schema_version()``, statement classification, the backend-level settings
(``change_hint``, the schema memo, capabilities) and the native
``upsert_rows`` written against a small transaction interface.

``dblayer.backends.postgres`` (asyncpg) and
``dblayer.backends.postgres_psycopg`` (psycopg 3) hold only what differs:
connecting, running statements, parameter adaptation, cancellation, COPY.
Nothing here imports a driver.
"""

from __future__ import annotations

import asyncio
import re
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from decimal import Decimal
from typing import Any, Protocol

from ..core.params import POSTGRES_FLAVOR, ParamStyle, _scan, leading_keyword
from ..core.results import Results, WriteResult
from ..core.schema import ColumnInfo, ForeignKey, Index, Schema, TableInfo, Trigger
from ..errors import (
    DblayerError,
    IntegrityError,
    QueryError,
    QueryInterrupted,
    ResourceLimitExceeded,
    TransactionExpired,
    WriteConflict,
)
from .base import Capabilities, Dialect

__all__ = [
    "PostgresDialectBase",
    "PostgresBackendBase",
    "map_sqlstate",
    "read_schema",
    "VERSION_SQL",
    "ConnTx",
    "upsert_rows_native",
]

_FK_ACTIONS = {
    "a": "NO ACTION",
    "r": "RESTRICT",
    "c": "CASCADE",
    "n": "SET NULL",
    "d": "SET DEFAULT",
}

_MAX_IDENTIFIER = 63
MAX_PARAMS = 32767  # PostgreSQL's limit on parameters in one statement


def interrupted() -> QueryInterrupted:
    return QueryInterrupted("query interrupted (time limit exceeded or cancelled)")


def map_sqlstate(state: str, message: str) -> DblayerError:
    """The dblayer exception for a server error, by SQLSTATE. One table for
    both drivers."""
    if state == "57014":
        return interrupted()
    if state in ("40001", "40P01"):
        return WriteConflict(message)
    if state in ("25P03", "25P04"):
        # idle_in_transaction_session_timeout, transaction_timeout (PG 17+)
        return TransactionExpired(
            "the server ended the session: the transaction ran past "
            "its deadline and was rolled back"
        )
    if state.startswith("23"):
        return IntegrityError(message, state)
    if state[:2] in ("53", "54"):
        return ResourceLimitExceeded(message)
    return QueryError(message, state or None)


class PostgresDialectBase(Dialect):
    """The PostgreSQL dialect. Subclasses add the driver's exceptions in
    ``map_driver_exception``."""

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
        if isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
            return interrupted()
        return self.map_driver_exception(exc)

    def map_driver_exception(self, exc: BaseException) -> DblayerError | None:
        return None


# -- statement classification -------------------------------------------------

DML = frozenset({"INSERT", "UPDATE", "DELETE", "MERGE"})
# Statements that manage the transaction themselves: never wrapped in a
# statement savepoint.
TX_CONTROL = frozenset(
    {"BEGIN", "START", "COMMIT", "END", "ROLLBACK", "ABORT", "SAVEPOINT", "RELEASE", "PREPARE"}
)
NO_SCHEMA_CHANGE = frozenset(
    {"SELECT", "INSERT", "UPDATE", "DELETE", "MERGE", "VALUES", "TABLE", "SHOW", "SET",
     "EXPLAIN", "COPY", "SAVEPOINT", "RELEASE", "ROLLBACK", "COMMIT", "BEGIN", "START"}
)
SAVEPOINT = "dblayer_stmt"


def rowcount_from_status(status: str | None) -> int:
    """Rows affected from a command tag such as ``INSERT 0 3``."""
    if not status:
        return -1
    last = status.rsplit(" ", 1)[-1]
    return int(last) if last.isdigit() else -1


def has_top_level_returning(sql: str) -> bool:
    depth = 0
    for kind, start, end in _scan(sql, POSTGRES_FLAVOR, words=True):
        if kind == "(":
            depth += 1
        elif kind == ")":
            depth = max(0, depth - 1)
        elif kind == "word" and depth == 0 and sql[start:end].upper() == "RETURNING":
            return True
    return False


def row_bytes(row: Sequence[Any]) -> int:
    """The size estimate ``max_result_bytes`` is checked against."""
    size = 0
    for v in row:
        if v is None:
            size += 1
        elif isinstance(v, (str, bytes)):
            size += len(v)
        else:
            size += 8
    return size


def begin_sql(
    mode: str, deadline_ms: int | None, grace_ms: int, *, transaction_timeout: bool = False
) -> str:
    """``BEGIN`` for a transaction mode plus, with a deadline, ``SET LOCAL
    idle_in_transaction_session_timeout`` (deadline + grace), for one simple
    query. ``transaction_timeout=True`` (PostgreSQL 17+) also sets
    ``transaction_timeout``, which counts from BEGIN whatever the session
    is doing, so it also ends a transaction whose idle timer was disarmed
    (see ``postgres_psycopg``: a Flush message after Sync does that)."""
    if mode == "write":
        sql = "BEGIN READ WRITE"
    elif mode == "read":
        sql = "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"
    else:
        raise ValueError(f"unknown transaction mode {mode!r}")
    if deadline_ms is not None:
        server_ms = int(deadline_ms) + grace_ms
        sql += f"; SET LOCAL idle_in_transaction_session_timeout = {server_ms}"
        if transaction_timeout:
            sql += f"; SET LOCAL transaction_timeout = {server_ms}"
    return sql


# -- backend-level state ----------------------------------------------------------


class PostgresBackendBase:
    """Settings and state shared by both PostgreSQL backends: one
    ``Database`` is one schema of the database named by ``dsn``.

    ``schema_ttl``: seconds after which ``change_hint()`` moves on by itself,
    so schema changes made by other clients are noticed within that time
    (``Database.refresh_schema()`` notices them at once).
    ``max_write_connections``: concurrent write connections per database
    (default: the database's ``max_read_connections``).
    ``server_deadline_grace_ms``: added to a transaction's deadline for the
    server-side ``idle_in_transaction_session_timeout``.
    ``time_limit_grace_ms``: added to a time limit for the client's own
    timeout, the backstop behind ``statement_timeout``.
    ``statement_savepoints``: each call inside a transaction runs in a
    savepoint, so a failed statement does not abort the transaction.
    """

    dialect: Dialect

    def __init__(
        self,
        dsn: str | None = None,
        *,
        schema: str = "public",
        create_schema: bool = True,
        schema_ttl: float = 300.0,
        application_name: str = "dblayer",
        statement_savepoints: bool = True,
        max_write_connections: int | None = None,
        server_deadline_grace_ms: int = 1000,
        time_limit_grace_ms: int = 500,
        connect_timeout: float = 10.0,
        server_settings: Mapping[str, str] | None = None,
        connect_kwargs: Mapping[str, Any] | None = None,
    ) -> None:
        if schema_ttl <= 0:
            raise ValueError("schema_ttl must be positive")
        self.dsn = dsn
        self.schema = schema
        self.create_schema = create_schema
        self.schema_ttl = schema_ttl
        self.application_name = application_name
        self.statement_savepoints = statement_savepoints
        self.max_write_connections = max_write_connections
        self.server_deadline_grace_ms = server_deadline_grace_ms
        self.time_limit_grace_ms = time_limit_grace_ms
        self.connect_timeout = connect_timeout
        self.server_settings = dict(server_settings or {})
        self.connect_kwargs = dict(connect_kwargs or {})
        self.capabilities = Capabilities(
            concurrent_writers=True,
            transactional_ddl=True,
            implicit_rowid=False,
            cheap_change_hint=False,
            hard_kill=False,
            memory_limit=False,
            table_definition_sql=False,
            full_text_search=False,
            sleep_releases_everything=True,
            returning=True,
            snapshot_reads=True,
            replace_keeps_views=False,
            # Statement savepoints contain a failure, as SQLite does.
            statement_errors_abort_transaction=not statement_savepoints,
        )
        self._schema_ready = False
        self._hint_generation = 0
        self._schema_memo: Schema | None = None

    def __repr__(self) -> str:
        return f"{type(self).__name__}(schema={self.schema!r})"

    def session_settings(self, *, write: bool) -> dict[str, str]:
        """Server settings every connection starts with."""
        return {
            "search_path": self.dialect.quote_identifier(self.schema),
            "application_name": self.application_name,
            "statement_timeout": "0",
            "idle_in_transaction_session_timeout": "0",
            "default_transaction_read_only": "off" if write else "on",
            **self.server_settings,
        }

    def change_hint(self) -> object:
        """No cheap change detector exists: a token that moves every
        ``schema_ttl`` seconds and on ``invalidate_change_hint()``. Never
        opens a connection."""
        return (self._hint_generation, int(time.monotonic() // self.schema_ttl))

    def invalidate_change_hint(self) -> None:
        """Called by ``Database.refresh_schema()``."""
        self._hint_generation += 1

    async def close(self) -> None:
        # Connections belong to the core pool, which closes them; the
        # backend holds nothing else.
        self._schema_ready = False
        self._schema_memo = None


# -- native operations, over any driver ---------------------------------------------


class _NativeConn(Protocol):
    """What ``ConnTx`` needs from a backend connection: run one statement
    (already in ``$n`` style) inside the open transaction without a
    savepoint of its own, run one statement for many parameter sets, and
    read the schema through the backend's memo."""

    backend: PostgresBackendBase

    async def run_native(self, sql: str, args: list[Any]) -> Results: ...
    async def run_native_many(self, sql: str, seq: list[list[Any]]) -> WriteResult: ...
    async def cached_schema(self) -> Schema: ...


class ConnTx:
    """The ``TxLike`` the core's default operations are written against,
    over one connection inside its open transaction. Statements run without
    their own savepoints: the whole native operation runs in one."""

    def __init__(self, conn: _NativeConn) -> None:
        self._conn = conn
        self.dialect = conn.backend.dialect
        self.capabilities = conn.backend.capabilities
        self._schema: Schema | None = None

    @staticmethod
    def precheck(name: str, args: Mapping[str, Any]) -> bool:
        """Cases the native operations take, decided from the arguments
        alone."""
        rows = args.get("rows")
        if not rows:
            return False
        if name == "insert_rows" and (args.get("ignore") or args.get("replace")):
            return False  # COPY has no conflict handling
        return True

    async def execute(self, sql: str, params: Any = None) -> Results:
        from ..core.params import rewrite

        native, args = rewrite(sql, params, ParamStyle.NUMERIC_DOLLAR, POSTGRES_FLAVOR)
        try:
            return await self._conn.run_native(native, list(args))
        finally:
            if leading_keyword(sql, POSTGRES_FLAVOR) not in NO_SCHEMA_CHANGE:
                self._schema = None

    async def execute_many(self, sql: str, params_seq: Any) -> WriteResult:
        from ..core.params import rewrite

        rewritten = [rewrite(sql, p, ParamStyle.NUMERIC_DOLLAR, POSTGRES_FLAVOR) for p in params_seq]
        if not rewritten:
            return WriteResult(rowcount=0)
        return await self._conn.run_native_many(rewritten[0][0], [list(p) for _, p in rewritten])

    async def schema(self) -> Schema:
        if self._schema is None:
            self._schema = await self._conn.cached_schema()
        return self._schema


async def upsert_rows_native(
    tx: ConnTx,
    table: str,
    rows: list[dict[str, Any]],
    pk: list[str] | None = None,
    alter: bool = False,
) -> object:
    """Native ``upsert_rows``: one multi-row ``INSERT ... ON CONFLICT`` per
    group of rows with the same columns. Returns ``NotImplemented`` (before
    running anything) when the default path must take the call."""
    from ..core.operations import OperationResult, _ensure_table, _groups

    pk_cols = tuple(pk or ())
    schema = await tx.schema()
    info = schema.tables.get(table)
    if not pk_cols:
        pk_cols = info.primary_keys if info else ()
    try:
        unusable = (
            not pk_cols
            or (info is None and not pk)
            or any(row.get(p) is None for row in rows for p in pk_cols)
            or len({tuple(row[p] for p in pk_cols) for row in rows}) != len(rows)
        )
    except TypeError:  # unhashable key values
        unusable = True
    if unusable:
        # The default path raises the right error, or (duplicate keys)
        # updates the same row twice, which one statement cannot.
        return NotImplemented
    created, added = await _ensure_table(tx, table, rows, pk_cols, alter)
    d = tx.dialect
    total = 0
    for cols, group in _groups(rows):
        per = max(1, MAX_PARAMS // len(cols))
        for start in range(0, len(group), per):
            part = group[start : start + per]
            one = d.upsert_sql(table, cols, pk_cols)
            head, tail = one.split(" VALUES ", 1)
            conflict = tail[tail.index(")") + 1 :]
            marks = ", ".join("(" + ", ".join("?" for _ in cols) + ")" for _ in part)
            r = await tx.execute(
                f"{head} VALUES {marks}{conflict}", [row[c] for row in part for c in cols]
            )
            total += max(r.rowcount, 0)
    return OperationResult(rowcount=total, table_created=created, columns_added=added)


# -- introspection --------------------------------------------------------------

VERSION_SQL = """
with ns as (select oid from pg_namespace where nspname = $1)
select md5(coalesce(string_agg(x, '|' order by x), '')) from (
  select 'c' || c.oid || ':' || c.relname || ':' || c.relkind::text
    from pg_class c where c.relnamespace = (select oid from ns)
  union all
  select 'a' || a.attrelid || ':' || a.attnum || ':' || a.attname || ':' || a.atttypid
         || ':' || a.atttypmod || ':' || a.attnotnull || ':' || a.attgenerated::text
    from pg_attribute a join pg_class c on c.oid = a.attrelid
   where c.relnamespace = (select oid from ns) and a.attnum > 0 and not a.attisdropped
  union all
  select 'd' || d.oid from pg_attrdef d join pg_class c on c.oid = d.adrelid
   where c.relnamespace = (select oid from ns)
  union all
  select 'k' || k.oid || ':' || k.conname from pg_constraint k
   where k.connamespace = (select oid from ns)
  union all
  select 't' || t.oid || ':' || t.tgname || ':' || t.tgenabled::text from pg_trigger t
    join pg_class c on c.oid = t.tgrelid
   where c.relnamespace = (select oid from ns) and not t.tgisinternal
  union all
  select 'r' || r.oid || ':' || md5(r.ev_action::text) from pg_rewrite r
    join pg_class c on c.oid = r.ev_class
   where c.relnamespace = (select oid from ns)
) parts(x)
"""

# "char" catalog columns (relkind, contype, ...) are cast to text: asyncpg
# returns the "char" type as bytes.
_RELATIONS_SQL = """
select c.oid, c.relname, c.relkind::text
  from pg_class c join pg_namespace n on n.oid = c.relnamespace
 where n.nspname = $1 and c.relkind in ('r', 'p', 'v', 'm', 'f')
 order by c.oid
"""

_COLUMNS_SQL = """
select a.attrelid, a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull,
       case when a.attgenerated = '' then pg_get_expr(d.adbin, d.adrelid) end,
       a.attgenerated <> '' as generated
  from pg_attribute a
  join pg_class c on c.oid = a.attrelid
  join pg_namespace n on n.oid = c.relnamespace
  left join pg_attrdef d on d.adrelid = a.attrelid and d.adnum = a.attnum
 where n.nspname = $1 and c.relkind in ('r', 'p', 'v', 'm', 'f')
   and a.attnum > 0 and not a.attisdropped
 order by a.attrelid, a.attnum
"""

_CONSTRAINTS_SQL = """
select k.conrelid, k.conname, k.contype::text,
       array(select a.attname from unnest(k.conkey) with ordinality u(n, i)
             join pg_attribute a on a.attrelid = k.conrelid and a.attnum = u.n order by u.i),
       f.relname,
       array(select a.attname from unnest(k.confkey) with ordinality u(n, i)
             join pg_attribute a on a.attrelid = k.confrelid and a.attnum = u.n order by u.i),
       k.confupdtype::text, k.confdeltype::text, pg_get_constraintdef(k.oid)
  from pg_constraint k
  join pg_class c on c.oid = k.conrelid
  join pg_namespace n on n.oid = c.relnamespace
  left join pg_class f on f.oid = k.confrelid
 where n.nspname = $1 and k.contype in ('p', 'u', 'f', 'c')
 order by k.conrelid, k.contype, k.conname
"""

_INDEXES_SQL = """
select i.indrelid, ic.relname, i.indisunique, i.indisprimary,
       exists(select 1 from pg_constraint k where k.conindid = i.indexrelid and k.contype = 'u'),
       i.indpred is not null,
       pg_get_indexdef(i.indexrelid),
       array(select a.attname from unnest(i.indkey::int2[]) with ordinality u(n, o)
             left join pg_attribute a on a.attrelid = i.indrelid and a.attnum = u.n
             where u.o <= i.indnkeyatts order by u.o)
  from pg_index i
  join pg_class ic on ic.oid = i.indexrelid
  join pg_class t on t.oid = i.indrelid
  join pg_namespace n on n.oid = t.relnamespace
 where n.nspname = $1
 order by ic.relname
"""

_TRIGGERS_SQL = """
select t.tgname, c.relname, pg_get_triggerdef(t.oid)
  from pg_trigger t
  join pg_class c on c.oid = t.tgrelid
  join pg_namespace n on n.oid = c.relnamespace
 where n.nspname = $1 and not t.tgisinternal
 order by t.oid
"""

SCHEMA_QUERIES: tuple[str, ...] = (
    VERSION_SQL,
    _RELATIONS_SQL,
    _COLUMNS_SQL,
    _CONSTRAINTS_SQL,
    _INDEXES_SQL,
    _TRIGGERS_SQL,
)
"""The catalog queries ``read_schema`` runs, in order, each taking the schema
name as ``$1``."""

type FetchMany = Callable[[Sequence[str], str], Awaitable[Sequence[Sequence[Sequence[Any]]]]]
"""``fetch_many(queries, schema)``: the rows of each query, run in order in
one snapshot, with ``schema`` as ``$1``."""


_LONG_TYPES = (
    (re.compile(r"^character varying\b"), "varchar"),
    (re.compile(r"^character\b"), "char"),
    (re.compile(r"^bit varying\b"), "varbit"),
    (re.compile(r"^timestamp(\(\d+\))? with time zone"), r"timestamptz\1"),
    (re.compile(r"^timestamp(\(\d+\))? without time zone"), r"timestamp\1"),
    (re.compile(r"^time(\(\d+\))? with time zone"), r"timetz\1"),
    (re.compile(r"^time(\(\d+\))? without time zone"), r"time\1"),
)


def short_type(formatted: str) -> str:
    """``format_type`` output in the short spelling people write
    (``varchar(20)``, ``timestamptz``), upper-cased like the dialect's type
    names (``BIGINT``, ``DOUBLE PRECISION``)."""
    for pattern, repl in _LONG_TYPES:
        formatted = pattern.sub(repl, formatted, count=1)
    return formatted.upper()


async def read_schema(fetch_many: FetchMany, schema: str) -> Schema:
    """The whole schema from the catalog. ``fetch_many`` decides how the
    queries travel (one after another on asyncpg, one pipeline on
    psycopg)."""
    results = await fetch_many(SCHEMA_QUERIES, schema)
    version_rows, relations, column_rows, constraint_rows, index_rows, trigger_rows = results
    version = version_rows[0][0]
    columns: dict[int, list[Any]] = {}
    for oid, name, type_, not_null, default, generated in column_rows:
        columns.setdefault(oid, []).append((name, type_, not_null, default, generated))
    pks: dict[int, tuple[str, ...]] = {}
    fks: dict[int, list[ForeignKey]] = {}
    checks: dict[int, list[str]] = {}
    for oid, _name, kind, cols, other, other_cols, on_upd, on_del, cdef in constraint_rows:
        if kind == "c":
            checks.setdefault(oid, []).append(cdef)
        elif kind == "p":
            pks[oid] = tuple(cols)
        elif kind == "f":
            fks.setdefault(oid, []).append(
                ForeignKey(
                    columns=tuple(cols),
                    other_table=other,
                    other_columns=tuple(other_cols),
                    on_update=_FK_ACTIONS.get(on_upd, on_upd),
                    on_delete=_FK_ACTIONS.get(on_del, on_del),
                )
            )
    indexes: dict[int, list[Index]] = {}
    for oid, iname, unique, primary, constraint_u, partial, idef, icols in index_rows:
        origin = "pk" if primary else ("u" if constraint_u else "c")
        indexes.setdefault(oid, []).append(
            Index(
                name=iname,
                columns=tuple(icols),
                unique=unique,
                origin=origin,
                partial=partial,
                definition=idef,
            )
        )
    triggers = {
        name: Trigger(name=name, table=table, definition=defn)
        for name, table, defn in trigger_rows
    }
    tables: dict[str, TableInfo] = {}
    views: dict[str, TableInfo] = {}
    for oid, name, kind in relations:
        pk = pks.get(oid, ())
        cols = tuple(
            ColumnInfo(
                name=c,
                type=short_type(t),
                not_null=nn,
                default=dflt,
                primary_key=pk.index(c) + 1 if c in pk else 0,
                generated=gen,
            )
            for c, t, nn, dflt, gen in columns.get(oid, ())
        )
        if kind in ("v", "m"):
            views[name] = TableInfo(name=name, kind="view", columns=cols)
        else:
            tables[name] = TableInfo(
                name=name,
                kind="table",
                columns=cols,
                foreign_keys=tuple(fks.get(oid, ())),
                indexes=tuple(indexes.get(oid, ())),
                virtual=kind == "f",
                checks=tuple(checks.get(oid, ())),
            )
    return Schema(version=version, tables=tables, views=views, triggers=triggers)
