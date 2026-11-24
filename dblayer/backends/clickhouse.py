"""ClickHouse backend (experimental): chDB, ClickHouse embedded in-process.

Facts this module relies on were checked against chDB 4.5.1 (ClickHouse
26.11.1.1) by ``scripts/clickhouse_verify.py``; the README's ClickHouse
section has the capability summary.

* **One embedded server per process.** chDB refuses a second data
  directory while any connection to the first is open. So a ``ClickHouse``
  backend is one ClickHouse *database* (``CREATE DATABASE``) inside a data
  directory that every backend in the process shares, as the PostgreSQL
  backends put one ``Database`` in one schema of a server. Asking for a
  different directory while the server is up raises ``NotSupported``.
  The server starts with the first connection and stops when the last one
  closes. Each pooled "connection" is a chDB connection (its own session:
  settings, current database); queries on different connections run in
  parallel.
* **No transactions** (``capabilities.transactions`` false): ``begin`` /
  ``commit`` / ``rollback`` do nothing, every statement takes effect when
  it runs. ``db.transaction()`` still holds the write slot, so writers stay
  serialised, which is what makes the key checks of named operations exact
  (``core/emulated_keys.py``). A rolled-back block that wrote logs a
  warning that its writes were kept.
* **Keys are not enforced** by ClickHouse (``unique_constraints`` false).
  Tables are ``MergeTree`` ordered by the primary key (``ORDER BY
  tuple()`` without one); named operations check keys themselves.
  ``UPDATE`` is a lightweight update (tables are created with the block
  number and offset columns it needs; other tables fall back to ``ALTER
  TABLE ... UPDATE``), ``DELETE`` a lightweight delete, both synchronous so
  a read after a write sees it. Neither reports a row count, so the
  backend counts matching rows first (one extra ``SELECT count()``).
* **Read connections** run with ``readonly=1``: no writes, no DDL, no
  settings changes, no table functions (``file``, ``url``, ``remote``,
  ``s3`` ...). Two host accesses are *not* stopped by ``readonly=1`` and
  are refused here: ``INTO OUTFILE`` and the scalar ``file()`` function
  (chDB reads any absolute path with it). So are ``FORMAT`` clauses
  (results are decoded from Arrow), ``SET`` and ``USE``, and more than one
  statement per call. These checks run on the core's scanner, which must
  read SQL as ClickHouse's lexer does (``#`` comments, backslash escapes
  in quoted names); SQL containing a backslash is checked again in
  ClickHouse's own reading of it (``formatQuery``). Write connections get
  the same host checks unless ``allow_host_access``.
* **Not isolated:** every database of the process is one ClickHouse
  server, and ``readonly=1`` reads any of them (``other.table``,
  ``merge()``, ``system.processes`` shows other connections' SQL).
* **Time limits** are enforced with ``KILL QUERY`` from a control
  connection (every statement carries a ``/* dblayer-...:n */`` tag), since
  settings cannot change per call on a ``readonly=1`` connection. ClickHouse
  checks for cancellation between blocks of rows; a single long-running
  step in between is not interrupted until it ends.
* **Parameters** are ClickHouse's own typed query parameters
  (``{p1:Int64}``), with the type chosen from the Python value; values are
  sent apart from the SQL text, in ClickHouse's escaped form, and never
  parsed as SQL.
* **Results** arrive as Arrow (``pyarrow`` comes with chDB). ``String``
  holds text and bytes alike (``distinct_blob_type`` false): a value that is
  valid UTF-8 is returned as ``str``, anything else as ``bytes``.

Capability rounding: ``hard_kill`` false; ``memory_limit`` true
(``max_memory_usage`` per query); ``cheap_change_hint`` true (stats of the
database's data directory); ``table_definition_sql`` true (``SHOW CREATE``
text); ``snapshot_reads`` false (each statement reads its own snapshot of
the parts; a read transaction does not hold one).
"""

from __future__ import annotations

import contextlib
import hashlib
import importlib.util
import logging
import os
import re
import threading
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from concurrent.futures import Executor
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import UUID

from ..core.executor import run_blocking
from ..core.params import (
    CLICKHOUSE_FLAVOR,
    ParamStyle,
    _scan,
    dollar_parameters,
    leading_keyword,
    statement_count,
)
from ..core.results import Column, Results, WriteResult
from ..core.schema import ColumnDef, ColumnInfo, ForeignKey, Index, Schema, TableInfo
from ..errors import (
    DblayerError,
    IntegrityError,
    NotSupported,
    ParameterError,
    QueryError,
    QueryInterrupted,
    ResourceLimitExceeded,
)
from .base import Capabilities, Dialect, NativeParams
from .duckdb import _Interrupter  # one shared deadline thread design

if TYPE_CHECKING:
    import pyarrow as pa

__all__ = ["ClickHouse", "ClickHouseDialect", "ClickHouseConnection", "chdb_unavailable"]

log = logging.getLogger(__name__)


def chdb_unavailable() -> str | None:
    """``None`` when chDB can be imported here, else why not."""
    if importlib.util.find_spec("chdb") is None:
        return "chdb is not installed (uv run --python 3.14 --extra clickhouse pytest; no wheels for 3.15 yet)"
    return None


# -- dialect -------------------------------------------------------------------------

_PORTABLE = {"integer": "Int64", "float": "Float64", "text": "String", "blob": "String"}
_SQL_TYPES = {"date": "Date32", "timestamp": "DateTime64(6)", "datetime": "DateTime64(6)"}
_CODE = re.compile(r"Code: (\d+)\.")
_NAME = re.compile(r"\(([A-Z][A-Z0-9_]+)\)\s*(?:\(version [^)]*\))?\.?\s*$")


def _nullable(t: str) -> bool:
    return t.startswith(("Nullable(", "LowCardinality(Nullable("))


class ClickHouseDialect(Dialect):
    name = "clickhouse"
    paramstyle = ParamStyle.NUMERIC_DOLLAR
    flavor = CLICKHOUSE_FLAVOR
    type_map = _PORTABLE
    # Key columns (the ORDER BY) cannot be renamed, retyped or dropped with
    # ALTER: those changes rebuild (native_alter_for).
    native_alter = frozenset({"rename", "drop", "types"})
    rowid_column = None
    foreign_keys_enforced_sql = None
    index_type = "minmax"
    """Data-skipping index type ``create_index`` makes (ClickHouse has no
    B-tree secondary indexes)."""

    def native_type(self, type_name: str) -> str:
        return _PORTABLE.get(type_name.lower(), type_name)

    def _column_type(self, col: ColumnDef, *, key: bool) -> str:
        """Portable types are ``Nullable(...)`` unless NOT NULL or part of
        the key (SQL columns accept NULL by default; ClickHouse's do not,
        and a sorting key cannot be Nullable). Native types pass through."""
        if col.type.lower() in _PORTABLE:
            t = _PORTABLE[col.type.lower()]
            return t if (col.not_null or key) else f"Nullable({t})"
        if col.type.lower() in _SQL_TYPES:
            # ClickHouse's DATE / TIMESTAMP aliases are Date (1970-2149) and
            # DateTime (seconds); SQL's types hold any year and fractions.
            t = _SQL_TYPES[col.type.lower()]
            return t if (col.not_null or key) else f"Nullable({t})"
        if key and col.type.startswith("Nullable(") and col.type.endswith(")"):
            return col.type[len("Nullable(") : -1]
        return col.type

    def column_sql(self, col: ColumnDef, *, key: bool = False) -> str:
        t = self._column_type(col, key=key)
        parts = [self.quote_identifier(col.name), t]
        if not _nullable(t) and (
            col.not_null or key or col.type.lower() in _PORTABLE or col.type.lower() in _SQL_TYPES
        ):
            # Sessions run with data_type_default_nullable=1, which would
            # make the type Nullable; a native type without NOT NULL is
            # left to that default, as other engines' columns are nullable.
            parts.append("NOT NULL")
        if col.default is not None:
            parts.append(self.default_clause(col.default))
        return " ".join(parts)

    def literal(self, value: Any) -> str:
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, str):
            return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
        return super().literal(value)

    def blob_literal(self, value: bytes) -> str:
        return f"unhex('{value.hex()}')"

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
        if foreign_keys:
            raise NotSupported("ClickHouse has no foreign keys (capabilities.foreign_keys is false)")
        if uniques:
            raise NotSupported("ClickHouse has no UNIQUE constraints")
        keyset = set(pk)
        body = ",\n    ".join(self.column_sql(c, key=c.name in keyset) for c in columns)
        order = f"({self.quote_identifiers(pk)})" if pk else "tuple()"
        ine = "IF NOT EXISTS " if if_not_exists else ""
        # The block number/offset columns make plain UPDATE (a lightweight
        # update) work on the table.
        return (
            f"CREATE TABLE {ine}{self.quote_identifier(name)} (\n    {body}\n)\n"
            f"ENGINE = MergeTree ORDER BY {order}\n"
            "SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1"
        )

    def rename_table_sql(self, old: str, new: str) -> str:
        return f"RENAME TABLE {self.quote_identifier(old)} TO {self.quote_identifier(new)}"

    def exchange_tables_sql(self, staging: str, target: str) -> list[str] | None:
        return [
            f"EXCHANGE TABLES {self.quote_identifier(staging)} AND {self.quote_identifier(target)}"
        ]

    def add_column_sql(self, table: str, col: ColumnDef) -> str:
        return f"ALTER TABLE {self.quote_identifier(table)} ADD COLUMN {self.column_sql(col)}"

    def alter_column_type_sql(self, table: str, column: str, type_name: str) -> str:
        t = self._column_type(ColumnDef(column, type_name), key=False)
        return (
            f"ALTER TABLE {self.quote_identifier(table)} MODIFY COLUMN "
            f"{self.quote_identifier(column)} {t}{'' if _nullable(t) else ' NOT NULL'}"
        )

    def native_alter_for(
        self,
        info: TableInfo,
        *,
        rename: Mapping[str, str],
        drop: Sequence[str],
        types: Mapping[str, str],
    ) -> frozenset[str]:
        """ClickHouse refuses to rename, retype or drop a sorting-key column
        (ALTER_OF_COLUMN_IS_FORBIDDEN), and to drop or retype a column a
        data-skipping index uses; those changes rebuild."""
        keyed = set(info.primary_keys)
        indexed = {c for i in info.indexes for c in i.columns if c is not None}
        native = set(self.native_alter)
        if keyed & set(rename):
            native.discard("rename")
        if (keyed | indexed) & set(types):
            native.discard("types")
        if (keyed | indexed) & set(drop):
            native.discard("drop")
        return frozenset(native)

    def create_index_sql(
        self,
        name: str,
        table: str,
        columns: Sequence[str],
        *,
        unique: bool = False,
        if_not_exists: bool = False,
    ) -> str:
        if unique:
            raise NotSupported("ClickHouse has no unique indexes (capabilities.unique_constraints)")
        return (
            f"CREATE INDEX {'IF NOT EXISTS ' if if_not_exists else ''}{self.quote_identifier(name)} "
            f"ON {self.quote_identifier(table)} ({self.quote_identifiers(columns)}) "
            f"TYPE {self.index_type} GRANULARITY 1"
        )

    def insert_sql(
        self,
        table: str,
        columns: Sequence[str],
        *,
        conflict: Any = None,
        conflict_columns: Sequence[str] = (),
    ) -> str:
        if conflict is not None:
            raise NotSupported(
                "ClickHouse has no ON CONFLICT; named operations check keys in "
                "dblayer.core.emulated_keys instead"
            )
        return self._insert_head(table, columns)

    def upsert_sql(self, table: str, columns: Sequence[str], pk: Sequence[str], *, update_columns: Sequence[str] | None = None) -> str:  # noqa: E501
        raise NotSupported("ClickHouse has no ON CONFLICT (see dblayer.core.emulated_keys)")

    def map_exception(self, exc: BaseException) -> DblayerError | None:
        if isinstance(exc, DblayerError):
            return None
        message = str(exc)
        m = _CODE.search(message)
        if m is None or not isinstance(exc, (RuntimeError, Exception)):
            return None
        if type(exc).__name__ not in ("ChdbError", "RuntimeError"):
            return None
        code = int(m.group(1))
        n = _NAME.search(message.splitlines()[0] if message else "")
        name = n.group(1) if n else f"CODE_{code}"
        if code in (159, 394):  # TIMEOUT_EXCEEDED, QUERY_WAS_CANCELLED
            return QueryInterrupted(f"query interrupted (time limit exceeded or cancelled): {name}")
        if code in (241, 396, 158, 307):  # memory, result rows/bytes, too many bytes
            return ResourceLimitExceeded(message)
        if code == 164:
            return QueryError(message, "READ_ONLY")
        if code == 349 or "Cannot convert NULL" in message or "Cannot insert NULL value" in message:
            # NULL into a non-Nullable column (NOT NULL)
            return IntegrityError(message, "NOT_NULL")
        return QueryError(message, name)


# -- parameters --------------------------------------------------------------------------

_ESCAPES = str.maketrans({"\\": "\\\\", "\t": "\\t", "\n": "\\n", "\r": "\\r", "\0": "\\0"})


def _param(v: Any) -> tuple[str, str]:
    """ClickHouse type and escaped-format text of a parameter value."""
    if v is None:
        return "Nullable(Nothing)", "\\N"
    if isinstance(v, bool):
        return "Bool", "true" if v else "false"
    if isinstance(v, int):
        if -(2**63) <= v < 2**63:
            return "Int64", str(v)
        if 0 <= v < 2**64:
            return "UInt64", str(v)
        # Int128/Int256 exist, but come back through Arrow as raw bytes.
        raise ParameterError(f"integer {v} does not fit in 64 bits", code="BAD_PARAMETERS")
    if isinstance(v, float):
        if v != v:
            return "Float64", "nan"
        if v in (float("inf"), float("-inf")):
            return "Float64", "inf" if v > 0 else "-inf"
        return "Float64", repr(v)
    if isinstance(v, str):
        return "String", v.translate(_ESCAPES)
    if isinstance(v, (bytes, bytearray, memoryview)):
        return "String", "".join(f"\\x{b:02x}" for b in bytes(v))
    if isinstance(v, datetime):
        if v.tzinfo is not None:
            v = v.astimezone(timezone.utc)
            return "DateTime64(6, 'UTC')", v.replace(tzinfo=None).isoformat(" ")
        return "DateTime64(6)", v.isoformat(" ")
    if isinstance(v, date):
        return "Date32", v.isoformat()
    if isinstance(v, Decimal):
        t = v.as_tuple()
        scale = max(0, -t.exponent) if isinstance(t.exponent, int) else 0
        return f"Decimal(76, {min(scale, 76)})", str(v)
    if isinstance(v, UUID):
        return "UUID", str(v)
    raise ParameterError(
        f"cannot pass a {type(v).__name__} as a ClickHouse parameter", code="BAD_PARAMETERS"
    )


_INLINE_NUMBERS_ABOVE = 64


def _number_literal(v: Any) -> str | None:
    """SQL text of a number (or NULL) generated here, never from the
    caller's text, so it cannot carry SQL; None for anything else."""
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int) and -(2**63) <= v < 2**64:
        return str(v) if v >= 0 else f"({v})"
    if isinstance(v, float):
        if v != v:
            return "nan"
        if v in (float("inf"), float("-inf")):
            return "inf" if v > 0 else "(-inf)"
        return repr(v) if v >= 0 else f"({v!r})"
    return None


def bind(sql: str, params: NativeParams, suffix: str = "") -> tuple[str, dict[str, str]]:
    """Replace the core's ``$n`` placeholders with ``{pn:Type}`` and return
    the values in ClickHouse's escaped text form.

    ClickHouse parses each typed parameter separately (about 25 us each:
    an ``IN`` list of 500 took 14 ms, against 2.4 ms as literals), so in a
    statement with more than 64 placeholders numbers and NULLs are written
    as literals made here from the Python value; strings and bytes stay
    parameters."""
    spans = dollar_parameters(sql, CLICKHOUSE_FLAVOR)
    if not spans:
        return sql, {}
    values = list(params.values()) if isinstance(params, Mapping) else list(params)
    inline = len(spans) > _INLINE_NUMBERS_ABOVE
    out: list[str] = []
    pos = 0
    bound: dict[str, str] = {}
    for start, end, n in spans:
        if n > len(values):
            raise ParameterError(f"no value for parameter ${n}", code="MISSING_PARAMETER")
        if inline:
            lit = _number_literal(values[n - 1])
            if lit is not None:
                out.append(sql[pos:start])
                out.append(lit)
                pos = end
                continue
        typ, text = _param(values[n - 1])
        name = f"p{n}{suffix}"
        out.append(sql[pos:start])
        out.append("{" + name + ":" + typ + "}")
        bound[name] = text
        pos = end
    out.append(sql[pos:])
    return "".join(out), bound


# -- statement checks --------------------------------------------------------------------

_RESULT_KINDS = frozenset({"SELECT", "WITH", "SHOW", "DESCRIBE", "DESC", "EXPLAIN", "EXISTS"})
# Refused on read connections before they reach ClickHouse (which refuses
# them too, with readonly=1). Anything else is left to the engine, so a
# misspelt "selec 1" gets ClickHouse's syntax error.
_WRITE_KINDS = frozenset(
    {
        "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "RENAME", "EXCHANGE",
        "TRUNCATE", "OPTIMIZE", "SYSTEM", "KILL", "ATTACH", "DETACH", "GRANT", "REVOKE",
        "UNDROP", "MOVE", "BACKUP", "RESTORE",
    }
)
# Without host access (always so on read connections) these are refused
# before they reach ClickHouse. readonly=1 refuses table functions, but not
# the scalar file() function, which reads any file the process can read:
# chDB does not confine it to user_files_path. The names are every table
# function and table engine of ClickHouse 26.11 except those that only touch
# this server's own tables or generate data; a test compares them with
# system.table_functions and system.table_engines, so a new version's new
# names fail a test instead of passing unnoticed.
_SAFE_TABLE_FUNCTIONS = frozenset(
    {
        "numbers", "numbers_mt", "zeros", "zeros_mt", "generateRandom", "generateSeries",
        "generate_series", "values", "null", "merge", "view", "viewIfPermitted", "viewExplain",
        "loop", "primes", "format", "input", "SQLStandardValues", "fuzzJSON", "fuzzQuery",
        "obfuscate", "mergeTreeIndex", "mergeTreeProjection", "mergeTreeAnalyzeIndexes",
        "mergeTreeAnalyzeIndexesUUID", "mergeTreeCodecBlockCounts", "mergeTreeTextIndex",
    }
)
# The Linux chDB build has more than the macOS one (Delta Lake, HDFS):
# test_host_function_lists_cover_this_clickhouse fails on any name in
# neither list.
_HOST_TABLE_FUNCTIONS = frozenset(
    {
        "arrowstream", "azureBlobStorage", "azureBlobStorageCluster", "bigquery", "cluster",
        "clusterAllReplicas", "cosn", "deltaLake", "deltaLakeAzure", "deltaLakeAzureCluster",
        "deltaLakeCluster", "deltaLakeLocal", "deltaLakeS3", "deltaLakeS3Cluster", "dictionary",
        "elasticsearch", "eval", "executable", "file", "fileCluster", "filesystem", "gcs", "hdfs",
        "hdfsCluster", "hudi", "hudiCluster", "iceberg", "icebergAzure", "icebergAzureCluster",
        "icebergCluster", "icebergHDFS", "icebergHDFSCluster", "icebergLocal",
        "icebergLocalCluster", "icebergS3", "icebergS3Cluster", "jdbc", "mongodb", "mysql", "odbc",
        "oss", "paimon", "paimonAzure", "paimonAzureCluster", "paimonCluster", "paimonHDFS",
        "paimonHDFSCluster", "paimonLocal", "paimonS3", "paimonS3Cluster", "postgresql", "prometheusQuery", "prometheusQueryRange", "python",
        "redis", "remote", "remoteSecure", "s3", "s3Cluster", "sqlite", "timeSeriesData",
        "timeSeriesMetricFamilies", "timeSeriesMetrics", "timeSeriesSamples",
        "timeSeriesSelector", "timeSeriesTags", "timeSeriesTimeRanges", "traceView", "url",
        "urlCluster", "ytsaurus",
    }
)
# Matched without regard to case (a wrong case is an unknown function to
# ClickHouse, so refusing it costs nothing). "file" is also the scalar
# function; catboostEvaluate loads a model from a path.
_HOST_FUNCTIONS = frozenset(n.lower() for n in _HOST_TABLE_FUNCTIONS) | {"catboostevaluate"}
_SAFE_ENGINES = frozenset(
    {
        "MergeTree", "AggregatingMergeTree", "CoalescingMergeTree", "CollapsingMergeTree",
        "GraphiteMergeTree", "ReplacingMergeTree", "SummingMergeTree",
        "VersionedCollapsingMergeTree", "Memory", "Log", "TinyLog", "StripeLog", "Null", "Set",
        "Join", "Buffer", "View", "MaterializedView", "Merge", "GenerateRandom", "Loop", "Alias",
        "FuzzJSON", "FuzzQuery", "Obfuscate",
    }
)
# ATTACH TABLE t FROM '/dir', BACKUP ... TO File(...), RESTORE name host paths.
_HOST_STATEMENTS = frozenset({"ATTACH", "BACKUP", "RESTORE"})

_REFUSED = {
    "BEGIN": "BEGIN/COMMIT/ROLLBACK are managed by dblayer (and ClickHouse has no transactions)",
    "START": "BEGIN/COMMIT/ROLLBACK are managed by dblayer (and ClickHouse has no transactions)",
    "COMMIT": "BEGIN/COMMIT/ROLLBACK are managed by dblayer (and ClickHouse has no transactions)",
    "ROLLBACK": "BEGIN/COMMIT/ROLLBACK are managed by dblayer (and ClickHouse has no transactions)",
    "SET": "SET would change a pooled connection; pass settings to ClickHouse() instead",
    "USE": "USE would change a pooled connection's database",
}


class _Statement:
    __slots__ = ("kind", "where", "target", "set_at", "index_without_type")

    def __init__(self, kind: str | None) -> None:
        self.kind = kind
        self.where: int | None = None  # offset of a top-level WHERE
        self.target: tuple[int, int] | None = None  # UPDATE / DELETE FROM table span
        self.set_at: int | None = None
        self.index_without_type = False  # CREATE INDEX with no TYPE clause


def _analyse(sql: str, *, host_access: bool) -> _Statement:
    """Statement kind plus what the backend needs to know about it, from
    the core's scanner (strings, comments and quoted names skipped)."""
    if statement_count(sql, CLICKHOUSE_FLAVOR) > 1:
        raise QueryError("only one statement can be executed at a time", "MULTIPLE_STATEMENTS")
    kind = leading_keyword(sql, CLICKHOUSE_FLAVOR)
    st = _Statement(kind)
    if kind is None:
        return st
    if kind in _REFUSED:
        raise QueryError(_REFUSED[kind], "STATEMENT_NOT_ALLOWED")
    if not host_access and kind in _HOST_STATEMENTS:
        raise QueryError(f"{kind} statements can reach the host; refused", "HOST_ACCESS")
    depth = 0
    prev: str | None = None
    before: str | None = None
    verb_end: int | None = None
    words: list[tuple[str, int, int, int]] = []
    for tok, start, end in _scan(sql, CLICKHOUSE_FLAVOR, words=True):
        if tok == "(":
            if not host_access and prev is not None and prev.lower() in _HOST_FUNCTIONS:
                raise QueryError(
                    f"{sql[words[-1][1]:words[-1][2]]}() can read or write files, run programs "
                    "or reach the network; refused",
                    "HOST_ACCESS",
                )
            depth += 1
            prev = None
            continue
        if tok == ")":
            depth = max(0, depth - 1)
            prev = None
            continue
        if tok != "word":
            prev = None
            continue
        w = sql[start:end].upper()
        words.append((w, start, end, depth))
        if not host_access and kind == "CREATE":
            if prev == "DICTIONARY" and before in ("CREATE", "REPLACE", "TEMPORARY"):
                raise QueryError(
                    "dictionaries can read files and reach the network; refused", "HOST_ACCESS"
                )
            if prev == "ENGINE" and sql[start:end] not in _SAFE_ENGINES:
                raise QueryError(
                    f"table engine {sql[start:end]} is not allowed (it may reach files or "
                    "the network)",
                    "HOST_ACCESS",
                )
        before = prev
        if not host_access and ((prev == "INTO" and w == "OUTFILE") or (prev == "FROM" and w == "INFILE")):
            raise QueryError(
                "INTO OUTFILE / FROM INFILE would touch files on the host; refused",
                "HOST_ACCESS",
            )
        if prev == "FORMAT" and depth == 0 and kind in _RESULT_KINDS:
            raise QueryError(
                "FORMAT clauses are not supported (results are decoded by dblayer); "
                "use db.export() for CSV or JSON",
                "FORMAT_NOT_ALLOWED",
            )
        prev = w
    top = [w[0] for w in words if w[3] == 0]
    if kind == "CREATE" and top[1:2] == ["INDEX"] or top[1:3] == ["UNIQUE", "INDEX"]:
        st.index_without_type = "TYPE" not in top
    if kind in ("UPDATE", "DELETE"):
        first = [w for w in words if w[3] == 0]
        try:
            i = 1 if kind == "UPDATE" else next(k for k, w in enumerate(first) if w[0] == "FROM") + 1
            verb_end = first[i - 1][2]
        except (StopIteration, IndexError):
            return st
        stop = next(
            (w for w in first[i:] if w[0] in ("SET", "WHERE", "IN", "ON", "SETTINGS")), None
        )
        if stop is not None:
            st.target = (verb_end, stop[1])
        st.set_at = next((w[1] for w in first if w[0] == "SET"), None)
        st.where = next((w[2] for w in first if w[0] == "WHERE"), None)
    return st


# -- Arrow results -------------------------------------------------------------------------


def _text(v: Any) -> Any:
    if isinstance(v, bytes):
        try:
            return v.decode("utf-8")
        except UnicodeDecodeError:
            return v
    if isinstance(v, list):
        return [_text(x) for x in v]
    if isinstance(v, tuple):
        return tuple(_text(x) for x in v)
    if isinstance(v, dict):
        return {_text(k): _text(x) for k, x in v.items()}
    return v


def _has_binary(t: "pa.DataType") -> bool:
    import pyarrow as pa

    if pa.types.is_binary(t) or pa.types.is_large_binary(t) or pa.types.is_fixed_size_binary(t):
        return True
    if pa.types.is_dictionary(t):
        return _has_binary(t.value_type)
    return any(_has_binary(t.field(i).type) for i in range(t.num_fields))


def _column_values(col: Any) -> list[Any]:
    import pyarrow as pa
    import pyarrow.compute as pc

    t = col.type
    if pa.types.is_dictionary(t):
        col = col.cast(t.value_type) if hasattr(col, "cast") else col.dictionary_decode()
        t = col.type
    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        try:
            return pc.cast(col, pa.string()).to_pylist()
        except (pa.ArrowInvalid, UnicodeDecodeError):
            return [_text(v) for v in col.to_pylist()]
    if _has_binary(t):
        return [_text(v) for v in col.to_pylist()]
    if pa.types.is_map(t):
        return [None if v is None else dict(v) for v in col.to_pylist()]
    return col.to_pylist()


_ARROW_NAMES = {
    "int8": "Int8", "int16": "Int16", "int32": "Int32", "int64": "Int64",
    "uint8": "UInt8", "uint16": "UInt16", "uint32": "UInt32", "uint64": "UInt64",
    "float": "Float32", "double": "Float64", "bool": "Bool", "binary": "String",
    "string": "String", "date32[day]": "Date32", "null": "Nullable(Nothing)",
}


def _type_name(field: Any) -> str:
    t = str(field.type)
    name = _ARROW_NAMES.get(t, t)
    if field.nullable and name != "Nullable(Nothing)":
        return f"Nullable({name})"
    return name


def _columns(schema: Any) -> tuple[Column, ...]:
    return tuple(Column(f.name, _type_name(f)) for f in schema)


def _rows(table: Any) -> list[tuple[Any, ...]]:
    if table.num_rows == 0:
        return []
    return list(zip(*(_column_values(c) for c in table.columns)))


def _read_arrow(data: bytes) -> Any:
    import pyarrow as pa

    return pa.ipc.open_file(pa.BufferReader(data)).read_all()


# -- the shared embedded server ------------------------------------------------------------


# Server-wide MergeTree defaults, so tables made by plain SQL DDL behave like
# the dialect's: plain UPDATE works (lightweight updates need the block
# columns) and a nullable column can be in the key (columns are Nullable by
# default, see _SESSION).
_SERVER_ARGS = (
    "merge_tree.allow_nullable_key=1",
    "merge_tree.enable_block_number_column=1",
    "merge_tree.enable_block_offset_column=1",
)

# Session settings of every connection.
_SESSION: dict[str, Any] = {
    "output_format_arrow_string_as_string": 0,
    "output_format_arrow_fixed_string_as_fixed_byte_array": 1,
    # Mutations and lightweight deletes finish before the statement returns.
    "mutations_sync": 2,
    "lightweight_deletes_sync": 2,
    # NULL into a non-Nullable column is an error, not the type's default.
    "insert_null_as_default": 0,
    "input_format_null_as_default": 0,
    # Plain SQL DDL as other engines take it: "create table t (x integer)"
    # makes a MergeTree with nullable columns (unless NOT NULL), ordered by
    # its PRIMARY KEY or by nothing.
    "default_table_engine": "MergeTree",
    "create_table_empty_primary_key_by_default": 1,
    "data_type_default_nullable": 1,
    "max_recursive_cte_evaluation_depth": 100_000_000,
    # SQL's outer joins: an unmatched side is NULL, not the type's default
    # (count(r.id) over a LEFT JOIN counted the 0s).
    "join_use_nulls": 1,
    "max_query_size": 64 << 20,
    "max_ast_elements": 1 << 22,
    # Native export (db.export) formats.
    "output_format_csv_crlf_end_of_line": 1,
    "format_csv_null_representation": "",
    "output_format_json_quote_64bit_integers": 0,
    "output_format_json_quote_denormals": 0,
    "output_format_json_escape_forward_slashes": 0,
    "output_format_json_validate_utf8": 1,
    "output_format_decimal_trailing_zeros": 1,
}


def _connection_string(path: Path) -> str:
    return f"{path}?{'&'.join(_SERVER_ARGS)}"


class _Server:
    """The process's one chDB server: its directory, how many connections
    are open, and a control connection for KILL QUERY and catalog work."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.path: Path | None = None
        self.open = 0
        self.starts = 0
        self._control: Any = None
        self._control_lock = threading.Lock()

    def connect(self, path: Path) -> Any:
        from chdb.state import sqlitelike

        with self.lock:
            if self.path is not None and self.path != path:
                raise NotSupported(
                    f"chDB already runs a server on {str(self.path)!r} in this process; "
                    f"it cannot open {str(path)!r} as well. Put every ClickHouse database "
                    "of a process in one data directory (ClickHouse(path, database=...))."
                )
            if self.path is None:
                path.mkdir(parents=True, exist_ok=True)
                self._control = sqlitelike.Connection(_connection_string(path))
                self.path = path
                self.starts += 1
            try:
                conn = sqlitelike.Connection(_connection_string(path))
            except BaseException:
                if self.open == 0:
                    self._shutdown()
                raise
            self.open += 1
            return conn

    def release(self, conn: Any) -> None:
        try:
            conn.close()
        finally:
            with self.lock:
                self.open -= 1
                if self.open == 0:
                    self._shutdown()

    def _shutdown(self) -> None:
        control, self._control = self._control, None
        self.path = None
        if control is not None:
            with self._control_lock:
                control.close()

    def control(self, sql: str) -> None:
        """Run a statement on the control connection (no result)."""
        with self._control_lock:
            if self._control is not None:
                self._control.query(sql, "TSV")

    def kill(self, tag: str) -> None:
        self.control(f"KILL QUERY WHERE startsWith(query, '/* {tag} */') ASYNC")


_server = _Server()
_interrupter = _Interrupter()
_REKILL_S = 0.02
_INSERT_ROWS_PER_STATEMENT = 1000
_FETCH_ROWS = 1024


# -- backend --------------------------------------------------------------------------------


class ClickHouse:
    """ClickHouse database ``database`` in the chDB data directory ``path``.
    See the module docstring."""

    dialect: Dialect

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        database: str = "default",
        create: bool = True,
        memory_limit: int | None = 1 << 30,
        threads: int | None = 2,
        exact_rowcount: bool = True,
        allow_host_access: bool = False,
        engine_export: bool = False,
        settings: Mapping[str, Any] | None = None,
        executor: Executor | None = None,
    ) -> None:
        if chdb_unavailable():
            raise ImportError("ClickHouse backend needs chDB (the 'clickhouse' extra)")
        self.path = Path(path).absolute()
        self.database = database
        self.create = create
        self.memory_limit = memory_limit
        """``max_memory_usage`` (bytes) for each query; ``None``: ClickHouse's default."""
        self.threads = threads
        self.exact_rowcount = exact_rowcount
        """Count the rows an UPDATE / DELETE matches before running it (the
        engine does not report it). False: rowcount is -1."""
        self.allow_host_access = allow_host_access
        """Let *write* connections reach the host: INTO OUTFILE / FROM
        INFILE, file() / url() / s3() and the other table functions, File /
        URL / ... engines, dictionaries, ATTACH, BACKUP, RESTORE. Read
        connections never can."""
        self.export_formats: frozenset[str] = (
            frozenset({"csv", "json"}) if engine_export else frozenset()
        )
        """``db.export()`` formats ClickHouse serialises itself. Empty by
        default: its CSV quotes every string and writes ``2`` for 2.0, its
        JSON writes floats the same way, and a ``String`` holding bytes is
        written raw, so the output is only *parse-equal* to
        docs/export-formats.md for text and integers, not identical as the
        contract asks. ``engine_export=True`` turns it on (CSV and the JSON
        shapes) for callers who accept that; the benchmark measures both."""
        self.settings = dict(settings or {})
        self.executor = executor
        self.dialect = ClickHouseDialect()
        self.capabilities = Capabilities(
            concurrent_writers=False,
            transactional_ddl=False,
            implicit_rowid=False,
            cheap_change_hint=True,
            hard_kill=False,
            memory_limit=memory_limit is not None,
            table_definition_sql=True,
            full_text_search=False,
            sleep_releases_everything=True,
            returning=False,
            snapshot_reads=False,
            statement_errors_abort_transaction=False,
            replace_keeps_views=True,
            transactions=False,
            unique_constraints=False,
            foreign_keys=False,
            distinct_blob_type=False,
            duplicate_column_names=False,
        )
        self._created = False
        self.connections_opened = 0
        self.fds_per_statement = 12
        """File descriptors one running statement may hold beyond its
        connection's (an INSERT writes a new part, several files, and the
        engine's pipeline threads hold more): about 8 measured under load
        in tests/conformance/test_lifecycle_fds.py, 12 allowed."""
        from collections import Counter

        self.native_calls: Counter[str] = Counter()
        """Named operations run by the native override, by name."""

    def __repr__(self) -> str:
        return f"ClickHouse({str(self.path)!r}, database={self.database!r})"

    @staticmethod
    def server_starts() -> int:
        """How many times the process's chDB server was started."""
        return _server.starts

    @staticmethod
    def server_running() -> bool:
        return _server.path is not None

    # -- Backend protocol ----------------------------------------------------

    async def open(self, *, write: bool) -> "ClickHouseConnection":
        made: list[Any] = []

        def connect() -> Any:
            made.append(self._connect(write))
            return made[0]

        try:
            conn = await run_blocking(self.executor, connect)
        except BaseException:
            # Cancelled while connecting (a deadline, a cancelled stream):
            # run_blocking waits for the connect but drops its result, and
            # an unreleased connection keeps the shared server up forever.
            if made:
                _server.release(made[0])
            raise
        return ClickHouseConnection(self, conn, write=write)

    def change_hint(self) -> object:
        """mtimes of the database's metadata directory and of each table's
        data directory (a new part, a mutation or a merge changes them):
        one ``scandir`` plus a ``stat`` per table, no connection."""
        out: list[tuple[str, int]] = []
        for sub in ("metadata", "data"):
            d = self.path / sub / self.database
            try:
                st = os.stat(d)
            except OSError:
                out.append((sub, -1))
                continue
            out.append((sub, st.st_mtime_ns))
            if sub == "data":
                try:
                    with os.scandir(d) as it:
                        for e in it:
                            try:
                                out.append((e.name, e.stat().st_mtime_ns))
                            except OSError:
                                pass
                except OSError:
                    pass
        return tuple(sorted(out))

    async def close(self) -> None:
        """Nothing is held per database: the server stops by itself when
        the last connection in the process closes."""

    async def drop_database(self) -> None:
        """Drop this ClickHouse database and everything in it (tests, the
        demo). Needs no open connection of this backend."""

        def run() -> None:
            conn = _server.connect(self.path)
            try:
                conn.query(
                    f"DROP DATABASE IF EXISTS {self.dialect.quote_identifier(self.database)} SYNC",
                    "TSV",
                )
            finally:
                _server.release(conn)

        await run_blocking(None, run)  # the registry's executor may be shut down

    # -- internals -------------------------------------------------------------

    def _connect(self, write: bool) -> Any:
        conn = _server.connect(self.path)
        try:
            q = self.dialect.quote_identifier(self.database)
            if not self._created:
                if self.create:
                    conn.query(f"CREATE DATABASE IF NOT EXISTS {q}", "TSV")
                self._created = True
            conn.query(f"USE {q}", "TSV")
            settings = dict(_SESSION)
            if self.memory_limit is not None:
                settings["max_memory_usage"] = self.memory_limit
            if self.threads is not None:
                settings["max_threads"] = self.threads
            settings.update(self.settings)
            conn.query(
                "SET " + ", ".join(f"{k} = {_setting(v)}" for k, v in settings.items()), "TSV"
            )
            if not write:
                # Last: from here on no setting can change on this connection.
                conn.query("SET readonly = 1", "TSV")
        except BaseException:
            _server.release(conn)
            raise
        self.connections_opened += 1
        return conn


_TSV_ESCAPE = re.compile(r"\\(.)", re.DOTALL)
_TSV_CHARS = {"n": "\n", "t": "\t", "r": "\r", "0": "\0", "b": "\b", "f": "\f"}


def _unescape_tsv(text: str) -> str:
    return _TSV_ESCAPE.sub(lambda m: _TSV_CHARS.get(m.group(1), m.group(1)), text)


def _setting(v: Any) -> str:
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, (int, float)):
        return repr(v)
    return "'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'"


class ClickHouseConnection:
    """One pooled chDB connection. Used by one task at a time."""

    def __init__(self, backend: ClickHouse, conn: Any, *, write: bool) -> None:
        self._backend = backend
        self._conn = conn
        self.write = write
        self._closed = False
        self._in_tx = False
        self._tx_writes = 0
        self._tag = "dblayer-" + uuid.uuid4().hex[:16]
        self._lock = threading.Lock()
        self._seq = 0
        self.last_seq = 0
        self._active: int | None = None
        self._cancelled = False

    def __repr__(self) -> str:
        return f"<ClickHouseConnection {'write' if self.write else 'read'} {self._backend.database}>"

    # -- running on the executor ---------------------------------------------------

    async def _run[T](self, fn: Callable[..., T], *args: Any) -> T:
        self._cancelled = False
        return await run_blocking(
            self._backend.executor, self._guarded, fn, args, on_cancel=self._on_cancel
        )

    def _guarded[T](self, fn: Callable[..., T], args: tuple[Any, ...]) -> T:
        if self._cancelled:
            raise QueryInterrupted("query interrupted (cancelled)")
        return fn(*args)

    def _on_cancel(self) -> None:
        with self._lock:
            self._cancelled = True
            seq = self._active
        if seq is not None:
            self._kill_while_active(seq)

    def _kill_while_active(self, seq: int) -> None:
        """KILL statement ``seq`` and repeat until it has returned: a KILL
        that arrives before the query is registered finds nothing."""
        with self._lock:
            if self._active != seq:
                return
        try:
            _server.kill(f"{self._tag}:{seq}")
        except Exception:
            pass
        _interrupter.schedule(_REKILL_S, lambda: self._kill_while_active(seq))

    @contextlib.contextmanager
    def _statement(self, time_limit_ms: int | None, seq: int | None = None) -> Iterator[str]:
        """Number the statement, mark it active and arm its time limit.
        Yields the tag comment to prefix the SQL with. A streamed query
        passes its first number (``last_seq``) for every later fetch, so a
        KILL finds it by the tag it was sent with."""
        with self._lock:
            if self._cancelled:
                raise QueryInterrupted("query interrupted (cancelled)")
            if seq is None:
                self._seq += 1
                seq = self._seq
            self.last_seq = seq
            self._active = seq
        token = None
        if time_limit_ms is not None:
            token = _interrupter.schedule(
                max(time_limit_ms, 0) / 1000, lambda: self._kill_while_active(seq)
            )
        try:
            yield f"/* {self._tag}:{seq} */ "
        finally:
            if token is not None:
                _interrupter.cancel(token)
            with self._lock:
                self._active = None

    # -- statements ------------------------------------------------------------------

    def _check(self, sql: str) -> _Statement:
        host_access = self.write and self._backend.allow_host_access
        st = _analyse(sql, host_access=host_access)
        if not host_access and st.kind is not None and "\\" in sql:
            # A backslash can spell a name inside a quoted identifier
            # (`fi\x6Ce` is file): check ClickHouse's own reading too.
            canonical = self._canonical(sql)
            if canonical is not None:
                _analyse(canonical, host_access=False)
        if st.kind is not None and not self.write and st.kind in _WRITE_KINDS:
            raise QueryError(
                f"attempt to write a read-only connection ({st.kind} statement)", "READ_ONLY"
            )
        return st

    def _canonical(self, sql: str) -> str | None:
        """The statement as ClickHouse's parser reads it (``formatQuery``:
        quoted names unescaped where possible); None if it does not parse,
        in which case running it fails too."""
        try:
            data = self._conn.query(
                "SELECT formatQuery({q:String})", "TSV", params={"q": sql.translate(_ESCAPES)}
            ).bytes()
        except Exception:
            return None
        return _unescape_tsv(data.decode("utf-8", "replace").removesuffix("\n"))

    async def execute(
        self,
        sql: str,
        params: NativeParams,
        *,
        time_limit_ms: int | None,
        max_rows: int | None,
        max_result_bytes: int | None = None,
    ) -> Results:
        return await self._run(self._execute, sql, params, time_limit_ms, max_rows, max_result_bytes)

    def _execute(
        self,
        sql: str,
        params: NativeParams,
        time_limit_ms: int | None,
        max_rows: int | None,
        max_result_bytes: int | None,
    ) -> Results:
        st = self._check(sql)
        if st.kind is None:
            return Results.from_tuples((), [])
        if st.index_without_type:
            # Standard CREATE INDEX: ClickHouse needs a data-skipping index type.
            sql = (
                sql.rstrip().removesuffix(";")
                + f" TYPE {self._backend.dialect.index_type} GRANULARITY 1"  # type: ignore[attr-defined]
            )
        if st.kind in ("UPDATE", "DELETE") and st.where is None:
            # ClickHouse needs a WHERE; SQL's "every row" is WHERE 1.
            sql = sql.rstrip().removesuffix(";") + " WHERE 1"
            st = self._check(sql)
        native, values = bind(sql, params)
        if st.kind in _RESULT_KINDS:
            return self._select(native, values, time_limit_ms, max_rows, max_result_bytes)
        return self._write(st, sql, params, native, values, time_limit_ms)

    def _select(
        self,
        sql: str,
        values: dict[str, str],
        time_limit_ms: int | None,
        max_rows: int | None,
        max_result_bytes: int | None,
    ) -> Results:
        low = self._conn._conn
        with self._statement(time_limit_ms) as tag:
            if max_rows is None and max_result_bytes is None:
                data = self._conn.query(tag + sql, "Arrow", params=values).bytes()
                if not data:
                    return Results.from_tuples((), [])
                table = _read_arrow(data)
                return Results.from_tuples(_columns(table.schema), _rows(table))
            limit = None if max_rows is None else max_rows + 1
            rows: list[tuple[Any, ...]] = []
            columns: tuple[Column, ...] | None = None
            size = 0
            res = low.send_query(tag + sql, "Arrow", params=values)
            done = False
            try:
                while limit is None or len(rows) < limit:
                    chunk = low.streaming_fetch_result(res)
                    if chunk is None:
                        done = True
                        break
                    data = chunk.bytes()
                    if not data:
                        if chunk.rows_read() == 0:
                            done = True
                            break
                        continue
                    table = _read_arrow(data)
                    if columns is None:
                        columns = _columns(table.schema)
                    if max_result_bytes is not None:
                        size += table.nbytes
                        if size > max_result_bytes:
                            raise ResourceLimitExceeded(
                                f"result exceeded max_result_bytes={max_result_bytes}"
                            )
                    rows.extend(_rows(table))
            finally:
                if not done:
                    with contextlib.suppress(Exception):
                        low.streaming_cancel_query(res)
            if columns is None:
                columns = self._describe(sql, values)
        truncated = max_rows is not None and len(rows) > max_rows
        if truncated:
            del rows[max_rows:]
        return Results.from_tuples(columns, rows, truncated=truncated)

    def _describe(self, sql: str, values: dict[str, str]) -> tuple[Column, ...]:
        """Column names and types of a result that produced no block."""
        try:
            data = self._conn.query(f"DESCRIBE ({sql})", "Arrow", params=values).bytes()
        except Exception:
            return ()
        if not data:
            return ()
        table = _read_arrow(data)
        return tuple(Column(n, t) for n, t in zip(_column_values(table.column(0)), _column_values(table.column(1))))

    def _write(
        self,
        st: _Statement,
        sql: str,
        params: NativeParams,
        native: str,
        values: dict[str, str],
        time_limit_ms: int | None,
    ) -> Results:
        rowcount = -1
        if st.kind in ("UPDATE", "DELETE") and self._backend.exact_rowcount and st.target:
            rowcount = self._count_matching(st, sql, params)
            if rowcount == 0:
                return Results.from_tuples((), [], rowcount=0)
        if self._in_tx:
            self._tx_writes += 1
        with self._statement(time_limit_ms) as tag:
            try:
                r = self._conn.query(tag + native, "TSV", params=values)
            except Exception as e:
                if st.kind == "UPDATE" and "Lightweight updates are not supported" in str(e):
                    mutation, mvalues = bind(_as_mutation(st, sql), params)
                    r = self._conn.query(tag + mutation, "TSV", params=mvalues)
                else:
                    raise
        if st.kind == "INSERT":
            rowcount = r.rows_written()
        return Results.from_tuples((), [], rowcount=rowcount)

    def _count_matching(self, st: _Statement, sql: str, params: NativeParams) -> int:
        assert st.target is not None
        table = sql[st.target[0] : st.target[1]].strip()
        cond = sql[st.where :] if st.where is not None else " 1"
        count_sql, values = bind(f"SELECT count() FROM {table} WHERE{cond}", params)
        data = self._conn.query(count_sql, "TSV", params=values).bytes()
        return int(data.strip() or 0)

    async def execute_batch(
        self,
        statements: Sequence[tuple[str, NativeParams]],
        *,
        time_limit_ms: int | None,
        max_rows: int | None,
    ) -> list[Results]:
        """Every statement in one executor hop (``SupportsBatch``)."""

        def run() -> list[Results]:
            return [self._execute(sql, p, time_limit_ms, max_rows, None) for sql, p in statements]

        return await self._run(run)

    async def execute_many(self, sql: str, params_seq: Sequence[NativeParams]) -> WriteResult:
        return await self._run(self._execute_many, sql, params_seq)

    def _execute_many(self, sql: str, params_seq: Sequence[NativeParams]) -> WriteResult:
        st = self._check(sql)
        if st.kind is None or not params_seq:
            return WriteResult(rowcount=0)
        values_at = _values_tuple(sql) if st.kind == "INSERT" else None
        if values_at is not None and _only_placeholders(sql[values_at:]):
            n = self._insert_arrow(sql[:values_at], params_seq)
            if n is not None:
                return WriteResult(rowcount=n)
        if values_at is None:
            total = 0
            for p in params_seq:
                r = self._execute(sql, p, None, None, None)
                total += max(r.rowcount, 0)
            return WriteResult(rowcount=total)
        # INSERT ... VALUES (...): many rows per statement.
        head, tuple_sql = sql[: values_at], sql[values_at:]
        total = 0
        for i in range(0, len(params_seq), _INSERT_ROWS_PER_STATEMENT):
            chunk = params_seq[i : i + _INSERT_ROWS_PER_STATEMENT]
            parts: list[str] = []
            values: dict[str, str] = {}
            for r, p in enumerate(chunk):
                t, v = bind(tuple_sql, p, suffix=f"_{r}")
                parts.append(t)
                values.update(v)
            if self._in_tx:
                self._tx_writes += 1
            with self._statement(None) as tag:
                res = self._conn.query(tag + head + ", ".join(parts), "TSV", params=values)
            total += res.rows_written()
        return WriteResult(rowcount=total)

    def _insert_arrow(self, head: str, params_seq: Sequence[NativeParams]) -> int | None:
        """``INSERT INTO t (cols) VALUES ($1, ..., $n)`` for many rows, sent as
        one Arrow stream (about 50 times faster than VALUES with parameters).
        Arrow columns are matched to the table's by name, so this needs the
        explicit column list and converts each value to the column's type
        as the native ``insert_rows`` does. None to decline (no column list,
        a qualified table name, a type this path does not convert, values
        that do not make a clean column): the caller uses VALUES."""
        target = _insert_target(head)
        if target is None:
            return None
        table, cols = target
        rows = [list(p.values()) if isinstance(p, Mapping) else list(p) for p in params_seq]
        if any(len(r) != len(cols) for r in rows):
            return None
        info = _table_info(self._conn, table)
        if info is None:
            return None
        types = info[0]
        if any(c not in types for c in cols) or len(set(cols)) != len(cols):
            return None
        data = _arrow_stream(cols, [types[c] for c in cols], [dict(zip(cols, r)) for r in rows])
        if data is None:
            return None
        if self._in_tx:
            self._tx_writes += 1
        d = self._backend.dialect
        with self._statement(None) as tag:
            ins = self._conn.send_insert(
                f"{tag}INSERT INTO {d.quote_identifier(table)} ({d.quote_identifiers(cols)})",
                "ArrowStream",
            )
            try:
                ins.append(data)
                return ins.finish().rows_written
            except BaseException:
                with contextlib.suppress(Exception):
                    ins.cancel()
                raise

    def stream(
        self,
        sql: str,
        params: NativeParams,
        *,
        chunk_rows: int,
        time_limit_ms: int | None = None,
        max_result_bytes: int | None = None,
    ) -> "_ClickHouseStream":
        return _ClickHouseStream(self, sql, params, chunk_rows, time_limit_ms, max_result_bytes)

    # -- transactions (none) -----------------------------------------------------------

    async def begin(self, mode: str) -> None:
        if mode not in ("write", "read"):
            raise ValueError(f"unknown transaction mode {mode!r}")
        self._in_tx = True
        self._tx_writes = 0

    async def commit(self) -> None:
        self._in_tx = False

    async def rollback(self) -> None:
        if self._in_tx and self._tx_writes:
            log.warning(
                "ClickHouse has no transactions: %d write statement(s) of a rolled-back "
                "transaction on %r were already applied and are kept",
                self._tx_writes,
                self._backend.database,
            )
        self._in_tx = False
        self._tx_writes = 0

    # -- schema ----------------------------------------------------------------------

    async def schema_version(self) -> object:
        return await self._run(self._schema_version)

    def _schema_version(self) -> str:
        data = self._conn.query(_VERSION_SQL, "TSV").bytes()
        return hashlib.sha1(data).hexdigest()[:16]

    async def schema(self) -> Schema:
        return await self._run(self._schema)

    def _schema(self) -> Schema:
        return _read_schema(self._conn, self._schema_version())

    # -- native operations -------------------------------------------------------------

    async def run_operation(self, name: str, args: dict[str, Any]) -> object:
        """``insert_rows`` into an existing table as one Arrow stream per
        group of rows with the same keys (``SupportsOperations``), with the
        same key check as the default path. Declines ``alter``, ``ignore``,
        ``replace``, a missing table, and column types it does not convert
        (the default path then runs)."""
        if name != "insert_rows" or not args.get("rows"):
            return NotImplemented
        if args.get("ignore") or args.get("replace") or args.get("alter"):
            return NotImplemented
        result = await self._run(self._native_insert, args["table"], args["rows"], args.get("pk"))
        if result is not NotImplemented:
            self._backend.native_calls[name] += 1
        return result

    def _native_insert(self, table: str, rows: list[dict[str, Any]], pk: Any) -> object:
        from ..core.operations import OperationResult, _groups

        info = _table_info(self._conn, table)
        if info is None:
            return NotImplemented
        types, table_pk = info
        groups = []
        for cols, group in _groups(rows):
            if any(c not in types for c in cols):
                return NotImplemented  # the default path reports the engine's error
            data = _arrow_stream(cols, [types[c] for c in cols], group)
            if data is None:
                return NotImplemented
            groups.append((cols, data))
        keys = tuple(pk) if isinstance(pk, (list, tuple)) else ((pk,) if pk else table_pk)
        if keys:
            self._check_new_keys(table, keys, rows)
        d = self._backend.dialect
        total = 0
        for cols, data in groups:
            if self._in_tx:
                self._tx_writes += 1
            with self._statement(None) as tag:
                ins = self._conn.send_insert(
                    f"{tag}INSERT INTO {d.quote_identifier(table)} ({d.quote_identifiers(cols)})",
                    "ArrowStream",
                )
                try:
                    ins.append(data)
                    total += ins.finish().rows_written
                except BaseException:
                    with contextlib.suppress(Exception):
                        ins.cancel()
                    raise
        return OperationResult(rowcount=total)

    def _check_new_keys(self, table: str, pk: Sequence[str], rows: list[dict[str, Any]]) -> None:
        from ..core.emulated_keys import _CHUNK

        d = self._backend.dialect
        keys = [tuple(r.get(c) for c in pk) for r in rows]
        if len(set(keys)) != len(keys):
            dup = next(k for i, k in enumerate(keys) if k in keys[:i])
            raise IntegrityError(_dup_message(table, pk, dup), "UNIQUE")
        cols = d.quote_identifiers(pk)
        key = cols if len(pk) == 1 else f"({cols})"
        for i in range(0, len(keys), _CHUNK):
            part = keys[i : i + _CHUNK]
            marks = ", ".join(
                ("?" if len(pk) == 1 else "(" + ", ".join("?" for _ in pk) + ")") for _ in part
            )
            from ..core.params import rewrite

            sql, params = rewrite(
                f"SELECT {cols} FROM {d.quote_identifier(table)} WHERE {key} IN ({marks}) LIMIT 1",
                [v for k in part for v in k],
                d.paramstyle,
                d.flavor,
            )
            native, values = bind(sql, params)
            data = self._conn.query(native, "Arrow", params=values).bytes()
            found = _rows(_read_arrow(data)) if data else []
            if found:
                raise IntegrityError(_dup_message(table, pk, found[0]), "UNIQUE")

    # -- export ----------------------------------------------------------------------

    @property
    def export_formats(self) -> frozenset[str]:
        return self._backend.export_formats

    def export(
        self,
        sql: str,
        params: NativeParams,
        *,
        encoding: str,
        max_rows: int | None,
        time_limit_ms: int | None,
        chunk_bytes: int,
    ) -> "_ClickHouseExport":
        return _ClickHouseExport(self, sql, params, encoding, max_rows, time_limit_ms)

    # -- misc --------------------------------------------------------------------------

    async def interrupt(self) -> None:
        with self._lock:
            seq = self._active
        if seq is not None:
            await run_blocking(self._backend.executor, self._kill_while_active, seq)

    async def close(self) -> None:
        if not self._closed:
            self._closed = True
            await run_blocking(self._backend.executor, _server.release, self._conn)


def _values_tuple(sql: str) -> int | None:
    """Offset of the ``(`` of a trailing ``VALUES (...)`` tuple, if the
    INSERT ends with exactly one."""
    toks = list(_scan(sql, CLICKHOUSE_FLAVOR, words=True))
    for k in range(len(toks) - 1, -1, -1):
        kind, start, end = toks[k]
        if kind == "word" and sql[start:end].upper() == "VALUES":
            if k + 1 < len(toks) and toks[k + 1][0] == "(":
                depth = 0
                for kind2, _s, e in toks[k + 1 :]:
                    if kind2 == "(":
                        depth += 1
                    elif kind2 == ")":
                        depth -= 1
                        if depth == 0:
                            return toks[k + 1][1] if not sql[e:].strip() else None
            return None
    return None


_PLACEHOLDER_TUPLE = re.compile(r"\(\s*\$1(\s*,\s*\$\d+)*\s*\)\s*")


_IDENT = r'(?:"(?:[^"]|"")*"|`(?:[^`]|``)*`|[A-Za-z_][A-Za-z0-9_]*)'
_INSERT_HEAD = re.compile(
    rf"\s*(?:/\*.*?\*/\s*)?INSERT\s+INTO\s+({_IDENT})\s*\(\s*({_IDENT}(?:\s*,\s*{_IDENT})*)\s*\)\s*VALUES\s*",
    re.IGNORECASE | re.DOTALL,
)


def _unquote(ident: str) -> str:
    if ident[0] == '"':
        return ident[1:-1].replace('""', '"')
    if ident[0] == "`":
        return ident[1:-1].replace("``", "`")
    return ident


def _insert_target(head: str) -> tuple[str, list[str]] | None:
    """Table and column names of ``INSERT INTO t (a, b) VALUES``; None for
    anything else (no column list, ``db.t``, a SELECT ...)."""
    m = _INSERT_HEAD.fullmatch(head)
    if m is None:
        return None
    cols = re.findall(_IDENT, m.group(2))
    return _unquote(m.group(1)), [_unquote(c) for c in cols]


def _only_placeholders(tuple_sql: str) -> bool:
    """``($1, $2, ...)`` in order, nothing else."""
    if not _PLACEHOLDER_TUPLE.fullmatch(tuple_sql):
        return False
    nums = [int(x) for x in re.findall(r"\$(\d+)", tuple_sql)]
    return nums == list(range(1, len(nums) + 1))


def _as_mutation(st: _Statement, sql: str) -> str:
    """``UPDATE t SET a = 1 WHERE c`` as ``ALTER TABLE t UPDATE a = 1 WHERE c``
    for tables without the lightweight-update columns."""
    assert st.target is not None and st.set_at is not None
    table = sql[st.target[0] : st.target[1]].strip()
    rest = sql[st.set_at + 3 :]
    return f"ALTER TABLE {table} UPDATE{rest}"


class _ClickHouseStream:
    """Chunks of a streamed query, one executor hop per chunk.
    ``time_limit_ms`` is a budget of time spent waiting on ClickHouse."""

    def __init__(
        self,
        conn: ClickHouseConnection,
        sql: str,
        params: NativeParams,
        chunk_rows: int,
        time_limit_ms: int | None,
        max_result_bytes: int | None,
    ) -> None:
        if chunk_rows < 1:
            raise ValueError("chunk_rows must be at least 1")
        self._conn = conn
        self._sql = sql
        self._params = params
        self._chunk_rows = chunk_rows
        self._remaining_ms: float | None = time_limit_ms
        self._max_bytes = max_result_bytes
        self._bytes = 0
        self._res: Any = None
        self._buffer: list[tuple[Any, ...]] = []
        self._eof = False
        self._done = False
        self._started = False
        self._query_seq: int | None = None
        self.columns: tuple[Column, ...] | None = None

    def __aiter__(self) -> "_ClickHouseStream":
        return self

    async def __anext__(self) -> list[tuple[Any, ...]]:
        if self._done:
            raise StopAsyncIteration
        try:
            chunk = await self._conn._run(self._next_chunk)
        except BaseException:
            await self.aclose()
            raise
        if not chunk:
            await self.aclose()
            raise StopAsyncIteration
        return chunk

    def _next_chunk(self) -> list[tuple[Any, ...]]:
        import time

        conn = self._conn
        low = conn._conn._conn
        while len(self._buffer) < self._chunk_rows and not self._eof:
            if self._remaining_ms is not None and self._remaining_ms <= 0:
                raise QueryInterrupted("query interrupted (time limit exceeded)")
            start = time.perf_counter()
            try:
                limit = None if self._remaining_ms is None else int(self._remaining_ms)
                with conn._statement(limit, self._query_seq) as tag:
                    if not self._started:
                        self._started = True
                        st = conn._check(self._sql)
                        if st.kind is None:
                            self.columns = ()
                            self._eof = True
                            break
                        if st.kind not in _RESULT_KINDS:
                            raise QueryError("stream() runs queries that return rows", "NOT_A_QUERY")
                        native, values = bind(self._sql, self._params)
                        self._native = (native, values)
                        self._res = low.send_query(tag + native, "Arrow", params=values)
                        self._query_seq = conn.last_seq
                    chunk = low.streaming_fetch_result(self._res)
            finally:
                if self._remaining_ms is not None:
                    self._remaining_ms -= (time.perf_counter() - start) * 1000
            data = None if chunk is None else chunk.bytes()
            if not data:
                if chunk is None or chunk.rows_read() == 0:
                    self._eof = True
                    self._res = None
                continue
            table = _read_arrow(data)
            if self.columns is None:
                self.columns = _columns(table.schema)
            if self._max_bytes is not None:
                self._bytes += table.nbytes
                if self._bytes > self._max_bytes:
                    raise ResourceLimitExceeded(f"result exceeded max_result_bytes={self._max_bytes}")
            self._buffer.extend(_rows(table))
        if self.columns is None and self._started and getattr(self, "_native", None):
            self.columns = conn._describe(*self._native)
        out, self._buffer = self._buffer[: self._chunk_rows], self._buffer[self._chunk_rows :]
        return out

    async def aclose(self) -> None:
        if self._done:
            return
        self._done = True
        res, self._res = self._res, None
        if res is not None and not self._conn._closed:
            low = self._conn._conn._conn

            def cancel() -> None:
                with contextlib.suppress(Exception):
                    low.streaming_cancel_query(res)

            await run_blocking(self._conn._backend.executor, cancel)


# -- introspection --------------------------------------------------------------------------

_VERSION_SQL = """
SELECT name, create_table_query FROM system.tables
WHERE database = currentDatabase() ORDER BY name
"""


def _split_key(expr: str) -> tuple[str, ...]:
    """``primary_key`` of system.tables (``a, b`` or ```a b`, c``) as names."""
    from .duckdb import _split_top_level

    out = []
    expr = expr.strip()
    while expr.startswith("(") and expr.endswith(")") and len(_split_top_level(expr)) == 1:
        inner = expr[1:-1].strip()
        if not inner or inner.count("(") != inner.count(")"):
            break
        expr = inner
    if expr in ("", "tuple()"):
        return ()
    for part in _split_top_level(expr.replace("`", '"')):
        part = part.strip()
        if len(part) >= 2 and part[0] == part[-1] == '"':
            part = part[1:-1].replace('""', '"').replace('\\"', '"')
        out.append(part)
    return tuple(out)


def _query_rows(conn: Any, sql: str) -> list[tuple[Any, ...]]:
    data = conn.query(sql, "Arrow").bytes()
    if not data:
        return []
    return _rows(_read_arrow(data))


def _read_schema(conn: Any, version: str) -> Schema:
    tables = _query_rows(
        conn,
        "SELECT name, engine, create_table_query, primary_key FROM system.tables "
        "WHERE database = currentDatabase() AND NOT is_temporary ORDER BY metadata_modification_time, name",
    )
    cols: dict[str, list[tuple[Any, ...]]] = {}
    for tname, cname, ctype, kind, expr in _query_rows(
        conn,
        "SELECT table, name, type, default_kind, default_expression FROM system.columns "
        "WHERE database = currentDatabase() ORDER BY table, position",
    ):
        cols.setdefault(tname, []).append((cname, ctype, kind, expr))
    indexes: dict[str, list[Index]] = {}
    for tname, iname, itype, expr, gran in _query_rows(
        conn,
        "SELECT table, name, type_full, expr, granularity FROM system.data_skipping_indices "
        "WHERE database = currentDatabase() ORDER BY table, name",
    ):
        names = {c[0] for c in cols.get(tname, [])}
        icols = tuple(c if c in names else None for c in _split_key(expr))
        d = ClickHouseDialect()
        indexes.setdefault(tname, []).append(
            Index(
                name=iname,
                columns=icols,
                unique=False,
                origin="c",
                definition=(
                    f"CREATE INDEX {d.quote_identifier(iname)} ON {d.quote_identifier(tname)} "
                    f"({expr}) TYPE {itype} GRANULARITY {gran}"
                ),
            )
        )
    out_tables: dict[str, TableInfo] = {}
    views: dict[str, TableInfo] = {}
    for name, engine, create_sql, primary_key in tables:
        pk = _split_key(primary_key or "")
        columns = tuple(
            ColumnInfo(
                name=cname,
                type=ctype,
                not_null=not ctype.startswith("Nullable(") and cname not in pk,
                default=expr if kind == "DEFAULT" and expr else None,
                primary_key=pk.index(cname) + 1 if cname in pk else 0,
                generated=kind in ("MATERIALIZED", "ALIAS", "EPHEMERAL"),
            )
            for cname, ctype, kind, expr in cols.get(name, [])
        )
        if engine in ("View", "MaterializedView", "LiveView", "WindowView"):
            views[name] = TableInfo(name=name, kind="view", columns=columns, definition=create_sql)
            continue
        out_tables[name] = TableInfo(
            name=name,
            kind="table",
            columns=columns,
            indexes=tuple(indexes.get(name, ())),
            definition=create_sql,
        )
    return Schema(version=version, tables=out_tables, views=views, triggers={})



# -- native insert -------------------------------------------------------------------------


def _dup_message(table: str, pk: Sequence[str], key: Sequence[Any]) -> str:
    return (
        f"UNIQUE constraint failed: {table}.{', '.join(pk)} (key {list(key)!r} already "
        "exists; checked by dblayer, the engine does not enforce keys)"
    )


def _table_info(conn: Any, table: str) -> tuple[dict[str, str], tuple[str, ...]] | None:
    """Column types and primary key of ``table`` (None if no such table)."""
    data = conn.query(
        "SELECT t.primary_key, c.name, c.type FROM system.tables t "
        "JOIN system.columns c ON c.database = t.database AND c.table = t.name "
        "WHERE t.database = currentDatabase() AND t.name = {t:String} "
        "AND t.engine NOT LIKE '%View' ORDER BY c.position",
        "Arrow",
        params={"t": table.translate(_ESCAPES)},
    ).bytes()
    rows = _rows(_read_arrow(data)) if data else []
    if not rows:
        return None
    return {r[1]: r[2] for r in rows}, _split_key(rows[0][0] or "")


def _arrow_type(ch_type: str) -> tuple[Any, bool] | None:
    """Arrow type to send for a ClickHouse column type, and whether NULL is
    allowed; None for types this path does not convert."""
    import pyarrow as pa

    nullable = ch_type.startswith("Nullable(")
    base = ch_type[len("Nullable(") : -1] if nullable else ch_type
    if base.startswith("LowCardinality("):
        base = base[len("LowCardinality(") : -1]
        if base.startswith("Nullable("):
            nullable, base = True, base[len("Nullable(") : -1]
    if base in ("Int8", "Int16", "Int32", "Int64"):
        return pa.int64(), nullable
    if base in ("UInt8", "UInt16", "UInt32", "UInt64"):
        return pa.uint64(), nullable
    if base in ("Float32", "Float64"):
        return pa.float64(), nullable
    if base == "String":
        return pa.binary(), nullable
    if base == "Bool":
        return pa.bool_(), nullable
    return None


def _arrow_stream(cols: Sequence[str], types: Sequence[str], rows: Sequence[Mapping[str, Any]]) -> bytes | None:
    """The rows as one Arrow IPC stream, or None to decline."""
    import pyarrow as pa

    arrays = []
    fields = []
    for c, t in zip(cols, types):
        at = _arrow_type(t)
        if at is None:
            return None
        atype, nullable = at
        values = [r[c] for r in rows]
        if not nullable and any(v is None for v in values):
            raise IntegrityError(f"NOT NULL constraint failed: {c}", "NOT_NULL")
        if atype == pa.binary():
            values = [v.encode() if isinstance(v, str) else v for v in values]
        elif atype != pa.bool_():
            values = [int(v) if isinstance(v, bool) else v for v in values]
        try:
            arrays.append(pa.array(values, type=atype))
        except (pa.ArrowInvalid, pa.ArrowTypeError, TypeError, OverflowError):
            return None
        fields.append(pa.field(c, atype, nullable=nullable))
    batch = pa.RecordBatch.from_arrays(arrays, schema=pa.schema(fields))
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    return sink.getvalue().to_pybytes()


# -- native export -------------------------------------------------------------------------

# ClickHouse output formats per export encoding. The session settings
# (_SESSION) bring them close to docs/export-formats.md: CRLF and NULL as an
# empty unquoted CSV field, JSON numbers unquoted, NaN and infinities as
# null, "/" not escaped, invalid UTF-8 replaced. TSV is not native: the
# CSV delimiter is a session setting, which a readonly=1 connection cannot
# change per query, and ClickHouse's own TSV escapes with backslashes.
_EXPORT_FORMATS = {
    "csv": "CSV",
    "json-objects": "JSONEachRow",
    "ndjson": "JSONEachRow",
    "json-arrays": "JSONCompactEachRow",
}


class _ClickHouseExport:
    """``SupportsExport`` source. Without ``max_rows`` the bytes come from
    ClickHouse's own formats (JSON lines are one row each, so the leading
    comma of the JSON shapes is added between lines). With ``max_rows`` the
    rows are read as Arrow and encoded by the core's Python encoder, since
    a CSV chunk cannot be split into rows without parsing it."""

    def __init__(
        self,
        conn: ClickHouseConnection,
        sql: str,
        params: NativeParams,
        encoding: str,
        max_rows: int | None,
        time_limit_ms: int | None,
    ) -> None:
        self._conn = conn
        self._sql = sql
        self._params = params
        self._encoding = encoding
        self._max_rows = max_rows
        self._time_limit_ms = time_limit_ms
        self.row_count = 0
        self.truncated = False
        self._res: Any = None
        self._stream: _ClickHouseStream | None = None
        self._encoder: Any = None
        self._first = True
        self._done = False
        self._query_seq: int | None = None
        self.native = max_rows is None

    async def start(self) -> Sequence[Column]:
        if not self.native:
            from ..core.export import PyRowEncoder

            self._stream = _ClickHouseStream(
                self._conn, self._sql, self._params, 1000, self._time_limit_ms, None
            )
            try:
                self._pending = await self._stream.__anext__()
            except StopAsyncIteration:
                self._pending = []
            cols = self._stream.columns or ()
            self._encoder = PyRowEncoder(self._encoding, cols)
            return cols
        return await self._conn._run(self._start_native)

    def _start_native(self) -> Sequence[Column]:
        conn = self._conn
        st = conn._check(self._sql)
        if st.kind not in _RESULT_KINDS:
            raise QueryError("export() runs queries that return rows", "NOT_A_QUERY")
        native, values = bind(self._sql, self._params)
        cols = conn._describe(native, values)
        fmt = _EXPORT_FORMATS[self._encoding]
        low = conn._conn._conn
        with conn._statement(self._time_limit_ms) as tag:
            self._res = low.send_query(tag + native, fmt, params=values)
            self._query_seq = conn.last_seq
        return cols

    def __aiter__(self) -> "_ClickHouseExport":
        return self

    async def __anext__(self) -> bytes:
        if self._done:
            raise StopAsyncIteration
        if not self.native:
            return await self._next_fallback()
        chunk = await self._conn._run(self._next_native)
        if chunk is None:
            self._done = True
            self._res = None
            raise StopAsyncIteration
        return chunk

    def _next_native(self) -> bytes | None:
        low = self._conn._conn._conn
        while True:
            with self._conn._statement(self._time_limit_ms, self._query_seq):
                chunk = low.streaming_fetch_result(self._res)
            if chunk is None:
                return None
            data = chunk.bytes()
            if not data:
                if chunk.rows_read() == 0:
                    return None
                continue
            lines = data.count(b"\n")
            self.row_count += lines
            if self._encoding in ("json-objects", "json-arrays"):
                body = data[:-1].replace(b"\n", b"\n,") + b"\n"
                if not self._first:
                    body = b"," + body
                data = body
            self._first = False
            return data

    async def _next_fallback(self) -> bytes:
        assert self._stream is not None and self._encoder is not None
        while True:
            rows = self._pending
            self._pending = []
            if not rows:
                try:
                    rows = await self._stream.__anext__()
                except StopAsyncIteration:
                    self._done = True
                    raise
            assert self._max_rows is not None
            room = self._max_rows - self.row_count
            if len(rows) > room:
                self.truncated = True
                rows = rows[:room]
            if rows:
                self.row_count += len(rows)
                out = self._encoder.encode(rows).encode()
            else:
                out = b""
            if self.truncated:
                self._done = True
                await self._stream.aclose()
            if out:
                return out
            if self._done:
                raise StopAsyncIteration

    async def aclose(self) -> None:
        self._done = True
        if self._stream is not None:
            await self._stream.aclose()
        res, self._res = self._res, None
        if res is not None and not self._conn._closed:
            low = self._conn._conn._conn

            def cancel() -> None:
                with contextlib.suppress(Exception):
                    low.streaming_cancel_query(res)

            await run_blocking(self._conn._backend.executor, cancel)
