"""DuckDB's side of ``db.export()``: DuckDB builds each output line itself.

DuckDB's own writers (``COPY (q) TO 'file' (FORMAT csv|json)``) need file
system access, which the backend turns off for every connection
(``enable_external_access=false``, locked), and they would differ from
``docs/export-formats.md`` anyway (blobs written raw, ``nan``, LF line
ends, bare ``NaN`` in JSON). So instead the user's query is wrapped in one
that computes, per row, the finished line as a VARCHAR, with SQL
expressions chosen per column type:

    SELECT <csv or json line expression>
    FROM (<user query>) AS _q(c0, c1, ...) LIMIT <max_rows + 1>

DuckDB evaluates those expressions vectorised, in its own threads; Python
receives one string per row (``fetchmany``) and joins them. Python never
sees a typed value: no datetime, Decimal or float object is built.
"""

from __future__ import annotations

import time
from collections.abc import Sequence
from json.encoder import encode_basestring  # type: ignore[attr-defined]
from typing import TYPE_CHECKING, Any

from ..core.export import PyRowEncoder
from ..core.results import Column
from .base import NativeParams

if TYPE_CHECKING:
    from .duckdb import DuckDBConnection

__all__ = ["DuckDBExport", "line_sql"]

_INTS = {"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UTINYINT", "USMALLINT", "UINTEGER",
         "UBIGINT", "UHUGEINT"}
_FLOATS = {"DOUBLE", "FLOAT"}


def _lit(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def _csv_text(c: str, delim: str) -> str:
    """A VARCHAR expression ``c`` as a field: quoted when empty or holding
    the delimiter, a quote, CR or LF."""
    d = "chr(9)" if delim == "\t" else "','"
    return (
        f"CASE WHEN {c} = '' OR contains({c}, {d}) OR contains({c}, '\"') OR contains({c}, chr(10)) "
        f"OR contains({c}, chr(13)) THEN '\"' || replace({c}, '\"', '\"\"') || '\"' ELSE {c} END"
    )


def _float_text(c: str, nonfinite: tuple[str, str, str]) -> str:
    nan, pinf, ninf = nonfinite
    d = f"{c}::DOUBLE"
    # DuckDB's DOUBLE -> VARCHAR is Python's repr() except for -0.0 (it
    # drops the sign) and the non-finite values.
    return (
        f"CASE WHEN isnan({d}) THEN {nan} WHEN isinf({d}) THEN (CASE WHEN {d} > 0 THEN {pinf} ELSE {ninf} END) "
        f"WHEN {d} = 0 AND signbit({d}) THEN '-0.0' ELSE {d}::VARCHAR END"
    )


def csv_field(c: str, typ: str, delim: str) -> str:
    t = typ.upper()
    if t in _INTS:
        body = f"{c}::VARCHAR"
    elif t in _FLOATS:
        body = _float_text(c, ("'NaN'", "'Infinity'", "'-Infinity'"))
    elif t == "BOOLEAN":
        body = f"CASE WHEN {c} THEN 'true' WHEN NOT {c} THEN 'false' END"
    elif t == "BLOB":
        body = f"CASE WHEN octet_length({c}) = 0 THEN '\"\"' ELSE to_base64({c}) END"
    else:
        body = _csv_text(f"{c}::VARCHAR", delim)
    return f"coalesce({body}, '')"


_UPPER_ESCAPES = ["000B", "000E", "000F", "001A", "001B", "001C", "001D", "001E", "001F"]


def _json_str(j: str) -> str:
    """DuckDB's ``to_json`` of a string escapes control characters as
    ``\\u001F``; Python's ``json`` (the definition) as ``\\u001f``. Only
    nine escapes have letters in them, so a replace chain fixes it, applied
    only to the strings that hold an escape at all."""
    fixed = j
    for h in _UPPER_ESCAPES:
        fixed = f"replace({fixed}, '\\u{h}', '\\u{h.lower()}')"
    return f"(CASE WHEN contains({j}, '\\u00') THEN {fixed} ELSE {j} END)"


def json_field(c: str, typ: str) -> str:
    t = typ.upper()
    if t in _INTS:
        body = f"{c}::VARCHAR"
    elif t in _FLOATS:
        body = _float_text(c, ("'null'", "'null'", "'null'"))
    elif t == "BOOLEAN":
        body = f"CASE WHEN {c} THEN 'true' WHEN NOT {c} THEN 'false' END"
    elif t == "BLOB":
        body = "'{\"$base64\":true,\"encoded\":\"' || to_base64(" + c + ") || '\"}'"
    elif t == "VARCHAR":
        body = _json_str(f"to_json({c})::VARCHAR")
    elif t.endswith("]") or t.startswith(("STRUCT", "MAP", "UNION", "JSON")):
        body = f"to_json({c})::VARCHAR"  # nested: DuckDB's JSON (unspecified)
    else:
        body = _json_str(f"to_json({c}::VARCHAR)::VARCHAR")  # dates, decimals, UUIDs: the text form
    return f"coalesce({body}, 'null')"


def line_sql(sql: str, names: Sequence[str], types: Sequence[str], encoding: str, limit: int | None) -> str:
    """The wrapping query whose one column is the finished line."""
    cols = [f"c{i}" for i in range(len(names))]
    if encoding in ("csv", "tsv"):
        delim = "," if encoding == "csv" else "\t"
        sep = " || " + _lit(delim) + " || "
        line = sep.join(csv_field(c, t, delim) for c, t in zip(cols, types)) + " || chr(13) || chr(10)"
    elif encoding == "json-arrays":
        line = "',[' || " + " || ',' || ".join(json_field(c, t) for c, t in zip(cols, types)) + " || ']' || chr(10)"
        if not cols:
            line = "',[]' || chr(10)"
    else:
        seen: set[str] = set()
        parts = []
        for name, c, t in zip(names, cols, types):
            if name in seen:
                continue  # a repeated name: the first occurrence wins
            seen.add(name)
            key = encode_basestring(name) + ":"
            parts.append(_lit(("," if parts else "") + key) + " || " + json_field(c, t))
        lead = "','" if encoding == "json-objects" else "''"
        line = f"{lead} || '{{' || " + (" || ".join(parts) if parts else "''") + " || '}' || chr(10)"
    alias = ", ".join(cols)
    tail = "" if limit is None else f" LIMIT {int(limit)}"
    return f"SELECT {line} FROM ({sql}) AS _q({alias}){tail}"


_FETCH = 2048


class DuckDBExport:
    """``ExportSource`` for ``DuckDBConnection``: one executor hop per chunk,
    each fetching lines until ``chunk_bytes``."""

    def __init__(self, conn: "DuckDBConnection", sql: str, params: NativeParams, *, encoding: str,
                 max_rows: int | None, time_limit_ms: int | None, chunk_bytes: int) -> None:
        self._conn = conn
        self._sql = sql
        self._params = params
        self._encoding = encoding
        self._max_rows = max_rows
        self._remaining_ms: float | None = time_limit_ms
        self._chunk_bytes = chunk_bytes
        self._wrapped = False
        self._done = False
        self._first = True
        self._py: PyRowEncoder | None = None  # statements that cannot be wrapped
        self.columns: tuple[Column, ...] | None = None
        self.row_count = 0
        self.truncated = False

    async def start(self) -> Sequence[Column]:
        await self._conn._run(self._start)
        assert self.columns is not None
        return self.columns

    def _timed(self, fn: Any) -> Any:
        conn = self._conn
        start = time.perf_counter()
        try:
            with conn._statement(None if self._remaining_ms is None else max(int(self._remaining_ms), 0)):
                return fn()
        finally:
            if self._remaining_ms is not None:
                self._remaining_ms -= (time.perf_counter() - start) * 1000

    def _start(self) -> None:
        conn = self._conn
        cur = conn._cur
        kind = conn._check(self._sql)
        if kind is None:
            self.columns = ()
            self._done = True
            return
        self._wrapped = conn._read_only_wrapper()
        limit = None if self._max_rows is None else self._max_rows + 1
        if kind != "SELECT":
            # PRAGMA, CALL, EXPLAIN... cannot be a subquery: encode in Python.
            def run() -> None:
                cur.execute(self._sql, self._params)

            self._timed(run)
            desc = cur.description or ()
            self.columns = tuple(Column(d[0], str(d[1])) for d in desc)
            self._py = PyRowEncoder(self._encoding, self.columns)
            if not desc:
                self._done = True
            return

        def describe() -> Any:
            # A relation is bound, not run, and keeps repeated column names
            # (a subquery would rename the second "a" to "a_1").
            return cur.sql(self._sql, params=self._params or None)

        rel = self._timed(describe)
        names, types = list(rel.columns), [str(t) for t in rel.types]
        self.columns = tuple(Column(n, t) for n, t in zip(names, types))
        if not names:
            self._done = True
            return
        q = line_sql(self._sql, names, types, self._encoding, limit)

        def execute() -> None:
            cur.execute(q, self._params)

        self._timed(execute)

    def __aiter__(self) -> "DuckDBExport":
        return self

    async def __anext__(self) -> bytes:
        if self._done:
            raise StopAsyncIteration
        try:
            chunk = await self._conn._run(self._next)
        except BaseException:
            await self.aclose(ok=False)
            raise
        if chunk is None:
            await self.aclose()
            raise StopAsyncIteration
        return chunk

    def _next(self) -> bytes | None:
        cur = self._conn._cur
        max_rows = self._max_rows
        parts: list[str] = []
        size = 0
        n_rows = 0

        def fetch() -> list[tuple[Any, ...]]:
            return cur.fetchmany(_FETCH)

        while size < self._chunk_bytes:
            batch = self._timed(fetch)
            if not batch:
                self._done = True
                break
            if max_rows is not None and self.row_count + n_rows + len(batch) > max_rows:
                batch = batch[: max_rows - self.row_count - n_rows]
                self.truncated = True
                self._done = True
            if self._py is not None:
                text = self._py.encode(batch)
            else:
                text = "".join([r[0] for r in batch])
            n_rows += len(batch)
            parts.append(text)
            size += len(text)
            if self._done:
                break
        if not n_rows:
            return None
        self.row_count += n_rows
        out = "".join(parts) if len(parts) > 1 else parts[0]
        if self._first and self._py is None:
            self._first = False
            if self._encoding in ("json-objects", "json-arrays"):
                out = out[1:]  # every line starts with ","; the first must not
        return out.encode()

    async def aclose(self, ok: bool = True) -> None:
        if self._wrapped:
            self._wrapped = False
            conn = self._conn
            if not conn._closed:
                from ..core.executor import run_blocking

                await run_blocking(conn._backend.executor, conn._end_wrapper, ok)
        self._done = True

