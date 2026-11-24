"""PostgreSQL's side of ``db.export()`` (psycopg 3): the server serialises.

``COPY (<query>) TO STDOUT`` makes the server write every row as text;
psycopg hands back one ``memoryview`` per row (PostgreSQL sends a CopyData
message per row). The user's query is wrapped in a projection that turns
each column into the text ``docs/export-formats.md`` asks for, by type:

* CSV and TSV: ``COPY ... (FORMAT csv, DELIMITER ...)``. PostgreSQL's own
  CSV quoting is the definition's (an empty string is ``""``, NULL is
  empty); booleans become ``true``/``false``, bytea base64. PostgreSQL ends
  rows with LF, so each row's last byte is replaced with CRLF here, which
  rides on the per-row loop psycopg imposes anyway. Floats keep
  PostgreSQL's shortest form (``1`` for 1.0, ``1e+15``): parse-equal.
* JSON: the projection builds each row's JSON text in SQL (``to_json`` per
  value; NaN and infinities ``null``; bytea as ``{"$base64": ...}``) and
  COPY sends it unescaped (CSV mode with control characters as quote and
  delimiter, which JSON text never contains raw). PostgreSQL 18 has no
  ``COPY ... (FORMAT json)``.

COPY takes no bind parameters, so ``$n`` parameters are inlined as
literals (psycopg's ``sql.Literal``) at the positions the shared
tokenizer finds.

Closing early: leaving psycopg's ``copy()`` block before the end leaves the
connection "busy" ("another command is already in progress"), so
``aclose()`` cancels on the server (``cancel_safe``) and drains first.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Sequence
from json.encoder import encode_basestring  # type: ignore[attr-defined]
from typing import TYPE_CHECKING, Any

from ..core.export import PyRowEncoder
from ..core.params import POSTGRES_FLAVOR, dollar_parameters, leading_keyword
from ..core.results import Column

if TYPE_CHECKING:
    from .postgres_psycopg import PsycopgConnection

__all__ = ["PsycopgExport", "inline_parameters"]

_BOOL, _BYTEA, _FLOAT4, _FLOAT8 = 16, 17, 700, 701
_INTS = {20, 21, 23, 26}
_TEXT = {25, 1043, 1042, 19}
_JSONS = {114, 3802}
_SCALARS = {1700, 1082, 1083, 1266, 1114, 1184, 2950, 1186}
"""numeric, date, time, timetz, timestamp, timestamptz, uuid, interval:
written in JSON as a string of the text form; other types (arrays,
composites, ranges...) as PostgreSQL's own JSON."""


def inline_parameters(sql: str, args: Sequence[Any], conn: Any) -> str:
    """``$n`` placeholders replaced with SQL literals (COPY has no
    parameters)."""
    if not args:
        return sql
    from psycopg import sql as psql

    out = sql
    for start, end, n in reversed(dollar_parameters(sql, POSTGRES_FLAVOR)):
        lit = psql.Literal(args[n - 1]).as_string(conn)
        out = out[:start] + lit + out[end:]
    return out


def _b64(c: str) -> str:
    return f"translate(encode({c}, 'base64'), E'\\n', '')"


def py_float(f: str, nonfinite: str | None = None) -> str:
    """``float8``/``float4`` text as Python's ``repr()`` writes it.
    PostgreSQL's shortest digits (``extra_float_digits`` 1, the default)
    are the digits Python picks; only the layout differs: ``1`` for 1.0 and
    ``-0`` for -0.0 (no ``.0``), and the exponent form from 1e15 (float8)
    or 1e7 (float4) where Python starts at 1e16. So: add ``.0`` to
    integral values, and spell values with an exponent from -4 to 15 out
    in positions."""
    t = f"({f})::text"
    m = f"split_part({t}, 'e', 1)"
    e = f"split_part({t}, 'e', 2)::int"
    digits = f"replace(ltrim({m}, '-'), '.', '')"
    sign = f"CASE WHEN left({t}, 1) = '-' THEN '-' ELSE '' END"
    positional = (
        f"{sign} || CASE WHEN {e} >= 0 THEN left(rpad({digits}, {e} + 1, '0'), {e} + 1) || '.' || "
        f"CASE WHEN length({digits}) > {e} + 1 THEN substr({digits}, {e} + 2) ELSE '0' END "
        f"ELSE '0.' || repeat('0', -{e} - 1) || {digits} END"
    )
    nf = f"WHEN {t} IN ('NaN', 'Infinity', '-Infinity') THEN {nonfinite} " if nonfinite else ""
    return (
        f"CASE {nf}WHEN {t} IN ('NaN', 'Infinity', '-Infinity') THEN {t} "
        f"WHEN strpos({t}, 'e') > 0 THEN (CASE WHEN {e} BETWEEN -4 AND 15 THEN {positional} ELSE {t} END) "
        f"WHEN strpos({t}, '.') = 0 THEN {t} || '.0' ELSE {t} END"
    )


def csv_expr(c: str, oid: int) -> str:
    if oid == _BOOL:
        return f"CASE WHEN {c} THEN 'true' WHEN NOT {c} THEN 'false' END"
    if oid == _BYTEA:
        return _b64(c)
    if oid in (_FLOAT4, _FLOAT8):
        return py_float(c)
    return c  # PostgreSQL's text form


def json_expr(c: str, oid: int) -> str:
    if oid in _INTS:
        body = f"{c}::text"
    elif oid in (_FLOAT4, _FLOAT8):
        body = py_float(c, "'null'")
    elif oid == _BOOL:
        body = f"CASE WHEN {c} THEN 'true' WHEN NOT {c} THEN 'false' END"
    elif oid == _BYTEA:
        body = "'{\"$base64\":true,\"encoded\":\"' || " + _b64(c) + " || '\"}'"
    elif oid in _TEXT:
        body = f"to_json({c}::text)::text"
    elif oid in _JSONS:
        body = f"{c}::text"  # unspecified: the column's own JSON
    else:
        # numeric, dates, times, UUIDs: a string holding the text form;
        # arrays and composites: PostgreSQL's JSON (unspecified)
        body = f"to_json({c}::text)::text" if oid in _SCALARS else f"to_json({c})::text"
    return f"coalesce({body}, 'null')"


def copy_sql(sql: str, names: Sequence[str], oids: Sequence[int], encoding: str, limit: int | None) -> str:
    cols = [f"c{i}" for i in range(len(names))]
    tail = "" if limit is None else f" LIMIT {int(limit)}"
    src = f"FROM ({sql}) AS _q({', '.join(cols)}){tail}"
    if encoding in ("csv", "tsv"):
        delim = "','" if encoding == "csv" else "E'\\t'"
        sel = ", ".join(f"{csv_expr(c, o)} AS {c}" for c, o in zip(cols, oids))
        return f"COPY (SELECT {sel} {src}) TO STDOUT (FORMAT csv, DELIMITER {delim})"
    if encoding == "json-arrays":
        line = "'[' || " + " || ',' || ".join(json_expr(c, o) for c, o in zip(cols, oids)) + " || ']'"
    else:
        seen: set[str] = set()
        parts = []
        for name, c, o in zip(names, cols, oids):
            if name in seen:
                continue
            seen.add(name)
            key = ("," if parts else "") + encode_basestring(name) + ":"
            parts.append("'" + key.replace("'", "''") + "' || " + json_expr(c, o))
        line = "'{' || " + " || ".join(parts) + " || '}'"
    return (
        f"COPY (SELECT {line} {src}) TO STDOUT "
        "(FORMAT csv, QUOTE E'\\x01', DELIMITER E'\\x02', ESCAPE E'\\x01')"
    )


_SELECTS = {"SELECT", "VALUES", "TABLE", "WITH"}


class PsycopgExport:
    """``ExportSource`` for ``PsycopgConnection``."""

    def __init__(self, conn: "PsycopgConnection", sql: str, params: Any, *, encoding: str,
                 max_rows: int | None, time_limit_ms: int | None, chunk_bytes: int) -> None:
        self._conn = conn
        self._sql = sql
        self._args = list(params) if params else []
        self._encoding = encoding
        self._max_rows = max_rows
        self._time_limit_ms = time_limit_ms
        self._chunk_bytes = chunk_bytes
        self._cm: Any = None
        self._copy: Any = None
        self._cur: Any = None  # the Python path (SHOW, EXPLAIN...)
        self._py: PyRowEncoder | None = None
        self._done = False
        self._first = True
        self.columns: tuple[Column, ...] | None = None
        self.row_count = 0
        self.truncated = False

    async def start(self) -> Sequence[Column]:
        conn = self._conn
        raw = conn._raw
        await conn._settle_timeout(self._time_limit_ms)
        verb = leading_keyword(self._sql, POSTGRES_FLAVOR)
        if verb not in _SELECTS:
            # Not something COPY can wrap: plain rows, encoded in Python.
            self._cur = raw.cursor()
            await self._cur.execute(self._sql, self._args)
            desc = self._cur.description or ()
            self.columns = tuple(Column(d.name) for d in desc)
            self._py = PyRowEncoder(self._encoding, self.columns)
            if not desc:
                self._done = True
            return self.columns
        cur = raw.cursor()
        await self._uncancellable(cur.execute(f"SELECT * FROM ({self._sql}) AS _q LIMIT 0", self._args))
        desc = cur.description or ()
        self.columns = tuple(Column(d.name) for d in desc)
        if not desc:
            self._done = True
            return self.columns
        inner = inline_parameters(self._sql, self._args, raw)
        limit = None if self._max_rows is None else self._max_rows + 1
        stmt = copy_sql(inner, [d.name for d in desc], [d.type_code for d in desc], self._encoding, limit)
        self._cm = raw.cursor().copy(stmt)
        self._copy = await self._uncancellable(self._cm.__aenter__())
        return self.columns

    async def _uncancellable[T](self, aw: Any) -> T:
        """Await a psycopg step so that a cancellation (a time limit, a
        client gone) cannot leave the connection mid-protocol: cancel on
        the server, let the step finish, then let the cancellation out."""
        task = asyncio.ensure_future(aw)
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            with contextlib.suppress(Exception):
                await self._conn._raw.cancel_safe()
            with contextlib.suppress(BaseException):
                result = await task
                if self._cm is not None and self._copy is None:
                    self._copy = result
            raise

    def __aiter__(self) -> "PsycopgExport":
        return self

    async def __anext__(self) -> bytes:
        if self._done:
            raise StopAsyncIteration
        try:
            out = await (self._next_py() if self._py is not None else self._next_copy())
        except BaseException:
            await self.aclose()
            raise
        if out is None:
            await self.aclose()
            raise StopAsyncIteration
        return out

    async def _next_copy(self) -> bytes | None:
        read = self._copy.read
        buf = bytearray()
        max_rows = self._max_rows
        csv_like = self._encoding in ("csv", "tsv")
        comma = self._encoding in ("json-objects", "json-arrays")
        n = 0
        while len(buf) < self._chunk_bytes:
            row = await read()
            if not row:
                self._done = True
                break
            if max_rows is not None and self.row_count + n >= max_rows:
                self.truncated = True
                self._done = True
                break
            if csv_like:
                buf += row[:-1]
                buf += b"\r\n"
            else:
                if comma and not self._first:
                    buf += b","
                buf += row
                self._first = False
            n += 1
        self.row_count += n
        if self._done:
            await self._finish_copy()
        return bytes(buf) if buf else None

    async def _next_py(self) -> bytes | None:
        assert self._py is not None
        rows = await self._cur.fetchmany(1000)
        max_rows = self._max_rows
        if max_rows is not None and self.row_count + len(rows) > max_rows:
            rows = rows[: max_rows - self.row_count]
            self.truncated = True
            self._done = True
        if not rows:
            self._done = True
            return None
        self.row_count += len(rows)
        return self._py.encode(rows).encode()

    async def _finish_copy(self) -> None:
        cm, self._cm = self._cm, None
        copy, self._copy = self._copy, None
        if cm is None:
            return
        # Done, or truncated: LIMIT max_rows + 1 means at most the end of
        # the COPY is left to read.
        with contextlib.suppress(Exception):
            while copy is not None and await copy.read():
                pass
        with contextlib.suppress(Exception):
            await cm.__aexit__(None, None, None)

    async def aclose(self) -> None:
        if self._cm is not None:
            if not self._done:
                self._done = True
                await self._finish_copy_early()
            else:
                await self._finish_copy()
        self._done = True
        cur, self._cur = self._cur, None
        if cur is not None:
            with contextlib.suppress(Exception):
                await cur.close()

    async def _finish_copy_early(self) -> None:
        cm, self._cm = self._cm, None
        copy, self._copy = self._copy, None
        raw = self._conn._raw
        with contextlib.suppress(Exception):
            await raw.cancel_safe()
        with contextlib.suppress(Exception):
            while await copy.read():
                pass
        with contextlib.suppress(Exception):
            await cm.__aexit__(None, None, None)
