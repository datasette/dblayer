"""``db.export()``: a query result as CSV, TSV, JSON or NDJSON bytes.

The formats are defined in ``docs/export-formats.md``. A backend
connection that implements ``SupportsExport`` produces the row bytes itself
("native"); otherwise this module serialises rows from ``stream()`` in
Python (the "fallback", also the baseline the benchmarks compare against).
Either way the core writes the header row or the JSON envelope, counts
bytes against ``max_bytes`` and enforces ``time_limit_ms``.
"""

from __future__ import annotations

import asyncio
import base64
import csv
import io
import json
import math
import time
from collections.abc import AsyncGenerator, AsyncIterator, Sequence
from dataclasses import dataclass
from json.encoder import encode_basestring  # type: ignore[attr-defined]
from typing import TYPE_CHECKING, Any, Literal

from ..errors import QueryInterrupted, ResourceLimitExceeded
from .common import Opt, map_errors, pick
from .params import Params
from .results import Column

if TYPE_CHECKING:
    from .database import Database

__all__ = [
    "ExportSpec",
    "ExportStream",
    "FORMATS",
    "JSON_SHAPES",
    "ENCODINGS",
    "PyRowEncoder",
    "csv_field",
    "text_form",
]

type ExportFormat = Literal["csv", "tsv", "json", "ndjson"]
type JsonShape = Literal["objects", "arrays", "array"]

FORMATS: tuple[str, ...] = ("csv", "tsv", "json", "ndjson")
JSON_SHAPES: tuple[str, ...] = ("objects", "arrays", "array")
ENCODINGS: tuple[str, ...] = ("csv", "tsv", "json-objects", "json-arrays", "ndjson")
"""What a backend is asked to produce: the bytes of the rows only. The
core adds the header row (CSV, TSV) and the JSON envelope."""

CONTENT_TYPES = {
    "csv": "text/csv; charset=utf-8",
    "tsv": "text/tab-separated-values; charset=utf-8",
    "json": "application/json; charset=utf-8",
    "ndjson": "application/x-ndjson",
}

DEFAULT_CHUNK_BYTES = 1 << 16


@dataclass(frozen=True, slots=True)
class ExportSpec:
    """Which format, shape and header. ``family`` is the key a backend's
    ``export_formats`` lists (``"csv"``, ``"tsv"``, ``"json"``; JSON
    covers every shape and NDJSON); ``encoding`` is what it is asked for."""

    format: str = "csv"
    json_shape: str = "objects"
    header: bool = True

    def __post_init__(self) -> None:
        if self.format not in FORMATS:
            raise ValueError(f"unknown export format {self.format!r}; expected one of {FORMATS}")
        if self.json_shape not in JSON_SHAPES:
            raise ValueError(f"unknown json_shape {self.json_shape!r}; expected one of {JSON_SHAPES}")

    @property
    def family(self) -> str:
        return "json" if self.format == "ndjson" else self.format

    @property
    def encoding(self) -> str:
        if self.format in ("csv", "tsv", "ndjson"):
            return self.format
        return "json-arrays" if self.json_shape == "arrays" else "json-objects"

    @property
    def content_type(self) -> str:
        return CONTENT_TYPES[self.format]

    def head(self, columns: Sequence[str]) -> bytes:
        """Bytes before the first row."""
        f = self.format
        if f in ("csv", "tsv"):
            if not self.header or not columns:
                return b""
            delim = "," if f == "csv" else "\t"
            return (delim.join(csv_field(str(c), delim) for c in columns) + "\r\n").encode()
        if f == "ndjson":
            return b""
        if self.json_shape == "array":
            return b"[\n"
        cols = ",".join(encode_basestring(str(c)) for c in columns)
        return f'{{"ok":true,"columns":[{cols}],"rows":[\n'.encode()

    def tail(self, truncated: bool) -> bytes:
        """Bytes after the last row."""
        if self.format != "json":
            return b""
        if self.json_shape == "array":
            return b"]\n"
        return b'],"truncated":true}\n' if truncated else b'],"truncated":false}\n'


# -- the text form and the Python row encoders --------------------------------------

_SIMPLE = frozenset({str, int, type(None)})


def _float_text(v: float) -> str:
    if v != v:
        return "NaN"
    if v in (math.inf, -math.inf):
        return "Infinity" if v > 0 else "-Infinity"
    return float.__repr__(v)


def _b64(v: bytes | bytearray | memoryview) -> str:
    return base64.b64encode(v).decode("ascii")


def text_form(v: Any) -> str | None:
    """The text form of one value (``None`` for NULL), as CSV and TSV
    fields use it."""
    t = type(v)
    if t is str:
        return v
    if t is int:
        return int.__repr__(v)
    if v is None:
        return None
    if t is float:
        return _float_text(v)
    if t is bool:
        return "true" if v else "false"
    if t in (bytes, bytearray, memoryview):
        return _b64(v)
    if isinstance(v, (list, tuple, dict)):
        return json.dumps(_jsonable(v), ensure_ascii=False, separators=(",", ":"))
    return str(v)


def csv_field(s: str | None, delim: str = ",") -> str:
    """One field by the CSV/TSV quoting rule (NULL is ``None``)."""
    if s is None:
        return ""
    if type(s) is not str:
        s = str(s)
    if s == "" or delim in s or '"' in s or "\n" in s or "\r" in s:
        return '"' + s.replace('"', '""') + '"'
    return s


def _jsonable(v: Any) -> Any:
    """A nested value (DuckDB list/struct/map, PostgreSQL array or json)
    with the format's rules applied inside it, for ``json.dumps``."""
    if isinstance(v, float):
        return v if math.isfinite(v) else None
    if isinstance(v, (bytes, bytearray, memoryview)):
        return {"$base64": True, "encoded": _b64(v)}
    if isinstance(v, (list, tuple)):
        return [_jsonable(x) for x in v]
    if isinstance(v, dict):
        return {str(k): _jsonable(x) for k, x in v.items()}
    if v is None or isinstance(v, (str, int, bool)):
        return v
    return str(v)


def json_value(v: Any) -> str:
    """One value as JSON text."""
    t = type(v)
    if t is str:
        return encode_basestring(v)
    if t is int:
        return int.__repr__(v)
    if v is None:
        return "null"
    if t is float:
        return float.__repr__(v) if math.isfinite(v) else "null"
    if t is bool:
        return "true" if v else "false"
    if t in (bytes, bytearray, memoryview):
        return '{"$base64":true,"encoded":"' + _b64(v) + '"}'
    if isinstance(v, (list, tuple, dict)):
        return json.dumps(_jsonable(v), ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    return encode_basestring(str(v))


class PyRowEncoder:
    """The fallback: rows (tuples of Python values) to the row bytes of one
    encoding. ``first`` tracks the leading-comma rule of the JSON shapes."""

    def __init__(self, encoding: str, columns: Sequence[str]) -> None:
        self.encoding = encoding
        self.ncols = len(columns)
        self.first = True
        if encoding in ("csv", "tsv"):
            self._delim = "," if encoding == "csv" else "\t"
            self._buf = io.StringIO()
            self._writer = csv.writer(
                self._buf,
                dialect="excel" if encoding == "csv" else "excel-tab",
                lineterminator="\r\n",
            )
        else:
            # Repeated names: the first occurrence wins (Row.as_dict()).
            seen: dict[str, int] = {}
            for i, c in enumerate(columns):
                seen.setdefault(str(c), i)
            self._keys = [(encode_basestring(k) + ":", i) for k, i in seen.items()]

    def encode(self, rows: Sequence[Sequence[Any]]) -> str:
        if self.encoding in ("csv", "tsv"):
            return self._csv(rows)
        return self._json(rows)

    def _csv(self, rows: Sequence[Sequence[Any]]) -> str:
        buf, writer, delim = self._buf, self._writer, self._delim
        simple = _SIMPLE
        one = self.ncols == 1
        for row in rows:
            vals = [v if type(v) in simple else text_form(v) for v in row]
            if one or "" in vals:
                # csv.writer cannot quote an empty string (nor leave a
                # lone NULL unquoted): write this row by hand.
                buf.write(delim.join([csv_field(v, delim) for v in vals]))
                buf.write("\r\n")
            else:
                writer.writerow(vals)
        out = buf.getvalue()
        buf.seek(0)
        buf.truncate()
        return out

    def _json(self, rows: Sequence[Sequence[Any]]) -> str:
        jv = json_value
        parts: list[str] = []
        lead = "" if self.first else ","
        if self.encoding == "json-arrays":
            for row in rows:
                parts.append(lead + "[" + ",".join([jv(v) for v in row]) + "]\n")
                lead = ","
        else:
            keys = self._keys
            if self.encoding == "ndjson":
                lead = ""
            for row in rows:
                parts.append(lead + "{" + ",".join([k + jv(row[i]) for k, i in keys]) + "}\n")
                if self.encoding != "ndjson":
                    lead = ","
        if rows:
            self.first = False
        return "".join(parts)


# -- the stream -------------------------------------------------------------------------


BACKSTOP_GRACE_S = 0.1
"""How long past ``time_limit_ms`` the core cancels a backend step itself."""


class ExportStream:
    """What ``db.export()`` returns: an async iterator of ``bytes`` chunks
    (each one the consumer's to keep), also an async context manager whose
    exit closes it and returns the connection, even after ``break`` or an
    exception.

    Metadata: ``columns`` (once the first chunk has been produced),
    ``row_count`` and ``bytes_sent`` (so far), ``truncated`` (final once the
    iterator is exhausted), ``native`` (whether the backend serialised;
    ``None`` until the first chunk), ``content_type``.
    """

    def __init__(
        self,
        db: "Database",
        sql: str,
        params: Params,
        spec: ExportSpec,
        *,
        time_limit_ms: int | None,
        max_rows: int | None,
        max_bytes: int | None,
        chunk_bytes: int,
        native: bool,
        chunk_rows: int,
    ) -> None:
        if chunk_bytes < 1:
            raise ValueError("chunk_bytes must be at least 1")
        if max_rows is not None and max_rows < 0:
            raise ValueError("max_rows must not be negative")
        self.spec = spec
        self.columns: tuple[Column, ...] | None = None
        self.row_count = 0
        self.bytes_sent = 0
        self.truncated = False
        self.native: bool | None = None
        self.done = False
        self._db = db
        self._sql = sql
        self._params = params
        self._time_limit_ms = time_limit_ms
        self._spent_ms = 0.0
        self._backstop_fired = False
        self._max_rows = max_rows
        self._max_bytes = max_bytes
        self._chunk_bytes = chunk_bytes
        self._want_native = native
        self._chunk_rows = chunk_rows
        self._gen: AsyncGenerator[bytes, None] = self._run()

    @property
    def content_type(self) -> str:
        return self.spec.content_type

    def __aiter__(self) -> "ExportStream":
        return self

    async def __anext__(self) -> bytes:
        return await self._gen.__anext__()

    async def aclose(self) -> None:
        await self._gen.aclose()

    async def __aenter__(self) -> "ExportStream":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    async def read_all(self) -> bytes:
        """The whole body (tests, small results)."""
        async with self:
            return b"".join([c async for c in self])

    # -- internals -------------------------------------------------------------

    async def _backend[T](self, aw: "asyncio.Future[T] | Any") -> T:
        """Await one step of the backend under the remaining time budget."""
        limit = self._time_limit_ms
        t0 = time.perf_counter()
        try:
            if limit is None:
                return await aw
            remaining = (limit - self._spent_ms) / 1000
            if remaining <= 0:
                if hasattr(aw, "close"):
                    aw.close()
                raise QueryInterrupted(f"export exceeded time_limit_ms={limit}")
            try:
                # A grace behind the backend's own limit (it was given the
                # same budget): the engine stopping its statement is the
                # clean path; this cancel is the backstop.
                async with asyncio.timeout(remaining + BACKSTOP_GRACE_S):
                    return await aw
            except TimeoutError:
                # The core cancelled a backend call mid-flight, past the
                # backend's own limit: do not trust that connection again
                # (psycopg can be left "busy" by a cancel that lands in its
                # cleanup; the DuckDB worker with its read transaction open).
                self._backstop_fired = True
                raise QueryInterrupted(f"export exceeded time_limit_ms={limit}") from None
        finally:
            self._spent_ms += (time.perf_counter() - t0) * 1000

    def _check_bytes(self, n: int) -> None:
        cap = self._max_bytes
        if cap is not None and self.bytes_sent + n > cap:
            raise ResourceLimitExceeded(f"export exceeded max_bytes={cap}")
        self.bytes_sent += n

    async def _run(self) -> AsyncIterator[bytes]:
        from .pool import _should_discard

        db = self._db
        db._check_open()
        backend = db._backend
        native_sql, native_params = db._native(self._sql, self._params)
        with map_errors(backend):
            pool = await db._use_pool()
            conn = await pool.acquire(write=False)
        discard = False
        try:
            formats = getattr(conn, "export_formats", None) or ()
            use_native = self._want_native and self.spec.family in formats and hasattr(conn, "export")
            self.native = use_native
            body = (
                self._native_body(conn, native_sql, native_params)
                if use_native
                else self._fallback_body(conn, native_sql, native_params)
            )
            try:
                async for chunk in body:
                    if chunk:
                        self._check_bytes(len(chunk))
                        yield chunk
            finally:
                await body.aclose()
            self.done = True
        except BaseException as e:
            discard = _should_discard(e)
            raise
        finally:
            await pool.release(conn, discard=discard or self._backstop_fired)

    async def _native_body(self, conn: Any, sql: str, params: Any) -> AsyncIterator[bytes]:
        spec = self.spec
        src = conn.export(
            sql,
            params,
            encoding=spec.encoding,
            max_rows=self._max_rows,
            time_limit_ms=self._time_limit_ms,
            chunk_bytes=self._chunk_bytes,
        )
        try:
            with map_errors(self._db._backend):
                columns = await self._backend(src.start())
            self.columns = tuple(columns)
            head = spec.head(self.columns)
            if head:
                yield head
            while True:
                with map_errors(self._db._backend):
                    try:
                        chunk = await self._backend(src.__anext__())
                    except StopAsyncIteration:
                        break
                self.row_count = src.row_count
                # Hand the consumer an object it owns: a memoryview into a
                # buffer the backend reuses would be overwritten later.
                yield chunk if type(chunk) is bytes else bytes(chunk)
            self.row_count = src.row_count
            self.truncated = bool(src.truncated)
            tail = spec.tail(self.truncated)
            if tail:
                yield tail
        finally:
            await src.aclose()

    async def _fallback_body(self, conn: Any, sql: str, params: Any) -> AsyncIterator[bytes]:
        spec = self.spec
        db = self._db
        max_rows = self._max_rows
        rs = conn.stream(
            sql,
            params,
            chunk_rows=self._chunk_rows,
            # The core enforces the budget (``_backend``); the backend's own
            # limit is the same budget, so either may fire first.
            time_limit_ms=self._time_limit_ms,
        )
        enc: PyRowEncoder | None = None
        pending: list[str] = []
        size = 0
        try:
            while True:
                if max_rows is not None and self.row_count >= max_rows and enc is not None:
                    # At the limit: one more chunk tells whether rows remain.
                    with map_errors(db._backend):
                        try:
                            extra = await self._backend(rs.__anext__())
                        except StopAsyncIteration:
                            break
                    if extra:
                        self.truncated = True
                        break
                    continue
                with map_errors(db._backend):
                    try:
                        chunk = await self._backend(rs.__anext__())
                    except StopAsyncIteration:
                        chunk = None
                if enc is None:
                    self.columns = tuple(rs.columns or ())
                    enc = PyRowEncoder(spec.encoding, self.columns)
                    head = spec.head(self.columns)
                    if head:
                        yield head
                if chunk is None:
                    break
                if max_rows is not None and self.row_count + len(chunk) > max_rows:
                    chunk = chunk[: max_rows - self.row_count]
                    self.truncated = True
                text = enc.encode(chunk)
                self.row_count += len(chunk)
                pending.append(text)
                size += len(text)
                if size >= self._chunk_bytes:
                    yield "".join(pending).encode()
                    pending.clear()
                    size = 0
                if self.truncated:
                    break
            if pending:
                yield "".join(pending).encode()
            tail = spec.tail(self.truncated)
            if tail:
                yield tail
        finally:
            await rs.aclose()


def make_export(
    db: "Database",
    sql: str,
    params: Params,
    *,
    format: str,
    json_shape: str,
    header: bool,
    time_limit_ms: Opt[int],
    max_rows: Opt[int],
    max_bytes: int | None,
    chunk_bytes: int,
    native: bool,
) -> ExportStream:
    spec = ExportSpec(format, json_shape, header)
    return ExportStream(
        db,
        sql,
        params,
        spec,
        time_limit_ms=pick(time_limit_ms, db.time_limit_ms),
        max_rows=pick(max_rows, db.max_rows),
        max_bytes=max_bytes,
        chunk_bytes=chunk_bytes,
        native=native,
        chunk_rows=db.chunk_rows,
    )
