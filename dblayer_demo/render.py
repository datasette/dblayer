"""Rendering helpers: cell values, and dblayer errors as readable messages."""

from __future__ import annotations

import datetime
import decimal
from typing import Any

from markupsafe import Markup, escape

from dblayer import (
    DatabaseClosed,
    DblayerError,
    IntegrityError,
    NotSupported,
    ParameterError,
    QueryError,
    QueryInterrupted,
    ResourceLimitExceeded,
    TableNotFound,
)

__all__ = ["cell", "is_numeric", "describe_error"]

TRUNCATE_AT = 300


def is_numeric(value: Any) -> bool:
    return isinstance(value, (int, float, decimal.Decimal)) and not isinstance(value, bool)


def cell(value: Any, truncate: bool = True) -> Markup:
    """One value as escaped HTML: NULL marked, blobs summarised, long text
    cut short (``truncate``) with its full length noted."""
    if value is None:
        return Markup('<span class="null">NULL</span>')
    if isinstance(value, (bytes, bytearray, memoryview)):
        data = bytes(value)
        preview = data[:12].hex(" ")
        more = " …" if len(data) > 12 else ""
        return Markup('<span class="blob">&lt;blob: {} bytes&gt; {}{}</span>').format(
            len(data), preview, more
        )
    if isinstance(value, bool):
        return escape(str(value).lower())
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return escape(value.isoformat(sep=" ") if isinstance(value, datetime.datetime) else value.isoformat())
    text = str(value)
    if truncate and len(text) > TRUNCATE_AT:
        return Markup('{}<span class="meta"> … ({:,} characters)</span>').format(
            text[:TRUNCATE_AT], len(text)
        )
    return escape(text)


def describe_error(e: BaseException, *, time_limit_ms: int | None = None) -> str:
    """A dblayer exception as a sentence for the page."""
    if isinstance(e, QueryInterrupted):
        limit = f" of {time_limit_ms} ms" if time_limit_ms else ""
        return f"The query was interrupted: it went past the time limit{limit}."
    if isinstance(e, ResourceLimitExceeded):
        return f"The query hit a resource limit: {e}"
    if isinstance(e, ParameterError):
        return f"Parameter error: {e}"
    if isinstance(e, TableNotFound):
        return str(e)
    if isinstance(e, IntegrityError):
        return f"Constraint violated: {e}"
    if isinstance(e, QueryError):
        code = f" [{e.code}]" if e.code else ""
        return f"SQL error{code}: {e}"
    if isinstance(e, NotSupported):
        return f"Not supported by this backend: {e}"
    if isinstance(e, DatabaseClosed):
        return "This database has been closed."
    if isinstance(e, DblayerError):
        return f"{type(e).__name__}: {e}"
    return f"Unexpected error: {type(e).__name__}: {e}"
