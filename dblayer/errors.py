"""The shared exception classes raised by every backend.

The original driver exception, where there is one, is attached as
``__cause__`` (``raise QueryError(...) from driver_exc``).
"""

from __future__ import annotations

__all__ = [
    "DblayerError",
    "QueryError",
    "IntegrityError",
    "ParameterError",
    "TableNotFound",
    "QueryInterrupted",
    "ResourceLimitExceeded",
    "TransactionClosed",
    "TransactionExpired",
    "WriteConflict",
    "NestedWriteError",
    "WorkerCrashed",
    "NotSupported",
    "DatabaseClosed",
    "SchemaChanged",
    "UnknownDatabase",
]


class DblayerError(Exception):
    """Base class for every exception this library raises."""


class QueryError(DblayerError):
    """The SQL failed. Carries the backend's message and error code."""

    def __init__(self, message: str, code: str | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.code = code

    def __str__(self) -> str:
        return self.message


class IntegrityError(QueryError):
    """A constraint was violated."""


class ParameterError(QueryError):
    """Parameters did not match the placeholders in the SQL.

    Not in the spec's table: raised by the core before anything reaches a
    backend, so it is the same on every backend.
    """


class TableNotFound(QueryError):
    """An introspection method was asked about a table or other object that
    does not exist. Not in the spec's table."""


class QueryInterrupted(DblayerError):
    """A statement passed its time limit (or was interrupted)."""


class ResourceLimitExceeded(DblayerError):
    """A memory or result-size limit was hit."""


class TransactionClosed(DblayerError):
    """A ``Transaction`` was used after its ``async with`` block ended (or
    after it was rolled back). Not in the spec's table, which only says
    "raises an error"."""


class TransactionExpired(TransactionClosed):
    """The transaction passed its deadline and was rolled back."""


class WriteConflict(DblayerError):
    """The backend aborted the transaction because of a concurrent write."""


class NestedWriteError(DblayerError):
    """A write was attempted on ``db`` from inside an open transaction on the
    same database (or, where ``concurrent_sessions`` is false, any statement
    outside the transaction from the task holding it)."""


class WorkerCrashed(DblayerError):
    """The worker process died or was killed."""


class NotSupported(DblayerError):
    """The backend lacks the capability this call needs."""


class DatabaseClosed(DblayerError):
    """The database was closed for good."""


class SchemaChanged(DblayerError):
    """A batch was sent with ``expect_schema_version`` and the schema had moved
    on. Nothing ran."""


class UnknownDatabase(DblayerError, KeyError):
    """``Registry.get()`` found no database of that name and the resolver
    (if any) returned ``None``."""

    def __str__(self) -> str:
        return Exception.__str__(self)
