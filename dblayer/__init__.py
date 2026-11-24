"""dblayer: an asyncio database layer that never hands out a raw connection."""

from .core.database import DEFAULT, Database
from .core.results import Column, Results, Row, WriteResult
from .errors import (
    DatabaseClosed,
    DblayerError,
    IntegrityError,
    NestedWriteError,
    NotSupported,
    ParameterError,
    QueryError,
    QueryInterrupted,
    ResourceLimitExceeded,
    SchemaChanged,
    TableNotFound,
    TransactionExpired,
    WorkerCrashed,
    WriteConflict,
)

__all__ = [
    "Database",
    "DEFAULT",
    "Results",
    "WriteResult",
    "Row",
    "Column",
    "DblayerError",
    "QueryError",
    "IntegrityError",
    "ParameterError",
    "TableNotFound",
    "QueryInterrupted",
    "ResourceLimitExceeded",
    "TransactionExpired",
    "WriteConflict",
    "NestedWriteError",
    "WorkerCrashed",
    "NotSupported",
    "DatabaseClosed",
    "SchemaChanged",
]
