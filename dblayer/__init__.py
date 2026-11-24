"""dblayer: an asyncio database layer that never hands out a raw connection."""

from .core.database import DEFAULT, Database
from .core.registry import Registry
from .core.operations import OperationResult
from .core.transactions import Transaction
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
    TransactionClosed,
    TransactionExpired,
    WorkerCrashed,
    UnknownDatabase,
    WriteConflict,
)

__all__ = [
    "Database",
    "Registry",
    "UnknownDatabase",
    "Transaction",
    "OperationResult",
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
    "TransactionClosed",
    "TransactionExpired",
    "WriteConflict",
    "NestedWriteError",
    "WorkerCrashed",
    "NotSupported",
    "DatabaseClosed",
    "SchemaChanged",
]
