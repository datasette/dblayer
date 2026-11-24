"""dblayer: an asyncio database layer that never hands out a raw connection.

The public names other than the exceptions are imported lazily (PEP 562),
so that the worker process (``python -m dblayer.worker``), which needs only
the SQLite backend, does not import ``Database``, ``Registry`` and asyncio
on every start.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

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

if TYPE_CHECKING:
    from .core.database import DEFAULT, Database
    from .core.operations import OperationResult
    from .core.registry import Registry, RegistryStats
    from .core.results import Column, Results, Row, WriteResult
    from .core.transactions import Transaction

_LAZY = {
    "Database": ".core.database",
    "DEFAULT": ".core.database",
    "Registry": ".core.registry",
    "RegistryStats": ".core.registry",
    "OperationResult": ".core.operations",
    "Transaction": ".core.transactions",
    "Column": ".core.results",
    "Results": ".core.results",
    "Row": ".core.results",
    "WriteResult": ".core.results",
}


def __getattr__(name: str) -> Any:
    module = _LAZY.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(importlib.import_module(module, __name__), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(_LAZY))


__all__ = [
    "Database",
    "Registry",
    "RegistryStats",
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
