"""Backends. Each later milestone adds its backend class here.

Backends with optional dependencies are imported lazily, so ``import
dblayer.backends`` needs only the standard library.
"""

from typing import Any

from .base import Backend, BackendConnection, Capabilities, Dialect
from .sqlite import SqliteInProcess

__all__ = ["Backend", "BackendConnection", "Capabilities", "Dialect", "SqliteInProcess", "Postgres"]

_LAZY = {"Postgres": (".postgres", "Postgres")}


def __getattr__(name: str) -> Any:
    if name in _LAZY:
        import importlib

        module, attr = _LAZY[name]
        return getattr(importlib.import_module(module, __name__), attr)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
