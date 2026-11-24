"""Small pieces shared by ``Database`` and ``Transaction``: the ``DEFAULT``
limit sentinel and error mapping."""

from __future__ import annotations

import asyncio
import enum
from collections.abc import Awaitable, Iterator
from contextlib import contextmanager
from typing import Any, Final

from ..backends.base import Backend
from ..errors import DblayerError

__all__ = ["DEFAULT", "Opt", "pick", "map_errors", "spawn"]

# Background tasks (deadline expiry, shielded cleanup, sleep transitions,
# idle reaping) are referenced here so they are not garbage collected while
# running.
_background: set[asyncio.Task[Any]] = set()


def spawn(coro: Awaitable[Any]) -> asyncio.Task[Any]:
    task = asyncio.ensure_future(coro)
    _background.add(task)
    task.add_done_callback(_background.discard)
    return task


class _Default(enum.Enum):
    DEFAULT = "DEFAULT"

    def __repr__(self) -> str:
        return "DEFAULT"


DEFAULT: Final = _Default.DEFAULT
"""Sentinel for "use the Database's default"; pass ``None`` for no limit."""

type Opt[T] = T | None | _Default


def pick[T](value: Opt[T], default: T | None) -> T | None:
    return default if value is DEFAULT else value  # type: ignore[return-value]


@contextmanager
def map_errors(backend: Backend) -> Iterator[None]:
    """Translate driver exceptions into dblayer exceptions, keeping the
    driver exception as ``__cause__``."""
    try:
        yield
    except DblayerError:
        raise
    except Exception as exc:
        mapped = backend.dialect.map_exception(exc)
        if mapped is None:
            raise
        raise mapped from exc
