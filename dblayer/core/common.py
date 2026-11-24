"""Small pieces shared by ``Database`` and ``Transaction``: the ``DEFAULT``
limit sentinel and error mapping."""

from __future__ import annotations

import enum
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Final

from ..backends.base import Backend
from ..errors import DblayerError

__all__ = ["DEFAULT", "Opt", "pick", "map_errors"]


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
