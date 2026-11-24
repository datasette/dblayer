"""ClickHouse (chDB) test helpers.

chDB runs one embedded server per process, at one data directory, so every
ClickHouse test database of a test session lives in the same directory,
one ClickHouse database each (as PostgreSQL tests use one schema each).
The directory is removed at exit.
"""

from __future__ import annotations

import atexit
import itertools
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

_root: list[Path] = []
_names = itertools.count(1)


def root() -> Path:
    if not _root:
        path = Path(tempfile.mkdtemp(prefix="dblayer-clickhouse-"))
        _root.append(path)
        atexit.register(shutil.rmtree, path, True)
    return _root[0]


def unavailable() -> str | None:
    from dblayer.backends.clickhouse import chdb_unavailable

    return chdb_unavailable()


def make(**kwargs: Any) -> Any:
    from dblayer.backends.clickhouse import ClickHouse

    return ClickHouse(root(), database=f"t{os.getpid()}_{next(_names)}", **kwargs)


async def cleanup(backend: Any) -> None:
    await backend.drop_database()
