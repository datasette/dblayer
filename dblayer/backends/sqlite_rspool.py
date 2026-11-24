"""Experimental: SQLite in a small pool of multi-tenant *Rust* worker processes.

``SqliteRustPool(path)`` is ``SqliteCPool`` with the child process swapped:
``sqlite-rs-worker/target/release/dblayer-sqlite-rsworker --multi``, a Rust
port of the C worker (``cworker/``) that speaks the same protocol, links the
same pinned SQLite amalgamation with the same compile options, and keeps a
thread per connection. Everything on the parent side (multiplexed pipes,
placement, kills and quarantine, read retry) is ``sqlite_cpool`` unchanged;
see that module for the blast-radius rules.

Build the binary first::

    make -C sqlite-rs-worker

The binary is found through, in order: the ``binary=`` argument, the
``DBLAYER_RSWORKER`` environment variable, then the default build location.
Without one, constructing a backend raises ``NotSupported`` naming the build
command. Differences from the C worker: ``hello`` says ``"implementation":
"rust"``, and export has no ``socket`` transport (``hello`` does not list it).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from ..errors import NotSupported
from .sqlite_cpool import CPool, SqliteCPool

__all__ = [
    "RustPool",
    "SqliteRustPool",
    "find_binary",
    "binary_unavailable",
    "default_pool",
    "close_default_pool",
    "BUILD_COMMAND",
    "ENV_VAR",
]

ENV_VAR = "DBLAYER_RSWORKER"
BUILD_COMMAND = "make -C sqlite-rs-worker"
DEFAULT_BINARY = (
    Path(__file__).resolve().parent.parent.parent / "sqlite-rs-worker" / "target" / "release" / "dblayer-sqlite-rsworker"
)


def find_binary(binary: str | os.PathLike[str] | None = None) -> str:
    """The Rust worker executable: ``binary``, else ``$DBLAYER_RSWORKER``,
    else the default build location. ``NotSupported`` if it is missing."""
    if binary is not None:
        candidate, source = Path(binary), "binary="
    elif os.environ.get(ENV_VAR):
        candidate, source = Path(os.environ[ENV_VAR]), f"${ENV_VAR}"
    else:
        candidate, source = DEFAULT_BINARY, "default location"
    if not (candidate.is_file() and os.access(candidate, os.X_OK)):
        raise NotSupported(
            f"Rust SQLite worker binary not built: run `{BUILD_COMMAND}` (looked for {candidate}, from {source})"
        )
    return str(candidate.resolve())


def binary_unavailable(binary: str | os.PathLike[str] | None = None) -> str | None:
    """``None`` if the Rust worker can run here, else the reason (for skips)."""
    try:
        find_binary(binary)
    except NotSupported as e:
        return str(e)
    return None


class RustPool(CPool):
    """``CPool`` running the Rust worker. Same options as ``CPool``."""

    def __init__(self, size: int = 2, *, binary: str | os.PathLike[str] | None = None, **options: Any) -> None:
        super().__init__(size, binary=find_binary(binary), **options)

    def __repr__(self) -> str:
        return f"<RustPool size={self.size} running={len(self.procs)}>"


_default: RustPool | None = None


def default_pool() -> RustPool:
    """The process-wide pool used when ``SqliteRustPool`` is given none."""
    global _default
    if _default is None:
        _default = RustPool()
    return _default


def close_default_pool() -> None:
    """Stop the processes of ``default_pool()``, if it was ever used."""
    if _default is not None:
        _default.close()


class SqliteRustPool(SqliteCPool):
    """SQLite on a shared pool of multi-tenant Rust worker processes. Options
    as ``SqliteCPool``; ``pool`` defaults to this module's ``default_pool()``
    (or a new ``RustPool(binary=...)`` when ``binary`` is given)."""

    __slots__ = ()

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        pool: CPool | None = None,
        binary: str | os.PathLike[str] | None = None,
        **options: Any,
    ) -> None:
        if pool is None:
            pool = RustPool(binary=binary) if binary is not None else default_pool()
        super().__init__(path, pool=pool, **options)
