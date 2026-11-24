"""Experimental: SQLite in a worker process written in C.

``SqliteCWorker(path)`` is ``SqliteWorker`` with the child process swapped:
instead of ``python -m dblayer.worker`` (about 85 ms to start: interpreter
plus imports) it runs ``cworker/build/dblayer-cworker``, a small C program
that speaks the same protocol (``docs/worker-protocol.md``) and links SQLite
directly. Everything on the parent side (pooling, placement, time limits,
kills, deadlines, frame caps) is the ``WorkerBackend`` machinery unchanged.

Build the binary first::

    make -C cworker

The binary is found through, in order: the ``binary=`` argument, the
``DBLAYER_CWORKER`` environment variable, then ``cworker/build/dblayer-cworker``
in the source tree. Without one, constructing a backend raises
``NotSupported`` naming the build command.

What the C worker cannot do, and how that shows:

* Named operations (``op``): they are Python functions. The worker's
  ``hello`` says ``"ops": false``, so ``run_operation`` declines
  (``NotImplemented``) without a round trip and the core runs the default
  implementation with one round trip per statement. An ``op`` sent anyway
  gets a ``NotSupported`` error.
* ``init`` hooks (``"module:function"``): impossible without Python; passing
  any raises ``NotSupported`` here. ``extensions=[path, ...]`` loads SQLite
  extensions by path instead (off unless given).
* Engines other than SQLite: ``open`` with another engine is refused.

Codec: MessagePack when the ``msgpack`` package is installed (the default
here), else JSON. The C worker implements both.
"""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from dataclasses import replace
from pathlib import Path
from typing import Any

from ..errors import NotSupported
from ..worker.protocol import available_codecs
from .sqlite_worker import (
    DEFAULT_KILL_GRACE_MS,
    DEFAULT_MAX_FRAME_BYTES,
    SharedWorkers,
    SqliteWorker,
    _intern,
)

__all__ = ["SqliteCWorker", "c_shared_workers", "find_binary", "binary_unavailable", "BUILD_COMMAND"]

ENV_VAR = "DBLAYER_CWORKER"
BUILD_COMMAND = "make -C cworker"
DEFAULT_BINARY = Path(__file__).resolve().parent.parent.parent / "cworker" / "build" / "dblayer-cworker"


_found: dict[tuple[str | None, str | None], str] = {}


def find_binary(binary: str | os.PathLike[str] | None = None) -> str:
    """The C worker executable: ``binary`` if given, else ``$DBLAYER_CWORKER``,
    else the default build location. Raises ``NotSupported`` if it is not
    an executable file. Successful lookups are cached (registering 10,000
    databases should not stat the binary 10,000 times)."""
    key = (None if binary is None else os.fspath(binary), os.environ.get(ENV_VAR))
    hit = _found.get(key)
    if hit is not None:
        return hit
    _found[key] = found = _find_binary(binary)
    return found


def _find_binary(binary: str | os.PathLike[str] | None) -> str:
    if binary is not None:
        candidate, source = Path(binary), "binary="
    elif os.environ.get(ENV_VAR):
        candidate, source = Path(os.environ[ENV_VAR]), f"${ENV_VAR}"
    else:
        candidate, source = DEFAULT_BINARY, "default location"
    if not (candidate.is_file() and os.access(candidate, os.X_OK)):
        raise NotSupported(
            f"C worker binary not built: run `{BUILD_COMMAND}` "
            f"(looked for {candidate}, from {source})"
        )
    return str(candidate.resolve())


def binary_unavailable(binary: str | os.PathLike[str] | None = None) -> str | None:
    """``None`` if the C worker can run here, else the reason (for skips)."""
    try:
        find_binary(binary)
    except NotSupported as e:
        return str(e)
    return None


def _default_codec() -> str:
    return "msgpack" if "msgpack" in available_codecs() else "json"


def c_shared_workers(
    size: int = 4,
    *,
    binary: str | os.PathLike[str] | None = None,
    codec: str | None = None,
    memory_limit_mb: int | None = None,
    kill_grace_ms: int = DEFAULT_KILL_GRACE_MS,
    max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
    linger: float = 5.0,
    debug: bool = False,
) -> SharedWorkers:
    """A ``SharedWorkers`` group of C worker processes (``placement="shared"``)."""
    group = SharedWorkers(
        size,
        codec=codec or _default_codec(),
        memory_limit_mb=memory_limit_mb,
        kill_grace_ms=kill_grace_ms,
        max_frame_bytes=max_frame_bytes,
        linger=linger,
        debug=debug,
    )
    group.config = replace(group.config, executable=find_binary(binary))
    return group


class SqliteCWorker(SqliteWorker):
    """SQLite in C worker processes. Options as ``SqliteWorker``, plus:

    * ``binary``: path to ``dblayer-cworker`` (see ``find_binary``).
    * ``extensions``: SQLite extension files loaded into every connection
      (the C worker's stand-in for ``init`` hooks).

    ``init`` hooks are not supported (``NotSupported``). The codec defaults
    to MessagePack when available.
    """

    __slots__ = ()

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        binary: str | os.PathLike[str] | None = None,
        init: Sequence[str] = (),
        extensions: Sequence[str | os.PathLike[str]] = (),
        sql_limits: Mapping[str, int] | None = None,
        codec: str | None = None,
        **options: Any,
    ) -> None:
        if init:
            raise NotSupported(
                "init hooks ('module:function') need the Python worker (SqliteWorker); "
                "the C worker can load SQLite extensions by path instead (extensions=[...])"
            )
        executable = find_binary(binary)
        workers = options.get("workers")
        if workers is not None and workers.config.executable is None:
            raise ValueError("SqliteCWorker with shared placement needs workers=c_shared_workers(...)")
        super().__init__(path, sql_limits=sql_limits, codec=codec or _default_codec(), **options)
        self.config = _intern(replace(self.config, executable=executable))
        if extensions:
            # Sent in ``open`` only: the local factory (which describes the
            # engine to the parent) knows nothing about extensions.
            self.engine_config["extensions"] = [os.fspath(Path(e).resolve()) for e in extensions]
