"""Experimental: SQLite compiled to WebAssembly, in a Node.js worker process.

``SqliteNodeWorker(path)`` is ``SqliteWorker`` with the child process
swapped: ``node nodeworker/src/main.mjs`` runs the official SQLite
WebAssembly build (``@sqlite.org/sqlite-wasm``, pinned in
``nodeworker/package.json``) with a file VFS of its own, and speaks the
worker protocol (``docs/worker-protocol.md``). Two layers of containment:
the WebAssembly sandbox (a memory cap that SQLite cannot exceed) inside a
process the parent can kill.

Set it up once::

    npm ci --prefix nodeworker

Lookup, in order: ``node=`` / ``script=`` arguments, the ``DBLAYER_NODE`` /
``DBLAYER_NODEWORKER`` environment variables, then ``node`` on ``PATH`` and
``nodeworker/src/main.mjs`` in the source tree. Without Node, the script or
its ``node_modules``, constructing a backend raises ``NotSupported`` saying
what is missing.

One process per database
------------------------
Node has no API for the POSIX advisory locks native SQLite uses, so the
worker's VFS keeps SQLite's lock states in memory: correct between the
connections inside one worker, invisible to every other process. This
backend therefore puts every connection of a database (the writer and the
readers) in **one** Node process (``DatabaseWorker`` placement, shared by
every ``SqliteNodeWorker`` on the same path in this Python process), and
starts a new one only after the previous one has exited. Requests to one
database run one at a time.

**Restriction:** while it is open here, nothing else may write the file
(no in-process SQLite backend, ``sqlite3`` shell, other worker, other
Python process), and nothing else should read it during writes: they
would not see this worker's locks, nor it theirs (see
``tests/worker/test_nodeworker_locking.py``). After the worker exits, any
SQLite can open the file, and a hot journal it left is rolled back as
usual. WAL mode is unavailable (no shared memory in the VFS).

What the Node worker cannot do, and how that shows: named operations
(``op``) and Python ``init`` hooks, as the C worker (``hello`` says
``"ops": false``; ``init`` raises ``NotSupported``); SQLite extensions (the
WebAssembly build omits ``load_extension``).
"""

from __future__ import annotations

import asyncio
import os
import shutil
import subprocess
import weakref
from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from pathlib import Path
from typing import Any

from ..errors import NotSupported, WorkerCrashed
from ..worker.protocol import available_codecs
from .duckdb_worker import DatabaseWorker
from .sqlite_worker import (
    DEFAULT_KILL_GRACE_MS,
    DEFAULT_MAX_FRAME_BYTES,
    SqliteWorker,
    WorkerConfig,
    WorkerProcess,
    _intern,
    _unwrap,
    _Waiter,
)

__all__ = [
    "SqliteNodeWorker",
    "find_node",
    "find_script",
    "unavailable",
    "SETUP_COMMAND",
    "NODE_ENV_VAR",
    "SCRIPT_ENV_VAR",
]

NODE_ENV_VAR = "DBLAYER_NODE"
SCRIPT_ENV_VAR = "DBLAYER_NODEWORKER"
SETUP_COMMAND = "npm ci --prefix nodeworker"
MIN_NODE = 22
DEFAULT_SCRIPT = Path(__file__).resolve().parent.parent.parent / "nodeworker" / "src" / "main.mjs"

_found: dict[tuple[str, str | None, str | None], str] = {}


def find_node(node: str | os.PathLike[str] | None = None) -> str:
    """The ``node`` executable: ``node`` if given, else ``$DBLAYER_NODE``,
    else ``node`` on ``PATH``. Raises ``NotSupported`` if it is missing or
    older than Node 22. Cached."""
    key = ("node", None if node is None else os.fspath(node), os.environ.get(NODE_ENV_VAR))
    hit = _found.get(key)
    if hit is None:
        _found[key] = hit = _find_node(node)
    return hit


def _find_node(node: str | os.PathLike[str] | None) -> str:
    if node is not None:
        candidate, source = os.fspath(node), "node="
    elif os.environ.get(NODE_ENV_VAR):
        candidate, source = os.environ[NODE_ENV_VAR], f"${NODE_ENV_VAR}"
    else:
        candidate, source = "node", "PATH"
    resolved = shutil.which(candidate)
    if resolved is None:
        raise NotSupported(f"Node.js not found (looked for {candidate!r}, from {source}); the Node worker needs Node {MIN_NODE}+")
    try:
        out = subprocess.run([resolved, "--version"], capture_output=True, text=True, timeout=30).stdout.strip()
        major = int(out.lstrip("v").split(".")[0])
    except (OSError, ValueError, subprocess.SubprocessError) as e:
        raise NotSupported(f"cannot run {resolved} --version: {e}") from e
    if major < MIN_NODE:
        raise NotSupported(f"Node {out} at {resolved} is too old; the Node worker needs Node {MIN_NODE}+")
    return resolved


def find_script(script: str | os.PathLike[str] | None = None) -> str:
    """The worker script: ``script`` if given, else ``$DBLAYER_NODEWORKER``,
    else ``nodeworker/src/main.mjs`` in the source tree. Raises
    ``NotSupported`` if it, or the npm packages next to it, are missing."""
    key = ("script", None if script is None else os.fspath(script), os.environ.get(SCRIPT_ENV_VAR))
    hit = _found.get(key)
    if hit is None:
        _found[key] = hit = _find_script(script)
    return hit


def _find_script(script: str | os.PathLike[str] | None) -> str:
    if script is not None:
        candidate, source = Path(script), "script="
    elif os.environ.get(SCRIPT_ENV_VAR):
        candidate, source = Path(os.environ[SCRIPT_ENV_VAR]), f"${SCRIPT_ENV_VAR}"
    else:
        candidate, source = DEFAULT_SCRIPT, "default location"
    if not candidate.is_file():
        raise NotSupported(f"Node worker script not found: {candidate} (from {source})")
    package = candidate.resolve().parent.parent
    if not (package / "node_modules" / "@sqlite.org" / "sqlite-wasm" / "package.json").is_file():
        raise NotSupported(
            f"the Node worker's npm packages are not installed: run `{SETUP_COMMAND}` "
            f"(looked in {package / 'node_modules'})"
        )
    return str(candidate.resolve())


def unavailable(node: str | os.PathLike[str] | None = None, script: str | os.PathLike[str] | None = None) -> str | None:
    """``None`` if the Node worker can run here, else the reason (for skips)."""
    try:
        find_node(node)
        find_script(script)
    except NotSupported as e:
        return str(e)
    return None


def _default_codec() -> str:
    return "msgpack" if "msgpack" in available_codecs() else "json"


class PipelinedWorkerProcess(WorkerProcess):
    """A worker process with one request in flight per connection, not one
    per process: the Node worker runs each connection on a thread of its
    own, so a writer waiting for a reader's lock does not hold up the
    reader's next request (``WorkerProcess`` sends one at a time, which
    with every connection of a database in one process would deadlock a
    commit against an open read transaction until the busy timeout)."""

    def __init__(self, config: WorkerConfig) -> None:
        super().__init__(config)
        self._owner_locks: dict[int, asyncio.Lock] = {}  # id(owner) -> its lock
        self._by_owner: dict[int, int] = {}  # id(owner) -> its in-flight request

    async def call(
        self,
        msg: dict[str, Any],
        *,
        owner: object,
        limit_ms: float | None = None,
        expect_frames: int | None = None,
        on_sent: Callable[[int], None] | None = None,
    ) -> Any:
        okey = id(owner)
        lock = self._owner_locks.get(okey)
        if lock is None:
            lock = self._owner_locks[okey] = asyncio.Lock()
        try:
            return await self._call_locked(lock, msg, owner, limit_ms, expect_frames, on_sent)
        finally:
            if not lock.locked() and self._owner_locks.get(okey) is lock:
                del self._owner_locks[okey]

    async def _call_locked(
        self,
        lock: asyncio.Lock,
        msg: dict[str, Any],
        owner: object,
        limit_ms: float | None,
        expect_frames: int | None,
        on_sent: Callable[[int], None] | None,
    ) -> Any:
        async with lock:
            self._check()
            mid = msg["id"] = msg.get("id") or next(self._ids)
            if expect_frames is not None and msg.get("type") == "more":
                del msg["id"]  # a ``more`` has no id of its own: frames carry the stream's
                mid = msg["stream"]
            fut: asyncio.Future[Any] = self.loop.create_future()
            self._waiters[mid] = _Waiter(fut, owner, expect_frames)
            try:
                self.send(msg)
            except BaseException:
                self._waiters.pop(mid, None)
                raise
            if on_sent is not None:
                on_sent(mid)
            timer = None
            if limit_ms is not None:
                timer = self.loop.call_later(
                    max(limit_ms, 0) / 1000 + self.config.kill_grace_ms / 1000,
                    self._kill_for_time, mid, limit_ms,
                )
            okey = id(owner)
            self._by_owner[okey] = mid
            # The most recent request: WorkerConnection's deadline overrun
            # uses it only to word the culprit's error.
            self.inflight = (mid, owner)
            try:
                reply = await self._wait(fut, mid)
            finally:
                if self._by_owner.get(okey) == mid:
                    del self._by_owner[okey]
                if self.inflight is not None and self.inflight[0] == mid:
                    self.inflight = None
                if timer is not None:
                    timer.cancel()
        if expect_frames is None:
            return _unwrap(reply, self)
        return reply

    def _dispatch(self, msg: dict[str, Any]) -> None:
        super()._dispatch(msg)
        if msg.get("fatal") and self.dead is None:
            # The worker exits after a fatal reply (it terminated a thread
            # whose statement would not stop). Stop using it now, even if
            # the caller was cancelled and never unwraps the reply, so the
            # pool replaces it before the next request.
            self.kill(WorkerCrashed(f"worker {self.pid} exited after a fatal error: {msg['error'].get('message')}"))

    def cancel(self, owner: object) -> None:
        mid = self._by_owner.get(id(owner))
        if mid is not None and self.dead is None:
            try:
                self.send({"type": "cancel", "target": mid})
            except Exception:
                pass


class NodeDatabaseWorker(DatabaseWorker):
    """``DatabaseWorker`` placement for the Node worker: one process for
    every connection of a database, requests pipelined per connection."""

    process_class = PipelinedWorkerProcess


_owners: weakref.WeakValueDictionary[str, DatabaseWorker] = weakref.WeakValueDictionary()


def _owner(path: str, *, node: str, script: str, codec: str, memory_limit_mb: int | None,
           kill_grace_ms: int, max_frame_bytes: int, debug: bool) -> DatabaseWorker:
    """The one ``DatabaseWorker`` for ``path`` in this Python process, so
    two backends on the same file share one Node process."""
    group = _owners.get(path)
    if group is None:
        group = NodeDatabaseWorker(
            codec=codec,
            memory_limit_mb=memory_limit_mb,
            kill_grace_ms=kill_grace_ms,
            max_frame_bytes=max_frame_bytes,
            debug=debug,
        )
        group.config = _intern(replace(group.config, executable=node, executable_args=(script,)))
        _owners[path] = group
        return group
    wanted = replace(
        group.config, codec=codec, memory_limit_mb=memory_limit_mb, kill_grace_ms=kill_grace_ms,
        max_frame_bytes=max_frame_bytes, debug=debug, executable=node, executable_args=(script,),
    )
    if wanted != group.config:
        raise NotSupported(
            f"{path} is already open through a Node worker with other settings ({group.config}); "
            "one Node process owns a database file at a time"
        )
    return group


class SqliteNodeWorker(SqliteWorker):
    """SQLite (WebAssembly) in a Node.js worker process, one per database.
    Options as ``SqliteWorker``, plus:

    * ``node``: the Node executable (see ``find_node``).
    * ``script``: the worker script (see ``find_script``).

    ``memory_limit_mb`` sets the WebAssembly memory maximum (every
    platform: SQLite cannot allocate past it), ``PRAGMA hard_heap_limit``
    and the executor thread's V8 heap limit. ``init`` hooks, extensions and
    ``placement``/``workers`` are not supported. The codec defaults to
    MessagePack when available.
    """

    __slots__ = ()

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        node: str | os.PathLike[str] | None = None,
        script: str | os.PathLike[str] | None = None,
        init: Sequence[str] = (),
        sql_limits: Mapping[str, int] | None = None,
        codec: str | None = None,
        memory_limit_mb: int | None = None,
        kill_grace_ms: int = DEFAULT_KILL_GRACE_MS,
        max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
        debug: bool = False,
        **options: Any,
    ) -> None:
        if init:
            raise NotSupported(
                "init hooks ('module:function') need the Python worker (SqliteWorker); "
                "the Node worker runs SQLite compiled to WebAssembly"
            )
        if options.get("extensions"):
            raise NotSupported("the Node worker cannot load SQLite extensions (the WebAssembly build omits load_extension)")
        if "workers" in options or "placement" in options:
            raise NotSupported(
                "the Node worker always runs one process per database (its VFS cannot share "
                "locks between processes); placement and workers cannot be chosen"
            )
        node_exe = find_node(node)
        script_path = find_script(script)
        codec = codec or _default_codec()
        abspath = os.path.abspath(os.fspath(path))
        workers = _owner(
            abspath, node=node_exe, script=script_path, codec=codec, memory_limit_mb=memory_limit_mb,
            kill_grace_ms=kill_grace_ms, max_frame_bytes=max_frame_bytes, debug=debug,
        )
        super().__init__(path, sql_limits=sql_limits, codec=codec, workers=workers, **options)
        self.capabilities = replace(self.capabilities, memory_limit=True)

    @property
    def process(self) -> Any:
        """The live Node process for this database, if any."""
        assert isinstance(self.workers, DatabaseWorker)
        return self.workers.process
