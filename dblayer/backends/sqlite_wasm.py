"""Experimental: SQLite compiled to WebAssembly, run in-process under wasmtime.

``SqliteWasm(path, memory_limit_mb=...)`` runs the C worker's request
handling (``cworker/src/worker.c``, built with ``-DCWORKER_WASM``) and the
pinned SQLite amalgamation as one WebAssembly module. Each pooled connection
is one instance (its own ``wasmtime.Store``): a sandbox with its own linear
memory, no process. The parent side is the worker backend's
(``WorkerConnection``, ``WorkerStream``) unchanged: requests are worker
protocol messages (``docs/worker-protocol.md``), handed to the instance as
one frame in linear memory, and every frame the request produces comes back
from the same call. Build the module first::

    make -C cworker wasm

It is found through the ``module=`` argument, ``$DBLAYER_SQLITE_WASM``, then
``cworker/build-wasm/dblayer-sqlite.wasm``. Without one, constructing a
backend raises ``NotSupported`` naming the build command. Needs the
``wasmtime`` package (the ``wasm`` extra).

Limits
------
* Time: the progress handler stops a statement at ``time_limit_ms`` (clean
  ``QueryInterrupted``; the instance carries on). A statement the progress
  handler cannot reach (one long C function call: ``instr``, ``like`` on
  huge strings, ``randomblob(1e9)``) is stopped by wasmtime's epoch
  interruption ``kill_grace_ms`` after the limit: the instance traps, the
  call raises ``QueryInterrupted``, the instance is discarded and the
  connection gets a fresh one on its next request (sub-millisecond). This
  is the worker's SIGKILL without a process, hence ``hard_kill=True``.
* Cancellation: the host sets a flag in the instance's memory that the
  progress handler and every statement start check, so it cannot be lost
  (no ``sqlite3_interrupt()`` race); a statement that does not stop within
  ``kill_grace_ms`` is trapped.
* Memory: ``memory_limit_mb`` caps the instance's linear memory exactly
  (``Store.set_limits``); SQLite's ``hard_heap_limit`` is set a little
  below it, so a large allocation fails inside SQLite with ``SQLITE_NOMEM``
  (``ResourceLimitExceeded``) and the instance carries on. The host
  process's memory is untouched. Without a limit the ceiling is wasm32's
  4 GB. ``memory_limit=True``.

Files and locking
-----------------
The module has no WASI filesystem at all. Its only VFS (``wasm_vfs.c``)
calls host functions defined here for every read, write, sync and lock.
The host owns every descriptor and implements SQLite's unix locking
protocol (the same ``fcntl()`` byte ranges as the native unix VFS: PENDING,
RESERVED and SHARED bytes at 1 GB), with an in-process table per inode as
SQLite's own ``unixInodeInfo`` does, so that instances in this process and
ordinary SQLite in other processes exclude each other correctly and a
closed descriptor never drops another instance's locks. An instance may
open only its own database file, its ``-journal`` and anonymous temporary
files: ``ATTACH`` of any other file and ``VACUUM INTO`` fail.

What is given up: WAL (no shared memory between instances; the VFS has no
``xShmMap``, so ``journal_mode=WAL`` stays ``delete`` and a WAL-mode file
cannot be opened), and memory-mapped I/O. Readers and a writer still run
concurrently under rollback-journal locking, as in-process SQLite does by
default. On Linux and macOS the host takes open file description locks
(``OFD_LOCKS``), which also conflict with the classic locks of ordinary
SQLite *in this same process* (the in-process backend, ``sqlite3``), and it
never closes a descriptor of a file while anyone holds a lock on it (that
would drop this process's classic locks): the two libraries can share a file
in one process. Elsewhere, or with ``DBLAYER_WASM_POSIX_LOCKS=1``, that
case is unsafe, the rule SQLite documents for two copies of the library in
one process.

Named operations run in the parent (the module has no Python), as for the
C worker. ``init`` hooks and extensions are not supported.
"""

from __future__ import annotations

import asyncio
import atexit
import ctypes
import errno
import fcntl
import itertools
import os
import struct
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from ..core.executor import run_blocking
from ..errors import NotSupported, QueryInterrupted, ResourceLimitExceeded, WorkerCrashed
from ..worker.protocol import DEFAULT_MAX_FRAME_BYTES, PROTOCOL_VERSION, available_codecs, get_codec
from .base import Capabilities
from .sqlite_worker import SqliteWorker, WorkerConnection, WorkerError, _unwrap

__all__ = [
    "SqliteWasm",
    "find_module",
    "module_unavailable",
    "BUILD_COMMAND",
    "runtime_stats",
]

ENV_VAR = "DBLAYER_SQLITE_WASM"
BUILD_COMMAND = "make -C cworker wasm"
DEFAULT_MODULE = (
    Path(__file__).resolve().parent.parent.parent / "cworker" / "build-wasm" / "dblayer-sqlite.wasm"
)
DEFAULT_KILL_GRACE_MS = 50
INLINE_S = 0.002
"""``inline_reads``: a read runs on the event loop's thread for at most
about this long (plus up to one epoch tick) before it is interrupted and
re-run on the executor."""
INLINE_GRACE_S = 0.004
"""...and one that ignores the interrupt is trapped this much later."""
EPOCH_TICK_S = 0.002
"""How often the engine's epoch advances while any call runs. Each tick
runs a small Python callback per running instance (deadline and kill
checks), so this is also the resolution of the hard stop."""

# -- finding the module --------------------------------------------------------

_found: dict[tuple[str | None, str | None], str] = {}


def find_module(module: str | os.PathLike[str] | None = None) -> str:
    """The ``.wasm`` file: ``module`` if given, else ``$DBLAYER_SQLITE_WASM``,
    else the default build location. Raises ``NotSupported`` if it is
    missing or the ``wasmtime`` package is not installed."""
    key = (None if module is None else os.fspath(module), os.environ.get(ENV_VAR))
    hit = _found.get(key)
    if hit is not None:
        return hit
    try:
        import wasmtime  # noqa: F401
    except ImportError:
        raise NotSupported(
            "the sqlite-wasm backend needs the wasmtime package (uv run --extra wasm ...)"
        ) from None
    if module is not None:
        candidate, source = Path(module), "module="
    elif os.environ.get(ENV_VAR):
        candidate, source = Path(os.environ[ENV_VAR]), f"${ENV_VAR}"
    else:
        candidate, source = DEFAULT_MODULE, "default location"
    if not candidate.is_file():
        raise NotSupported(
            f"SQLite WebAssembly module not built: run `{BUILD_COMMAND}` "
            f"(looked for {candidate}, from {source})"
        )
    _found[key] = found = str(candidate.resolve())
    return found


def module_unavailable(module: str | os.PathLike[str] | None = None) -> str | None:
    """``None`` if the backend can run here, else the reason (for skips)."""
    try:
        find_module(module)
    except NotSupported as e:
        return str(e)
    return None


# -- SQLite constants ------------------------------------------------------------

SQLITE_OK = 0
SQLITE_BUSY = 5
SQLITE_READONLY = 8
SQLITE_FULL = 13
SQLITE_CANTOPEN = 14
SQLITE_IOERR_READ = 266
SQLITE_IOERR_WRITE = 778
SQLITE_IOERR_FSYNC = 1034
SQLITE_IOERR_TRUNCATE = 1546
SQLITE_IOERR_FSTAT = 1802
SQLITE_IOERR_UNLOCK = 2058
SQLITE_IOERR_RDLOCK = 2314
SQLITE_IOERR_DELETE = 2570
SQLITE_IOERR_ACCESS = 3338
SQLITE_IOERR_CHECKRESERVEDLOCK = 3594
SQLITE_IOERR_LOCK = 3850
SQLITE_IOERR_CLOSE = 4106
SQLITE_IOERR_DELETE_NOENT = 5898

OPEN_READONLY = 0x1
OPEN_READWRITE = 0x2
OPEN_CREATE = 0x4
OPEN_DELETEONCLOSE = 0x8
OPEN_EXCLUSIVE = 0x10
OPEN_MAIN_DB = 0x100
ACCESS_READWRITE = 1

NO_LOCK, SHARED_LOCK, RESERVED_LOCK, PENDING_LOCK, EXCLUSIVE_LOCK = range(5)
PENDING_BYTE = 0x40000000
RESERVED_BYTE = PENDING_BYTE + 1
SHARED_FIRST = PENDING_BYTE + 2
SHARED_SIZE = 510

# -- fcntl locks ------------------------------------------------------------------


class _Busy(Exception):
    pass


if sys.platform == "darwin":
    # xnu has had open file description locks since macOS 10.15, with
    # Linux's semantics, but its header defines the commands only under
    # PRIVATE (sys/fcntl.h: F_OFD_SETLK 90, F_OFD_GETLK 92), so Python's
    # fcntl module does not export them. Probed below before use.
    _F_OFD_SETLK, _F_OFD_GETLK = 90, 92
else:
    _F_OFD_SETLK = getattr(fcntl, "F_OFD_SETLK", None)
    _F_OFD_GETLK = getattr(fcntl, "F_OFD_GETLK", None)


def _probe_ofd() -> bool:
    """Whether this kernel has open file description locks that conflict
    with classic POSIX locks taken through another descriptor of this same
    process (what makes them safe beside ordinary SQLite here)."""
    if _F_OFD_SETLK is None or os.environ.get("DBLAYER_WASM_POSIX_LOCKS") == "1":
        return False
    if not (sys.platform.startswith("linux") or sys.platform == "darwin"):
        return False
    try:
        fd, tmp = tempfile.mkstemp(prefix="dblayer-ofd-probe-")
    except OSError:
        return False
    other = -1
    try:
        try:
            other = os.open(tmp, os.O_RDWR | os.O_CLOEXEC)
        finally:
            os.unlink(tmp)
        fcntl.fcntl(fd, _F_OFD_SETLK, _flock(fcntl.F_WRLCK, 0, 1))
        try:
            fcntl.lockf(other, fcntl.LOCK_EX | fcntl.LOCK_NB, 1, 0, os.SEEK_SET)
        except OSError as e:
            return e.errno in (errno.EAGAIN, errno.EACCES)
        return False  # granted: no conflict within the process, useless here
    except OSError:
        return False
    finally:
        for d in (other, fd):
            if d >= 0:
                try:
                    os.close(d)
                except OSError:
                    pass


def _setlk(fd: int, kind: str, start: int, length: int) -> None:
    """A non-blocking record lock on ``[start, start+length)``; ``length``
    0 means to the end of the file. Raises ``_Busy`` when someone else
    holds a conflicting lock."""
    try:
        if OFD_LOCKS:
            t = {"r": fcntl.F_RDLCK, "w": fcntl.F_WRLCK, "u": fcntl.F_UNLCK}[kind]
            fcntl.fcntl(fd, _F_OFD_SETLK, _flock(t, start, length))
        else:
            cmd = {"r": fcntl.LOCK_SH | fcntl.LOCK_NB, "w": fcntl.LOCK_EX | fcntl.LOCK_NB, "u": fcntl.LOCK_UN}[kind]
            fcntl.lockf(fd, cmd, length, start, os.SEEK_SET)
    except OSError as e:
        if e.errno in (errno.EAGAIN, errno.EACCES):
            raise _Busy() from None
        raise


if sys.platform == "darwin":
    _FLOCK = "qqihh"  # l_start, l_len, l_pid, l_type, l_whence

    def _flock(type_: int, start: int, length: int) -> bytes:
        return struct.pack(_FLOCK, start, length, 0, type_, os.SEEK_SET)

    def _flock_type(raw: bytes) -> int:
        return struct.unpack(_FLOCK, raw)[3]
else:
    _FLOCK = "hhqqi4x"  # l_type, l_whence, l_start, l_len, l_pid (Linux, 64-bit)

    def _flock(type_: int, start: int, length: int) -> bytes:
        return struct.pack(_FLOCK, type_, os.SEEK_SET, start, length, 0)

    def _flock_type(raw: bytes) -> int:
        return struct.unpack(_FLOCK, raw)[0]


OFD_LOCKS = _probe_ofd()
"""Linux and macOS: open file description locks (``F_OFD_SETLK``). They
conflict with the classic POSIX locks ordinary SQLite takes, even within
this process, and closing some other descriptor of the file does not drop
them, so the in-process backend (or ``sqlite3``) in this same process is
excluded too. Elsewhere, or with ``DBLAYER_WASM_POSIX_LOCKS=1``, classic
``F_SETLK`` locks, which exclude other processes only."""


def _write_locked_by_other_process(fd: int, start: int, length: int) -> bool:
    cmd = _F_OFD_GETLK if OFD_LOCKS else fcntl.F_GETLK
    raw = fcntl.fcntl(fd, cmd, _flock(fcntl.F_WRLCK, start, length))
    return _flock_type(raw) != fcntl.F_UNLCK


class _Inode:
    """This process's view of one database file, shared by every instance
    that has it open: SQLite's ``unixInodeInfo``. POSIX locks belong to the
    process, so the strongest lock any handle holds is taken once, on the
    inode's own descriptor, and the counts decide when to release it. The
    descriptor is closed only when no handle remains, and then only when
    that cannot drop anyone's locks (see ``_parked``)."""

    __slots__ = ("key", "fd", "writable", "lock", "level", "n_shared", "n_lock", "refs")

    def __init__(self, key: tuple[int, int], fd: int, writable: bool) -> None:
        self.key = key
        self.fd = fd
        self.writable = writable
        self.lock = threading.Lock()
        self.level = NO_LOCK
        self.n_shared = 0
        self.n_lock = 0
        self.refs = 0


_inodes: dict[tuple[int, int], _Inode] = {}
_inodes_lock = threading.Lock()
_parked: dict[tuple[int, int], list[tuple[int, bool]]] = {}
"""Descriptors waiting to be closed, per file, with whether each is
read-write. [_inodes_lock] Closing *any* descriptor of a file drops every
classic POSIX lock this process holds on it: the host's own (classic mode)
and, in either mode, those of ordinary SQLite in this process (the
in-process backend, ``sqlite3``), which has its own lock table that cannot
see this one. So a descriptor is closed only when that cannot happen:"""


def _safe_to_close(fd: int, node: _Inode | None) -> bool:
    """With open file description locks, when no one at all holds a lock on
    the file (``F_OFD_GETLK`` over the whole file sees this process's
    classic locks as well as everyone else's; this descriptor holds none).
    With classic locks, when the host holds none on it (other libraries'
    locks in this process are invisible, the documented unsafe case)."""
    if node is not None and node.n_lock > 0 and not OFD_LOCKS:
        return False
    if not OFD_LOCKS:
        return True
    try:
        raw = fcntl.fcntl(fd, _F_OFD_GETLK, _flock(fcntl.F_WRLCK, 0, 0))
    except OSError:
        return True
    return _flock_type(raw) == fcntl.F_UNLCK


def _sweep(key: tuple[int, int] | None = None) -> None:
    """[_inodes_lock held] Close the parked descriptors that may now be
    closed (of one file, or of every file)."""
    for k in [key] if key is not None else list(_parked):
        fds = _parked.get(k)
        if not fds:
            continue
        node = _inodes.get(k)
        keep = []
        for fd, rw in fds:
            if _safe_to_close(fd, node):
                try:
                    os.close(fd)
                except OSError:
                    pass
            else:
                keep.append((fd, rw))
        if keep:
            _parked[k] = keep
        else:
            _parked.pop(k, None)


def _park(key: tuple[int, int], fd: int, writable: bool) -> None:
    """[_inodes_lock held] Close ``fd`` now if that is safe, else later."""
    _parked.setdefault(key, []).append((fd, writable))
    _sweep(key)


def _inode_open(path: str, want_write: bool) -> tuple[_Inode, bool]:
    """The shared inode for ``path`` (one descriptor per file per process,
    read-write if at all possible) and whether it is writable. A file this
    process already has open is found by ``stat()``, without opening (and so
    later closing) another descriptor of it."""
    try:
        st0 = os.stat(path)
        key0: tuple[int, int] | None = (st0.st_dev, st0.st_ino)
    except OSError:
        key0 = None
    with _inodes_lock:
        if _parked:
            _sweep()
        if key0 is not None:
            node = _inodes.get(key0)
            if node is not None:
                node.refs += 1
                return node, node.writable
            # A descriptor parked when this file was last released (someone
            # still held a lock then): take it back instead of opening another.
            for i, (fd, rw) in enumerate(_parked.get(key0, ())):
                if rw or not want_write:
                    try:
                        st = os.fstat(fd)
                    except OSError:
                        continue
                    if (st.st_dev, st.st_ino) == key0:
                        del _parked[key0][i]
                        node = _inodes[key0] = _Inode(key0, fd, rw)
                        node.refs = 1
                        return node, rw
    flags = os.O_CLOEXEC
    try:
        fd = os.open(path, flags | os.O_RDWR)
        writable = True
    except PermissionError:
        if want_write:
            raise
        fd = os.open(path, flags | os.O_RDONLY)
        writable = False
    st = os.fstat(fd)
    key = (st.st_dev, st.st_ino)
    with _inodes_lock:
        node = _inodes.get(key)
        if node is None:
            node = _inodes[key] = _Inode(key, fd, writable)
            fd = -1
        node.refs += 1
        if fd >= 0:
            # Opened concurrently by another thread: this descriptor never
            # holds a lock; close it when that is safe.
            _park(key, fd, writable)
    return node, node.writable


def _close_deferred(node: _Inode) -> None:
    """[node.lock held, the host's lock count on it just fell to 0]"""
    if node.key in _parked:
        with _inodes_lock:
            _sweep(node.key)


def _inode_release(node: _Inode) -> None:
    with _inodes_lock:
        node.refs -= 1
        if node.refs > 0:
            return
        if _inodes.get(node.key) is node:
            del _inodes[node.key]
        fd, node.fd = node.fd, -1  # no handle is left to use it
        if fd >= 0:
            _park(node.key, fd, node.writable)


def _lock(node: _Inode, h: "_Handle", level: int) -> int:
    """``unixLock``, line for line: take ``level`` for handle ``h``."""
    if h.lock >= level:
        return SQLITE_OK
    with node.lock:
        if h.lock != node.level and (node.level >= PENDING_LOCK or level > SHARED_LOCK):
            return SQLITE_BUSY
        if level == SHARED_LOCK and node.level in (SHARED_LOCK, RESERVED_LOCK):
            h.lock = SHARED_LOCK
            node.n_shared += 1
            node.n_lock += 1
            return SQLITE_OK
        fd = node.fd
        try:
            if level == SHARED_LOCK or (level == EXCLUSIVE_LOCK and h.lock == RESERVED_LOCK):
                _setlk(fd, "r" if level == SHARED_LOCK else "w", PENDING_BYTE, 1)
                if level == EXCLUSIVE_LOCK:
                    h.lock = node.level = PENDING_LOCK
            if level == SHARED_LOCK:
                try:
                    _setlk(fd, "r", SHARED_FIRST, SHARED_SIZE)
                finally:
                    _setlk(fd, "u", PENDING_BYTE, 1)
                h.lock = SHARED_LOCK
                node.n_lock += 1
                node.n_shared = 1
                node.level = SHARED_LOCK
                return SQLITE_OK
            if level == EXCLUSIVE_LOCK and node.n_shared > 1:
                return SQLITE_BUSY  # another handle in this process still reads
            if level == RESERVED_LOCK:
                _setlk(fd, "w", RESERVED_BYTE, 1)
            else:
                _setlk(fd, "w", SHARED_FIRST, SHARED_SIZE)
        except _Busy:
            return SQLITE_BUSY
        except OSError:
            return SQLITE_IOERR_LOCK
        h.lock = node.level = level
        return SQLITE_OK


def _unlock(node: _Inode, h: "_Handle", level: int) -> int:
    """``posixUnlock``: drop handle ``h`` to ``level`` (SHARED or NONE)."""
    if h.lock <= level:
        return SQLITE_OK
    with node.lock:
        fd = node.fd
        rc = SQLITE_OK
        try:
            if h.lock > SHARED_LOCK:
                if level == SHARED_LOCK:
                    try:
                        _setlk(fd, "r", SHARED_FIRST, SHARED_SIZE)
                    except (OSError, _Busy):
                        rc = SQLITE_IOERR_RDLOCK
                _setlk(fd, "u", PENDING_BYTE, 2)
                node.level = SHARED_LOCK
            if level == NO_LOCK:
                node.n_shared -= 1
                if node.n_shared == 0:
                    _setlk(fd, "u", 0, 0)
                    node.level = NO_LOCK
                node.n_lock -= 1
                if node.n_lock == 0:
                    _close_deferred(node)
        except (OSError, _Busy):
            rc = SQLITE_IOERR_UNLOCK
        h.lock = level
        return rc


def _check_reserved(node: _Inode) -> int:
    with node.lock:
        if node.level > SHARED_LOCK:
            return 1
        try:
            return 1 if _write_locked_by_other_process(node.fd, RESERVED_BYTE, 1) else 0
        except OSError:
            return -SQLITE_IOERR_CHECKRESERVEDLOCK


# -- host files ------------------------------------------------------------------


class _Handle:
    __slots__ = ("fd", "node", "lock", "writable", "path")

    def __init__(self, fd: int, node: _Inode | None, writable: bool, path: str | None) -> None:
        self.fd = fd
        self.node = node
        self.lock = NO_LOCK
        self.writable = writable
        self.path = path


class _HostFiles:
    """The files one instance has open, and the host functions its VFS
    imports. Paths are checked against an allow-list: the database file and
    its rollback journal."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.allowed = {db_path, db_path + "-journal"}
        self.handles: dict[int, _Handle] = {}
        self._ids = itertools.count(1)
        self.base = 0
        self.size = 0
        self.size_of: Callable[[], int] = lambda: 0

    # memory access, bounds-checked against the current memory size
    def _check(self, ptr: int, n: int) -> int:
        if ptr < 0 or n < 0:
            raise ValueError("bad pointer")
        end = ptr + n
        if end > self.size:
            self.size = self.size_of()
            if end > self.size:
                raise ValueError("pointer outside linear memory")
        return self.base + ptr

    def _get(self, ptr: int, n: int) -> bytes:
        return ctypes.string_at(self._check(ptr, n), n)

    def _path(self, ptr: int, n: int) -> str | None:
        p = os.fsdecode(self._get(ptr, n))
        return p if p in self.allowed else None

    def release_all(self) -> None:
        """Unlock and close everything (the instance is gone)."""
        for hid in list(self.handles):
            self.close(hid)

    # -- the imports (each returns a SQLite code; never raises) ------------------

    def open(self, ptr: int, n: int, flags: int, out_ptr: int) -> int:
        try:
            if n == 0:
                fd, tmp = tempfile.mkstemp(prefix="dblayer-wasm-")
                os.unlink(tmp)
                h = _Handle(fd, None, True, None)
                out = flags
            else:
                path = self._path(ptr, n)
                if path is None:
                    return -SQLITE_CANTOPEN
                writable = bool(flags & OPEN_READWRITE)
                out = flags
                if flags & OPEN_MAIN_DB:
                    if flags & OPEN_CREATE and not os.path.exists(path):
                        os.close(os.open(path, os.O_CREAT | os.O_WRONLY | os.O_CLOEXEC, 0o644))
                    try:
                        node, node_writable = _inode_open(path, writable and not flags & OPEN_CREATE)
                    except PermissionError:
                        if not writable:
                            raise
                        node, node_writable = _inode_open(path, False)
                    if writable and not node_writable:
                        writable = False
                        out = (flags & ~(OPEN_READWRITE | OPEN_CREATE)) | OPEN_READONLY
                    h = _Handle(node.fd, node, writable, path)
                else:
                    oflags = os.O_CLOEXEC | (os.O_RDWR if writable else os.O_RDONLY)
                    if flags & OPEN_CREATE:
                        oflags |= os.O_CREAT
                    if flags & OPEN_EXCLUSIVE:
                        oflags |= os.O_EXCL
                    fd = os.open(path, oflags, 0o644)
                    if flags & OPEN_DELETEONCLOSE:
                        os.unlink(path)
                    h = _Handle(fd, None, writable, path)
            hid = next(self._ids)
            self.handles[hid] = h
            ctypes.memmove(self._check(out_ptr, 4), struct.pack("<i", out), 4)
            return hid
        except Exception:
            return -SQLITE_CANTOPEN

    def close(self, hid: int) -> int:
        h = self.handles.pop(hid, None)
        if h is None:
            return SQLITE_OK
        try:
            if h.node is not None:
                _unlock(h.node, h, NO_LOCK)
                _inode_release(h.node)
            else:
                os.close(h.fd)
            return SQLITE_OK
        except Exception:
            return -SQLITE_IOERR_CLOSE

    def read(self, hid: int, buf: int, n: int, off: int) -> int:
        try:
            h = self.handles[hid]
            addr = self._check(buf, n)
            data = os.pread(h.fd, n, off)
            ctypes.memmove(addr, data, len(data))
            return len(data)
        except Exception:
            return -SQLITE_IOERR_READ

    def write(self, hid: int, buf: int, n: int, off: int) -> int:
        try:
            h = self.handles[hid]
            if not h.writable:
                return -SQLITE_READONLY
            data = self._get(buf, n)
            view = memoryview(data)
            while view:
                k = os.pwrite(h.fd, view, off)
                view = view[k:]
                off += k
            return SQLITE_OK
        except OSError as e:
            return -(SQLITE_FULL if e.errno == errno.ENOSPC else SQLITE_IOERR_WRITE)
        except Exception:
            return -SQLITE_IOERR_WRITE

    def truncate(self, hid: int, size: int) -> int:
        try:
            h = self.handles[hid]
            if not h.writable:
                return -SQLITE_READONLY
            os.ftruncate(h.fd, size)
            return SQLITE_OK
        except Exception:
            return -SQLITE_IOERR_TRUNCATE

    def sync(self, hid: int, flags: int) -> int:
        try:
            os.fsync(self.handles[hid].fd)
            return SQLITE_OK
        except Exception:
            return -SQLITE_IOERR_FSYNC

    def file_size(self, hid: int) -> int:
        try:
            return os.fstat(self.handles[hid].fd).st_size
        except Exception:
            return -SQLITE_IOERR_FSTAT

    def lock(self, hid: int, level: int) -> int:
        h = self.handles.get(hid)
        if h is None:
            return SQLITE_IOERR_LOCK
        if h.node is None:
            h.lock = level
            return SQLITE_OK
        return _lock(h.node, h, level)

    def unlock(self, hid: int, level: int) -> int:
        h = self.handles.get(hid)
        if h is None:
            return SQLITE_IOERR_UNLOCK
        if h.node is None:
            h.lock = level
            return SQLITE_OK
        return _unlock(h.node, h, level)

    def check_reserved(self, hid: int) -> int:
        h = self.handles.get(hid)
        if h is None or h.node is None:
            return 0
        return _check_reserved(h.node)

    def delete(self, ptr: int, n: int, sync_dir: int) -> int:
        try:
            path = self._path(ptr, n)
            if path is None:
                return -SQLITE_IOERR_DELETE
            try:
                os.unlink(path)
            except FileNotFoundError:
                return -SQLITE_IOERR_DELETE_NOENT
            if sync_dir:
                try:
                    dfd = os.open(os.path.dirname(path) or ".", os.O_RDONLY)
                    try:
                        os.fsync(dfd)
                    finally:
                        os.close(dfd)
                except OSError:
                    pass
            return SQLITE_OK
        except Exception:
            return -SQLITE_IOERR_DELETE

    def access(self, ptr: int, n: int, flags: int) -> int:
        try:
            path = self._path(ptr, n)
            if path is None:
                return 0
            if flags == ACCESS_READWRITE:
                return 1 if os.access(path, os.R_OK | os.W_OK) else 0
            try:
                st = os.stat(path)
            except OSError:
                return 0
            # As the unix VFS: an empty regular file does not count.
            return 1 if st.st_size > 0 or not os.path.isfile(path) else 0
        except Exception:
            return -SQLITE_IOERR_ACCESS

    def touch(self, ptr: int, n: int) -> int:
        try:
            path = self._path(ptr, n)
            if path is None:
                return -SQLITE_CANTOPEN
            try:
                os.close(os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_CLOEXEC, 0o644))
            except FileExistsError:
                pass
            return SQLITE_OK
        except Exception:
            return -SQLITE_CANTOPEN

    def sleep(self, micros: int) -> int:
        time.sleep(max(micros, 0) / 1e6)
        return 0


# -- the runtime: one engine and compiled module per process ---------------------


class _Raw:
    """The few wasmtime C API calls on the hot path, as ctypes prototypes of
    our own (wasmtime-py's ``Func`` wrappers allocate type objects on every
    call and every host call, all under the GIL, which made a wasm backend
    with eight busy threads slower than one). Built once per process."""

    def __init__(self) -> None:
        from wasmtime import _ffi as ffi  # type: ignore[attr-defined]

        self.ffi = ffi
        dll = ffi.dll

        def proto(name: str, restype: Any, *argtypes: Any) -> Any:
            addr = ctypes.cast(getattr(dll, name), ctypes.c_void_p).value
            return ctypes.CFUNCTYPE(restype, *argtypes)(addr)

        vp, sz = ctypes.c_void_p, ctypes.c_size_t
        self.func_call = proto("wasmtime_func_call_unchecked", vp, vp, vp, vp, sz, vp)
        self.memory_size = proto("wasmtime_memory_data_size", sz, vp, vp)
        self.memory_data = proto("wasmtime_memory_data", vp, vp, vp)
        self.error_new = proto("wasmtime_error_new", vp, ctypes.c_char_p)
        self.epoch_cb_type = ctypes.CFUNCTYPE(
            sz, vp, vp, ctypes.POINTER(ctypes.c_uint64), ctypes.POINTER(ctypes.c_uint8)
        )
        self.set_epoch_cb = proto(
            "wasmtime_store_epoch_deadline_callback", None, vp, self.epoch_cb_type, vp, vp
        )
        self.set_deadline = proto("wasmtime_context_set_epoch_deadline", None, vp, ctypes.c_uint64)
        self.set_fuel = proto("wasmtime_context_set_fuel", vp, vp, ctypes.c_uint64)
        # Host imports: (env, caller, args_and_results, n) -> error pointer.
        self.host_cb_type = ctypes.CFUNCTYPE(sz, vp, vp, vp, sz)

    def raise_for(self, err: int | None, trap: int | None) -> None:
        """Turn an error or trap pointer from a call into wasmtime-py's
        exception (``ExitTrap`` for ``proc_exit``)."""
        import wasmtime

        ffi = self.ffi
        if err:
            raise wasmtime.WasmtimeError._from_ptr(ctypes.cast(err, ctypes.POINTER(ffi.wasmtime_error_t)))
        if trap:
            raise wasmtime.Trap._from_ptr(ctypes.cast(trap, ctypes.POINTER(ffi.wasm_trap_t)))


_raw: _Raw | None = None
_tls = threading.local()
"""``files``: the ``_HostFiles`` of the instance this thread is running
(host imports run on the thread that called into the instance)."""

_I32 = struct.Struct("<i")
_I64 = struct.Struct("<q")
# Each argument is a 16-byte wasmtime_val_raw_t: an i32 or i64 in its low bytes.
_HOST_IMPORTS: dict[str, tuple[str, str, int]] = {
    # name: (params, result, error code returned when the host function fails)
    "open": ("iiii", "i", -SQLITE_CANTOPEN),
    "close": ("i", "i", -SQLITE_IOERR_CLOSE),
    "read": ("iiiq", "i", -SQLITE_IOERR_READ),
    "write": ("iiiq", "i", -SQLITE_IOERR_WRITE),
    "truncate": ("iq", "i", -SQLITE_IOERR_TRUNCATE),
    "sync": ("ii", "i", -SQLITE_IOERR_FSYNC),
    "size": ("i", "q", -SQLITE_IOERR_FSTAT),
    "lock": ("ii", "i", SQLITE_IOERR_LOCK),
    "unlock": ("ii", "i", SQLITE_IOERR_UNLOCK),
    "check_reserved": ("i", "i", -SQLITE_IOERR_CHECKRESERVEDLOCK),
    "delete": ("iii", "i", -SQLITE_IOERR_DELETE),
    "access": ("iii", "i", -SQLITE_IOERR_ACCESS),
    "touch": ("ii", "i", -SQLITE_CANTOPEN),
    "sleep": ("i", "i", 0),
}
_METHOD = {"size": "file_size"}


def _host_callback(raw: _Raw, name: str) -> Any:
    params, result, failed = _HOST_IMPORTS[name]
    unpack = struct.Struct("<" + "".join(p + ("12x" if p == "i" else "8x") for p in params)).unpack_from
    nbytes = 16 * max(len(params), 1)
    method = _METHOD.get(name, name)
    out = _I64 if result == "q" else _I32
    buf_t = ctypes.c_char * nbytes

    def cb(_env: Any, _caller: Any, args: int, _n: int) -> int:
        try:
            files = _tls.files
            r = getattr(files, method)(*unpack(buf_t.from_address(args)))
        except BaseException:
            r = failed
        out.pack_into(buf_t.from_address(args), 0, r)
        return 0

    return raw.host_cb_type(cb)


_by_key: dict[int, "_Instance"] = {}
_keys = itertools.count(1)


def _on_epoch(_ctx: Any, env: int | None, delta: Any, kind: Any) -> int:
    """Every epoch tick while an instance runs: stop it (an error is a
    trap) if it is past its hard deadline or was killed; else carry on."""
    try:
        inst = _by_key.get(env or 0)
        if inst is not None:
            now = time.monotonic()
            if inst.kill_reason is None:
                hard = inst.hard_deadline
                if hard is not None and now >= hard:
                    inst.kill_reason = "time"
                else:
                    soft = inst.soft_deadline
                    if soft is not None and now >= soft:
                        inst.write_cancel(2)  # backstop for the progress handler
            if inst.kill_reason is not None:
                assert _raw is not None
                return _raw.error_new(b"dblayer: execution stopped") or 0
    except BaseException:
        pass
    delta[0] = 1
    kind[0] = 0  # WASMTIME_UPDATE_DEADLINE_CONTINUE
    return 0


class _Runtime:
    """The wasmtime engine (epoch interruption on), the compiled module, a
    linker with the host imports, a pre-linked ``InstancePre`` and the epoch
    ticker thread, shared by every instance in the process.

    ``interruption`` is ``"epoch"`` (the backend's), or ``"fuel"`` / ``"none"``
    for the benchmark that compares their costs."""

    def __init__(self, module_path: str, interruption: str = "epoch") -> None:
        import wasmtime

        global _raw
        if _raw is None:
            _raw = _Raw()
        self.raw = _raw
        self.wasmtime = wasmtime
        self.interruption = interruption
        cfg = wasmtime.Config()
        cfg.epoch_interruption = interruption == "epoch"
        cfg.consume_fuel = interruption == "fuel"
        # Linear memory never moves (the default 4 GB reservation on 64-bit
        # hosts), so host functions can keep its base address.
        cfg.memory_may_move = False
        self.engine = wasmtime.Engine(cfg)
        t0 = time.perf_counter()
        self.module, self.module_source = _load_module(wasmtime, self.engine, module_path, interruption)
        self.compile_s = time.perf_counter() - t0
        self.module_path = module_path
        linker = wasmtime.Linker(self.engine)
        linker.define_wasi()
        ffi = _raw.ffi
        self._callbacks = []
        for name, (params, result, _failed) in _HOST_IMPORTS.items():
            vt = {"i": wasmtime.ValType.i32(), "q": wasmtime.ValType.i64()}
            ty = wasmtime.FuncType([vt[p] for p in params], [vt[result]])
            cb = _host_callback(_raw, name)
            self._callbacks.append(cb)  # kept alive for the process
            module_b, name_b = b"dblayer", name.encode()
            err = ffi.wasmtime_linker_define_func_unchecked(
                linker.ptr(), ctypes.create_string_buffer(module_b), len(module_b),
                ctypes.create_string_buffer(name_b), len(name_b), ty.ptr(),
                ctypes.cast(cb, ffi.wasmtime_func_unchecked_callback_t), None,
                ctypes.cast(0, ctypes.CFUNCTYPE(None, ctypes.c_void_p)),
            )
            if err:
                raise wasmtime.WasmtimeError._from_ptr(err)
        self.linker = linker
        self.pre = linker.instantiate_pre(self.module)
        self.epoch_cb = _raw.epoch_cb_type(_on_epoch)
        self._active = 0
        self._cond = threading.Condition()
        self._ticker: threading.Thread | None = None
        self.instances_created = 0
        self.traps = 0

    def enter(self) -> None:
        if self.interruption != "epoch":
            return
        with self._cond:
            self._active += 1
            if self._ticker is None or not self._ticker.is_alive():
                self._ticker = threading.Thread(target=self._tick, name="dblayer-wasm-epoch", daemon=True)
                self._ticker.start()
            elif self._active == 1:
                self._cond.notify()

    def leave(self) -> None:
        if self.interruption != "epoch":
            return
        with self._cond:
            self._active -= 1

    def _tick(self) -> None:
        engine = self.engine
        while True:
            with self._cond:
                while self._active == 0:
                    if not self._cond.wait(30.0):
                        if self._active == 0:
                            self._ticker = None
                            return
            time.sleep(EPOCH_TICK_S)
            engine.increment_epoch()


def _load_module(wasmtime: Any, engine: Any, path: str, flavour: str) -> tuple[Any, str]:
    """Compile the module, or load a copy compiled earlier by this version
    of wasmtime (``<path>.<version>.cwasm`` next to it, written when the
    directory is writable)."""
    try:
        from importlib.metadata import version

        wt = version("wasmtime")
    except Exception:
        wt = "unknown"
    st = os.stat(path)
    cached = f"{path}.{wt}.{flavour}.{st.st_size}.{st.st_mtime_ns}.cwasm"
    if os.path.exists(cached):
        try:
            return wasmtime.Module.deserialize_file(engine, cached), "cache"
        except Exception:
            pass
    module = wasmtime.Module.from_file(engine, path)
    try:
        tmp = f"{cached}.{os.getpid()}.tmp"
        with open(tmp, "wb") as f:
            f.write(module.serialize())
        os.replace(tmp, cached)
    except OSError:
        pass
    return module, "compiled"


_runtimes: dict[tuple[str, str], _Runtime] = {}
_runtime_lock = threading.Lock()


def _runtime(module_path: str, interruption: str = "epoch") -> _Runtime:
    key = (module_path, interruption)
    rt = _runtimes.get(key)
    if rt is None:
        with _runtime_lock:
            rt = _runtimes.get(key)
            if rt is None:
                rt = _runtimes[key] = _Runtime(module_path, interruption)
    return rt


@atexit.register
def _close_runtimes() -> None:
    """Free wasmtime objects while wasmtime-py's bindings still exist (left
    to interpreter teardown, their finalizers fail noisily)."""
    for key in list(_by_key):
        inst = _by_key.pop(key, None)
        if inst is not None:
            try:
                inst.close()
            except Exception:
                pass
    for rt in list(_runtimes.values()):
        for obj in (rt.pre, rt.linker, rt.module, rt.engine):
            try:
                obj.close()
            except Exception:
                pass
    _runtimes.clear()


def runtime_stats(module: str | os.PathLike[str] | None = None) -> dict[str, Any]:
    """Process-wide figures: compile (or cache load) time, instances
    created, instances discarded after a trap."""
    rt = _runtime(find_module(module))
    return {
        "compile_s": rt.compile_s,
        "module_source": rt.module_source,
        "instances_created": rt.instances_created,
        "traps": rt.traps,
    }


# -- one instance ------------------------------------------------------------------------


class _Trapped(Exception):
    """The instance trapped (or exited) and must be discarded."""

    def __init__(self, reason: str, detail: str) -> None:
        super().__init__(f"{reason}: {detail}")
        self.reason = reason
        self.detail = detail


class _Instance:
    """One store, one instance of the module, its host files. Used by one
    thread at a time. Guest calls go through ``wasmtime_func_call_unchecked``
    with a preallocated argument array; the GIL is released while the guest
    runs (ctypes releases it for every foreign call)."""

    def __init__(self, rt: _Runtime, db_path: str, memory_limit_bytes: int | None) -> None:
        w = rt.wasmtime
        raw = rt.raw
        self.rt = rt
        self.raw = raw
        self.files = files = _HostFiles(db_path)
        store = self.store = w.Store(rt.engine)
        if memory_limit_bytes is not None:
            store.set_limits(memory_size=memory_limit_bytes)
        self.memory_limit_bytes = memory_limit_bytes
        store.set_wasi(w.WasiConfig())  # no files, no environment, no arguments
        self.ctx = ctypes.cast(store._context(), ctypes.c_void_p).value
        self.kill_reason: str | None = None
        self.hard_deadline: float | None = None
        self.soft_deadline: float | None = None
        self.key = next(_keys)
        _by_key[self.key] = self
        if rt.interruption == "epoch":
            raw.set_epoch_cb(ctypes.cast(store.ptr(), ctypes.c_void_p), rt.epoch_cb, self.key, None)
            raw.set_deadline(self.ctx, 1 << 62)
        elif rt.interruption == "fuel":
            raw.set_fuel(self.ctx, 1 << 62)
        self._args = (ctypes.c_char * 32)()
        self._args_addr = ctypes.addressof(self._args)
        self._trap = ctypes.c_void_p()
        self._trap_addr = ctypes.addressof(self._trap)
        _tls.files = files
        try:
            instance = rt.pre.instantiate(store)
            ex = instance.exports(store)
            self._exports = ex  # keeps the Func objects (and their handles) alive
            self.memory = ex["memory"]
            self._mem_addr = ctypes.addressof(self.memory._memory)
            files.base = raw.memory_data(self.ctx, self._mem_addr)
            files.size_of = self.memory_bytes
            files.size = self.memory_bytes()
            self._f = {name: ctypes.addressof(ex[name]._func) for name in
                       ("_initialize", "dbl_init", "dbl_input", "dbl_io", "dbl_call", "dbl_cancel_flag")}
            self._invoke("_initialize")
            rc = self._invoke("dbl_init", 1 if os.environ.get("DBLAYER_WASM_DEBUG") else 0)
            if rc != 0:
                raise WorkerCrashed(f"SQLite did not initialise in the WebAssembly instance ({rc})")
            self.cancel_addr = files.base + self._invoke("dbl_cancel_flag")
            self._io = files._check(self._invoke("dbl_io") & 0xFFFFFFFF, 12)
            self._invoke("dbl_input", 4096)
        except BaseException:
            _by_key.pop(self.key, None)
            raise
        rt.instances_created += 1

    def _invoke(self, name: str, arg: int | None = None) -> int:
        """Call an export taking at most one i32 and returning at most one."""
        args = self._args
        if arg is not None:
            _I32.pack_into(args, 0, arg)
        self._trap.value = None
        err = self.raw.func_call(self.ctx, self._f[name], self._args_addr, 2, self._trap_addr)
        if err or self._trap.value:
            self.raw.raise_for(err, self._trap.value)
        return _I32.unpack_from(args, 0)[0]

    def memory_bytes(self) -> int:
        return self.raw.memory_size(self.ctx, self._mem_addr)

    def write_cancel(self, value: int) -> None:
        ctypes.c_int32.from_address(self.cancel_addr).value = value

    def call(self, payload: bytes) -> bytes:
        """Run one request (``b""``: only check transaction deadlines) and
        return the frames it produced. Raises ``_Trapped``."""
        w = self.rt.wasmtime
        _tls.files = self.files
        try:
            if self.rt.interruption == "epoch":
                self.raw.set_deadline(self.ctx, 1)
            n = len(payload)
            if n:
                io = (ctypes.c_uint32 * 3).from_address(self._io)
                if n > io[1]:  # the input buffer is too small: grow it
                    self._invoke("dbl_input", n)
                    if io[1] < n:
                        raise _Trapped("memory", "no memory for the request")
                ctypes.memmove(self.files._check(io[0], n), payload, n)
            out_len = self._invoke("dbl_call", n)
            if out_len < 0:
                if out_len == -2:
                    raise _Trapped("crash", "the instance could not decode the request")
                raise _Trapped("memory", "no memory for the reply")
            if out_len == 0:
                return b""
            out_ptr = (ctypes.c_uint32 * 3).from_address(self._io)[2]
            return ctypes.string_at(self.files._check(out_ptr, out_len), out_len)
        except _Trapped:
            raise
        except w.ExitTrap as e:
            code = getattr(e, "code", None)
            raise _Trapped("memory" if code == 70 else "crash", f"instance exited ({code})") from None
        except (w.Trap, w.WasmtimeError) as e:
            reason = self.kill_reason or ("memory" if self._near_limit() else "crash")
            raise _Trapped(reason, str(e).splitlines()[0] if str(e) else type(e).__name__) from None

    def _near_limit(self) -> bool:
        if self.memory_limit_bytes is None:
            return False
        try:
            return self.memory_bytes() + (1 << 20) > self.memory_limit_bytes
        except Exception:
            return False

    def discard(self) -> None:
        """After a trap."""
        self.rt.traps += 1
        self.close()

    def close(self) -> None:
        """Release the files and free the store now (its linear memory goes
        back to the OS at once)."""
        _by_key.pop(self.key, None)
        self.files.release_all()
        self.files.size_of = lambda: 0
        self.files.size = 0
        self._exports = {}
        try:
            self.store.close()
        except Exception:
            pass


# -- the "process": an instance behind the WorkerProcess interface --------------------


@dataclass(frozen=True, slots=True)
class WasmConfig:
    """Settings shared by every instance of one backend."""

    module: str
    codec: str = "msgpack"
    memory_limit_mb: int | None = None
    kill_grace_ms: int = DEFAULT_KILL_GRACE_MS
    deadline_slack_ms: int = 50
    max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES
    debug: bool = False
    inline_reads: bool = False
    """Run reads on read connections on the event loop's thread first
    (``SqliteWasm(inline_reads=True)``)."""
    recycle_memory_mb: int | None = 64
    """Linear memory never shrinks: an instance that once grew to hold a
    big result keeps that memory. Past this size (and outside a
    transaction or an open stream) it is replaced by a fresh instance
    before the next request, which hands the memory back to the OS."""


_RECYCLE_AFTER = frozenset({"execute", "execute_many", "batch", "schema", "schema_version", "commit", "rollback"})


class _Call:
    __slots__ = ("mid", "cancelled", "active", "limit_ms")

    def __init__(self, mid: int, limit_ms: float | None) -> None:
        self.mid = mid
        self.limit_ms = limit_ms
        self.cancelled = False
        self.active = False


class WasmProcess:
    """A WebAssembly instance with the interface ``WorkerConnection``
    expects of a ``WorkerProcess``: ``call`` runs one request on the
    executor and returns its reply (or stream frames); ``kill`` traps a
    running call and discards the instance."""

    pid = None
    recorder = None

    def __init__(self, config: WasmConfig, db_path: str, executor: Callable[[], Any]) -> None:
        self.config = config
        self.db_path = db_path
        self._executor = executor
        self.loop = asyncio.get_running_loop()
        self.hello: dict[str, Any] = {}
        self.codec = get_codec("json")
        self.lock = asyncio.Lock()
        self._tlock = threading.Lock()
        self._ids = itertools.count(1)
        self.dead: BaseException | None = None
        self._kill_for: Mapping[int, BaseException] = {}
        self.notice_handlers: dict[Any, Callable[[dict[str, Any]], None]] = {}
        self.inflight: tuple[int, object] | None = None
        self.handles = 0
        self.idle_timer: asyncio.TimerHandle | None = None
        self._inst: _Instance | None = None
        self._current: _Call | None = None
        self._timers: list[asyncio.TimerHandle] = []
        self.started_at = 0.0
        self._open_msg: dict[str, Any] | None = None
        self._needs_recycle = False
        self.recycled = 0
        self.inline_runs = 0
        self.inline_reruns = 0

    def __repr__(self) -> str:
        return f"<WasmProcess {'dead' if self.dead else 'alive'} {self.db_path}>"

    @property
    def alive(self) -> bool:
        return self.dead is None

    def stderr_text(self) -> str:
        return ""

    # -- start / stop ---------------------------------------------------------------

    async def start(self) -> None:
        self.started_at = time.perf_counter()
        async with self.lock:
            await self._new_instance()

    async def _new_instance(self) -> None:
        """[self.lock held] Instantiate and say hello, in one executor hop."""
        self._inst, self.hello = await run_blocking(self._executor(), self._instantiate)
        self.codec = get_codec(self.hello.get("codec", "json"))

    def _instantiate(self) -> tuple[_Instance, dict[str, Any]]:
        """Executor thread."""
        cfg = self.config
        limit = None if cfg.memory_limit_mb is None else int(cfg.memory_limit_mb) * 1024 * 1024
        inst = _Instance(_runtime(cfg.module), self.db_path, limit)
        codecs = [cfg.codec] if cfg.codec == "json" else [cfg.codec, "json"]
        json = get_codec("json")
        msg = {"type": "hello", "protocol": PROTOCOL_VERSION, "codecs": codecs,
               "max_frame_bytes": cfg.max_frame_bytes, "id": next(self._ids)}
        try:
            out = inst.call(json.encode(msg))
            size = int.from_bytes(out[:4], "big")
            reply = json.decode(out[4:4 + size])
        except BaseException:
            inst.close()
            raise
        return inst, reply

    async def _recycle(self) -> None:
        """[self.lock held] Replace an idle instance whose memory has grown:
        a new instance, the same ``open`` replayed (no transaction is open,
        so there is no state to carry over)."""
        self._needs_recycle = False
        old, self._inst = self._inst, None
        if old is not None:
            old.close()
        await self._new_instance()
        if self._open_msg is not None:
            await self._exchange(dict(self._open_msg), owner=None, limit_ms=None, expect_frames=None, on_sent=None)
        self.recycled += 1

    def _release(self) -> None:
        """Discard the instance (no call is running on it)."""
        inst, self._inst = self._inst, None
        for t in self._timers:
            t.cancel()
        self._timers.clear()
        if inst is not None:
            inst.close()

    def kill(self, exc: BaseException | None = None, *, for_ids: Mapping[int, BaseException] | None = None) -> None:
        """Stop now: a running call traps at the next epoch tick; the
        instance is discarded either way."""
        if exc is None:
            exc = WorkerCrashed("WebAssembly instance was stopped")
        if self.dead is None:
            self.dead = exc
            self._kill_for = dict(for_ids or {})
        with self._tlock:
            cur = self._current
            inst = self._inst
            if cur is not None and cur.active and inst is not None:
                inst.kill_reason = inst.kill_reason or "killed"
                return  # the executor thread discards it when the call returns
        if inst is not None and (cur is None or not cur.active):
            self._release()

    def close(self) -> None:
        """Graceful: roll back and close every connection inside the
        instance (one short call on this thread), then release its files.
        Never blocks on a running call."""
        if self.dead is not None:
            return
        self.dead = WorkerCrashed("WebAssembly instance was closed")
        with self._tlock:
            cur = self._current
            busy = cur is not None and cur.active
            inst = self._inst
        if busy and inst is not None:
            inst.kill_reason = inst.kill_reason or "killed"
            return
        if inst is not None:
            try:
                inst.write_cancel(0)
                inst.call(self.codec.encode({"type": "sleep", "id": next(self._ids)}))
            except Exception:
                pass
        self._release()

    # -- calls ----------------------------------------------------------------------------

    def _invoke(self, payload: bytes, call: _Call, inline_s: float | None = None) -> bytes:
        """Executor thread (or the event loop's, for an inline read: then
        ``inline_s`` is its budget, enforced by the epoch ticker)."""
        inst = self._inst
        if inst is None or self.dead is not None:
            raise _Trapped("closed", "instance was closed")
        with self._tlock:
            inst.write_cancel(1 if call.cancelled else 0)
            call.active = True
        limit = call.limit_ms
        now = time.monotonic()
        if inline_s is not None:
            inst.soft_deadline = now + inline_s
            inst.hard_deadline = now + inline_s + INLINE_GRACE_S
        elif limit is not None:
            inst.soft_deadline = now + max(limit, 0) / 1000 + 0.005
            inst.hard_deadline = now + (max(limit, 0) + self.config.kill_grace_ms) / 1000
        else:
            inst.soft_deadline = inst.hard_deadline = None
        if call.cancelled:
            inst.hard_deadline = now + self.config.kill_grace_ms / 1000
        rt = inst.rt
        rt.enter()
        try:
            return inst.call(payload)
        except _Trapped as t:
            with self._tlock:
                call.active = False
            self._inst = None
            if self.dead is None:
                # Seen by the next call even if this one's caller is gone
                # (cancelled): the connection then gets a new instance.
                self.dead = WorkerCrashed(f"WebAssembly instance discarded after a trap ({t})")
            inst.discard()
            raise
        finally:
            rt.leave()
            with self._tlock:
                call.active = False
            if self.dead is not None and self._inst is not None:
                # killed or closed while running: discard now
                self._inst = None
                inst.discard()

    def _on_cancel(self, call: _Call) -> None:
        """The awaiting task was cancelled (event loop thread)."""
        with self._tlock:
            call.cancelled = True
            inst = self._inst
            if call.active and inst is not None:
                inst.write_cancel(1)
                hard = time.monotonic() + self.config.kill_grace_ms / 1000
                if inst.hard_deadline is None or inst.hard_deadline > hard:
                    inst.hard_deadline = hard

    def _check(self) -> None:
        if self.dead is not None:
            raise WorkerCrashed(str(self.dead))

    async def call(
        self,
        msg: dict[str, Any],
        *,
        owner: object,
        limit_ms: float | None = None,
        expect_frames: int | None = None,
        on_sent: Callable[[int], None] | None = None,
    ) -> Any:
        async with self.lock:
            self._check()
            if self._needs_recycle:
                await self._recycle()
            reply = await self._exchange(
                msg, owner=owner, limit_ms=limit_ms, expect_frames=expect_frames, on_sent=on_sent
            )
            if msg.get("type") == "open":
                self._open_msg = {k: v for k, v in msg.items() if k != "id"}
            elif (
                expect_frames is None
                and self.config.recycle_memory_mb is not None
                and msg.get("type") in _RECYCLE_AFTER
                and not reply.get("in_tx")
                and self._inst is not None
                and self._inst.memory_bytes() > self.config.recycle_memory_mb << 20
            ):
                self._needs_recycle = True
        return reply

    async def _exchange(
        self,
        msg: dict[str, Any],
        *,
        owner: object,
        limit_ms: float | None,
        expect_frames: int | None,
        on_sent: Callable[[int], None] | None,
    ) -> Any:
        """[self.lock held] One request and its frames."""
        if expect_frames is not None and msg.get("type") == "more":
            msg.pop("id", None)
            mid = msg["stream"]
        else:
            mid = msg["id"] = msg.get("id") or next(self._ids)
        payload = self.codec.encode(msg)
        if len(payload) > self.config.max_frame_bytes:
            raise ResourceLimitExceeded(
                f"request too large to send: frame of {len(payload)} bytes exceeds the "
                f"{self.config.max_frame_bytes}-byte frame size cap"
            )
        if on_sent is not None:
            on_sent(mid)
        call = _Call(mid, limit_ms)
        frames: list[dict[str, Any]] | None = None
        if (
            self.config.inline_reads
            and expect_frames is None
            and msg.get("type") == "execute"
            and self._open_msg is not None
            and self._open_msg.get("mode") == "read"
            and (limit_ms is None or limit_ms > 2 * INLINE_S * 1000)
        ):
            frames = await self._try_inline(payload, call)
        if frames is None:
            self._current = call
            self.inflight = (mid, owner)
            try:
                out = await run_blocking(
                    self._executor(), self._invoke, payload, call, on_cancel=lambda: self._on_cancel(call)
                )
            except _Trapped as t:
                raise self._trapped(t, call) from None
            finally:
                self.inflight = None
                self._current = None
            if self.dead is not None:
                raise self._kill_for.get(mid, self.dead)
            frames = self._frames(out)
        if msg.get("type") == "begin" and msg.get("deadline_ms") is not None:
            self._arm_tick(msg["deadline_ms"])
        mine = [f for f in frames if f.get("id") == mid]
        if expect_frames is None:
            if not mine:
                raise WorkerCrashed(f"WebAssembly instance sent no reply to request {mid}")
            return _unwrap(mine[-1], self)  # type: ignore[arg-type]
        return mine

    async def _try_inline(self, payload: bytes, call: _Call) -> list[dict[str, Any]] | None:
        """[self.lock held] Run a read on the event loop's thread, with no
        executor hop, under a budget of ``INLINE_S``: the epoch ticker
        interrupts it there (or traps it ``INLINE_GRACE_S`` later), and
        then it runs again from the start on the executor, as an
        ``SqliteApsw(inline_reads=True)`` read does. Returns the frames, or
        ``None`` to run it on the executor."""
        t0 = time.monotonic()
        try:
            out = self._invoke(payload, call, INLINE_S)
        except _Trapped:
            # Over budget in one C call: the instance is gone. A new one
            # (as after recycling) and the read runs on the executor.
            self.inline_reruns += 1
            self.dead = None
            self._inst = None
            await self._recycle()
            self.recycled -= 1
            return None
        frames = self._frames(out)  # notices are handled either way
        mine = [f for f in frames if f.get("id") == call.mid]
        if (
            mine
            and isinstance(mine[-1].get("error"), dict)
            and mine[-1]["error"].get("type") == "QueryInterrupted"
            and time.monotonic() - t0 >= INLINE_S
        ):
            self.inline_reruns += 1
            return None
        self.inline_runs += 1
        return frames

    def _trapped(self, t: _Trapped, call: _Call) -> BaseException:
        """The exception a call gets when its instance trapped."""
        if self.dead is None:
            self.dead = WorkerCrashed(f"WebAssembly instance discarded after a trap ({t})")
        given = self._kill_for.get(call.mid)
        if given is not None:
            return given
        if t.reason == "time" and not call.cancelled:
            exc: BaseException = QueryInterrupted(
                f"query did not stop within its time limit ({call.limit_ms or 0:.0f} ms) plus "
                f"{self.config.kill_grace_ms} ms; its WebAssembly instance was stopped and discarded"
            )
        elif t.reason == "time" or (t.reason == "killed" and call.cancelled):
            exc = QueryInterrupted("query cancelled; its WebAssembly instance was stopped and discarded")
        elif t.reason == "memory":
            exc = ResourceLimitExceeded(
                f"WebAssembly instance ran out of memory (memory limit {self.config.memory_limit_mb} MB); discarded"
            )
        else:
            exc = WorkerCrashed(f"WebAssembly instance trapped and was discarded: {t.detail}")
        exc.__cause__ = WorkerError(str(t))
        return exc

    def _frames(self, out: bytes) -> list[dict[str, Any]]:
        frames: list[dict[str, Any]] = []
        pos = 0
        n = len(out)
        decode = self.codec.decode
        while pos + 4 <= n:
            size = int.from_bytes(out[pos:pos + 4], "big")
            payload = out[pos + 4:pos + 4 + size]
            pos += 4 + size
            msg = decode(payload)
            if not isinstance(msg, dict):
                continue
            if msg.get("type") == "hello":
                codec = msg.get("codec")
                frames.append(msg)
                if codec and codec != self.codec.name:
                    self.codec = get_codec(codec)
                    decode = self.codec.decode
                continue
            if msg.get("id") is None:
                handler = self.notice_handlers.get(msg.get("conn")) or self.notice_handlers.get(None)
                if handler is not None:
                    handler(msg)
                continue
            frames.append(msg)
        return frames

    # -- transaction deadlines: nothing runs between calls, so poke the instance --------

    def _arm_tick(self, deadline_ms: int) -> None:
        self._timers = [t for t in self._timers if not t.cancelled()]
        self._timers.append(self.loop.call_later(deadline_ms / 1000 + 0.001, self._spawn_tick))

    def _spawn_tick(self) -> None:
        if self.dead is None:
            self.loop.create_task(self._tick())

    async def _tick(self) -> None:
        async with self.lock:
            if self.dead is not None:
                return
            call = _Call(0, None)
            self._current = call
            try:
                out = await run_blocking(self._executor(), self._invoke, b"", call)
            except _Trapped as t:
                self._trapped(t, call)
                return
            except Exception:
                return
            finally:
                self._current = None
            self._frames(out)

    def cancel(self, owner: object) -> None:
        cur = self.inflight
        call = self._current
        if cur is not None and cur[1] is owner and call is not None:
            self._on_cancel(call)


# -- the backend ------------------------------------------------------------------------------

_caps_cache: dict[Capabilities, Capabilities] = {}


def _wasm_capabilities(engine_caps: Capabilities) -> Capabilities:
    caps = _caps_cache.get(engine_caps)
    if caps is None:
        caps = _caps_cache[engine_caps] = replace(engine_caps, hard_kill=True, memory_limit=True)
    return caps


class SqliteWasm(SqliteWorker):
    """SQLite in WebAssembly instances, in this process. Options:

    * ``memory_limit_mb``: hard cap on each instance's linear memory (one
      instance per pooled connection). SQLite's heap limit is set about
      12% below it.
    * ``kill_grace_ms`` (default 50): how long past a time limit or a cancel
      a statement may run before its instance is trapped.
    * ``recycle_memory_mb`` (default 64): replace an instance whose linear
      memory has grown past this, between requests and outside a
      transaction (linear memory never shrinks). ``None``: never.
    * ``inline_reads`` (default off): reads on read connections run on the
      event loop's thread first, with no executor hop, for at most about
      ``INLINE_S`` plus an epoch tick (a statement that ignores the
      interrupt: plus ``INLINE_GRACE_S``, then its instance is replaced);
      a read over budget runs again on the executor.
    * ``module``: path to ``dblayer-sqlite.wasm`` (see ``find_module``).
    * ``sql_limits``, ``immutable``, ``create``, ``busy_timeout``: as
      ``SqliteWorker``.
    * ``executor``: as every blocking backend (``UsesExecutor``).

    Capability flags: as the in-process file backend, plus ``hard_kill``
    (a statement that ignores interrupts is stopped by trapping its
    instance, the equivalent of the worker's SIGKILL) and ``memory_limit``.
    """

    __slots__ = ("executor", "wasm_config")

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        module: str | os.PathLike[str] | None = None,
        memory_limit_mb: int | None = None,
        kill_grace_ms: int = DEFAULT_KILL_GRACE_MS,
        recycle_memory_mb: int | None = 64,
        inline_reads: bool = False,
        codec: str | None = None,
        executor: Any = None,
        max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
        debug: bool = False,
        **options: Any,
    ) -> None:
        if options.get("init"):
            raise NotSupported("init hooks ('module:function') need the Python worker (SqliteWorker)")
        if options.get("workers") is not None or options.get("placement", "dedicated") != "dedicated":
            raise NotSupported("SqliteWasm has dedicated placement only: one instance per connection")
        found = find_module(module)
        codec = codec or ("msgpack" if "msgpack" in available_codecs() else "json")
        super().__init__(path, codec=codec, max_frame_bytes=max_frame_bytes, **options)
        if memory_limit_mb is not None:
            if memory_limit_mb < 8:
                raise ValueError("memory_limit_mb must be at least 8")
            heap = max(memory_limit_mb - max(4, memory_limit_mb // 8), memory_limit_mb // 2)
            self.engine_config["memory_limit_mb"] = heap
        self.executor = executor
        self.wasm_config = WasmConfig(
            module=found,
            codec=codec,
            memory_limit_mb=memory_limit_mb,
            kill_grace_ms=kill_grace_ms,
            max_frame_bytes=max_frame_bytes,
            debug=debug,
            recycle_memory_mb=recycle_memory_mb,
            inline_reads=inline_reads,
        )
        self.capabilities = _wasm_capabilities(self._local.capabilities)

    def __repr__(self) -> str:
        return f"SqliteWasm({self._path!r})"

    def _new_process(self) -> WasmProcess:  # type: ignore[override]
        return WasmProcess(self.wasm_config, self._path, lambda: self.executor)

    async def open(self, *, write: bool) -> WorkerConnection:
        return await super().open(write=write)
