"""The object-storage stand-in from the spec: a local directory plays the
remote store, and ``LocalCache`` supplies a registry's ``on_wake``,
``on_sleep`` and ``on_evict`` hooks for databases whose file lives in a
local cache directory with a disk quota.

Rules it implements (and the tests check):

* ``on_wake`` fetches the file if it is not on local disk (cold), then
  enforces the quota by evicting least recently woken asleep databases.
* ``on_sleep`` copies a database out when its file changed since the last
  copy (the file is closed and consistent at that point).
* ``on_evict`` deletes the local file only once the store holds a copy
  identical to it: a writable database whose latest writes were not copied
  out is copied first, and if that fails nothing is deleted.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import shutil
from collections import OrderedDict
from pathlib import Path
from typing import Any


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class StoreUnavailable(OSError):
    pass


class ObjectStore:
    """A directory standing in for S3. Uploads are atomic (write then
    rename). ``fail_puts`` / ``fail_gets`` inject failures."""

    def __init__(self, root: Path, *, latency: float = 0.0) -> None:
        self.root = root
        root.mkdir(parents=True, exist_ok=True)
        self.latency = latency
        self.fail_puts = False
        self.fail_gets = False
        self.puts = 0
        self.gets = 0

    def _key(self, name: str) -> Path:
        return self.root / (name.replace("/", "__") + ".db")

    def digest(self, name: str) -> str | None:
        key = self._key(name)
        return _digest(key) if key.exists() else None

    async def put(self, name: str, src: Path) -> None:
        if self.latency:
            await asyncio.sleep(self.latency)
        if self.fail_puts:
            raise StoreUnavailable(f"put {name}: store unavailable")
        key = self._key(name)
        tmp = key.with_suffix(".tmp")
        await asyncio.to_thread(shutil.copyfile, src, tmp)
        os.replace(tmp, key)
        self.puts += 1

    async def get(self, name: str, dest: Path) -> None:
        if self.latency:
            await asyncio.sleep(self.latency)
        if self.fail_gets:
            raise StoreUnavailable(f"get {name}: store unavailable")
        tmp = dest.with_suffix(".download")
        await asyncio.to_thread(shutil.copyfile, self._key(name), tmp)
        os.replace(tmp, dest)
        self.gets += 1


class LocalCache:
    """Hooks for a ``Registry`` whose databases are files in ``local_dir``
    backed by ``store``. ``quota_bytes`` bounds local disk use (checked after
    each wake). ``registry`` must be set before use."""

    def __init__(self, store: ObjectStore, local_dir: Path, *, quota_bytes: int | None = None) -> None:
        self.store = store
        self.local_dir = local_dir
        local_dir.mkdir(parents=True, exist_ok=True)
        self.quota_bytes = quota_bytes
        self.registry: Any = None
        self.recent: OrderedDict[str, None] = OrderedDict()
        """Database names, least recently woken first."""
        self.events: list[tuple[str, str]] = []
        self.peak_local_bytes = 0

    def path_for(self, name: str) -> Path:
        return self.local_dir / (name.replace("/", "__") + ".db")

    def local_bytes(self) -> int:
        return sum(p.stat().st_size for p in self.local_dir.iterdir() if p.is_file())

    # -- hooks -----------------------------------------------------------------

    async def on_wake(self, db: Any) -> None:
        name = db.name
        path = self.path_for(name)
        if not path.exists() and (db.state == "cold" or self.store.digest(name) is not None):
            self.events.append(("fetch", name))
            await self.store.get(name, path)
        # Otherwise it is local already, or brand new (the backend creates it).
        self.events.append(("wake", name))
        self.recent[name] = None
        self.recent.move_to_end(name)
        await self.enforce_quota(keep=name)

    async def on_sleep(self, db: Any) -> None:
        name = db.name
        path = self.path_for(name)
        if path.exists() and _digest(path) != self.store.digest(name):
            self.events.append(("copy out", name))
            await self.store.put(name, path)
        self.events.append(("sleep", name))

    async def on_evict(self, db: Any) -> bool:
        name = db.name
        path = self.path_for(name)
        if not path.exists():
            return True
        if _digest(path) != self.store.digest(name):
            # Written since the last copy: copy out first. If this raises,
            # nothing is deleted and the database stays asleep, not cold.
            self.events.append(("copy out", name))
            await self.store.put(name, path)
            if _digest(path) != self.store.digest(name):
                return False
        for suffix in ("", "-journal", "-wal", "-shm"):
            Path(str(path) + suffix).unlink(missing_ok=True)
        self.events.append(("evict", name))
        self.recent.pop(name, None)
        return True

    # -- quota -------------------------------------------------------------------

    async def enforce_quota(self, *, keep: str | None = None) -> None:
        used = self.local_bytes()
        self.peak_local_bytes = max(self.peak_local_bytes, used)
        if self.quota_bytes is None or used <= self.quota_bytes:
            return
        for name in list(self.recent):
            if used <= self.quota_bytes:
                break
            if name == keep:
                continue
            path = self.path_for(name)
            size = path.stat().st_size if path.exists() else 0
            try:
                evicted = await self.registry.evict(name)
            except OSError:
                continue  # could not copy out: keep it local
            if evicted:
                used -= size
