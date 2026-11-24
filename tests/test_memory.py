"""Memory per idle database: an unused or asleep ``Database`` is one small
object (target from the spec: under 2 KB each)."""

from __future__ import annotations

import gc
import tracemalloc
from pathlib import Path

from dblayer import Database
from dblayer.backends.sqlite import SqliteInProcess

N = 2000


def _bytes_per_database(tmp_path: Path) -> float:
    paths = [str(tmp_path / f"db{i}.db") for i in range(N)]
    names = [f"db{i}" for i in range(N)]
    gc.collect()
    tracemalloc.start()
    try:
        before = tracemalloc.get_traced_memory()[0]
        dbs = [Database(SqliteInProcess(p), name=n) for p, n in zip(paths, names)]
        gc.collect()
        after = tracemalloc.get_traced_memory()[0]
    finally:
        tracemalloc.stop()
    assert len(dbs) == N
    return (after - before) / N


def test_unused_database_is_small(tmp_path):
    per_db = _bytes_per_database(tmp_path)
    print(f"\n{per_db:.0f} bytes per unused Database + SqliteInProcess")
    assert per_db < 1024


def test_database_has_no_instance_dict(tmp_path):
    db = Database(SqliteInProcess(tmp_path / "x.db"))
    assert not hasattr(db, "__dict__")
    assert not hasattr(db._backend, "__dict__")
    # Nothing heavy is created until first use.
    assert db._pool is None and db._schema_cache is None and db._staging is None
