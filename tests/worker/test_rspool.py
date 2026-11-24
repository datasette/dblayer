"""``SqliteRustPool`` (``dblayer.backends.sqlite_rspool``) itself: finding
the binary, its version, and that the pool runs the Rust worker. Everything
else the Rust worker does is tested by the C worker's own tests, which run
on both binaries (``tests/native_impl.py``)."""

from __future__ import annotations

import subprocess

import pytest

from dblayer import Database, NotSupported
from dblayer.backends.sqlite_rspool import (
    BUILD_COMMAND,
    ENV_VAR,
    RustPool,
    SqliteRustPool,
    binary_unavailable,
    find_binary,
)

UNAVAILABLE = binary_unavailable()
built = pytest.mark.skipif(UNAVAILABLE is not None, reason=UNAVAILABLE or "")


def test_binary_lookup_order(tmp_path, monkeypatch):
    fake = tmp_path / "nope"
    monkeypatch.setenv(ENV_VAR, str(fake))
    with pytest.raises(NotSupported) as e:
        find_binary()
    assert BUILD_COMMAND in str(e.value) and ENV_VAR in str(e.value)
    with pytest.raises(NotSupported, match="not built"):
        SqliteRustPool(tmp_path / "x.db", binary=fake)
    monkeypatch.delenv(ENV_VAR)
    if UNAVAILABLE is None:
        real = find_binary()
        assert real.endswith("sqlite-rs-worker/target/release/dblayer-sqlite-rsworker")
        assert find_binary(real) == real
        monkeypatch.setenv(ENV_VAR, str(fake))
        assert find_binary(real) == real  # an explicit path beats the variable


@built
def test_version_names_the_sqlite_build():
    out = subprocess.run([find_binary(), "--version"], capture_output=True, text=True, check=True).stdout
    assert out.startswith("dblayer-sqlite-rsworker protocol 1, SQLite 3.53.1")
    assert "-DSQLITE_ENABLE_FTS5" in out  # the C worker's compile options


@built
async def test_pool_runs_the_rust_worker(tmp_path):
    pool = RustPool(1, linger=0)
    db = Database(SqliteRustPool(tmp_path / "r.db", pool=pool))
    try:
        await db.execute_write("create table t (id integer primary key, v)")
        w = await db.execute_write("insert into t (v) values (:v)", {"v": "x"})
        assert w.last_pk == 1
        hello = pool.processes()[0].hello
        assert hello["implementation"] == "rust" and hello["multi"] is True
        assert "socket" not in hello["export"]["transports"]
    finally:
        await db.close()
        pool.close()
