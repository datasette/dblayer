"""The parent-side backend (``SqliteWorker``) through ``Database``, beyond
what the conformance suite covers: process model, wire details, errors."""

from __future__ import annotations

import os

import pytest

from dblayer import Database, IntegrityError, QueryError
from dblayer.backends.sqlite_worker import SqliteWorker, live_worker_pids


async def test_execute_round_trip(tmp_path):
    db = Database(SqliteWorker(tmp_path / "a.db"))
    try:
        await db.execute_write("create table t (id integer primary key, v blob)")
        w = await db.execute_write("insert into t (v) values (:v)", {"v": b"\x00\x01"})
        assert (w.rowcount, w.last_pk) == (1, 1)
        r = await db.execute("select id, v from t where id = ?", [1])
        assert r.columns == ("id", "v") and r.rows == [(1, b"\x00\x01")]
        assert r.rows[0]["v"] == b"\x00\x01"
    finally:
        await db.close()


async def test_errors_map_to_shared_classes(tmp_path):
    db = Database(SqliteWorker(tmp_path / "e.db"))
    try:
        await db.execute_write("create table t (id integer primary key)")
        await db.execute_write("insert into t values (1)")
        with pytest.raises(IntegrityError) as e:
            await db.execute_write("insert into t values (1)")
        assert e.value.code == "SQLITE_CONSTRAINT_PRIMARYKEY"
        assert "sqlite3.IntegrityError" in str(e.value.__cause__)
        with pytest.raises(QueryError) as e2:
            await db.execute("select nope")
        assert e2.value.code == "SQLITE_ERROR"
    finally:
        await db.close()


async def test_one_process_per_connection_and_none_when_asleep(tmp_path):
    db = Database(SqliteWorker(tmp_path / "p.db"))
    try:
        await db.execute_write("create table t (x)")
        await db.execute("select 1")
        pool = db._pool
        assert pool is not None
        conns = [e.conn for e in pool._idle_read + pool._idle_write]
        pids = {c.pid for c in conns}
        assert len(pids) == 2 and all(p != os.getpid() for p in pids)
        assert pids <= set(live_worker_pids())
        await db.sleep()
        assert db.state == "asleep"
        assert all(not c.process.alive for c in conns)
    finally:
        await db.close()
