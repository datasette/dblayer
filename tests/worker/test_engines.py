"""Connection init hooks, and the engine boundary: the worker runs any
in-process backend named in ``open``, not only SQLite."""

from __future__ import annotations

import os

import pytest

from dblayer import Database, QueryError
from dblayer.backends.sqlite_worker import SharedWorkers, SqliteWorker, WorkerBackend


async def test_init_hook_registers_sql_function_in_the_worker(tmp_path):
    db = Database(SqliteWorker(tmp_path / "i.db", init=["tests.worker.hooks:register_functions"]))
    try:
        r = await db.execute("select reverse_text('abc'), worker_pid()")
        assert r.rows[0][0] == "cba"
        assert r.rows[0][1] != os.getpid()  # it ran in the worker
        await db.execute_write("create table t (s text)")
        await db.execute_write("insert into t values (reverse_text('xyz'))")  # write connection too
        assert (await db.execute("select s from t")).rows == [("zyx",)]
    finally:
        await db.close()


async def test_failing_or_missing_init_hook_fails_the_open(tmp_path):
    for hook, needle in [("tests.worker.hooks:fail", "always fails"), ("tests.worker.hooks:nope", "cannot import")]:
        db = Database(SqliteWorker(tmp_path / "f.db", init=[hook]))
        try:
            with pytest.raises(QueryError, match=needle):
                await db.execute("select 1")
        finally:
            await db.close()


def test_init_names_must_be_module_function(tmp_path):
    with pytest.raises(ValueError):
        SqliteWorker(tmp_path / "x.db", init=["no_colon"])


async def test_sql_limits_apply_in_the_worker(tmp_path):
    db = Database(SqliteWorker(tmp_path / "l.db", sql_limits={"length": 1000}))
    try:
        assert (await db.execute("select length(zeroblob(1000))")).rows == [(1000,)]
        with pytest.raises(Exception, match="too big|TOOBIG"):
            await db.execute("select zeroblob(1001)")
    finally:
        await db.close()


async def test_duckdb_runs_in_a_worker_through_the_same_protocol(tmp_path):
    pytest.importorskip("duckdb")
    # DuckDB locks its file per process, so a read worker and a write worker
    # cannot both open one database: its connections must share one
    # process (and one instance), here a one-process shared group.
    group = SharedWorkers(size=1)
    b = WorkerBackend(tmp_path / "d.duckdb", engine="dblayer.backends.duckdb:DuckDB", workers=group)
    assert b.dialect.name == "duckdb"
    assert b.capabilities.hard_kill and not b.capabilities.implicit_rowid
    db = Database(b)
    try:
        await db.create_table("t", {"id": "integer", "v": "text"}, pk="id")
        res = await db.insert_rows("t", [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}])
        assert res.rowcount == 2
        assert (await db.execute("select id, v from t order by id")).rows == [(1, "a"), (2, "b")]
        assert "t" in (await db.schema()).tables
        async with db.transaction() as tx:
            await tx.execute("update t set v = 'c' where id = 1")
        assert (await db.execute("select v from t where id = 1")).rows == [("c",)]
        assert len(group.procs) == 1
    finally:
        await db.close()
        group.close()
