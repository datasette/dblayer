"""What crosses the wire for each API call: one message per call where the
spec promises it (schema, named operations, batch), credit-based
backpressure for streams, transactions pinned to one worker."""

from __future__ import annotations

import asyncio

import pytest

from dblayer import Database
from dblayer.backends.sqlite_worker import SqliteWorker, WorkerConnection, WorkerStream


def _requests(rec):
    return [m["type"] for d, m in rec if d == "->" and "type" in m]


async def _recording(db: Database, write: bool):
    """Check a connection out and back in, so the pool holds it idle, and
    start recording its frames."""
    pool = await db._use_pool()
    conn = await pool.acquire(write=write)
    await pool.release(conn)
    assert isinstance(conn, WorkerConnection)
    rec: list = []
    conn.process.recorder = rec
    return conn, rec


@pytest.fixture
async def db(tmp_path):
    d = Database(SqliteWorker(tmp_path / "m.db"), name="m")
    await d.execute_write("create table posts (id integer primary key, title text)")
    yield d
    await d.close()


async def test_named_operation_is_one_message(db):
    conn, rec = await _recording(db, write=True)
    result = await db.insert_rows("posts", [{"id": 1, "title": "a", "slug": "x"}, {"id": 2, "title": "b", "slug": "y"}], alter=True)
    assert result.rowcount == 2 and result.columns_added == ("slug",)
    assert _requests(rec) == ["begin", "op", "commit"]


async def test_interactive_insert_with_alter_is_several_round_trips(db):
    conn, rec = await _recording(db, write=True)
    async with db.transaction() as tx:
        cols = await tx.table_columns("posts")
        if "slug" not in [c.name for c in cols]:
            await tx.execute('alter table posts add column slug text')
        await tx.execute_many(
            "insert into posts (id, title, slug) values (:id, :title, :slug)",
            [{"id": 1, "title": "a", "slug": "x"}, {"id": 2, "title": "b", "slug": "y"}],
        )
    assert _requests(rec) == ["begin", "schema", "execute", "execute_many", "commit"]


async def test_schema_is_one_message(db):
    conn, rec = await _recording(db, write=False)
    s = await db.schema()
    assert "posts" in s.tables
    assert _requests(rec) == ["schema"]


async def test_batch_is_one_message(db):
    conn, rec = await _recording(db, write=True)
    out = await db.batch([("insert into posts (title) values (:t)", {"t": "a"}), "insert into posts (title) values ('b')"])
    assert [r.rowcount for r in out] == [1, 1]
    assert _requests(rec) == ["begin", "batch", "commit"]


async def test_transaction_pins_its_worker(db):
    pids = set()
    async with db.transaction() as tx:
        for i in range(5):
            await tx.execute("insert into posts (title) values (:t)", {"t": str(i)})
            pids.add(tx._conn.pid)  # type: ignore[union-attr]
    assert len(pids) == 1


async def test_stream_backpressure_with_credit(db):
    await db.insert_rows("posts", [{"title": str(i)} for i in range(100)])
    conn, rec = await _recording(db, write=False)
    got = []
    async with db.stream("select id from posts order by id", chunk_rows=10) as chunks:
        async for chunk in chunks:
            got.extend(r[0] for r in chunk)
            if len(got) == 20:
                # Two chunks taken = one credit's worth: the worker has sent
                # nothing more and holds its cursor, waiting for "more".
                await asyncio.sleep(0.05)
                frames_in = [m for d, m in rec if d == "<-"]
                assert len(frames_in) == 2
    assert got == list(range(1, 101))
    sent = _requests(rec)
    assert sent[0] == "stream" and set(sent[1:]) == {"more"}
    assert WorkerStream.STREAM_CREDIT == 2
    # 10 chunks two at a time (stream + 4 more), then one more for the end.
    assert len(sent) == 6


async def test_abandoned_stream_closes_worker_cursor(db):
    await db.insert_rows("posts", [{"title": str(i)} for i in range(100)])
    conn, rec = await _recording(db, write=False)
    async with db.stream("select id from posts", chunk_rows=5) as chunks:
        async for chunk in chunks:
            break
    assert _requests(rec)[-1] == "cancel"
    assert rec[-1][1].get("end") and rec[-1][1].get("cancelled")
    # The connection is clean and reusable.
    assert (await db.execute("select count(*) from posts")).rows == [(100,)]
