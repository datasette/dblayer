"""db.sweep_staged(max_age): dropping orphaned staging tables."""

import time
from datetime import timedelta

from dblayer.core.staged import STAGING_PREFIX, staging_created_at, staging_name


async def make_orphan(db, age_seconds):
    """What a crash mid-import leaves behind: a staging table, created
    age_seconds ago (the age is in its name)."""
    name = staging_name(time.time() - age_seconds)
    await db.create_table(name, {"id": "integer", "v": "text"}, pk="id")
    await db.insert_rows(name, [{"id": 1, "v": "x"}])
    return name


def test_staging_name_round_trip():
    t = time.time()
    name = staging_name(t)
    assert name.startswith(STAGING_PREFIX)
    assert staging_created_at(name) == float(int(t))
    assert staging_created_at("posts") is None
    assert staging_created_at(STAGING_PREFIX + "x") is None


async def test_sweep_drops_only_old_orphans(db):
    old = await make_orphan(db, 3600)
    young = await make_orphan(db, 5)
    await db.create_table("_dblayer_import_not_a_staging_name", {"a": "text"})
    await db.create_table("posts", {"a": "text"})
    assert await db.sweep_staged(600) == [old]
    names = set((await db.schema()).tables)
    assert old not in names and young in names
    assert "posts" in names and "_dblayer_import_not_a_staging_name" in names


async def test_sweep_timedelta_and_zero(db):
    a = await make_orphan(db, 120)
    assert await db.sweep_staged(timedelta(minutes=5)) == []
    assert await db.sweep_staged(timedelta(minutes=1)) == [a]
    b = await make_orphan(db, 0)
    assert await db.sweep_staged(0) == [b]


async def test_orphans_hidden_but_readable(db):
    name = await make_orphan(db, 10)
    assert name not in await db.table_names()
    assert (await db.execute(f"select v from {name}")).rows == [("x",)]
    assert [c.name for c in await db.table_columns(name)] == ["id", "v"]


async def test_sweep_skips_live_import(db):
    async with db.staged_import("t", pk="id") as imp:
        await imp.write([{"id": 1}])
        assert await db.sweep_staged(0) == []
    assert await db.table_names() == ["t"]


async def test_sweep_survives_reopen(make_backend):
    """The age is discoverable from a fresh Database object, as after a
    crash."""
    from dblayer import Database

    backend = make_backend()
    db = Database(backend)
    name = await make_orphan(db, 7200)
    await db.sleep()
    db2 = Database(backend)
    try:
        assert await db2.sweep_staged(3600) == [name]
    finally:
        await db2.close()
        await db.close()
