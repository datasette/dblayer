"""SQLite-specific details of the alter_table rebuild and staged-import
swap. Not parametrised: these construct SqliteInProcess directly."""

import pytest

from dblayer import Database, NotSupported
from dblayer.backends.sqlite import SqliteInProcess


@pytest.fixture(params=[False, True], ids=["default", "native-flag"])
async def db(tmp_path, request):
    db = Database(SqliteInProcess(tmp_path / "data.db"), native_operations=request.param)
    yield db
    await db.close()


async def test_rowids_preserved_for_tables_without_pk(db):
    await db.execute_write("create table t (a text, b text)")
    for i in range(5):
        await db.execute_write("insert into t values (?, ?)", [str(i), "x"])
    await db.execute_write("delete from t where a in ('1', '3')")
    await db.alter_table("t", drop=["b"])
    assert (await db.execute("select rowid, a from t")).rows == [(1, "0"), (3, "2"), (5, "4")]


async def test_triggers_recreated(db):
    await db.execute_write("create table t (id integer primary key, a text, b text)")
    await db.execute_write("create table log (msg text)")
    await db.execute_write(
        "create trigger t_ins after insert on t begin insert into log values (new.a); end"
    )
    await db.alter_table("t", drop=["b"])
    assert "t_ins" in (await db.schema()).triggers
    await db.execute_write("insert into t (a) values ('hi')")
    assert (await db.execute("select msg from log")).rows == [("hi",)]


async def test_rename_column_rewrites_triggers_and_views(db):
    await db.execute_write("create table t (id integer primary key, a text)")
    await db.execute_write("create view v as select a from t")
    await db.alter_table("t", rename={"a": "z"})
    await db.execute_write("insert into t (z) values ('q')")
    assert (await db.execute("select * from v")).rows == [("q",)]


async def test_unique_constraint_preserved(db):
    await db.execute_write("create table t (id integer primary key, a text unique, b text)")
    await db.alter_table("t", drop=["b"])
    unique = [i for i in await db.indexes("t") if i.unique]
    assert [i.columns for i in unique] == [("a",)]


@pytest.mark.parametrize(
    "ddl",
    [
        "create table t (id integer primary key, a text check (a <> ''), b text)",
        "create table t (id text primary key, a text, b text) without rowid",
        "create table t (id integer primary key, a text, b text) strict",
        "create table t (id integer primary key, a text, b text, c text generated always as (a || b))",
    ],
    ids=["check", "without-rowid", "strict", "generated"],
)
async def test_refuses_features_rebuild_would_lose(db, ddl):
    await db.execute_write(ddl)
    with pytest.raises(NotSupported):
        await db.alter_table("t", drop=["b"])
    assert [c.name for c in await db.table_columns("t")][:3] == ["id", "a", "b"]


async def test_foreign_keys_on_refuses_rebuild_of_referenced_table(tmp_path):
    db = Database(
        SqliteInProcess(tmp_path / "fk.db", init=lambda c: c.execute("pragma foreign_keys = on"))
    )
    try:
        await db.execute_write("create table p (id integer primary key, name text)")
        await db.execute_write(
            "create table c (id integer primary key, pid integer references p(id) on delete cascade)"
        )
        await db.execute_write("insert into p values (1, 'a')")
        await db.execute_write("insert into c values (1, 1)")
        with pytest.raises(NotSupported):
            await db.alter_table("p", types={"name": "blob"})
        # Child rows were not cascaded away.
        assert (await db.execute("select count(*) from c")).rows[0][0] == 1
        # Rebuilding the child (nothing references it) is fine.
        await db.alter_table("c", types={"pid": "text"})
        assert (await db.execute("select count(*) from c")).rows[0][0] == 1
    finally:
        await db.close()


async def test_foreign_keys_off_rebuild_of_referenced_table_keeps_children(db):
    await db.execute_write("create table p (id integer primary key, name text)")
    await db.execute_write(
        "create table c (id integer primary key, pid integer references p(id) on delete cascade)"
    )
    await db.execute_write("insert into p values (1, 'a')")
    await db.execute_write("insert into c values (1, 1)")
    await db.alter_table("p", types={"name": "blob"})
    assert (await db.execute("select count(*) from c")).rows[0][0] == 1
    assert (await db.execute("pragma foreign_key_check")).rows == []


# -- staged import replace: dependents (spec: "the suite records which") ------


async def _parent_child(db):
    await db.execute_write("create table p (id integer primary key, name text)")
    await db.execute_write(
        "create table c (id integer primary key, pid integer references p(id) on delete cascade)"
    )
    await db.execute_write("insert into p values (1, 'a')")
    await db.execute_write("insert into c values (1, 1)")


async def test_replace_recreates_triggers(db):
    await db.execute_write("create table t (id integer primary key, a text)")
    await db.execute_write("create table log (msg text)")
    await db.execute_write(
        "create trigger t_ins after insert on t begin insert into log values (new.a); end"
    )
    async with db.staged_import("t", mode="replace") as imp:
        await imp.write([{"id": 1, "a": "x"}])
    # Staging writes did not fire it; it fires on the replaced table.
    assert (await db.execute("select count(*) from log")).rows[0][0] == 0
    await db.execute_write("insert into t (a) values ('y')")
    assert (await db.execute("select msg from log")).rows == [("y",)]


async def test_replace_referenced_table_foreign_keys_off(db):
    # Recorded behaviour: works; children keep referencing the name and are
    # not cascaded. Rows may now dangle (foreign_key_check reports them).
    await _parent_child(db)
    async with db.staged_import("p", mode="replace") as imp:
        await imp.write([{"id": 2, "name": "b"}])
    assert (await db.execute("select count(*) from c")).rows[0][0] == 1
    assert (await db.foreign_keys("c"))[0].other_table == "p"
    assert len((await db.execute("pragma foreign_key_check")).rows) == 1


async def test_replace_referenced_table_foreign_keys_on_refused(tmp_path):
    # Recorded behaviour: NotSupported at the swap, nothing changed.
    db = Database(
        SqliteInProcess(tmp_path / "fk.db", init=lambda c: c.execute("pragma foreign_keys = on"))
    )
    try:
        await _parent_child(db)
        with pytest.raises(NotSupported):
            async with db.staged_import("p", mode="replace") as imp:
                await imp.write([{"id": 1, "name": "new"}])
        assert (await db.execute("select name from p")).rows == [("a",)]
        assert (await db.execute("select count(*) from c")).rows[0][0] == 1
        assert [n for n in (await db.schema()).tables if n.startswith("_dblayer_")] == []
    finally:
        await db.close()


async def test_replace_preserves_unique_constraint(db):
    await db.execute_write("create table t (id integer primary key, a text unique)")
    async with db.staged_import("t", mode="replace") as imp:
        await imp.write([{"id": 1, "a": "x"}])
    from dblayer import IntegrityError

    with pytest.raises(IntegrityError):
        await db.execute_write("insert into t (a) values ('x')")
