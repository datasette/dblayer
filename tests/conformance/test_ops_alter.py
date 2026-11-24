"""alter_table: rename, drop, retype, change pk. Native ALTER where the
dialect offers it, otherwise a sqlite-utils-style rebuild. Through the
default and override paths."""

import pytest

from dblayer import NotSupported, OperationResult


async def dump(db, table, order="1"):
    return (await db.execute(f"select * from {table} order by {order}")).dicts()


@pytest.fixture
async def people(ops_db):
    await ops_db.create_table(
        "people",
        {"id": "integer", "name": "text", "age": "text", "email": "text"},
        pk="id",
        not_null=["name"],
        defaults={"email": "none"},
    )
    await ops_db.create_index("people", ["name"], name="idx_people_name")
    await ops_db.create_index("people", ["email"], name="idx_people_email", unique=True)
    await ops_db.insert_rows(
        "people",
        [
            {"id": 1, "name": "ann", "age": "30", "email": "a@x"},
            {"id": 2, "name": "bob", "age": "41", "email": "b@x"},
        ],
    )
    return ops_db


async def test_rename_column(people):
    assert await people.alter_table("people", rename={"name": "full_name"}) == OperationResult()
    cols = [c.name for c in await people.table_columns("people")]
    assert cols == ["id", "full_name", "age", "email"]
    assert (await dump(people, "people"))[0]["full_name"] == "ann"
    idx = {i.name: i.columns for i in await people.indexes("people")}
    assert idx["idx_people_name"] == ("full_name",)


async def test_drop_column(people):
    await people.alter_table("people", drop=["age"])
    assert [c.name for c in await people.table_columns("people")] == ["id", "name", "email"]
    assert await dump(people, "people") == [
        {"id": 1, "name": "ann", "email": "a@x"},
        {"id": 2, "name": "bob", "email": "b@x"},
    ]


async def test_change_type(people):
    await people.alter_table("people", types={"age": "integer"})
    cols = {c.name: c.type.lower() for c in await people.table_columns("people")}
    assert "int" in cols["age"]
    assert [r["age"] for r in await dump(people, "people")] == [30, 41]


async def test_change_pk(people):
    await people.alter_table("people", pk="email")
    assert await people.primary_keys("people") == ["email"]
    assert len(await dump(people, "people")) == 2


async def test_rebuild_preserves_constraints_indexes_and_defaults(people):
    await people.alter_table("people", types={"age": "integer"}, drop=[])
    cols = {c.name: c for c in await people.table_columns("people")}
    assert cols["name"].not_null
    assert cols["email"].default is not None and "none" in cols["email"].default
    assert await people.primary_keys("people") == ["id"]
    idx = {i.name: i for i in await people.indexes("people") if i.origin == "c"}
    assert set(idx) == {"idx_people_name", "idx_people_email"}
    assert idx["idx_people_email"].unique
    await people.insert_rows("people", [{"id": 3, "name": "cy"}])
    assert (await dump(people, "people"))[-1]["email"] == "none"


async def test_index_on_dropped_column_goes_with_it(people):
    await people.alter_table("people", drop=["email"], types={"age": "integer"})
    names = {i.name for i in await people.indexes("people") if i.origin == "c"}
    assert names == {"idx_people_name"}


async def test_several_changes_at_once(people):
    await people.alter_table(
        "people", rename={"name": "who"}, drop=["email"], types={"age": "integer"}
    )
    assert await dump(people, "people") == [
        {"id": 1, "who": "ann", "age": 30},
        {"id": 2, "who": "bob", "age": 41},
    ]


async def test_alter_bad_arguments(people):
    with pytest.raises(ValueError):
        await people.alter_table("people", drop=["nope"])
    with pytest.raises(ValueError):
        await people.alter_table("people", rename={"name": "x"}, drop=["name"])
    with pytest.raises(ValueError):
        await people.alter_table("people", pk="nope")
    assert await people.alter_table("people") == OperationResult()


async def test_outbound_foreign_keys_preserved(ops_db):
    await ops_db.create_table("authors", {"id": "integer"}, pk="id")
    await ops_db.create_table(
        "posts",
        {"id": "integer", "author_id": "integer", "body": "text"},
        pk="id",
        foreign_keys=[("author_id", "authors")],
    )
    await ops_db.alter_table("posts", types={"body": "blob"})
    fks = await ops_db.foreign_keys("posts")
    assert [(f.columns, f.other_table, f.other_columns) for f in fks] == [
        (("author_id",), "authors", ("id",))
    ]


async def test_inbound_foreign_keys_still_point_at_table(ops_db):
    await ops_db.create_table("authors", {"id": "integer", "name": "text"}, pk="id")
    await ops_db.create_table(
        "posts", {"id": "integer", "author_id": "integer"}, pk="id",
        foreign_keys=[("author_id", "authors")],
    )
    await ops_db.insert_rows("authors", [{"id": 1, "name": "a"}])
    await ops_db.insert_rows("posts", [{"id": 1, "author_id": 1}])
    if not ops_db._backend.dialect.alter_referenced_tables:
        # DuckDB: a referenced table can be neither altered nor dropped.
        with pytest.raises(NotSupported):
            await ops_db.alter_table("authors", types={"name": "blob"})
        assert [c.type for c in await ops_db.table_columns("authors")][1] != "BLOB"
        return
    await ops_db.alter_table("authors", types={"name": "blob"})
    fks = await ops_db.foreign_keys("posts")
    assert fks[0].other_table == "authors"
    assert len(await dump(ops_db, "posts")) == 1


async def test_views_survive_alter(ops_db):
    await ops_db.create_table("t", {"id": "integer", "a": "text", "b": "text"}, pk="id")
    await ops_db.insert_rows("t", [{"id": 1, "a": "x", "b": "y"}])
    await ops_db.execute_write("create view v as select id, a from t")
    await ops_db.alter_table("t", drop=["b"])
    assert (await ops_db.execute("select * from v")).rows == [(1, "x")]


async def test_alter_inside_transaction_rolls_back(people):
    with pytest.raises(RuntimeError):
        async with people.transaction() as tx:
            await tx.alter_table("people", drop=["age"])
            assert "age" not in [c.name for c in await tx.table_columns("people")]
            raise RuntimeError
    assert "age" in [c.name for c in await people.table_columns("people")]


async def test_no_scratch_tables_left(people):
    await people.alter_table("people", types={"age": "integer"})
    schema = await people.schema()
    assert sorted(schema.tables) == ["people"]
