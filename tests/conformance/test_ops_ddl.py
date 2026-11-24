"""Named DDL operations: create_table, drop_table, rename_table, add_column,
create_index. Every test runs through the default path and the override
path (``ops_db``) and asserts the same results."""

import pytest

from dblayer import NotSupported, OperationResult, QueryError, TableNotFound

from ..helpers import is_text_type


def col_types(cols):
    return {c.name: c.type.lower() for c in cols}


async def test_create_table_portable_types(ops_db):
    r = await ops_db.create_table(
        "posts",
        {"id": "integer", "title": "text", "score": "float", "data": "blob"},
        pk="id",
    )
    assert r == OperationResult(table_created=True)
    assert await ops_db.table_names() == ["posts"]
    cols = await ops_db.table_columns("posts")
    assert [c.name for c in cols] == ["id", "title", "score", "data"]
    types = col_types(cols)
    assert "int" in types["id"] and is_text_type(types["title"])
    native = ops_db._backend.dialect
    # ClickHouse reports nullable columns as Nullable(<type>).
    assert native.native_type("float").lower() in types["score"]
    assert native.native_type("blob").lower() in types["data"]
    assert await ops_db.primary_keys("posts") == ["id"]


async def test_create_table_python_types_and_native_passthrough(ops_db):
    await ops_db.create_table("t", {"a": int, "b": str, "c": float, "d": bytes, "e": "varchar(20)"})
    types = col_types(await ops_db.table_columns("t"))
    assert "int" in types["a"] and is_text_type(types["b"])
    # Passed through unchanged (DuckDB accepts the length and drops it).
    # ClickHouse: VARCHAR(n) is an alias of String.
    assert types["e"] in ("varchar(20)", "character varying(20)", "varchar", "nullable(string)")


async def test_create_table_compound_pk_not_null_defaults(ops_db):
    await ops_db.create_table(
        "tags",
        {"post_id": "integer", "tag": "text", "weight": "integer", "note": "text"},
        pk=("tag", "post_id"),
        not_null=["weight"],
        defaults={"weight": 1, "note": "it's"},
    )
    assert await ops_db.primary_keys("tags") == ["tag", "post_id"]
    cols = {c.name: c for c in await ops_db.table_columns("tags")}
    assert cols["weight"].not_null and not cols["note"].not_null
    await ops_db.execute_write("insert into tags (post_id, tag) values (1, 'x')")
    assert (await ops_db.execute("select weight, note from tags")).rows == [(1, "it's")]


@pytest.mark.requires("foreign_keys")
async def test_create_table_foreign_keys(ops_db):
    await ops_db.create_table("authors", {"id": "integer", "name": "text"}, pk="id")
    await ops_db.create_table(
        "posts",
        {"id": "integer", "author_id": "integer", "parent_id": "integer"},
        pk="id",
        foreign_keys=[("author_id", "authors"), ("parent_id", "posts", "id")],
    )
    fks = sorted(await ops_db.foreign_keys("posts"), key=lambda f: f.columns)
    assert [(f.columns, f.other_table, f.other_columns) for f in fks] == [
        (("author_id",), "authors", ("id",)),
        (("parent_id",), "posts", ("id",)),
    ]


async def test_create_table_exists(ops_db):
    await ops_db.create_table("t", {"a": "text"})
    with pytest.raises(QueryError):
        await ops_db.create_table("t", {"a": "text"})
    r = await ops_db.create_table("t", {"b": "text"}, if_not_exists=True)
    assert r == OperationResult(table_created=False)
    assert [c.name for c in await ops_db.table_columns("t")] == ["a"]


async def test_create_table_bad_arguments(ops_db):
    with pytest.raises(ValueError):
        await ops_db.create_table("t", {"a": "text"}, pk="nope")
    with pytest.raises(TypeError):
        await ops_db.create_table("t", {"a": dict})
    assert await ops_db.table_names() == []


async def test_drop_table(ops_db):
    await ops_db.create_table("t", {"a": "text"})
    assert await ops_db.drop_table("t") == OperationResult()
    assert await ops_db.table_names() == []
    with pytest.raises(QueryError):
        await ops_db.drop_table("t")
    await ops_db.drop_table("t", if_exists=True)


async def test_rename_table(ops_db):
    await ops_db.create_table("old", {"a": "text"})
    await ops_db.execute_write("insert into old values ('x')")
    await ops_db.rename_table("old", "new")
    assert await ops_db.table_names() == ["new"]
    assert (await ops_db.execute("select a from new")).rows == [("x",)]


async def test_add_column(ops_db):
    await ops_db.create_table("t", {"a": "text"})
    r = await ops_db.add_column("t", "n", "integer")
    assert r.columns_added == ("n",)
    await ops_db.add_column("t", "s")  # default type text
    types = col_types(await ops_db.table_columns("t"))
    assert list(types) == ["a", "n", "s"]
    assert "int" in types["n"] and is_text_type(types["s"])
    with pytest.raises(QueryError):
        await ops_db.add_column("t", "n", "integer")


async def test_create_index(ops_db):
    await ops_db.create_table("t", {"a": "text", "b": "integer"})
    await ops_db.create_index("t", ["a", "b"])
    unique = ops_db.capabilities.unique_constraints
    if unique:
        await ops_db.create_index("t", "b", name="b_unique", unique=True)
    else:
        with pytest.raises(NotSupported):
            await ops_db.create_index("t", "b", name="b_unique", unique=True)
    idx = {i.name: i for i in await ops_db.indexes("t")}
    assert idx["idx_t_a_b"].columns == ("a", "b") and not idx["idx_t_a_b"].unique
    assert not unique or idx["b_unique"].unique
    with pytest.raises(QueryError):
        await ops_db.create_index("t", ["a", "b"])
    await ops_db.create_index("t", ["a", "b"], if_not_exists=True)


async def test_operation_on_missing_table(ops_db):
    with pytest.raises(TableNotFound):
        await ops_db.update_row("missing", 1, {"a": 1})
    with pytest.raises(QueryError):
        await ops_db.add_column("missing", "a")


@pytest.mark.requires("transactions")
async def test_operations_inside_transaction_are_atomic(ops_db):
    with pytest.raises(RuntimeError):
        async with ops_db.transaction() as tx:
            await tx.create_table("t", {"a": "text"})
            await tx.add_column("t", "b")
            assert [c.name for c in await tx.table_columns("t")] == ["a", "b"]
            raise RuntimeError
    assert await ops_db.table_names() == []


async def test_operation_invalidates_schema_cache(ops_db):
    assert await ops_db.table_names() == []
    await ops_db.create_table("t", {"a": "text"})
    assert await ops_db.table_names() == ["t"]
    await ops_db.add_column("t", "b")
    assert [c.name for c in await ops_db.table_columns("t")] == ["a", "b"]
    await ops_db.drop_table("t")
    assert await ops_db.table_names() == []
