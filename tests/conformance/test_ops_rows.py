"""Row operations: insert_rows, upsert_rows, update_row, delete_rows, on db
and on tx, through the default and override paths."""

import asyncio

import pytest

from dblayer import IntegrityError, NotSupported, OperationResult, QueryError

from ..helpers import is_text_type


async def dump(db, table, order="1"):
    return (await db.execute(f"select * from {table} order by {order}")).dicts()


@pytest.fixture
async def posts(ops_db):
    await ops_db.create_table(
        "posts", {"id": "integer", "title": "text", "n": "integer"}, pk="id", defaults={"n": 0}
    )
    return ops_db


# -- insert_rows ---------------------------------------------------------------


async def test_insert_rows_creates_table(ops_db):
    r = await ops_db.insert_rows(
        "t", [{"id": 1, "name": "a", "score": 1.5, "data": b"x", "flag": True}], pk="id"
    )
    assert r == OperationResult(rowcount=1, table_created=True)
    cols = {c.name: c.type.lower() for c in await ops_db.table_columns("t")}
    assert "int" in cols["id"] and is_text_type(cols["name"]) and "int" in cols["flag"]
    assert await ops_db.primary_keys("t") == ["id"]
    assert await dump(ops_db, "t") == [
        {"id": 1, "name": "a", "score": 1.5, "data": b"x", "flag": 1}
    ]


async def test_insert_rows_types_from_first_non_null(ops_db):
    await ops_db.insert_rows("t", [{"a": None, "b": 1}, {"a": 2, "b": None}])
    cols = {c.name: c.type.lower() for c in await ops_db.table_columns("t")}
    assert "int" in cols["a"] and "int" in cols["b"]


async def test_insert_rows_existing_table(posts):
    r = await posts.insert_rows("posts", [{"id": i, "title": f"t{i}"} for i in range(1, 6)])
    assert r == OperationResult(rowcount=5)
    rows = await dump(posts, "posts")
    assert len(rows) == 5 and rows[0] == {"id": 1, "title": "t1", "n": 0}


async def test_insert_rows_missing_keys_get_defaults(posts):
    await posts.insert_rows(
        "posts", [{"id": 1, "title": "a", "n": 5}, {"id": 2, "title": "b"}, {"id": 3}]
    )
    assert await dump(posts, "posts") == [
        {"id": 1, "title": "a", "n": 5},
        {"id": 2, "title": "b", "n": 0},
        {"id": 3, "title": None, "n": 0},
    ]


async def test_insert_rows_unknown_column_without_alter(posts):
    with pytest.raises(QueryError):
        await posts.insert_rows("posts", [{"id": 1, "nope": 1}])


async def test_insert_rows_alter(posts):
    r = await posts.insert_rows(
        "posts", [{"id": 1, "title": "a", "slug": "a-1"}, {"id": 2, "tags": 3}], alter=True
    )
    assert r == OperationResult(rowcount=2, columns_added=("slug", "tags"))
    rows = await dump(posts, "posts")
    assert rows[0]["slug"] == "a-1" and rows[1]["tags"] == 3


async def test_insert_rows_integrity_error(posts):
    await posts.insert_rows("posts", [{"id": 1, "title": "a"}])
    with pytest.raises(IntegrityError):
        await posts.insert_rows("posts", [{"id": 1, "title": "b"}])


async def test_insert_rows_ignore(posts):
    await posts.insert_rows("posts", [{"id": 1, "title": "a"}])
    r = await posts.insert_rows(
        "posts", [{"id": 1, "title": "b"}, {"id": 2, "title": "c"}], ignore=True
    )
    assert r.rowcount == 1
    assert [r["title"] for r in await dump(posts, "posts")] == ["a", "c"]


async def test_insert_rows_ignore_still_raises_not_null(ops_db):
    await ops_db.create_table("t", {"id": "integer", "a": "text"}, pk="id", not_null=["a"])
    with pytest.raises(IntegrityError):
        await ops_db.insert_rows("t", [{"id": 1, "a": None}], ignore=True)


async def test_insert_rows_replace(posts):
    await posts.insert_rows("posts", [{"id": 1, "title": "a", "n": 7}, {"id": 2, "title": "b"}])
    r = await posts.insert_rows(
        "posts", [{"id": 1, "title": "new"}, {"id": 3, "title": "c"}], replace=True
    )
    assert r.rowcount == 2
    # Whole row replaced: n went back to its default.
    assert await dump(posts, "posts") == [
        {"id": 1, "title": "new", "n": 0},
        {"id": 2, "title": "b", "n": 0},
        {"id": 3, "title": "c", "n": 0},
    ]


async def test_insert_rows_replace_needs_pk(ops_db):
    await ops_db.create_table("loose", {"a": "text"})
    with pytest.raises(ValueError):
        await ops_db.insert_rows("loose", [{"a": "x"}], replace=True)
    with pytest.raises(ValueError):
        await ops_db.insert_rows("loose", [{"a": "x"}], replace=True, ignore=True)


async def test_insert_rows_empty(ops_db):
    assert await ops_db.insert_rows("t", []) == OperationResult()
    assert await ops_db.table_names() == []


async def test_insert_rows_generator_and_batches(posts):
    rows = ({"id": i, "title": str(i)} for i in range(1, 251))
    r = await posts.insert_rows("posts", rows, batch_size=100)
    assert r.rowcount == 250
    assert (await posts.execute("select count(*) from posts")).rows[0][0] == 250


async def test_insert_rows_batches_are_separate_transactions(posts):
    # A failure in the third batch leaves the first two committed.
    rows = [{"id": i, "title": str(i)} for i in range(1, 21)] + [{"id": 1, "title": "dup"}]
    with pytest.raises(IntegrityError):
        await posts.insert_rows("posts", rows, batch_size=10)
    assert (await posts.execute("select count(*) from posts")).rows[0][0] == 20


async def test_insert_rows_batches_let_other_writers_in(posts):
    order = []
    started = asyncio.Event()

    def rows():
        for i in range(1, 31):
            if i == 11:
                started.set()
            yield {"id": i, "title": str(i)}

    async def other():
        await started.wait()
        await posts.execute_write("insert into posts (id, title) values (1000, 'other')")
        order.append("other")

    async def importer():
        await posts.insert_rows("posts", rows(), batch_size=10)
        order.append("import")

    await asyncio.gather(importer(), other())
    if posts.capabilities.concurrent_writers:
        # No write queue: the other writer never waits for the import, so
        # which finishes first is down to timing.
        assert sorted(order) == ["import", "other"]
    else:
        assert order == ["other", "import"]


async def test_insert_rows_on_tx_is_atomic(posts):
    with pytest.raises(IntegrityError):
        async with posts.transaction() as tx:
            await tx.insert_rows("posts", [{"id": i, "title": str(i)} for i in range(1, 300)])
            await tx.insert_rows("posts", [{"id": 1, "title": "dup"}])
    assert (await posts.execute("select count(*) from posts")).rows[0][0] == 0


async def test_override_path_dispatches(posts):
    await posts.insert_rows("posts", [{"id": 1, "title": "a"}])
    await posts.insert_rows("posts", [{"id": 2, "title": "b"}], alter=True)
    backend = posts._backend
    if posts.op_path == "default":
        assert not hasattr(backend, "calls") or not backend.calls
    elif hasattr(backend, "calls"):
        assert backend.calls["insert_rows"] == 1  # the simple case
        assert backend.declined["insert_rows"] == 1  # alter=True fell back
        assert backend.declined["create_table"] == 1
    assert len(await dump(posts, "posts")) == 2


# -- upsert_rows ---------------------------------------------------------------


async def test_upsert_rows(posts):
    await posts.insert_rows("posts", [{"id": 1, "title": "a", "n": 7}])
    r = await posts.upsert_rows("posts", [{"id": 1, "title": "A"}, {"id": 2, "title": "b"}])
    assert r.rowcount == 2
    # Columns the row does not mention are left alone.
    assert await dump(posts, "posts") == [
        {"id": 1, "title": "A", "n": 7},
        {"id": 2, "title": "b", "n": 0},
    ]


async def test_upsert_rows_creates_table_with_pk(ops_db):
    r = await ops_db.upsert_rows("t", [{"k": "x", "v": 1}], pk="k")
    assert r.table_created and await ops_db.primary_keys("t") == ["k"]
    await ops_db.upsert_rows("t", [{"k": "x", "v": 2}])
    assert await dump(ops_db, "t") == [{"k": "x", "v": 2}]


async def test_upsert_rows_needs_pk(ops_db):
    with pytest.raises(ValueError):
        await ops_db.upsert_rows("t", [{"k": "x"}])
    await ops_db.create_table("loose", {"a": "text"})
    with pytest.raises(ValueError):
        await ops_db.upsert_rows("loose", [{"a": "x"}])


async def test_upsert_rows_row_without_pk_value(posts):
    with pytest.raises(ValueError):
        await posts.upsert_rows("posts", [{"title": "no id"}])


async def test_upsert_rows_alter(posts):
    r = await posts.upsert_rows("posts", [{"id": 1, "extra": "e"}], alter=True)
    assert r.columns_added == ("extra",)
    assert (await dump(posts, "posts"))[0]["extra"] == "e"


async def test_upsert_rows_compound_pk(ops_db):
    await ops_db.upsert_rows("t", [{"a": 1, "b": 2, "v": "x"}], pk=["a", "b"])
    await ops_db.upsert_rows("t", [{"a": 1, "b": 2, "v": "y"}, {"a": 1, "b": 3, "v": "z"}])
    assert await dump(ops_db, "t", "a, b") == [
        {"a": 1, "b": 2, "v": "y"},
        {"a": 1, "b": 3, "v": "z"},
    ]


# -- update_row / delete_rows ----------------------------------------------------


async def test_update_row(posts):
    await posts.insert_rows("posts", [{"id": 1, "title": "a"}, {"id": 2, "title": "b"}])
    r = await posts.update_row("posts", 1, {"title": "A", "n": 3})
    assert r == OperationResult(rowcount=1)
    assert (await dump(posts, "posts"))[0] == {"id": 1, "title": "A", "n": 3}
    assert (await posts.update_row("posts", 99, {"title": "x"})).rowcount == 0
    assert (await posts.update_row("posts", 1, {})).rowcount == 0


async def test_update_row_compound_key(ops_db):
    await ops_db.insert_rows("t", [{"a": 1, "b": 2, "v": "x"}], pk=["a", "b"])
    assert (await ops_db.update_row("t", (1, 2), {"v": "y"})).rowcount == 1
    assert (await ops_db.update_row("t", [1, 2], {"v": "z"})).rowcount == 1
    assert (await dump(ops_db, "t"))[0]["v"] == "z"
    with pytest.raises(ValueError):
        await ops_db.update_row("t", 1, {"v": "bad"})


async def test_delete_rows(posts):
    await posts.insert_rows("posts", [{"id": i, "title": str(i)} for i in range(1, 6)])
    r = await posts.delete_rows("posts", [1, 3, 99])
    assert r == OperationResult(rowcount=2)
    assert [r["id"] for r in await dump(posts, "posts")] == [2, 4, 5]
    assert (await posts.delete_rows("posts", [])).rowcount == 0


async def test_delete_rows_compound_key(ops_db):
    await ops_db.insert_rows("t", [{"a": 1, "b": i} for i in range(3)], pk=["a", "b"])
    assert (await ops_db.delete_rows("t", [(1, 0), [1, 2]])).rowcount == 2
    assert await dump(ops_db, "t") == [{"a": 1, "b": 1}]


@pytest.mark.requires("implicit_rowid")
async def test_rowid_addressing_without_pk(ops_db):
    await ops_db.insert_rows("loose", [{"a": "x"}, {"a": "y"}])
    assert (await ops_db.update_row("loose", 2, {"a": "Y"})).rowcount == 1
    assert (await ops_db.delete_rows("loose", [1])).rowcount == 1
    assert await dump(ops_db, "loose") == [{"a": "Y"}]


@pytest.mark.requires_not("implicit_rowid")
async def test_no_pk_no_rowid_not_supported(ops_db):
    await ops_db.insert_rows("loose", [{"a": "x"}])
    with pytest.raises(NotSupported):
        await ops_db.update_row("loose", 1, {"a": "y"})
    with pytest.raises(NotSupported):
        await ops_db.delete_rows("loose", [1])


async def test_row_operations_on_tx(posts):
    async with posts.transaction() as tx:
        await tx.insert_rows("posts", [{"id": 1, "title": "a"}, {"id": 2, "title": "b"}])
        await tx.upsert_rows("posts", [{"id": 2, "title": "B"}])
        await tx.update_row("posts", 1, {"n": 9})
        await tx.delete_rows("posts", [2])
        assert (await tx.execute("select id, title, n from posts")).rows == [(1, "a", 9)]
    assert await dump(posts, "posts") == [{"id": 1, "title": "a", "n": 9}]


async def test_both_paths_identical(make_backend, backend_spec):
    """The same sequence through a default-path and an override-path
    database; final contents must match exactly."""
    from dblayer import Database

    from ..override_backend import OverrideBackend

    native = make_backend()
    if not backend_spec.native_operations:
        native = OverrideBackend(native)
    dbs = [Database(make_backend(), native_operations=False), Database(native)]
    out = []
    try:
        for db in dbs:
            await db.create_table("posts", {"id": "integer", "title": "text"}, pk="id")
            await db.insert_rows("posts", [{"id": i, "title": f"t{i}"} for i in range(1, 51)])
            await db.insert_rows("posts", [{"id": 51, "title": "x", "extra": 1}], alter=True)
            await db.insert_rows("posts", [{"id": 1, "title": "dup"}], ignore=True)
            await db.insert_rows("posts", [{"id": 7, "title": "seven"}], replace=True)
            await db.upsert_rows("posts", [{"id": 2, "title": "two"}])
            await db.delete_rows("posts", [3, 4, 5])
            await db.update_row("posts", 6, {"extra": 6})
            out.append(await dump(db, "posts"))
    finally:
        for db in dbs:
            await db.close()
    assert out[0] == out[1]
    if isinstance(native, OverrideBackend):
        assert native.calls["insert_rows"] >= 1 and native.calls["delete_rows"] == 1
